package tempo

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

// TestTempoBucketNs covers the Log2Bucketize re-binning used to match Tempo's
// histogram_over_time response shape.
func TestTempoBucketNs(t *testing.T) {
	f := func(vmrange string, want uint64) {
		t.Helper()
		got := tempoBucketNs(vmrange)
		if got != want {
			t.Fatalf("tempoBucketNs(%q) = %d; want %d", vmrange, got, want)
		}
	}

	// Exact power-of-2 upper bounds round to themselves.
	f("0...8192", 8192)
	f("0...16384", 16384)
	f("4096...8192", 8192)

	// Non-power-of-2 upper bounds round up to the next power of 2.
	f("0...100000", 131072)      // ceil(log2(100000))=17 → 2^17 = 131072
	f("50000...106583", 131072)  // upper 106583 → 2^17
	f("106583...226974", 262144) // upper 226974 → 2^18

	// Realistic vmrange in scientific notation (nanosecond boundaries).
	f("5.995e+08...6.813e+08", 1<<30) // hi=681300000 → 2^30 = 1073741824

	// Single-value vmrange — VictoriaLogs emits "0" or a single number for
	// degenerate buckets.
	f("0", 0)
	f("1", 1)
	f("8192", 8192)
	f("8193", 16384)

	// Sub-nanosecond underflow buckets (VL emits "0...1.000e-09" for
	// zero-duration spans) must return 0 so the caller drops them — Tempo
	// never emits such a bucket.
	f("0...1.000e-09", 0)
	f("0...0.5", 0)

	// Malformed input must not panic; falls through to 0.
	f("garbage", 0)
}

type metricsRangeFixtureSpan struct {
	name     string
	method   string
	p        string
	bucket   string
	traceID  string
	spanID   string
	end      time.Time
	duration int64
}

type metricsRangeLabel struct {
	Key   string `json:"key"`
	Value struct {
		StringValue string   `json:"stringValue"`
		DoubleValue *float64 `json:"doubleValue"`
	} `json:"value"`
}

type metricsRangeSample struct {
	TimestampMs string  `json:"timestampMs"`
	Value       float64 `json:"value"`
}

type metricsRangeExemplar struct {
	Labels      []metricsRangeLabel `json:"labels"`
	TimestampMs string              `json:"timestampMs"`
	Value       float64             `json:"value"`
}

type metricsRangeSeries struct {
	Labels    []metricsRangeLabel    `json:"labels"`
	Samples   []metricsRangeSample   `json:"samples"`
	Exemplars []metricsRangeExemplar `json:"exemplars"`
}

type metricsRangeResponse struct {
	Series []metricsRangeSeries `json:"series"`
	Status string               `json:"status"`
}

// TestMetricsQueryRangeExemplarsMatchGrouping exercises the public Tempo range
// handler against flushed local span rows. The 2-GET/3-POST distribution makes
// round-robin attachment cross a group regardless of exemplar ordering. The
// zero-duration POST spans disappear from the histogram output, leaving only
// GET series to which POST exemplars must not be attached.
func TestMetricsQueryRangeExemplarsMatchGrouping(t *testing.T) {
	storagePathFlag := flag.Lookup("storageDataPath")
	if storagePathFlag == nil {
		t.Fatal("storageDataPath flag is not registered")
	}
	oldStoragePath := storagePathFlag.Value.String()
	if err := flag.Set("storageDataPath", t.TempDir()); err != nil {
		t.Fatalf("cannot set storageDataPath: %s", err)
	}
	t.Cleanup(func() {
		if err := flag.Set("storageDataPath", oldStoragePath); err != nil {
			t.Errorf("cannot restore storageDataPath: %s", err)
		}
	})

	vtstorage.Init()
	t.Cleanup(vtstorage.Stop)

	start := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	end := start.Add(10 * time.Minute)
	spans := []metricsRangeFixtureSpan{
		{name: "GET", method: "GET", p: "0.3", bucket: "0.5", traceID: "00000000000000000000000000000001", spanID: "0000000000000001", end: start.Add(61 * time.Second), duration: int64(time.Second)},
		{name: "GET", method: "HEAD", p: "0.3", bucket: "0.7", traceID: "00000000000000000000000000000002", spanID: "0000000000000002", end: start.Add(69 * time.Second), duration: int64(time.Second)},
		{name: "POST", method: "POST", p: "0.5", bucket: "1.073741824", traceID: "00000000000000000000000000000003", spanID: "0000000000000003", end: start.Add(77 * time.Second)},
		{name: "POST", method: "POST", p: "0.5", bucket: "1.073741824", traceID: "00000000000000000000000000000004", spanID: "0000000000000004", end: start.Add(85 * time.Second)},
		{name: "POST", method: "POST", p: "0.5", bucket: "1.073741824", traceID: "00000000000000000000000000000005", spanID: "0000000000000005", end: start.Add(93 * time.Second)},
	}
	rows := logstorage.GetLogRows([]string{otelpb.ResourceAttrServiceName, otelpb.NameField}, nil, nil, nil, "-")
	defer logstorage.PutLogRows(rows)
	for _, span := range spans {
		endNs := span.end.UnixNano()
		// Raw p and __bucket fields overlap with synthetic response-label keys.
		fields := []logstorage.Field{
			{Name: otelpb.ResourceAttrServiceName, Value: "frontend"},
			{Name: otelpb.NameField, Value: span.name},
			{Name: otelpb.SpanAttrPrefixField + "http.method", Value: span.method},
			{Name: "p", Value: span.p},
			{Name: "__bucket", Value: span.bucket},
			{Name: otelpb.TraceIDField, Value: span.traceID},
			{Name: otelpb.SpanIDField, Value: span.spanID},
			{Name: otelpb.DurationField, Value: strconv.FormatInt(span.duration, 10)},
		}
		rows.MustAdd(logstorage.TenantID{}, endNs, fields, -1)
	}
	(&vtstorage.Storage{}).MustAddRows(rows)
	if err := flushLocalStorage(t); err != nil {
		t.Fatalf("cannot flush local storage fixture: %s", err)
	}
	spansByID := readStoredMetricSpans(t, start, end)
	if len(spansByID) != len(spans) {
		t.Fatalf("fixture storage returned %d spans; want %d", len(spansByID), len(spans))
	}

	ungrouped := queryMetricsRange(t, `{ resource.service.name = "frontend" } | rate()`, start, end)
	if len(ungrouped.Series) != 1 || !hasPositiveMetricSample(ungrouped.Series[0].Samples) {
		t.Fatalf("fixture did not produce one ungrouped rate series with data: %+v", ungrouped.Series)
	}
	checkMetricSampleTimestamps(t, ungrouped.Series[0].Samples, start, end)
	ungroupedSeen := make(map[string]int)
	checkSeriesExemplars(t, ungrouped.Series[0], nil, spansByID, ungroupedSeen, start, end)
	checkFixtureExemplars(t, ungroupedSeen, spans, "ungrouped")

	timeGrouped := queryMetricsRange(t, `{ resource.service.name = "frontend" } | rate() by (_time)`, start, end)
	if len(timeGrouped.Series) != 1 {
		t.Fatalf("fixture did not produce one time-grouped series; got %d", len(timeGrouped.Series))
	}
	checkMetricSampleTimestamps(t, timeGrouped.Series[0].Samples, start, end)
	timeGroupedSeen := make(map[string]int)
	checkSeriesExemplars(t, timeGrouped.Series[0], nil, spansByID, timeGroupedSeen, start, end)
	checkFixtureExemplars(t, timeGroupedSeen, spans, "time-grouped")

	ungroupedQuantiles := queryMetricsRange(t, `{ resource.service.name = "frontend" } | quantile_over_time(duration, 0.5, 0.9)`, start, end)
	if len(ungroupedQuantiles.Series) != 2 {
		t.Fatalf("fixture did not produce two ungrouped quantile series; got %d", len(ungroupedQuantiles.Series))
	}
	quantileSeen := make(map[string]int)
	quantileExemplarsPerSeries := make([]int, len(ungroupedQuantiles.Series))
	for i, series := range ungroupedQuantiles.Series {
		checkMetricSampleTimestamps(t, series.Samples, start, end)
		quantileExemplarsPerSeries[i] = checkSeriesExemplars(t, series, nil, spansByID, quantileSeen, start, end)
	}
	checkFixtureExemplars(t, quantileSeen, spans, "ungrouped quantile")
	quantileDistributionDifference := quantileExemplarsPerSeries[0] - quantileExemplarsPerSeries[1]
	if quantileDistributionDifference != 1 && quantileDistributionDifference != -1 {
		t.Errorf("ungrouped quantile exemplar distribution changed: got %v; want one link per input exemplar", quantileExemplarsPerSeries)
	}

	grouped := queryMetricsRange(t, `{ resource.service.name = "frontend" } | rate() by (name)`, start, end)
	if len(grouped.Series) != 2 {
		t.Fatalf("fixture did not produce both grouped rate series; got %d", len(grouped.Series))
	}
	groupedSeen := make(map[string]int)
	for _, series := range grouped.Series {
		checkMetricSampleTimestamps(t, series.Samples, start, end)
		checkSeriesExemplars(t, series, []string{"name"}, spansByID, groupedSeen, start, end)
	}
	checkFixtureExemplars(t, groupedSeen, spans, "grouped")

	groupedQuantiles := queryMetricsRange(t, `{ resource.service.name = "frontend" } | quantile_over_time(duration, 0.5, 0.9) by (name)`, start, end)
	if len(groupedQuantiles.Series) != 4 {
		t.Fatalf("fixture did not produce four grouped quantile series; got %d", len(groupedQuantiles.Series))
	}
	groupedQuantileSeen := make(map[string]int)
	for _, series := range groupedQuantiles.Series {
		checkMetricSampleTimestamps(t, series.Samples, start, end)
		checkSeriesExemplars(t, series, []string{"name"}, spansByID, groupedQuantileSeen, start, end)
	}
	checkFixtureExemplars(t, groupedQuantileSeen, spans, "grouped quantile")

	// Raw p is a valid field even though quantile responses also emit a synthetic p label.
	groupedP := queryMetricsRange(t, `{ resource.service.name = "frontend" } | quantile_over_time(duration, 0.5, 0.9) by (p)`, start, end)
	if len(groupedP.Series) != 4 {
		t.Fatalf("fixture did not produce four raw-p quantile series; got %d", len(groupedP.Series))
	}
	groupedPSeen := make(map[string]int)
	for _, series := range groupedP.Series {
		pLabels := 0
		for _, label := range series.Labels {
			if label.Key == "p" {
				pLabels++
			}
		}
		if pLabels != 2 {
			t.Fatalf("fixture series does not contain both raw and quantile p labels: %+v", series.Labels)
		}
		checkMetricSampleTimestamps(t, series.Samples, start, end)
		checkSeriesExemplars(t, series, []string{"p"}, spansByID, groupedPSeen, start, end)
	}
	checkFixtureExemplars(t, groupedPSeen, spans, "grouped p quantile")

	multiField := queryMetricsRange(t, `{ resource.service.name = "frontend" } | rate() by (name, span.http.method)`, start, end)
	if len(multiField.Series) != 3 {
		t.Fatalf("fixture did not produce three name/method series; got %d", len(multiField.Series))
	}
	multiFieldSeen := make(map[string]int)
	for _, series := range multiField.Series {
		checkMetricSampleTimestamps(t, series.Samples, start, end)
		checkSeriesExemplars(t, series, []string{"name", "span.http.method"}, spansByID, multiFieldSeen, start, end)
	}
	checkFixtureExemplars(t, multiFieldSeen, spans, "multi-field grouped")

	// These unqualified fields are valid empty groups in this fixture. Each
	// collides with an exemplar result alias if aliases are not made unique.
	for _, field := range []string{"tid", "sid", "dur"} {
		response := queryMetricsRange(t, `{ resource.service.name = "frontend" } | rate() by (`+field+")", start, end)
		if len(response.Series) != 1 {
			t.Fatalf("fixture did not produce one empty %s group; got %d series", field, len(response.Series))
		}
		value, ok := metricLabelValue(response.Series[0].Labels, field)
		if !ok || value != "" {
			t.Fatalf("fixture %s group is not explicitly empty: labels=%+v", field, response.Series[0].Labels)
		}
		checkMetricSampleTimestamps(t, response.Series[0].Samples, start, end)
		seen := make(map[string]int)
		checkSeriesExemplars(t, response.Series[0], []string{field}, spansByID, seen, start, end)
		checkFixtureExemplars(t, seen, spans, "grouped by "+field)
	}

	histogram := queryMetricsRange(t, `{ resource.service.name = "frontend" } | histogram_over_time(duration) by (name)`, start, end)
	if len(histogram.Series) != 1 {
		t.Fatalf("fixture did not leave one histogram series after dropping zero-duration buckets; got %d", len(histogram.Series))
	}
	checkMetricSampleTimestamps(t, histogram.Series[0].Samples, start, end)
	histogramSeen := make(map[string]int)
	checkSeriesExemplars(t, histogram.Series[0], []string{"name"}, spansByID, histogramSeen, start, end)
	var getSpans []metricsRangeFixtureSpan
	for _, span := range spans {
		if span.name == "GET" {
			getSpans = append(getSpans, span)
		}
	}
	checkFixtureExemplars(t, histogramSeen, getSpans, "single-series")

	bucketHistogram := queryMetricsRange(t, `{ resource.service.name = "frontend" } | histogram_over_time(duration) by (__bucket)`, start, end)
	if len(bucketHistogram.Series) != 2 {
		t.Fatalf("fixture did not produce two raw-bucket histogram series; got %d", len(bucketHistogram.Series))
	}
	bucketHistogramSeen := make(map[string]int)
	for _, series := range bucketHistogram.Series {
		bucketLabels := 0
		for _, label := range series.Labels {
			if label.Key == "__bucket" {
				bucketLabels++
			}
		}
		if bucketLabels != 2 {
			t.Fatalf("fixture series does not contain both raw and histogram bucket labels: %+v", series.Labels)
		}
		checkMetricSampleTimestamps(t, series.Samples, start, end)
		checkSeriesExemplars(t, series, []string{"__bucket"}, spansByID, bucketHistogramSeen, start, end)
	}
	checkFixtureExemplars(t, bucketHistogramSeen, getSpans, "grouped raw-bucket histogram")
}

func flushLocalStorage(t *testing.T) error {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/internal/force_flush", nil)
	w := httptest.NewRecorder()
	if !vtstorage.RequestHandler(w, r) {
		return fmt.Errorf("vtstorage did not handle force-flush request")
	}
	if w.Code != http.StatusOK {
		return fmt.Errorf("force-flush returned status %d: %s", w.Code, w.Body.String())
	}
	return nil
}

func readStoredMetricSpans(t *testing.T, start, end time.Time) map[string]metricsRangeFixtureSpan {
	t.Helper()

	q, err := logstorage.ParseQueryAtTimestamp(`* | fields trace_id, span_id, name, "span_attr:http.method", p, __bucket`, end.UnixNano())
	if err != nil {
		t.Fatalf("cannot parse stored-span fixture query: %s", err)
	}
	q.AddTimeFilter(start.UnixNano(), end.UnixNano())
	var stats logstorage.QueryStats
	qctx := logstorage.NewQueryContext(context.Background(), &stats, []logstorage.TenantID{{}}, q, false, nil)

	spans := make(map[string]metricsRangeFixtureSpan)
	var mu sync.Mutex
	var missingColumns bool
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		columns := db.GetColumns(false)
		var traceIDColumn, spanIDColumn, nameColumn, methodColumn, pColumn, bucketColumn *logstorage.BlockColumn
		for i := range columns {
			c := &columns[i]
			switch c.Name {
			case otelpb.TraceIDField:
				traceIDColumn = c
			case otelpb.SpanIDField:
				spanIDColumn = c
			case otelpb.NameField:
				nameColumn = c
			case otelpb.SpanAttrPrefixField + "http.method":
				methodColumn = c
			case "p":
				pColumn = c
			case "__bucket":
				bucketColumn = c
			}
		}
		if traceIDColumn == nil || spanIDColumn == nil || nameColumn == nil || methodColumn == nil || pColumn == nil || bucketColumn == nil {
			mu.Lock()
			missingColumns = true
			mu.Unlock()
			return
		}
		for i := range db.RowsCount() {
			span := metricsRangeFixtureSpan{
				traceID: strings.Clone(traceIDColumn.Values[i]),
				spanID:  strings.Clone(spanIDColumn.Values[i]),
				name:    strings.Clone(nameColumn.Values[i]),
				method:  strings.Clone(methodColumn.Values[i]),
				p:       strings.Clone(pColumn.Values[i]),
				bucket:  strings.Clone(bucketColumn.Values[i]),
			}
			mu.Lock()
			spans[span.traceID+":"+span.spanID] = span
			mu.Unlock()
		}
	}
	if err := vtstorage.RunQuery(qctx, writeBlock); err != nil {
		t.Fatalf("cannot resolve stored spans: %s", err)
	}
	if missingColumns {
		t.Fatal("stored-span fixture query omitted an ID or grouping column")
	}
	return spans
}

func queryMetricsRange(t *testing.T, query string, start, end time.Time) metricsRangeResponse {
	t.Helper()

	params := url.Values{}
	params.Set("q", query)
	params.Set("start", strconv.FormatInt(start.Unix(), 10))
	params.Set("end", strconv.FormatInt(end.Unix(), 10))
	params.Set("step", time.Minute.String())
	r := httptest.NewRequest(http.MethodGet, "/select/tempo/api/metrics/query_range?"+params.Encode(), nil)
	w := httptest.NewRecorder()
	if !RequestHandler(context.Background(), w, r) {
		t.Fatalf("Tempo handler did not handle metrics query %q", query)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("metrics query %q returned status %d: %s", query, w.Code, w.Body.String())
	}
	if contentType := w.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("metrics query %q returned Content-Type %q; want application/json", query, contentType)
	}
	var response metricsRangeResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("metrics query %q returned invalid JSON: %s; body=%s", query, err, w.Body.String())
	}
	if response.Status != "COMPLETE" {
		t.Fatalf("metrics query %q returned status %q; want COMPLETE", query, response.Status)
	}
	return response
}

func checkSeriesExemplars(t *testing.T, series metricsRangeSeries, groupFields []string, spansByID map[string]metricsRangeFixtureSpan, seen map[string]int, start, end time.Time) int {
	t.Helper()

	groupValues := make([]string, len(groupFields))
	for i, field := range groupFields {
		value, ok := metricLabelValue(series.Labels, field)
		if !ok {
			t.Fatalf("series is missing grouping label %q: %+v", field, series.Labels)
		}
		groupValues[i] = value
	}
	for _, exemplar := range series.Exemplars {
		span, ok := resolveMetricsExemplar(exemplar, spansByID)
		if !ok {
			t.Errorf("exemplar identity mismatch: labels do not resolve to an inserted span: %+v", exemplar.Labels)
			continue
		}
		key := span.traceID + ":" + span.spanID
		seen[key]++
		for i, field := range groupFields {
			if groupValues[i] != span.groupValue(field) {
				t.Errorf("exemplar group mismatch: trace %q span %q has %s=%q but its series has %s=%q", span.traceID, span.spanID, field, span.groupValue(field), field, groupValues[i])
			}
		}
		checkExemplarTimestamp(t, exemplar, series.Samples, start, end)
	}
	return len(series.Exemplars)
}

func checkFixtureExemplars(t *testing.T, seen map[string]int, spans []metricsRangeFixtureSpan, queryKind string) {
	t.Helper()
	if len(seen) != len(spans) {
		t.Errorf("%s exemplar count mismatch: got %d distinct spans; want %d", queryKind, len(seen), len(spans))
	}
	for _, span := range spans {
		key := span.traceID + ":" + span.spanID
		if seen[key] != 1 {
			t.Errorf("%s exemplar count mismatch: span %q occurred %d times; want once", queryKind, span.spanID, seen[key])
		}
	}
}

func metricLabelValue(labels []metricsRangeLabel, key string) (string, bool) {
	for _, label := range labels {
		if label.Key == key {
			if label.Value.DoubleValue != nil {
				return strconv.FormatFloat(*label.Value.DoubleValue, 'g', -1, 64), true
			}
			return label.Value.StringValue, true
		}
	}
	return "", false
}

func resolveMetricsExemplar(exemplar metricsRangeExemplar, spansByID map[string]metricsRangeFixtureSpan) (metricsRangeFixtureSpan, bool) {
	traceID, hasTraceID := metricLabelValue(exemplar.Labels, "trace:id")
	spanID, hasSpanID := metricLabelValue(exemplar.Labels, "span:id")
	if !hasTraceID || !hasSpanID {
		return metricsRangeFixtureSpan{}, false
	}
	span, ok := spansByID[traceID+":"+spanID]
	return span, ok
}

func hasPositiveMetricSample(samples []metricsRangeSample) bool {
	for _, sample := range samples {
		if sample.Value > 0 {
			return true
		}
	}
	return false
}

func checkMetricSampleTimestamps(t *testing.T, samples []metricsRangeSample, start, end time.Time) {
	t.Helper()
	if len(samples) == 0 {
		t.Fatal("fixture series has no samples")
	}

	startMs, endMs := start.UnixMilli(), end.UnixMilli()
	var previous int64
	for i, sample := range samples {
		timestampMs, err := strconv.ParseInt(sample.TimestampMs, 10, 64)
		if err != nil {
			t.Fatalf("sample timestamp %q is not an integer string: %s", sample.TimestampMs, err)
		}
		if timestampMs < startMs || timestampMs > endMs {
			t.Errorf("sample timestamp %d is outside [%d,%d]", timestampMs, startMs, endMs)
		}
		if i > 0 && timestampMs < previous {
			t.Errorf("sample timestamps are not ordered: %d follows %d", timestampMs, previous)
		}
		previous = timestampMs
	}
}

func checkExemplarTimestamp(t *testing.T, exemplar metricsRangeExemplar, samples []metricsRangeSample, start, end time.Time) {
	t.Helper()

	timestampMs, err := strconv.ParseInt(exemplar.TimestampMs, 10, 64)
	if err != nil {
		t.Errorf("exemplar timestamp %q is not an integer string: %s", exemplar.TimestampMs, err)
		return
	}
	if timestampMs < start.UnixMilli() || timestampMs > end.UnixMilli() {
		t.Errorf("exemplar timestamp %d is outside the requested range", timestampMs)
	}

	stepMs := int64(time.Minute / time.Millisecond)
	nearestDistance := stepMs + 1
	var nearestValue float64
	for _, sample := range samples {
		sampleTimestampMs, parseErr := strconv.ParseInt(sample.TimestampMs, 10, 64)
		if parseErr != nil {
			continue
		}
		distance := timestampMs - sampleTimestampMs
		if distance < 0 {
			distance = -distance
		}
		if distance < nearestDistance {
			nearestDistance = distance
			nearestValue = sample.Value
		}
	}
	if nearestDistance > stepMs {
		t.Errorf("exemplar timestamp %d is not within one sample step of its series", timestampMs)
	}
	if exemplar.Value != nearestValue {
		t.Errorf("exemplar value %v is not snapped to its nearest sample value %v", exemplar.Value, nearestValue)
	}
}

func (span metricsRangeFixtureSpan) groupValue(field string) string {
	switch field {
	case "name":
		return span.name
	case "span.http.method":
		return span.method
	case "p":
		return span.p
	case "__bucket":
		return span.bucket
	default:
		return ""
	}
}
