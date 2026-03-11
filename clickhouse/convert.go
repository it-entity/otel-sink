package clickhouse

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func TracesFromProto(req *coltracepb.ExportTraceServiceRequest) []TraceRow {
	var rows []TraceRow
	for _, rs := range req.GetResourceSpans() {
		svc := extractServiceName(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				startNano := span.GetStartTimeUnixNano()
				endNano := span.GetEndTimeUnixNano()
				rows = append(rows, TraceRow{
					TraceID:      hex.EncodeToString(span.GetTraceId()),
					SpanID:       hex.EncodeToString(span.GetSpanId()),
					ParentSpanID: hex.EncodeToString(span.GetParentSpanId()),
					ServiceName:  svc,
					SpanName:     span.GetName(),
					SpanKind:     spanKindString(span.GetKind()),
					StartTime:    nanoToTime(startNano),
					EndTime:      nanoToTime(endNano),
					DurationNs:   int64(endNano - startNano),
					StatusCode:   statusCodeString(span.GetStatus()),
					Attributes:   kvToJSON(span.GetAttributes()),
				})
			}
		}
	}
	return rows
}

func MetricsFromProto(req *colmetricspb.ExportMetricsServiceRequest) []MetricRow {
	var rows []MetricRow
	for _, rm := range req.GetResourceMetrics() {
		svc := extractServiceName(rm.GetResource())
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					for _, dp := range d.Gauge.GetDataPoints() {
						rows = append(rows, numberDataPointToRow(m.GetName(), "gauge", svc, dp))
					}
				case *metricspb.Metric_Sum:
					for _, dp := range d.Sum.GetDataPoints() {
						rows = append(rows, numberDataPointToRow(m.GetName(), "sum", svc, dp))
					}
				case *metricspb.Metric_Histogram:
					for _, dp := range d.Histogram.GetDataPoints() {
						rows = append(rows, MetricRow{
							MetricName:  m.GetName(),
							MetricType:  "histogram",
							ServiceName: svc,
							Timestamp:   nanoToTime(dp.GetTimeUnixNano()),
							Value:       dp.GetSum(),
							Attributes:  kvToJSON(dp.GetAttributes()),
						})
					}
				case *metricspb.Metric_ExponentialHistogram:
					for _, dp := range d.ExponentialHistogram.GetDataPoints() {
						rows = append(rows, MetricRow{
							MetricName:  m.GetName(),
							MetricType:  "exponential_histogram",
							ServiceName: svc,
							Timestamp:   nanoToTime(dp.GetTimeUnixNano()),
							Value:       dp.GetSum(),
							Attributes:  kvToJSON(dp.GetAttributes()),
						})
					}
				case *metricspb.Metric_Summary:
					for _, dp := range d.Summary.GetDataPoints() {
						rows = append(rows, MetricRow{
							MetricName:  m.GetName(),
							MetricType:  "summary",
							ServiceName: svc,
							Timestamp:   nanoToTime(dp.GetTimeUnixNano()),
							Value:       dp.GetSum(),
							Attributes:  kvToJSON(dp.GetAttributes()),
						})
					}
				}
			}
		}
	}
	return rows
}

func LogsFromProto(req *collogspb.ExportLogsServiceRequest) []LogRow {
	var rows []LogRow
	for _, rl := range req.GetResourceLogs() {
		svc := extractServiceName(rl.GetResource())
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				ts := lr.GetTimeUnixNano()
				if ts == 0 {
					ts = lr.GetObservedTimeUnixNano()
				}
				rows = append(rows, LogRow{
					Timestamp:    nanoToTime(ts),
					Severity:     uint8(lr.GetSeverityNumber()),
					SeverityText: lr.GetSeverityText(),
					ServiceName:  svc,
					Body:         anyValueToString(lr.GetBody()),
					Attributes:   kvToJSON(lr.GetAttributes()),
					TraceID:      hex.EncodeToString(lr.GetTraceId()),
					SpanID:       hex.EncodeToString(lr.GetSpanId()),
				})
			}
		}
	}
	return rows
}

// helpers

func extractServiceName(res *resourcepb.Resource) string {
	if res == nil {
		return ""
	}
	for _, kv := range res.GetAttributes() {
		if kv.GetKey() == "service.name" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

func nanoToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

func spanKindString(k tracepb.Span_SpanKind) string {
	switch k {
	case tracepb.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	default:
		return "UNSPECIFIED"
	}
}

func statusCodeString(s *tracepb.Status) string {
	if s == nil {
		return "UNSET"
	}
	switch s.GetCode() {
	case tracepb.Status_STATUS_CODE_OK:
		return "OK"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}

func numberDataPointToRow(name, typ, svc string, dp *metricspb.NumberDataPoint) MetricRow {
	var val float64
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		val = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		val = float64(v.AsInt)
	}
	return MetricRow{
		MetricName:  name,
		MetricType:  typ,
		ServiceName: svc,
		Timestamp:   nanoToTime(dp.GetTimeUnixNano()),
		Value:       val,
		Attributes:  kvToJSON(dp.GetAttributes()),
	}
}

func kvToJSON(attrs []*commonpb.KeyValue) string {
	if len(attrs) == 0 {
		return "{}"
	}
	m := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToInterface(kv.GetValue())
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	if s, ok := v.GetValue().(*commonpb.AnyValue_StringValue); ok {
		return s.StringValue
	}
	b, _ := json.Marshal(anyValueToInterface(v))
	return string(b)
}

func anyValueToInterface(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		arr := make([]any, 0, len(val.ArrayValue.GetValues()))
		for _, item := range val.ArrayValue.GetValues() {
			arr = append(arr, anyValueToInterface(item))
		}
		return arr
	case *commonpb.AnyValue_KvlistValue:
		m := make(map[string]any, len(val.KvlistValue.GetValues()))
		for _, kv := range val.KvlistValue.GetValues() {
			m[kv.GetKey()] = anyValueToInterface(kv.GetValue())
		}
		return m
	default:
		return fmt.Sprintf("%v", v)
	}
}
