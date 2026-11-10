package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

// Exemplar label names, the same as llm-d-router's. Grafana links an exemplar
// to its trace by the trace_id label name.
const (
	exemplarTraceIDLabel = "trace_id"
	exemplarSpanIDLabel  = "span_id"
)

// observeWithTraceExemplar records value and attaches the trace in ctx as a
// Prometheus exemplar, so a point on a latency graph links to the request's
// trace.
//
// An unsampled span attaches nothing: no trace was exported for it, so the link
// would go nowhere. span_id is only added for a recording span, the one this
// process started and exports; a sampled span context without one gets
// trace_id alone.
func observeWithTraceExemplar(ctx context.Context, observer prometheus.Observer, value float64) {
	spanCtx := trace.SpanContextFromContext(ctx)
	exemplarObserver, ok := observer.(prometheus.ExemplarObserver)
	if !ok || !spanCtx.IsSampled() {
		observer.Observe(value)
		return
	}
	// trace_id and span_id together are 63 runes, under the 128-rune exemplar limit.
	labels := prometheus.Labels{exemplarTraceIDLabel: spanCtx.TraceID().String()}
	if trace.SpanFromContext(ctx).IsRecording() {
		labels[exemplarSpanIDLabel] = spanCtx.SpanID().String()
	}
	exemplarObserver.ObserveWithExemplar(value, labels)
}
