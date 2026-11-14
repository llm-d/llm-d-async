package metrics

import (
	"context"
	"maps"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// inferenceLatencyExemplar returns the sample count of a queue's inference
// latency series and the labels of the exemplar it carries, or nil if none.
func inferenceLatencyExemplar(t *testing.T, queueID string) (uint64, map[string]string) {
	t.Helper()
	obs, err := InferenceLatencyTime.GetMetricWithLabelValues(queueID, "exemplar-queue", "exemplar-pool")
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	m := &dto.Metric{}
	if err := obs.(prometheus.Metric).Write(m); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var labels map[string]string
	for _, bucket := range m.GetHistogram().GetBucket() {
		if e := bucket.GetExemplar(); e != nil {
			labels = map[string]string{}
			for _, l := range e.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
		}
	}
	return m.GetHistogram().GetSampleCount(), labels
}

func TestRecordInferenceLatencyTraceExemplar(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	recording, span := tp.Tracer("exemplar-test").Start(context.Background(), "process-request")
	defer span.End()

	remoteTraceID := trace.TraceID{0x4b, 0xf9, 0x2f, 0x35}
	remote := func(flags trace.TraceFlags) context.Context {
		return trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    remoteTraceID,
			SpanID:     trace.SpanID{0x01},
			TraceFlags: flags,
		}))
	}

	for _, tc := range []struct {
		name string
		ctx  context.Context
		want map[string]string
	}{
		{
			name: "recording span",
			ctx:  recording,
			want: map[string]string{
				exemplarTraceIDLabel: span.SpanContext().TraceID().String(),
				exemplarSpanIDLabel:  span.SpanContext().SpanID().String(),
			},
		},
		{
			// A sampled trace with no span of this process: the span ID would name
			// a span this process never exported.
			name: "propagated span",
			ctx:  remote(trace.FlagsSampled),
			want: map[string]string{exemplarTraceIDLabel: remoteTraceID.String()},
		},
		{
			// No trace was exported, so a link would go nowhere.
			name: "unsampled span",
			ctx:  remote(0),
		},
		{
			name: "no span",
			ctx:  context.Background(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			RecordInferenceLatency(tc.ctx, 42, tc.name, "exemplar-queue", "exemplar-pool")

			count, got := inferenceLatencyExemplar(t, tc.name)
			if count != 1 {
				t.Errorf("sample count = %d, want 1", count)
			}
			if !maps.Equal(got, tc.want) {
				t.Errorf("exemplar labels = %v, want %v", got, tc.want)
			}
		})
	}
}
