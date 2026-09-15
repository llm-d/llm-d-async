package sqlqueue

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTracingRecordsPostgresStatements(t *testing.T) {
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	s, err := Open(ctx, pgURL, WithTracerProvider(tp))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	require.NoError(t, s.TruncateForTest(ctx))

	now := time.Now()
	ownAll(t, s, "a", now)
	require.NoError(t, s.Enqueue(ctx, req("traced", now.Unix()+60)))
	require.Equal(t, []string{"traced"}, dispatchIDs(t, s, "a", now, 10))

	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		byID[span.SpanContext().SpanID()] = span
	}
	var dispatchSQL bool
	for _, span := range recorder.Ended() {
		for _, attr := range span.Attributes() {
			if !strings.Contains(attr.Value.AsString(), "UPDATE async_requests SET dispatch_epoch = p.epoch") {
				continue
			}
			var ancestors []string
			for p, ok := byID[span.Parent().SpanID()]; ok; p, ok = byID[p.Parent().SpanID()] {
				ancestors = append(ancestors, p.Name())
			}
			assert.Contains(t, ancestors, "sqlqueue.Dispatch")
			dispatchSQL = true
		}
	}
	assert.True(t, dispatchSQL, "the dispatch statement is recorded under its operation span")
}
