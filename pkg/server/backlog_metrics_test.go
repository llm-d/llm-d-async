package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type oneShotBacklogReporter struct {
	cancel context.CancelFunc
	stats  []pipeline.QueueBacklogStat
}

func (r oneShotBacklogReporter) QueueBacklog(context.Context) ([]pipeline.QueueBacklogStat, error) {
	r.cancel()
	return r.stats, errors.New("partial backlog read")
}

func TestPollBacklogRecordsPerQueueSourceAvailability(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reporter := oneShotBacklogReporter{
		cancel: cancel,
		stats: []pipeline.QueueBacklogStat{
			{QueueID: "q-poll-ok", QueueName: "queue-poll-ok", PoolName: "pool-poll", Depth: 7, SourceAvailable: true},
			{QueueID: "q-poll-bad", QueueName: "queue-poll-bad", PoolName: "pool-poll", Depth: 0, SourceAvailable: false},
		},
	}

	pollBacklog(ctx, reporter, time.Hour)

	if got := testutil.ToFloat64(metrics.BrokerBacklog.WithLabelValues("q-poll-ok", "queue-poll-ok", "pool-poll")); got != 7 {
		t.Fatalf("healthy backlog = %v, want 7", got)
	}
	if got := testutil.ToFloat64(metrics.BrokerBacklogSourceAvailable.WithLabelValues("q-poll-ok", "queue-poll-ok", "pool-poll")); got != 1 {
		t.Fatalf("healthy source availability = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.BrokerBacklogSourceAvailable.WithLabelValues("q-poll-bad", "queue-poll-bad", "pool-poll")); got != 0 {
		t.Fatalf("failed source availability = %v, want 0", got)
	}
}
