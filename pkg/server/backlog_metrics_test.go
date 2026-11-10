package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/deadlinescaling"
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

	pollBacklog(ctx, reporter, time.Hour, nil)

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

// scalingBacklogReporter reports one queue's backlog, and deadline_scaling
// settings for the queue IDs in configs.
type scalingBacklogReporter struct {
	oneShotBacklogReporter
	configs map[string]deadlinescaling.Config
}

func (r scalingBacklogReporter) DeadlineScaling(queueID string) (deadlinescaling.Config, bool) {
	cfg, ok := r.configs[queueID]
	return cfg, ok
}

// expiring builds cumulative deadline-proximity counts with n items due
// within 2 minutes (and none sooner).
func expiring(n int64) []pipeline.ExpiringCount {
	out := make([]pipeline.ExpiringCount, 0, len(metrics.DeadlineProximityBuckets()))
	for i, b := range metrics.DeadlineProximityBuckets() {
		count := int64(0)
		if b >= 2*time.Minute {
			count = n
		}
		out = append(out, pipeline.ExpiringCount{Window: metrics.DeadlineProximityBucketLabels()[i], Count: count})
	}
	return out
}

func TestPollBacklogPublishesDeadlineRequiredReplicas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reporter := scalingBacklogReporter{
		oneShotBacklogReporter: oneShotBacklogReporter{cancel: cancel, stats: []pipeline.QueueBacklogStat{
			{QueueID: "q-scale", QueueName: "queue-scale", PoolName: "pool-scale", Depth: 240, SourceAvailable: true, ExpiringCounts: expiring(240)},
			{QueueID: "q-plain", QueueName: "queue-plain", PoolName: "pool-scale", Depth: 240, SourceAvailable: true, ExpiringCounts: expiring(240)},
			{QueueID: "q-pubsub", QueueName: "queue-pubsub", PoolName: "pool-scale", Depth: 240, SourceAvailable: true},
		}},
		configs: map[string]deadlinescaling.Config{
			"q-scale":  {ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10},
			"q-pubsub": {ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10},
		},
	}
	metrics.SetDeadlineRequiredReplicas("q-plain", "queue-plain", "pool-scale", 3) // left from settings a reload removed

	pollBacklog(ctx, reporter, time.Hour, nil)

	// 240 items due within 2m, 2 per second per replica after a 60s cold start: 2 replicas.
	if got := testutil.ToFloat64(metrics.DeadlineRequiredReplicas.WithLabelValues("q-scale", "queue-scale", "pool-scale")); got != 2 {
		t.Fatalf("required replicas = %v, want 2", got)
	}
	for _, queue := range [][3]string{{"q-plain", "queue-plain", "pool-scale"}, {"q-pubsub", "queue-pubsub", "pool-scale"}} {
		if metrics.DeadlineRequiredReplicas.DeleteLabelValues(queue[0], queue[1], queue[2]) {
			t.Fatalf("queue %s has a required-replicas series; only queues with deadline_scaling and deadline counts get one", queue[0])
		}
	}
}

func TestRemoveQueueSnapshotsDropsDeadlineRequiredReplicas(t *testing.T) {
	metrics.SetDeadlineRequiredReplicas("q-gone", "queue-gone", "pool-gone", 4)
	metrics.RemoveQueueSnapshots("q-gone", "queue-gone", "pool-gone")
	if metrics.DeadlineRequiredReplicas.DeleteLabelValues("q-gone", "queue-gone", "pool-gone") {
		t.Fatal("a removed queue kept its required-replicas series")
	}
}

func TestPollBacklogDropsDeadlineRequiredReplicasOnFailedRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := deadlinescaling.Config{ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10}
	reporter := scalingBacklogReporter{
		oneShotBacklogReporter: oneShotBacklogReporter{cancel: cancel, stats: []pipeline.QueueBacklogStat{
			{QueueID: "q-read-ok", QueueName: "queue-read-ok", PoolName: "pool-read", Depth: 240, SourceAvailable: true, ExpiringCounts: expiring(240)},
			{QueueID: "q-read-bad", QueueName: "queue-read-bad", PoolName: "pool-read", SourceAvailable: true, ExpiringCounts: expiring(0), ExpiringCountsFailed: true},
		}},
		configs: map[string]deadlinescaling.Config{"q-read-ok": cfg, "q-read-bad": cfg},
	}
	metrics.SetDeadlineRequiredReplicas("q-read-bad", "queue-read-bad", "pool-read", 3) // from the previous poll

	pollBacklog(ctx, reporter, time.Hour, nil)

	if got := testutil.ToFloat64(metrics.DeadlineRequiredReplicas.WithLabelValues("q-read-ok", "queue-read-ok", "pool-read")); got != 2 {
		t.Fatalf("required replicas = %v, want 2", got)
	}
	if metrics.DeadlineRequiredReplicas.DeleteLabelValues("q-read-bad", "queue-read-bad", "pool-read") {
		t.Fatal("the queue whose deadline read failed kept its required-replicas series")
	}
}

func TestPollBacklogPublishesPoolDeadlineRequiredReplicas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reporter := scalingBacklogReporter{
		oneShotBacklogReporter: oneShotBacklogReporter{cancel: cancel, stats: []pipeline.QueueBacklogStat{
			{QueueID: "q-pool", QueueName: "queue-pool", PoolName: "pool-shared", SourceAvailable: true, ExpiringCounts: expiring(60)},
			{QueueID: "q-own", QueueName: "queue-own", PoolName: "pool-shared", SourceAvailable: true, ExpiringCounts: expiring(120)},
			{QueueID: "q-other", QueueName: "queue-other", PoolName: "pool-unscaled", SourceAvailable: true, ExpiringCounts: expiring(240)},
		}},
		configs: map[string]deadlinescaling.Config{"q-own": {ThroughputRPS: 4, ColdStartSeconds: 60, MaxReplicas: 10}},
	}
	pools := map[string]deadlinescaling.Config{"pool-shared": {ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10}}

	pollBacklog(ctx, reporter, time.Hour, pools)

	// Due within 2m, after a 60s cold start: 60 items at the pool's 2 rps plus
	// 120 at q-own's 4 rps are 60 replica-seconds -> 1. Each queue alone needs 1.
	if got := testutil.ToFloat64(metrics.PoolDeadlineRequiredReplicas.WithLabelValues("pool-shared")); got != 1 {
		t.Fatalf("pool required replicas = %v, want 1", got)
	}
	if metrics.PoolDeadlineRequiredReplicas.DeleteLabelValues("pool-unscaled") {
		t.Fatal("a pool without deadline_scaling has a required-replicas series")
	}
}

// cleanBacklogReporter is oneShotBacklogReporter without the read error.
type cleanBacklogReporter struct{ oneShotBacklogReporter }

func (r cleanBacklogReporter) QueueBacklog(ctx context.Context) ([]pipeline.QueueBacklogStat, error) {
	stats, _ := r.oneShotBacklogReporter.QueueBacklog(ctx)
	return stats, nil
}

// A pool with no queues after a successful read has nothing due: 0, not a
// missing series that would keep an autoscaler on its fallback. A failed read
// still drops it.
func TestPollBacklogPublishesZeroForPoolWithoutQueues(t *testing.T) {
	pools := map[string]deadlinescaling.Config{"pool-empty": {ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10}}
	other := []pipeline.QueueBacklogStat{
		{QueueID: "q-elsewhere", QueueName: "queue-elsewhere", PoolName: "pool-other", SourceAvailable: true, ExpiringCounts: expiring(240)},
	}
	metrics.PoolDeadlineRequiredReplicas.Reset()
	metrics.SetPoolDeadlineRequiredReplicas("pool-empty", 3) // from before the reload

	ctx, cancel := context.WithCancel(context.Background())
	pollBacklog(ctx, cleanBacklogReporter{oneShotBacklogReporter{cancel: cancel, stats: other}}, time.Hour, pools)
	if n := testutil.CollectAndCount(metrics.PoolDeadlineRequiredReplicas); n != 1 {
		t.Fatalf("%d pool series, want pool-empty's", n)
	}
	if got := testutil.ToFloat64(metrics.PoolDeadlineRequiredReplicas); got != 0 {
		t.Fatalf("pool required replicas = %v, want 0", got)
	}

	ctx, cancel = context.WithCancel(context.Background())
	pollBacklog(ctx, oneShotBacklogReporter{cancel: cancel, stats: other}, time.Hour, pools)
	if n := testutil.CollectAndCount(metrics.PoolDeadlineRequiredReplicas); n != 0 {
		t.Fatal("the pool kept its required-replicas series after a failed read")
	}
}

func TestPollBacklogDropsPoolDeadlineRequiredReplicasOnFailedRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reporter := oneShotBacklogReporter{cancel: cancel, stats: []pipeline.QueueBacklogStat{
		{QueueID: "q-pool-ok", QueueName: "queue-pool-ok", PoolName: "pool-read", SourceAvailable: true, ExpiringCounts: expiring(240)},
		{QueueID: "q-pool-bad", QueueName: "queue-pool-bad", PoolName: "pool-read", SourceAvailable: true, ExpiringCounts: expiring(0), ExpiringCountsFailed: true},
	}}
	metrics.SetPoolDeadlineRequiredReplicas("pool-read", 3) // from the previous poll

	pollBacklog(ctx, reporter, time.Hour, map[string]deadlinescaling.Config{"pool-read": {ThroughputRPS: 2, ColdStartSeconds: 60, MaxReplicas: 10}})

	if metrics.PoolDeadlineRequiredReplicas.DeleteLabelValues("pool-read") {
		t.Fatal("the pool kept its required-replicas series after a failed read on one of its queues")
	}
}
