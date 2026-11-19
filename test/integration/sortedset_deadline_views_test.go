//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedSetDeadlineViews_PollToMetrics validates the full cross-package
// contract behind async_deadline_proximity_millis: a RedisSortedSetFlow built
// through its public constructor reads cumulative ZCOUNT bucket counts from a
// live sorted set, returns them as pipeline.QueueBacklogStat.ExpiringCounts,
// and those views land in a Prometheus snapshot histogram via the metric
// helper pollBacklog uses.
func TestSortedSetDeadlineViews_PollToMetrics(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	defer rdb.Close()
	ctx := context.Background()
	const queueName = "int-deadline-queue"

	cfg := redis.SortedSetConfig{
		URL:             "redis://" + s.Addr(),
		ResultQueueName: "int-results",
		PollIntervalMs:  1000,
		BatchSize:       10,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:  queueName,
			IGWBaseURL: "http://gw",
		}},
	}
	cfg.ApplyDefaults()
	flow, err := redis.NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	require.NoError(t, err)

	// Seed four requests with known deadline offsets: one expired, one within
	// 1m, one within 30m, one within 2h. Offsets keep at least 5s margin from
	// bucket boundaries: QueueBacklog reads ZCOUNT against its own clock,
	// which may be up to a second ahead of the test's.
	now := time.Now().Unix()
	offsets := []int64{-10, 40, 1000, 4000}
	for i, off := range offsets {
		deadline := now + off
		ir := api.NewInternalRequest(
			api.InternalRouting{RequestQueueName: queueName},
			&api.RequestMessage{
				ID:       fmt.Sprintf("int-msg-%d", i),
				Created:  now,
				Deadline: deadline,
				Payload:  testPayload(map[string]any{"prompt": "hi"}),
			},
		)
		irBytes, err := json.Marshal(ir)
		require.NoError(t, err)
		err = rdb.ZAdd(ctx, queueName, goredis.Z{
			Score: float64(deadline), Member: string(irBytes),
		}).Err()
		require.NoError(t, err)
	}

	stats, err := flow.QueueBacklog(ctx)
	require.NoError(t, err)
	require.Len(t, stats, 1, "one configured queue")

	stat := stats[0]
	assert.Equal(t, queueName, stat.QueueName)
	assert.Equal(t, "default", stat.PoolName, "WorkerPoolID defaults to default")
	assert.Equal(t, int64(4), stat.Depth)

	// Cumulative le counts for offsets {-10, 40, 1000, 4000}:
	// {0:1, 1000:1, 5000:1, 15000:1, 30000:1, 60000:2, 120000:2, 300000:2,
	//  600000:2, 1800000:3, 3600000:3, 7200000:4, 21600000:4, 86400000:4}.
	require.Len(t, stat.ExpiringCounts, len(metrics.DeadlineProximityBucketLabels()),
		"one cumulative count per histogram bucket")
	for i, wantLabel := range metrics.DeadlineProximityBucketLabels() {
		assert.Equal(t, wantLabel, stat.ExpiringCounts[i].Window, "windows follow the bucket order")
	}
	wantCounts := []int64{1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 4}
	for i, want := range wantCounts {
		assert.Equal(t, want, stat.ExpiringCounts[i].Count,
			"cumulative count for window %s", stat.ExpiringCounts[i].Window)
	}

	// Observe the views exactly as pollBacklog does, then assert the series.
	metrics.DeadlineProximity.Reset()
	counts := make([]int64, 0, len(stat.ExpiringCounts))
	for _, ec := range stat.ExpiringCounts {
		counts = append(counts, ec.Count)
	}
	metrics.SetDeadlineProximity(stat.QueueID, stat.QueueName, stat.PoolName, counts)

	h := histogramFor(t, metrics.DeadlineProximity, map[string]string{
		"queue_id": stat.QueueID, "queue_name": stat.QueueName, "pool_name": stat.PoolName,
	})
	assert.Equal(t, uint64(4), h.GetSampleCount(), "one queued item per sample")
	assert.Equal(t, float64(6645000), h.GetSampleSum(),
		"estimated sum = midpoints: 45000 + 1200000 + 5400000")
	require.Len(t, h.GetBucket(), len(metrics.DeadlineProximityBucketLabels()))
	for i, b := range h.GetBucket() {
		assert.Equal(t, float64(metrics.DeadlineProximityBuckets()[i].Milliseconds()), b.GetUpperBound(),
			"upper bound %d", i)
		assert.Equal(t, wantCounts[i], int64(b.GetCumulativeCount()), "cumulative count %d", i)
	}
}

// histogramFor gathers a histogram collector and returns the histogram whose
// labels match wantLabels exactly.
func histogramFor(t *testing.T, c prometheus.Collector, wantLabels map[string]string) *dto.Histogram {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	for m := range ch {
		var dtoMetric dto.Metric
		require.NoError(t, m.Write(&dtoMetric))
		labels := make(map[string]string, len(dtoMetric.GetLabel()))
		for _, lp := range dtoMetric.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		if len(labels) != len(wantLabels) {
			continue
		}
		match := true
		for k, v := range wantLabels {
			if labels[k] != v {
				match = false
				break
			}
		}
		if match {
			return dtoMetric.GetHistogram()
		}
	}
	t.Fatalf("no series with labels %v", wantLabels)
	return nil
}
