//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"

	"github.com/alicebob/miniredis/v2"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sortedSetCfg builds the single-queue transport config used by the loss
// tests, with optional durable-dequeue overrides.
func sortedSetCfg(addr, igwBaseURL string, leaseTTLSeconds, reclaimIntervalMs int64) redis.SortedSetConfig {
	cfg := redis.SortedSetConfig{
		URL:             "redis://" + addr,
		RetryQueueName:  "retry-sortedset",
		ResultQueueName: "result-list",
		PollIntervalMs:  50,
		BatchSize:       10,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:      "request-sortedset",
			WorkerPoolID:   "default",
			RequestPathURL: "/v1/completions",
			IGWBaseURL:     igwBaseURL,
		}},
	}
	if leaseTTLSeconds > 0 {
		cfg.ClaimLeaseTTLSeconds = leaseTTLSeconds
	}
	if reclaimIntervalMs > 0 {
		cfg.ClaimReclaimIntervalMs = reclaimIntervalMs
	}
	cfg.ApplyDefaults()
	return cfg
}

// newShutdownLossFlow builds a RedisSortedSetFlow over miniredis with a single
// queue and returns the flow plus a raw client for Redis-side accounting.
// leaseTTLSeconds/reclaimIntervalMs override the durable-dequeue tuning so
// tests can exercise lease expiry quickly (0 keeps the defaults).
func newShutdownLossFlow(t *testing.T, workers int, igwBaseURL string, leaseTTLSeconds int64, reclaimIntervalMs int64) (*redis.RedisSortedSetFlow, *goredis.Client, string) {
	t.Helper()
	s := miniredis.RunT(t)

	flow, err := redis.NewRedisSortedSetFlow(sortedSetCfg(s.Addr(), igwBaseURL, leaseTTLSeconds, reclaimIntervalMs),
		[]pipeline.WorkerPoolConfig{{ID: "default", Workers: workers}}, nil)
	require.NoError(t, err)

	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return flow, rdb, "request-sortedset"
}

// newFlowOnSameRedis builds an additional flow against an existing Redis
// (used to simulate a replacement instance taking over from a dead one).
func newFlowOnSameRedis(t *testing.T, rdb *goredis.Client, igwBaseURL string, leaseTTLSeconds, reclaimIntervalMs int64) *redis.RedisSortedSetFlow {
	t.Helper()
	flow, err := redis.NewRedisSortedSetFlow(sortedSetCfg(rdb.Options().Addr, igwBaseURL, leaseTTLSeconds, reclaimIntervalMs),
		[]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	require.NoError(t, err)
	return flow
}

func enqueueShutdownLossRequests(t *testing.T, rdb *goredis.Client, queue string, ids []string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		ir := asyncapi.NewInternalRequest(
			asyncapi.InternalRouting{RequestQueueName: queue},
			&asyncapi.RequestMessage{
				ID:       id,
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(5 * time.Minute).Unix(),
				Payload:  testPayload(map[string]any{"model": "test", "prompt": "hello"}),
			},
		)
		member, err := ir.MarshalJSON()
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(ctx, queue, goredis.Z{
			Score:  float64(time.Now().Add(5 * time.Minute).Unix()),
			Member: string(member),
		}).Err())
	}
}

func containsID(member, id string) bool {
	return len(member) > 0 && indexOf(member, "\"id\":\""+id+"\"") >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// TestLeaseExpiry_TakeoverRedeliversClaims validates claim lease expiry
// and takeover: Flow A claims requests and stops heartbeating (simulating
// an abandoned instance). Once its lease lapses, replacement Flow C reclaims
// the work and processes each accepted request.
func TestLeaseExpiry_TakeoverRedeliversClaims(t *testing.T) {
	var killHits atomic.Int64
	releaseKilled := make(chan struct{})
	killedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		killHits.Add(1)
		<-releaseKilled // block forever, like an inference that never returns
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		close(releaseKilled)
		killedServer.Close()
	}()

	// Flow A: initial instance with short lease + fast reclaim.
	flowA, rdbA, queue := newShutdownLossFlow(t, 1, killedServer.URL, 1, 100)

	workerCtxA, workerCancelA := context.WithCancel(context.Background())
	flowCtxA, flowCancelA := context.WithCancel(context.Background())
	var wgA sync.WaitGroup
	t.Cleanup(func() {
		select {
		case <-releaseKilled:
		default:
			close(releaseKilled)
		}
		workerCancelA()
		flowCancelA()
		wgA.Wait()
		flowA.Shutdown()
	})

	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	dispatchA := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flowA.RequestChannels(), pools)
	mergedA := dispatchA.Channels["default"]

	clientA := asyncworker.NewHTTPInferenceClient(killedServer.Client())
	wgA.Add(1)
	go func() {
		defer wgA.Done()
		asyncworker.WorkerWithGate(workerCtxA, workerCtxA,
			pipeline.Characteristics{}, clientA, mergedA, flowA.RetryChannel(), flowA.ResultChannel(),
			time.Minute, nil, nil)
	}()

	flowA.Start(flowCtxA)

	ids := []string{"kill-a", "kill-b", "kill-c"}
	enqueueShutdownLossRequests(t, rdbA, queue, ids)

	// Deterministic progress point: A has claimed every request and its
	// worker is blocked inside the never-returning inference call.
	waitUntil(t, 5*time.Second, func() bool { return killHits.Load() >= 1 })
	claimedKey := queue + ":claimed"
	for _, id := range ids {
		exists, err := rdbA.HExists(context.Background(), claimedKey, id).Result()
		require.NoError(t, err)
		require.True(t, exists, "flow A should hold claim for %s", id)
	}

	// Stop all of Flow A's loops (consumer, mover, reclaimer, heartbeater)
	// before starting Flow C so the original instance is fully inert.
	flowA.StopConsuming()
	flowA.Shutdown()

	// Replacement flow C: healthy IGW returning success immediately.
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer successServer.Close()

	flowC := newFlowOnSameRedis(t, rdbA, successServer.URL, 1, 100)
	workerCtxC, workerCancelC := context.WithCancel(context.Background())
	flowCtxC, flowCancelC := context.WithCancel(context.Background())

	dispatchC := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flowC.RequestChannels(), pools)
	mergedC := dispatchC.Channels["default"]

	clientC := asyncworker.NewHTTPInferenceClient(successServer.Client())
	var wgC sync.WaitGroup
	wgC.Add(1)
	go func() {
		defer wgC.Done()
		asyncworker.WorkerWithGate(workerCtxC, workerCtxC,
			pipeline.Characteristics{}, clientC, mergedC, flowC.RetryChannel(), flowC.ResultChannel(),
			time.Minute, nil, nil)
	}()
	flowC.Start(flowCtxC)

	t.Cleanup(func() {
		flowC.StopConsuming()
		workerCancelC()
		flowCancelC()
		wgC.Wait()
		flowC.Shutdown()
	})

	// Every accepted request must produce a terminal record upon takeover.
	waitUntil(t, 15*time.Second, func() bool {
		n, err := rdbA.LLen(context.Background(), "result-list").Result()
		return err == nil && n == int64(len(ids))
	})
	time.Sleep(300 * time.Millisecond) // let any late duplicates attempt to land
	n, _ := rdbA.LLen(context.Background(), "result-list").Result()
	assert.Equal(t, int64(len(ids)), n, "terminal records produced for accepted requests")

	raw, _ := rdbA.LRange(context.Background(), "result-list", 0, -1).Result()
	got := map[string]int{}
	for _, m := range raw {
		for _, id := range ids {
			if containsID(m, id) {
				got[id]++
			}
		}
	}
	for _, id := range ids {
		assert.Equal(t, 1, got[id], "request %s must have exactly one result", id)
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
