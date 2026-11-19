//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/llm-d/llm-d-async/producer"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedisClusterMode_RoundTrip drives the sorted-set flow and the producer
// in cluster mode through a full submit → dispatch → durable receive → ack
// cycle, plus a cancellation, with the result list deliberately placed in a
// different hash slot than the request queue so the cross-slot ack path is
// the one exercised.
//
// It always runs against miniredis, which emulates a single-node cluster
// (CLUSTER SLOTS etc.) so a genuine go-redis cluster client bootstraps and
// routes through it. Set REDIS_CLUSTER_ADDRS to a comma-separated node list
// (for example "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002") to run the
// same body against a real multi-node cluster, which additionally enforces
// CROSSSLOT on every script.
func TestRedisClusterMode_RoundTrip(t *testing.T) {
	t.Run("miniredis single-node cluster", func(t *testing.T) {
		s := miniredis.RunT(t)
		runClusterRoundTrip(t, []string{s.Addr()})
	})
	t.Run("real cluster", func(t *testing.T) {
		raw := os.Getenv("REDIS_CLUSTER_ADDRS")
		if raw == "" {
			t.Skip("set REDIS_CLUSTER_ADDRS=host:port,host:port,... to run against a real Redis Cluster")
		}
		runClusterRoundTrip(t, strings.Split(raw, ","))
	})
}

func runClusterRoundTrip(t *testing.T, addrs []string) {
	t.Helper()
	ctx := context.Background()
	rdb := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = rdb.Close() })
	require.NoError(t, rdb.Ping(ctx).Err())

	// Fixed, untagged names so the hash-tag wrapping of the private keys is
	// exercised. Their slots differ (11501 for the queue, 2455 for the result
	// list, 13554 for the retry queue), which puts the result ack on the
	// cross-slot path. Computed offline: miniredis answers CLUSTER KEYSLOT
	// with a constant, so it cannot be asked at run time.
	const (
		queue      = "cluster-it-requests"
		resultList = "cluster-it-results"
		retryQueue = "cluster-it-retry"
	)
	// Per-key DEL: a multi-key DEL across slots is rejected by a cluster.
	cleanup := func() {
		for _, key := range []string{queue, "{" + queue + "}:claimed", "{" + queue + "}:claim-owners", "{" + queue + "}:claims-idx",
			retryQueue, resultList, "{" + resultList + "}:result-claimed", "{" + resultList + "}:result-claim-owners",
			"{" + resultList + "}:result-claims-idx", "{" + resultList + "}:result-ack-tombstones"} {
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	var hits atomic.Int64
	inference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":"success"}`))
	}))
	defer inference.Close()

	cfg := redis.SortedSetConfig{
		URL:                    "redis://" + addrs[0],
		Mode:                   redis.ModeCluster,
		Addrs:                  addrs[1:],
		ResultQueueName:        resultList,
		RetryQueueName:         retryQueue,
		PollIntervalMs:         50,
		BatchSize:              10,
		ClaimLeaseTTLSeconds:   5,
		ClaimReclaimIntervalMs: 100,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName: queue, WorkerPoolID: "default", IGWBaseURL: inference.URL,
		}},
	}
	cfg.ApplyDefaults()
	require.NoError(t, cfg.Validate())
	flow, err := redis.NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 2}}, nil)
	require.NoError(t, err)

	prod, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
		RedisURL:         "redis://" + addrs[0],
		RedisMode:        producer.RedisModeCluster,
		RedisAddrs:       addrs[1:],
		RequestQueueName: queue,
		ResultQueueName:  resultList,
	}, producer.WithResultClaimReclaimInterval(20*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = prod.Close() })
	// A second producer stores payloads apart from the envelope, which adds a
	// per-request payload key in yet another slot.
	pointer, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
		RedisURL:         "redis://" + addrs[0],
		RedisMode:        producer.RedisModeCluster,
		RedisAddrs:       addrs[1:],
		RequestQueueName: queue,
		ResultQueueName:  resultList,
	}, producer.WithPayloadKeys())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pointer.Close() })

	// A request cancelled before the dispatcher ever sees it must surface as
	// a CANCELLED result and never reach inference.
	deadline := time.Now().Add(time.Minute).Unix()
	submit := func(id string) {
		t.Helper()
		via := prod
		if strings.HasPrefix(id, "p") {
			via = pointer
		}
		require.NoError(t, via.SubmitRequest(ctx, &api.RequestMessage{
			ID: id, Created: time.Now().Unix(), Deadline: deadline,
			Payload: json.RawMessage(`{"model":"test","prompt":"` + id + `"}`),
		}))
	}
	submit("cancelled")
	require.NoError(t, prod.CancelRequests(ctx, []string{"cancelled"}))
	for _, id := range []string{"a", "b", "c", "p1", "p2"} {
		submit(id)
	}
	depth, err := prod.RequestQueueDepth(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(6), depth)

	workerCtx, workerCancel := context.WithCancel(ctx)
	flowCtx, flowCancel := context.WithCancel(ctx)
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 2}}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)
	var workerWG sync.WaitGroup
	for range 2 {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			asyncworker.WorkerWithGate(workerCtx, workerCtx, pipeline.Characteristics{},
				asyncworker.NewHTTPInferenceClient(inference.Client()), dispatch.Channels["default"],
				flow.RetryChannel(), flow.ResultChannel(), time.Minute, nil, nil)
		}()
	}
	flow.Start(flowCtx)
	t.Cleanup(func() {
		flow.StopConsuming()
		workerCancel()
		flowCancel()
		workerWG.Wait()
		flow.Shutdown()
	})

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 20*time.Second)
	defer receiveCancel()
	got := map[string]*api.ResultMessage{}
	for len(got) < 6 {
		delivery, err := prod.ReceiveResult(receiveCtx)
		require.NoError(t, err, "received so far: %v", got)
		got[delivery.Result.ID] = delivery.Result
		require.NoError(t, prod.AckResult(ctx, delivery))
	}
	for _, id := range []string{"a", "b", "c", "p1", "p2"} {
		require.Contains(t, got, id)
		assert.Empty(t, got[id].ErrorCode, "request %s should have succeeded", id)
		assert.NotEmpty(t, got[id].Routing.RequestToken)
	}
	assert.Equal(t, api.ErrCodeCancelled, got["cancelled"].ErrorCode)
	assert.Equal(t, int64(5), hits.Load(), "the cancelled request must not reach inference")
	for _, id := range []string{"p1", "p2"} {
		n, err := rdb.Exists(ctx, api.RequestPayloadKey(id, got[id].Routing.RequestToken)).Result()
		require.NoError(t, err)
		assert.Zero(t, n, "payload key of %s should be deleted with its result", id)
	}

	// Everything the cycle created must be gone: no pending work, no live
	// claims on either side, and the per-request markers cleaned up.
	waitUntil(t, 5*time.Second, func() bool {
		n, _ := rdb.HLen(ctx, "{"+queue+"}:claimed").Result()
		return n == 0
	})
	for _, key := range []string{queue, "{" + queue + "}:claim-owners", "{" + queue + "}:claims-idx",
		resultList, "{" + resultList + "}:result-claimed", "{" + resultList + "}:result-claim-owners"} {
		n, err := rdb.Exists(ctx, key).Result()
		require.NoError(t, err)
		assert.Zero(t, n, "key %s should be gone", key)
	}
	// One EXISTS per key: the two markers sit in different slots, and a
	// multi-key EXISTS across slots is a CROSSSLOT error, not a zero.
	for _, id := range []string{"a", "b", "c", "p1", "p2", "cancelled"} {
		for _, key := range []string{api.RequestActiveTokenKey(id), api.RequestCancellationKey(id)} {
			waitUntil(t, 5*time.Second, func() bool {
				n, err := rdb.Exists(ctx, key).Result()
				return err == nil && n == 0
			})
		}
	}
	noResultCtx, noResultCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer noResultCancel()
	extra, err := prod.ReceiveResult(noResultCtx)
	assert.Nil(t, extra, "no duplicate results")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
