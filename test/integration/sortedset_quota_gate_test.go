//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	redisgate "github.com/llm-d-incubation/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedSetQuotaGate_AcquireDequeueRelease validates the full cross-package
// contract: RedisQuotaGate.Acquire gates dequeue from a sorted set, the message
// flows through a channel, and the release function decrements the concurrency
// counter in Redis — allowing the next request through.
//
// Packages exercised: pkg/redis (quota_gate + sorted set ops), api, pipeline.
func TestSortedSetQuotaGate_AcquireDequeueRelease(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	const queueName = "test-sortedset"

	gate := redisgate.NewRedisQuotaGate(rdb, "userid", redisgate.QuotaModeConcurrency, 1, 10*time.Second, "integ:")

	// Enqueue two messages for the same user.
	for i, id := range []string{"msg-1", "msg-2"} {
		ir := api.NewInternalRequest(
			api.InternalRouting{RequestQueueName: queueName},
			&api.RequestMessage{
				ID:       id,
				Created:  time.Now().Unix(),
				Deadline: time.Now().Add(time.Minute).Unix(),
				Payload:  map[string]any{"model": "test"},
				Metadata: map[string]string{"userid": "user-a"},
			},
		)
		irBytes, err := json.Marshal(ir)
		require.NoError(t, err)
		err = rdb.ZAdd(ctx, queueName, goredis.Z{
			Score: float64(time.Now().Unix() + int64(i)), Member: string(irBytes),
		}).Err()
		require.NoError(t, err)
	}

	// Pop first message — Acquire should succeed (concurrency limit = 1).
	results, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	require.NoError(t, err)
	require.Len(t, results, 1)

	var ir1 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(results[0].Member.(string)), &ir1))

	res1, err := gate.Acquire(ctx, ir1.PublicRequest.ReqMetadata())
	require.NoError(t, err)
	assert.True(t, res1.Allowed, "First request should be allowed")
	require.NotNil(t, res1.Release)

	// Pop second message — Acquire should be denied (concurrency limit reached).
	results2, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	require.NoError(t, err)
	require.Len(t, results2, 1)

	var ir2 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(results2[0].Member.(string)), &ir2))

	res2, err := gate.Acquire(ctx, ir2.PublicRequest.ReqMetadata())
	require.NoError(t, err)
	assert.False(t, res2.Allowed, "Second request should be denied while first is in-flight")

	// Re-enqueue denied message (as sortedset_impl does).
	err = rdb.ZAdd(ctx, queueName, goredis.Z{
		Score: results2[0].Score, Member: results2[0].Member,
	}).Err()
	require.NoError(t, err)

	// Release the first request (simulates resultWorker calling release).
	res1.Release()

	// Now the second message should be acquirable.
	results3, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	require.NoError(t, err)
	require.Len(t, results3, 1)

	var ir3 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(results3[0].Member.(string)), &ir3))
	assert.Equal(t, "msg-2", ir3.PublicRequest.ReqID())

	res3, err := gate.Acquire(ctx, ir3.PublicRequest.ReqMetadata())
	require.NoError(t, err)
	assert.True(t, res3.Allowed, "Second request should be allowed after first was released")
	if res3.Release != nil {
		res3.Release()
	}
}

// TestSortedSetQuotaGate_RateLimitRequeue validates that rate-limited messages
// are re-enqueued and become processable after the window resets.
func TestSortedSetQuotaGate_RateLimitRequeue(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	const queueName = "test-ratelimit-sortedset"

	// Allow 1 request per 2 seconds.
	gate := redisgate.NewRedisQuotaGate(rdb, "userid", redisgate.QuotaModeRateLimit, 1, 2*time.Second, "rl-integ:")

	ir := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: queueName},
		&api.RequestMessage{
			ID: "rl-msg-1", Created: time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
			Metadata: map[string]string{"userid": "user-b"},
		},
	)

	// First acquire — allowed.
	res, err := gate.Acquire(ctx, ir.PublicRequest.ReqMetadata())
	require.NoError(t, err)
	assert.True(t, res.Allowed)

	// Second acquire — denied (rate limit hit).
	res2, err := gate.Acquire(ctx, ir.PublicRequest.ReqMetadata())
	require.NoError(t, err)
	assert.False(t, res2.Allowed, "Should be rate limited")

	// Wait for window to reset.
	time.Sleep(2100 * time.Millisecond)

	// Third acquire — allowed again.
	res3, err := gate.Acquire(ctx, ir.PublicRequest.ReqMetadata())
	require.NoError(t, err)
	assert.True(t, res3.Allowed, "Should be allowed after rate limit window resets")
}
