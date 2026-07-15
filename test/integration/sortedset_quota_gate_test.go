//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	redisgate "github.com/llm-d/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedSetQuotaGate_AcquireDequeueRelease validates the full cross-package
// contract: RedisQuotaGate.Apply gates dequeue from a sorted set, the message
// flows through a channel, and the release function decrements the concurrency
// counter in Redis — allowing the next request through.
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

	// Pop first message — Apply should succeed (concurrency limit = 1).
	results, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	require.NoError(t, err)
	require.Len(t, results, 1)

	var ir1 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(results[0].Member.(string)), &ir1))

	var releases1 []pipeline.GateReleaseFunc
	verdict1, err := gate.Apply(ctx, &ir1, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict1.Action, "First request should be allowed")
	assert.NotEmpty(t, releases1)

	// Pop second message — Apply should be denied (concurrency limit reached).
	results2, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	require.NoError(t, err)
	require.Len(t, results2, 1)

	var ir2 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(results2[0].Member.(string)), &ir2))

	var releases2 []pipeline.GateReleaseFunc
	verdict2, err := gate.Apply(ctx, &ir2, &releases2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict2.Action, "Second request should be denied while first is in-flight")

	// Re-enqueue denied message (as sortedset_impl does).
	err = rdb.ZAdd(ctx, queueName, goredis.Z{
		Score: results2[0].Score, Member: results2[0].Member,
	}).Err()
	require.NoError(t, err)

	// Release the first request (simulates resultWorker calling release).
	for _, f := range releases1 {
		f()
	}

	// Now the second message should be acquirable.
	results3, err := rdb.ZPopMin(ctx, queueName, 1).Result()
	require.NoError(t, err)
	require.Len(t, results3, 1)

	var ir3 api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(results3[0].Member.(string)), &ir3))
	assert.Equal(t, "msg-2", ir3.PublicRequest.ReqID())

	var releases3 []pipeline.GateReleaseFunc
	verdict3, err := gate.Apply(ctx, &ir3, &releases3)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict3.Action, "Second request should be allowed after first was released")
	for _, f := range releases3 {
		f()
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

	// First Apply — allowed.
	var releases1 []pipeline.GateReleaseFunc
	verdict1, err := gate.Apply(ctx, ir, &releases1)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict1.Action)

	// Second Apply — denied (rate limit hit).
	ir2 := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: queueName},
		&api.RequestMessage{
			ID: "rl-msg-2", Created: time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
			Metadata: map[string]string{"userid": "user-b"},
		},
	)
	var releases2 []pipeline.GateReleaseFunc
	verdict2, err := gate.Apply(ctx, ir2, &releases2)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict2.Action, "Should be rate limited")

	// Wait for window to reset.
	time.Sleep(2100 * time.Millisecond)

	// Third Apply — allowed again.
	var releases3 []pipeline.GateReleaseFunc
	verdict3, err := gate.Apply(ctx, ir2, &releases3)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict3.Action, "Should be allowed after rate limit window resets")
}
