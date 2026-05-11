//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedisPubSub_PublishSubscribeResultDelivery validates that a message
// published on a Redis channel is received by a subscriber and that results
// can be written back to a result list. This uses an in-process miniredis
// server — no external Redis instance is required.
func TestRedisPubSub_PublishSubscribeResultDelivery(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const requestQueue = "integration-request-queue"
	const resultQueue = "integration-result-list"

	// Subscribe before publishing so the subscriber is ready.
	sub := rdb.Subscribe(ctx, requestQueue)
	defer sub.Close()
	ch := sub.Channel()

	// Build and publish a request message.
	ir := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: requestQueue},
		&api.RequestMessage{
			ID:       "pubsub-test-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test-model", "prompt": "hello"},
		},
	)
	irBytes, err := json.Marshal(ir)
	require.NoError(t, err)

	err = rdb.Publish(ctx, requestQueue, string(irBytes)).Err()
	require.NoError(t, err)

	// Receive the published message on the subscriber.
	select {
	case msg := <-ch:
		var received api.InternalRequest
		err := json.Unmarshal([]byte(msg.Payload), &received)
		require.NoError(t, err)
		assert.Equal(t, "pubsub-test-1", received.PublicRequest.ReqID())
		assert.Equal(t, requestQueue, received.RequestQueueName)
	case <-ctx.Done():
		t.Fatal("Timed out waiting for published message")
	}

	// Simulate writing a result to a Redis list (analogous to result worker).
	result := api.ResultMessage{ID: "pubsub-test-1", Payload: `{"text":"world"}`}
	resultBytes, err := json.Marshal(result)
	require.NoError(t, err)
	err = rdb.RPush(ctx, resultQueue, string(resultBytes)).Err()
	require.NoError(t, err)

	// Verify the result was written.
	vals, err := rdb.LRange(ctx, resultQueue, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, vals, 1)
	var readBack api.ResultMessage
	err = json.Unmarshal([]byte(vals[0]), &readBack)
	require.NoError(t, err)
	assert.Equal(t, "pubsub-test-1", readBack.ID)
	assert.Equal(t, `{"text":"world"}`, readBack.Payload)
}

// TestRedisSortedSet_EnqueueDequeueRetryRoundTrip validates the sorted-set
// retry flow: a message is added to a sorted set with a future score (backoff),
// is not retrievable before its due time, and becomes retrievable after.
func TestRedisSortedSet_EnqueueDequeueRetryRoundTrip(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	const retrySet = "integration-retry-sortedset"

	ir := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: "test-queue", RetryCount: 1},
		&api.RequestMessage{
			ID:       "retry-roundtrip-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Minute).Unix(),
			Payload:  map[string]any{"model": "test"},
		},
	)
	irBytes, err := json.Marshal(ir)
	require.NoError(t, err)

	// Schedule for 2 seconds in the future.
	dueTime := float64(time.Now().Unix()) + 2
	err = rdb.ZAdd(ctx, retrySet, redis.Z{Score: dueTime, Member: string(irBytes)}).Err()
	require.NoError(t, err)

	// Should not be retrievable before the due time.
	results, err := rdb.ZRangeByScore(ctx, retrySet, &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Unix()),
	}).Result()
	require.NoError(t, err)
	assert.Empty(t, results, "Message should not be due yet")

	// Fast-forward miniredis time.
	s.FastForward(3 * time.Second)

	// Now it should be retrievable.
	results, err = rdb.ZRangeByScore(ctx, retrySet, &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", time.Now().Unix()+3),
	}).Result()
	require.NoError(t, err)
	require.Len(t, results, 1)

	var received api.InternalRequest
	err = json.Unmarshal([]byte(results[0]), &received)
	require.NoError(t, err)
	assert.Equal(t, "retry-roundtrip-1", received.PublicRequest.ReqID())
	assert.Equal(t, 1, received.RetryCount)
}
