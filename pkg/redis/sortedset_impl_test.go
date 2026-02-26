package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/redis/go-redis/v9"
)

func TestSortedSetFlow_BasicMessageFlow(t *testing.T) {
	// Start miniredis server
	s := miniredis.RunT(t)
	defer s.Close()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create flow with custom settings
	requestQueue := "test-request-queue"

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []SortedSetRequestChannelData{
			{
				requestChannel: api.RequestChannel{
					Channel:            make(chan api.RequestMessage),
					InferenceObjective: "test-objective",
					RequestPathURL:     "/v1/completions",
				},
				queueName: requestQueue,
			},
		},
		retryChannel:  make(chan api.RetryMessage),
		resultChannel: make(chan api.ResultMessage),
		pollInterval:  100 * time.Millisecond,
	}

	// Add a test message to the sorted set
	testMsg := api.RequestMessage{
		Id:              "test-msg-1",
		DeadlineUnixSec: "9999999999",
		Payload: map[string]any{
			"model":  "test-model",
			"prompt": "Hello, world!",
		},
	}

	msgBytes, err := json.Marshal(testMsg)
	if err != nil {
		t.Fatalf("Failed to marshal test message: %v", err)
	}

	// Add to sorted set with current time as score (ready to process)
	score := float64(time.Now().Unix())
	err = rdb.ZAdd(ctx, requestQueue, redis.Z{
		Score:  score,
		Member: string(msgBytes),
	}).Err()
	if err != nil {
		t.Fatalf("Failed to add message to sorted set: %v", err)
	}

	// Verify message was added
	count, err := rdb.ZCard(ctx, requestQueue).Result()
	if err != nil || count != 1 {
		t.Fatalf("Expected 1 message in sorted set, got %d", count)
	}

	// Start the flow
	go flow.sortedSetRequestWorker(ctx, flow.requestChannels[0].requestChannel.Channel, requestQueue)

	// Wait for message to be processed
	select {
	case msg := <-flow.requestChannels[0].requestChannel.Channel:
		if msg.Id != "test-msg-1" {
			t.Errorf("Expected message ID 'test-msg-1', got '%s'", msg.Id)
		}
		if msg.Metadata[SORTEDSET_QUEUE_NAME_KEY] != requestQueue {
			t.Errorf("Expected queue name '%s', got '%s'", requestQueue, msg.Metadata[SORTEDSET_QUEUE_NAME_KEY])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Verify message was removed from sorted set
	count, err = rdb.ZCard(ctx, requestQueue).Result()
	if err != nil || count != 0 {
		t.Fatalf("Expected 0 messages in sorted set after processing, got %d", count)
	}
}

func TestSortedSetFlow_RetryMechanism(t *testing.T) {
	// Start miniredis server
	s := miniredis.RunT(t)
	defer s.Close()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	requestQueue := "test-request-queue"

	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		retryChannel:  make(chan api.RetryMessage, 10), // Buffered channel
		resultChannel: make(chan api.ResultMessage),
		pollInterval:  100 * time.Millisecond,
	}

	// Start the retry worker
	go flow.sortedSetRetryWorker(ctx)

	// Give the goroutine time to start
	time.Sleep(100 * time.Millisecond)

	// Create a retry message
	retryMsg := api.RetryMessage{
		EmbelishedRequestMessage: api.EmbelishedRequestMessage{
			RequestMessage: api.RequestMessage{
				Id:              "retry-msg-1",
				DeadlineUnixSec: "9999999999",
				RetryCount:      1,
				Payload: map[string]any{
					"model":  "test-model",
					"prompt": "Retry this",
				},
				Metadata: map[string]string{
					SORTEDSET_QUEUE_NAME_KEY: requestQueue,
				},
			},
			RequestPathURL: "/v1/completions",
			Metadata: map[string]string{
				SORTEDSET_QUEUE_NAME_KEY: requestQueue,
			},
		},
		BackoffDurationSeconds: 5.0,
	}

	// Send retry message (non-blocking due to buffered channel)
	select {
	case flow.retryChannel <- retryMsg:
		t.Log("Retry message sent successfully")
	case <-time.After(1 * time.Second):
		t.Fatal("Failed to send retry message - channel blocked")
	}

	// Wait for processing with retries
	var count int64
	var err error
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		count, err = rdb.ZCard(ctx, requestQueue).Result()
		if err == nil && count == 1 {
			break
		}
	}

	// Verify message was added back to sorted set
	if err != nil {
		t.Fatalf("Failed to get sorted set count: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 message in sorted set after retry, got %d", count)
	}
	t.Logf("Successfully verified message in sorted set")

	// Verify the score is in the future (current time + backoff)
	results, err := rdb.ZRangeWithScores(ctx, requestQueue, 0, -1).Result()
	if err != nil {
		t.Fatalf("Failed to get sorted set entries: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(results))
	}

	expectedScore := float64(time.Now().Unix()) + 5.0
	actualScore := results[0].Score
	// Allow 2 second tolerance for test execution time
	if actualScore < expectedScore-2 || actualScore > expectedScore+2 {
		t.Errorf("Expected score around %f, got %f", expectedScore, actualScore)
	}

	// Verify the message content
	var msg api.RequestMessage
	err = json.Unmarshal([]byte(results[0].Member.(string)), &msg)
	if err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}
	if msg.Id != "retry-msg-1" {
		t.Errorf("Expected message ID 'retry-msg-1', got '%s'", msg.Id)
	}
	if msg.RetryCount != 1 {
		t.Errorf("Expected retry count 1, got %d", msg.RetryCount)
	}
}

func TestSortedSetFlow_ResultToList(t *testing.T) {
	// Start miniredis server
	s := miniredis.RunT(t)
	defer s.Close()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultQueue := "test-result-queue"

	// Temporarily override the flag value for testing
	originalResultQueueName := *ssResultQueueName
	*ssResultQueueName = resultQueue
	defer func() { *ssResultQueueName = originalResultQueueName }()

	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		retryChannel:  make(chan api.RetryMessage),
		resultChannel: make(chan api.ResultMessage),
		pollInterval:  100 * time.Millisecond,
	}

	// Start the result worker
	go flow.listResultWorker(ctx)

	// Send a result message
	resultMsg := api.ResultMessage{
		Id:      "result-msg-1",
		Payload: `{"response": "This is the result"}`,
		Metadata: map[string]string{
			"test": "metadata",
		},
	}

	flow.resultChannel <- resultMsg

	// Wait a bit for processing
	time.Sleep(200 * time.Millisecond)

	// Verify message was added to the list
	length, err := rdb.LLen(ctx, resultQueue).Result()
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}
	if length != 1 {
		t.Fatalf("Expected 1 message in result list, got %d", length)
	}

	// Pop from the right (FIFO - LPUSH + RPOP)
	msgStr, err := rdb.RPop(ctx, resultQueue).Result()
	if err != nil {
		t.Fatalf("Failed to pop message from list: %v", err)
	}

	// Verify the content
	var retrievedMsg api.ResultMessage
	err = json.Unmarshal([]byte(msgStr), &retrievedMsg)
	if err != nil {
		t.Fatalf("Failed to unmarshal result message: %v", err)
	}

	if retrievedMsg.Id != "result-msg-1" {
		t.Errorf("Expected result ID 'result-msg-1', got '%s'", retrievedMsg.Id)
	}
	if retrievedMsg.Payload != `{"response": "This is the result"}` {
		t.Errorf("Unexpected payload: %s", retrievedMsg.Payload)
	}
}

func TestSortedSetFlow_MessageNotReadyYet(t *testing.T) {
	// Start miniredis server
	s := miniredis.RunT(t)
	defer s.Close()

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	requestQueue := "test-request-queue"

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []SortedSetRequestChannelData{
			{
				requestChannel: api.RequestChannel{
					Channel:        make(chan api.RequestMessage),
					RequestPathURL: "/v1/completions",
				},
				queueName: requestQueue,
			},
		},
		retryChannel:  make(chan api.RetryMessage),
		resultChannel: make(chan api.ResultMessage),
		pollInterval:  100 * time.Millisecond,
	}

	// Add a test message with future score (not ready yet)
	testMsg := api.RequestMessage{
		Id:              "future-msg",
		DeadlineUnixSec: "9999999999",
		Payload: map[string]any{
			"model": "test-model",
		},
	}

	msgBytes, err := json.Marshal(testMsg)
	if err != nil {
		t.Fatalf("Failed to marshal test message: %v", err)
	}

	// Set score to 10 seconds in the future
	futureScore := float64(time.Now().Unix() + 10)
	err = rdb.ZAdd(ctx, requestQueue, redis.Z{
		Score:  futureScore,
		Member: string(msgBytes),
	}).Err()
	if err != nil {
		t.Fatalf("Failed to add message to sorted set: %v", err)
	}

	// Start the flow
	go flow.sortedSetRequestWorker(ctx, flow.requestChannels[0].requestChannel.Channel, requestQueue)

	// Wait and verify no message is received (it's not ready yet)
	select {
	case msg := <-flow.requestChannels[0].requestChannel.Channel:
		t.Fatalf("Should not have received message yet, but got: %s", msg.Id)
	case <-time.After(500 * time.Millisecond):
		// Expected - message should not be processed yet
	}

	// Verify message is still in sorted set
	count, err := rdb.ZCard(ctx, requestQueue).Result()
	if err != nil || count != 1 {
		t.Fatalf("Expected 1 message still in sorted set, got %d", count)
	}
}
