package redis

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/redis/go-redis/v9"
)

// noopGate returns a gate that always returns full budget (1.0)
func noopGate() flowcontrol.DispatchGate {
	return flowcontrol.ConstOpenGate()
}

// Test helper to create test flow and Redis
func setupTest(t *testing.T) (*miniredis.Miniredis, *redis.Client, context.Context, context.CancelFunc) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	return s, rdb, ctx, cancel
}

func TestSortedSetFlow_MessageProcessing(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "test-queue"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   api.RequestChannel{Channel: make(chan api.RequestMessage)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		Id:              "msg-1",
		DeadlineUnixSec: "9999999999",
		Payload:         map[string]any{"test": "data"},
	}
	msgBytes, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue)

	select {
	case received := <-flow.requestChannels[0].channel.Channel:
		if received.Id != "msg-1" {
			t.Errorf("Expected msg-1, got %s", received.Id)
		}
		if received.Metadata[SORTEDSET_QUEUE_NAME_KEY] != queue {
			t.Error("Queue name not set in metadata")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Verify queue is empty
	if count, _ := rdb.ZCard(ctx, queue).Result(); count != 0 {
		t.Errorf("Expected empty queue, got %d messages", count)
	}
}

func TestSortedSetFlow_DeadlineOrdering(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "priority-queue"
	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	now := time.Now().Unix()
	messages := []struct {
		id       string
		deadline int64
	}{
		{"low", now + 1000},
		{"high", now + 100},
		{"urgent", now + 50},
	}

	for _, m := range messages {
		msg := api.RequestMessage{Id: m.id, DeadlineUnixSec: strconv.FormatInt(m.deadline, 10)}
		msgBytes, _ := json.Marshal(msg)
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(m.deadline), Member: string(msgBytes)})
	}

	msgChannel := make(chan api.RequestMessage, 10)
	go flow.requestWorker(ctx, msgChannel, queue)

	var processed []string
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgChannel:
			processed = append(processed, msg.Id)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout")
		}
	}

	expected := []string{"urgent", "high", "low"}
	for i, id := range expected {
		if processed[i] != id {
			t.Errorf("Position %d: expected %s, got %s", i, id, processed[i])
		}
	}
}

func TestSortedSetFlow_ExpiredMessages(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "expired-queue"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   api.RequestChannel{Channel: make(chan api.RequestMessage)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	pastDeadline := time.Now().Unix() - 100
	msg := api.RequestMessage{Id: "expired", DeadlineUnixSec: strconv.FormatInt(pastDeadline, 10)}
	msgBytes, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(pastDeadline), Member: string(msgBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue)

	select {
	case msg := <-flow.requestChannels[0].channel.Channel:
		t.Fatalf("Should not receive expired message: %s", msg.Id)
	case <-time.After(300 * time.Millisecond):
		// Expected - message expired
	}

	// Verify message was removed
	if count, _ := rdb.ZCard(ctx, queue).Result(); count != 0 {
		t.Errorf("Expired message not removed, count=%d", count)
	}
}

func TestSortedSetFlow_MalformedMessages(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "malformed-queue"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   api.RequestChannel{Channel: make(chan api.RequestMessage)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	testCases := []struct {
		name   string
		member string
	}{
		{"invalid-json", `{invalid json`},
		{"missing-deadline", `{"id":"test","payload":{}}`},
		{"invalid-deadline", `{"id":"test","deadline":"not-a-number","payload":{}}`},
	}

	for _, tc := range testCases {
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: tc.member})
	}

	// Add valid message after malformed ones
	validMsg := api.RequestMessage{Id: "valid", DeadlineUnixSec: "9999999999"}
	validBytes, _ := json.Marshal(validMsg)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(validBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue)

	// Should skip malformed and receive valid message
	select {
	case msg := <-flow.requestChannels[0].channel.Channel:
		if msg.Id != "valid" {
			t.Errorf("Expected valid message, got %s", msg.Id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout - malformed messages might be blocking")
	}
}

func TestSortedSetFlow_RetryBackoff(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "retry-queue"
	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		retryChannel: make(chan api.RetryMessage, 1),
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	go flow.retryWorker(ctx)

	retryMsg := api.RetryMessage{
		EmbelishedRequestMessage: api.EmbelishedRequestMessage{
			RequestMessage: api.RequestMessage{
				Id:              "retry-1",
				DeadlineUnixSec: "9999999999",
				RetryCount:      1,
			},
			Metadata: map[string]string{SORTEDSET_QUEUE_NAME_KEY: queue},
		},
		BackoffDurationSeconds: 2.0,
	}

	flow.retryChannel <- retryMsg
	time.Sleep(100 * time.Millisecond)

	results, _ := rdb.ZRangeWithScores(ctx, queue, 0, -1).Result()
	if len(results) != 1 {
		t.Fatalf("Expected 1 retry message, got %d", len(results))
	}

	expectedScore := float64(time.Now().Unix()) + 2.0
	if results[0].Score < expectedScore-1 || results[0].Score > expectedScore+1 {
		t.Errorf("Retry score incorrect: expected ~%f, got %f", expectedScore, results[0].Score)
	}
}

func TestSortedSetFlow_ResultFIFO(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "result-queue"
	origQueue := *ssResultQueueName
	*ssResultQueueName = queue
	defer func() { *ssResultQueueName = origQueue }()

	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		resultChannel: make(chan api.ResultMessage, 2),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
		gate:          noopGate(),
	}

	go flow.resultWorker(ctx)

	flow.resultChannel <- api.ResultMessage{Id: "first", Payload: "result1"}
	flow.resultChannel <- api.ResultMessage{Id: "second", Payload: "result2"}
	time.Sleep(100 * time.Millisecond)

	// RPOP should get FIFO order
	first, _ := rdb.RPop(ctx, queue).Result()
	second, _ := rdb.RPop(ctx, queue).Result()

	var msg1, msg2 api.ResultMessage
	json.Unmarshal([]byte(first), &msg1)  // nolint:errcheck
	json.Unmarshal([]byte(second), &msg2) // nolint:errcheck

	if msg1.Id != "first" || msg2.Id != "second" {
		t.Errorf("FIFO order broken: got %s, %s", msg1.Id, msg2.Id)
	}
}

func TestSortedSetFlow_NoRaceCondition(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "race-queue"
	numMessages := 20

	for i := 0; i < numMessages; i++ {
		msg := api.RequestMessage{Id: string(rune('A' + i)), DeadlineUnixSec: "9999999999"}
		msgBytes, _ := json.Marshal(msg)
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})
	}

	var wg sync.WaitGroup
	processed := make(chan string, numMessages*2)

	for w := 0; w < 3; w++ {
		wg.Add(1)
		flow := &RedisSortedSetFlow{rdb: rdb, pollInterval: 20 * time.Millisecond, batchSize: 10, gate: noopGate()}
		msgChan := make(chan api.RequestMessage, 10)

		go func() {
			defer wg.Done()
			workerCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			go flow.requestWorker(workerCtx, msgChan, queue)
			for {
				select {
				case msg := <-msgChan:
					processed <- msg.Id
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()
	close(processed)

	seen := make(map[string]int)
	for id := range processed {
		seen[id]++
	}

	for id, count := range seen {
		if count > 1 {
			t.Errorf("Duplicate processing: %s processed %d times", id, count)
		}
	}

	if len(seen) != numMessages {
		t.Errorf("Expected %d unique messages, got %d", numMessages, len(seen))
	}
}

func TestSortedSetFlow_ContextCancellation(t *testing.T) {
	s, rdb, ctx, _ := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck

	queue := "cancel-queue"
	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		retryChannel:  make(chan api.RetryMessage),
		resultChannel: make(chan api.ResultMessage),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
		gate:          noopGate(),
	}

	workerCtx, cancel := context.WithCancel(ctx)
	msgChan := make(chan api.RequestMessage)

	done := make(chan bool)
	go func() {
		flow.requestWorker(workerCtx, msgChan, queue)
		done <- true
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Worker stopped gracefully
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Worker did not stop after context cancellation")
	}
}

func TestSortedSetFlow_Integration(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "integration-queue"
	origQueue := *ssRequestQueueName
	*ssRequestQueueName = queue
	defer func() { *ssRequestQueueName = origQueue }()

	flow := NewRedisSortedSetFlow()
	flow.rdb = rdb // Override with test Redis
	flow.pollInterval = 50 * time.Millisecond

	flow.Start(ctx)

	// Add message
	msg := api.RequestMessage{Id: "integration", DeadlineUnixSec: "9999999999"}
	msgBytes, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})

	// Should be received on first request channel
	select {
	case received := <-flow.RequestChannels()[0].Channel:
		if received.Id != "integration" {
			t.Errorf("Expected integration, got %s", received.Id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Integration test timeout")
	}
}

func TestSortedSetFlow_ZeroBudget(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "zero-budget-queue"

	// Create a gate with zero budget initially
	var budgetValue atomic.Uint64 // Store as bits to represent float64

	budgetValue.Store(math.Float64bits(0.0))
	gate := flowcontrol.DispatchGateFunc(func(ctx context.Context) float64 {
		return math.Float64frombits(budgetValue.Load())
	})

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   api.RequestChannel{Channel: make(chan api.RequestMessage)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         gate,
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		Id:              "test-zero-budget",
		DeadlineUnixSec: "9999999999",
		Payload:         map[string]any{"test": "data"},
	}
	msgBytes, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue)

	// Wait for several poll cycles - message should NOT be pulled (budget=0)
	select {
	case <-flow.requestChannels[0].channel.Channel:
		t.Fatal("Should not receive message when budget is 0")
	case <-time.After(200 * time.Millisecond):
		// Expected - no message pulled
	}

	// Message should still be in Redis
	count, _ := rdb.ZCard(ctx, queue).Result()
	if count != 1 {
		t.Errorf("Expected message to remain in Redis with budget=0, got count=%d", count)
	}

	// Increase budget to full capacity
	budgetValue.Store(math.Float64bits(1.0))

	// Message should now be pulled
	select {
	case received := <-flow.requestChannels[0].channel.Channel:
		if received.Id != "test-zero-budget" {
			t.Errorf("Expected test-zero-budget, got %s", received.Id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message should be pulled after budget increased")
	}
}

func TestSortedSetFlow_PartialBudget(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "partial-budget-queue"

	// Gate with 30% budget - should process floor(10*0.3)=3 messages per cycle
	gate := flowcontrol.DispatchGateFunc(func(ctx context.Context) float64 {
		return 0.3
	})

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   api.RequestChannel{Channel: make(chan api.RequestMessage, 20)},
			queueName: queue,
		}},
		pollInterval: 200 * time.Millisecond,
		batchSize:    10,
		gate:         gate,
	}

	// Add 10 messages
	for i := 0; i < 10; i++ {
		msg := api.RequestMessage{
			Id:              "msg-" + strconv.Itoa(i),
			DeadlineUnixSec: "9999999999",
		}
		msgBytes, _ := json.Marshal(msg)
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix() + int64(i)), Member: string(msgBytes)})
	}

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue)

	// Wait for one poll cycle (200ms interval + buffer)
	time.Sleep(250 * time.Millisecond)

	// With 30% budget and batchSize=10, floor(10*0.3)=3 messages should be processed per cycle
	// After one cycle, 7 messages should remain
	remaining, _ := rdb.ZCard(ctx, queue).Result()
	if remaining != 7 {
		t.Errorf("Expected 7 messages remaining with 30%% budget (3 pulled), got %d remaining", remaining)
	}
}

func TestSortedSetFlow_WithDispatchGateOption(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "option-test-queue"
	origQueue := *ssRequestQueueName
	*ssRequestQueueName = queue
	defer func() { *ssRequestQueueName = origQueue }()

	// Use WithDispatchGate option
	gate := flowcontrol.ConstOpenGate()

	flow := NewRedisSortedSetFlow(WithDispatchGate(gate))
	flow.rdb = rdb
	flow.pollInterval = 50 * time.Millisecond

	flow.Start(ctx)

	// Add message
	msg := api.RequestMessage{Id: "option-test", DeadlineUnixSec: "9999999999"}
	msgBytes, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})

	select {
	case received := <-flow.RequestChannels()[0].Channel:
		if received.Id != "option-test" {
			t.Errorf("Expected option-test, got %s", received.Id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for message with WithDispatchGate option")
	}
}
