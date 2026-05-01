package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

func newTestMQFlow(rdb *redis.Client) *RedisMQFlow {
	return &RedisMQFlow{
		rdb:           rdb,
		resultChannel: make(chan api.ResultMessage, resultChannelBuffer),
	}
}

func TestPubsubResultWorker_BatchPublish(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "result-pubsub-queue"
	flow := newTestMQFlow(rdb)

	// Subscribe so published messages are captured.
	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	// Pre-fill the channel with multiple results before starting the worker
	// so they are all available for a single batch drain.
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		flow.resultChannel <- api.ResultMessage{
			ID:      "msg-" + string(rune('A'+i)),
			Payload: "payload-" + string(rune('A'+i)),
		}
	}

	go flow.resultWorker(ctx, queue)

	received := make(map[string]bool)
	timeout := time.After(2 * time.Second)
	for len(received) < numMessages {
		select {
		case msg := <-pubsubCh:
			var rm api.ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			received[rm.ID] = true
		case <-timeout:
			t.Fatalf("Timeout: received only %d/%d messages", len(received), numMessages)
		}
	}

	for i := 0; i < numMessages; i++ {
		id := "msg-" + string(rune('A'+i))
		if !received[id] {
			t.Errorf("Missing message %s", id)
		}
	}
}

func TestPubsubResultWorker_SingleMessage(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "result-single-queue"
	flow := newTestMQFlow(rdb)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	go flow.resultWorker(ctx, queue)

	// Send a single message — should be flushed immediately as a batch of 1.
	flow.resultChannel <- api.ResultMessage{ID: "solo", Payload: "data"}

	select {
	case msg := <-pubsubCh:
		var rm api.ResultMessage
		if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}
		if rm.ID != "solo" {
			t.Errorf("Expected id 'solo', got %s", rm.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for single message")
	}
}

func TestMarshalResultMessage_Fallback(t *testing.T) {
	// A normal message should marshal fine.
	msg := api.ResultMessage{ID: "ok", Payload: "data"}
	result := marshalResultMessage(msg)

	var rm api.ResultMessage
	if err := json.Unmarshal([]byte(result), &rm); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}
	if rm.ID != "ok" {
		t.Errorf("Expected id 'ok', got %s", rm.ID)
	}
}

func TestPubsubResultWorker_ContextCancellation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	flow := newTestMQFlow(rdb)

	done := make(chan bool)
	go func() {
		flow.resultWorker(ctx, "cancel-queue")
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

func TestPubsubResultWorker_BatchSizeCap(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "batch-cap-queue"
	flow := newTestMQFlow(rdb)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	// Send more than maxBatchSize messages. The worker should still
	// deliver all of them across multiple pipeline flushes.
	totalMessages := maxBatchSize + 10
	for i := 0; i < totalMessages; i++ {
		flow.resultChannel <- api.ResultMessage{
			ID:      "cap-" + strconv.Itoa(i),
			Payload: "data",
		}
	}

	go flow.resultWorker(ctx, queue)

	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < totalMessages {
		select {
		case msg := <-pubsubCh:
			var rm api.ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			received[rm.ID] = true
		case <-timeout:
			t.Fatalf("Timeout: received only %d/%d messages", len(received), totalMessages)
		}
	}
}

func TestPubsubResultWorker_ConcurrentProducers(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "concurrent-queue"
	flow := newTestMQFlow(rdb)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	go flow.resultWorker(ctx, queue)

	// Simulate multiple inference workers sending results concurrently.
	numProducers := 8
	msgsPerProducer := 5
	totalMessages := numProducers * msgsPerProducer

	var wg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < msgsPerProducer; i++ {
				flow.resultChannel <- api.ResultMessage{
					ID:      "p" + strconv.Itoa(producerID) + "-" + strconv.Itoa(i),
					Payload: "data",
				}
			}
		}(p)
	}
	wg.Wait()

	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < totalMessages {
		select {
		case msg := <-pubsubCh:
			var rm api.ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}
			if received[rm.ID] {
				t.Errorf("Duplicate message: %s", rm.ID)
			}
			received[rm.ID] = true
		case <-timeout:
			t.Fatalf("Timeout: received only %d/%d messages", len(received), totalMessages)
		}
	}
}

func TestPubsubResultWorker_RetryAfterFailure(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queue := "retry-pubsub-queue"
	flow := newTestMQFlow(rdb)

	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	pubsubCh := sub.Channel()

	// Start worker, then inject error so first Exec fails.
	go flow.resultWorker(ctx, queue)
	time.Sleep(50 * time.Millisecond)

	s.SetError("READONLY simulated failure")
	flow.resultChannel <- api.ResultMessage{ID: "retry-msg", Payload: "data"}

	// Wait for the first attempt to fail.
	time.Sleep(150 * time.Millisecond)

	// Clear error so retry succeeds.
	s.SetError("")

	select {
	case msg := <-pubsubCh:
		var rm api.ResultMessage
		if err := json.Unmarshal([]byte(msg.Payload), &rm); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}
		if rm.ID != "retry-msg" {
			t.Errorf("Expected retry-msg, got %s", rm.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for retried message")
	}
}

func TestMQRetryWorker_RequeuesOnShutdown(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	retryQueue := *retryQueueName
	queueName := "req-queue"

	// Use a blocking (unbuffered) request channel so the worker blocks on send.
	reqCh := make(chan *api.InternalRequest)
	flow := &RedisMQFlow{
		rdb:           rdb,
		resultChannel: make(chan api.ResultMessage, resultChannelBuffer),
		retryChannel:  make(chan api.RetryMessage),
		requestChannels: []RequestChannelData{{
			requestChannel: api.RequestChannel{Channel: reqCh},
			queueName:      queueName,
		}},
	}

	// Seed the retry sorted set with 3 messages that are immediately due.
	now := time.Now().Unix()
	for i := 0; i < 3; i++ {
		ir := api.NewInternalRequest(
			api.InternalRouting{RequestQueueName: queueName},
			&api.RequestMessage{
				ID:       "retry-" + strconv.Itoa(i),
				Created:  now,
				Deadline: now + 3600,
			},
		)
		bytes, _ := json.Marshal(ir)
		rdb.ZAdd(ctx, retryQueue, redis.Z{Score: float64(time.Now().Unix() - 1), Member: string(bytes)})
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		flow.retryWorker(workerCtx, rdb)
		close(done)
	}()

	// Consume exactly one message so the worker can pop all three from Redis.
	select {
	case <-reqCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for first message")
	}

	// Cancel while the worker is blocked sending the second message.
	workerCancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryWorker did not stop after context cancellation")
	}

	// The remaining messages should have been requeued to the retry sorted set.
	count, err := rdb.ZCard(ctx, retryQueue).Result()
	if err != nil {
		t.Fatalf("ZCard error: %v", err)
	}
	if count == 0 {
		t.Fatal("Expected requeued retry messages, got 0")
	}
}

func TestPopDueRetryMessages_PopsDueAndRemovesFromSortedSet(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx := context.Background()
	queue := "retry-pop-test"
	now := time.Now().Unix()

	due := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: "request-queue"},
		&api.RequestMessage{ID: "due", Created: 1, Deadline: now + 60},
	)
	future := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: "request-queue"},
		&api.RequestMessage{ID: "future", Created: 1, Deadline: now + 120},
	)

	dueBytes, err := json.Marshal(due)
	if err != nil {
		t.Fatalf("marshal due message: %v", err)
	}
	futureBytes, err := json.Marshal(future)
	if err != nil {
		t.Fatalf("marshal future message: %v", err)
	}

	if err := rdb.ZAdd(ctx, queue,
		redis.Z{Score: float64(now - 1), Member: string(dueBytes)},
		redis.Z{Score: float64(now + 60), Member: string(futureBytes)},
	).Err(); err != nil {
		t.Fatalf("seed retry sorted set: %v", err)
	}

	items, err := popDueRetryMessages(ctx, rdb, queue, now, 10)
	if err != nil {
		t.Fatalf("pop due messages: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected exactly one popped message, got %d", len(items))
	}

	var popped api.InternalRequest
	if err := json.Unmarshal([]byte(items[0]), &popped); err != nil {
		t.Fatalf("unmarshal popped message: %v", err)
	}
	if popped.PublicRequest == nil || popped.PublicRequest.ReqID() != "due" {
		t.Fatalf("expected popped message id 'due', got %v", popped.PublicRequest)
	}

	remaining, err := rdb.ZCard(ctx, queue).Result()
	if err != nil {
		t.Fatalf("read remaining queue size: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected one remaining future message, got %d", remaining)
	}
}

func TestPopDueRetryMessages_ConcurrentCallers_NoDuplicatePops(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	ctx := context.Background()
	queue := "retry-pop-concurrent-test"
	now := time.Now().Unix()
	totalMessages := 40

	for i := 0; i < totalMessages; i++ {
		ir := api.NewInternalRequest(
			api.InternalRouting{RequestQueueName: "request-queue"},
			&api.RequestMessage{ID: "msg-" + strconv.Itoa(i), Created: 1, Deadline: now + 300},
		)
		msgBytes, err := json.Marshal(ir)
		if err != nil {
			t.Fatalf("marshal seed message %d: %v", i, err)
		}
		if err := rdb.ZAdd(ctx, queue, redis.Z{
			Score:  float64(now),
			Member: string(msgBytes),
		}).Err(); err != nil {
			t.Fatalf("seed retry queue: %v", err)
		}
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		seenID = make(map[string]int, totalMessages)
	)

	workerCount := 4
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				items, err := popDueRetryMessages(ctx, rdb, queue, now, 3)
				if err != nil {
					t.Errorf("pop due retry messages: %v", err)
					return
				}
				if len(items) == 0 {
					return
				}

				for _, raw := range items {
					var msg api.InternalRequest
					if err := json.Unmarshal([]byte(raw), &msg); err != nil {
						t.Errorf("unmarshal popped message: %v", err)
						return
					}
					if msg.PublicRequest == nil {
						t.Errorf("empty request")
						return
					}
					mu.Lock()
					seenID[msg.PublicRequest.ReqID()]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if len(seenID) != totalMessages {
		t.Fatalf("expected %d unique popped messages, got %d", totalMessages, len(seenID))
	}

	for id, count := range seenID {
		if count != 1 {
			t.Fatalf("message %s popped %d times, expected exactly once", id, count)
		}
	}

	remaining, err := rdb.ZCard(ctx, queue).Result()
	if err != nil {
		t.Fatalf("read remaining queue size: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected queue to be empty after concurrent pops, got %d", remaining)
	}
}
