package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
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

	// Subscribe and wait for confirmation before publishing.
	sub := rdb.Subscribe(ctx, queue)
	defer sub.Close() // nolint:errcheck
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe confirmation: %v", err)
	}
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
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe confirmation: %v", err)
	}
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
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe confirmation: %v", err)
	}
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
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe confirmation: %v", err)
	}
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
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("Subscribe confirmation: %v", err)
	}
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

	retryQueue := "retry-sortedset"
	queueName := "req-queue"

	// Use a blocking (unbuffered) request channel so the worker blocks on send.
	reqCh := make(chan *api.InternalRequest)
	flow := &RedisMQFlow{
		rdb:            rdb,
		resultChannel:  make(chan api.ResultMessage, resultChannelBuffer),
		retryChannel:   make(chan pipeline.RetryMessage),
		retryQueueName: retryQueue,
		requestChannels: []RequestChannelData{{
			requestChannel: pipeline.RequestChannel{Channel: reqCh},
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

func TestRequestWorker_ReconnectsAfterChannelClose(t *testing.T) {
	s := miniredis.RunT(t)
	addr := s.Addr()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close() // nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueName := "reconnect-test-queue"
	msgChannel := make(chan *api.InternalRequest, 10)

	go requestWorker(ctx, rdb, msgChannel, queueName)

	time.Sleep(200 * time.Millisecond)

	// Restart miniredis on the same address to force subscription channel close and reconnect
	s.Close()
	s2 := miniredis.NewMiniRedis()
	if err := s2.StartAddr(addr); err != nil {
		t.Fatalf("failed to restart miniredis on same addr: %v", err)
	}
	defer s2.Close()

	// Wait for reconnectDelay (10s) + subscribe time
	time.Sleep(reconnectDelay + time.Second)

	now := time.Now().Unix()
	ir := api.NewInternalRequest(
		api.InternalRouting{},
		&api.RequestMessage{ID: "after-reconnect", Created: now, Deadline: now + 3600},
	)
	bytes, _ := json.Marshal(ir)
	rdb2 := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb2.Close() // nolint:errcheck
	rdb2.Publish(ctx, queueName, string(bytes))

	select {
	case msg := <-msgChannel:
		if msg.PublicRequest == nil || msg.PublicRequest.ReqID() != "after-reconnect" {
			t.Fatalf("expected after-reconnect, got %v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: requestWorker did not recover after reconnect")
	}
}

func TestRetryRedisOp_SuccessOnFirstAttempt(t *testing.T) {
	var calls atomic.Int32
	err := retryRedisOp(context.Background(), func(_ context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("expected 1 call, got %d", c)
	}
}

func TestRetryRedisOp_SuccessAfterRetry(t *testing.T) {
	var calls atomic.Int32
	err := retryRedisOp(context.Background(), func(_ context.Context) error {
		if calls.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if c := calls.Load(); c != 2 {
		t.Fatalf("expected 2 calls, got %d", c)
	}
}

func TestRetryRedisOp_AllRetriesExhausted(t *testing.T) {
	var calls atomic.Int32
	sentinel := errors.New("persistent")
	err := retryRedisOp(context.Background(), func(_ context.Context) error {
		calls.Add(1)
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if c := calls.Load(); c != int32(maxRetries) {
		t.Fatalf("expected %d calls, got %d", maxRetries, c)
	}
}

func TestRetryRedisOp_BackoffSkippedOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_ = retryRedisOp(ctx, func(_ context.Context) error {
		return errors.New("fail")
	})
	elapsed := time.Since(start)

	// Normal backoff would be 100ms + 200ms = 300ms between 3 attempts.
	// With a cancelled context the select falls through immediately.
	// Use a generous upper bound to avoid flakiness on slow CI runners.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected backoff to be skipped (elapsed %v), but it took too long", elapsed)
	}
}

func TestRetryRedisOp_UsesBackgroundCtxAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var received []context.Context
	_ = retryRedisOp(ctx, func(c context.Context) error {
		received = append(received, c)
		return errors.New("fail")
	})

	for i, c := range received {
		if c.Err() != nil {
			t.Errorf("attempt %d: expected non-cancelled context, got err=%v", i, c.Err())
		}
	}
}

// TestNextRetryPollInterval covers the pure backoff math for retryWorker.
// See #100.
func TestNextRetryPollInterval(t *testing.T) {
	base := 1 * time.Second
	max := 30 * time.Second

	tests := []struct {
		name    string
		current time.Duration
		want    time.Duration
	}{
		{"zero current snaps to base", 0, base},
		{"below base snaps to base", 100 * time.Millisecond, base},
		{"base doubles to 2x", base, 2 * time.Second},
		{"2s doubles to 4s", 2 * time.Second, 4 * time.Second},
		{"8s doubles to 16s (still below cap)", 8 * time.Second, 16 * time.Second},
		{"16s doubles to 30s (capped at max)", 16 * time.Second, 30 * time.Second},
		{"at-cap stays at cap", max, max},
		{"above cap clamps to cap", 60 * time.Second, max},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextRetryPollInterval(tt.current, base, max)
			if got != tt.want {
				t.Errorf("nextRetryPollInterval(%v, %v, %v) = %v, want %v",
					tt.current, base, max, got, tt.want)
			}
		})
	}
}

// TestMQRetryWorker_ExitsPromptlyOnCancelDuringSleep verifies that ctx
// cancellation interrupts the worker's sleep instead of waiting out the full
// interval. With maxRetryPollInterval overridden to several seconds and the
// retry queue empty (so the worker reaches the long sleep), cancelling the
// context should cause the worker to exit within ~100ms rather than after
// the full interval. Regression guard for the cancellable-sleep change in #100.
func TestMQRetryWorker_ExitsPromptlyOnCancelDuringSleep(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close() // nolint:errcheck

	// Override intervals: short base so the worker quickly reaches the long
	// max-clamped sleep, long max so an uncancellable sleep would visibly stall.
	oldBase, oldMax := baseRetryPollInterval, maxRetryPollInterval
	baseRetryPollInterval = 5 * time.Millisecond
	maxRetryPollInterval = 5 * time.Second
	defer func() {
		baseRetryPollInterval = oldBase
		maxRetryPollInterval = oldMax
	}()

	flow := &RedisMQFlow{
		rdb:             rdb,
		resultChannel:   make(chan api.ResultMessage, resultChannelBuffer),
		retryChannel:    make(chan pipeline.RetryMessage),
		requestChannels: []RequestChannelData{},
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		flow.retryWorker(workerCtx, rdb)
		close(done)
	}()

	// Let the worker run long enough to traverse the backoff curve and reach
	// the max-clamped sleep, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancelStart := time.Now()
	workerCancel()

	select {
	case <-done:
		exitLatency := time.Since(cancelStart)
		if exitLatency > 500*time.Millisecond {
			t.Errorf("retryWorker took %v to exit after cancel; want < 500ms (uncancellable sleep would have taken up to %v)",
				exitLatency, maxRetryPollInterval)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retryWorker did not exit within 2s of context cancellation")
	}
}

func TestNewRedisMQFlow_PoolRequiredAndValidation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	connOpts := ConnectionOptions{URL: "redis://" + s.Addr()}

	// Case 1: worker_pool_id is missing from configuration, and pool "default" does not exist
	opts := PubSubFlowOptions{QueuesConfig: `[{"queue_name":"test-queue","inference_objective":"obj","igw_base_url":"http://gw"}]`}
	_, err := NewRedisMQFlow(opts, connOpts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when worker_pool_id is missing and 'default' pool does not exist, got nil")
	}

	// Case 5: worker_pool_id is missing, but only a single 'default' pool is specified
	opts = PubSubFlowOptions{QueuesConfig: `[{"queue_name":"test-queue","inference_objective":"obj","igw_base_url":"http://gw"}]`}
	_, err = NewRedisMQFlow(opts, connOpts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id is missing but default pool exists: %v", err)
	}

	// Case 6: worker_pool_id is specified as custom, but only a single 'default' pool is specified
	opts = PubSubFlowOptions{QueuesConfig: `[{"queue_name":"test-queue","worker_pool_id":"custom-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`}
	_, err = NewRedisMQFlow(opts, connOpts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when worker_pool_id is custom but only default pool exists, got nil")
	}

	// Case 2: worker_pool_id is specified but pool does not exist
	opts = PubSubFlowOptions{QueuesConfig: `[{"queue_name":"test-queue","worker_pool_id":"non-existent","inference_objective":"obj","igw_base_url":"http://gw"}]`}
	_, err = NewRedisMQFlow(opts, connOpts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when specified worker_pool_id does not exist, got nil")
	}

	// Case 3: worker_pool_id specified and pool exists, but igw_base_url is missing
	opts = PubSubFlowOptions{QueuesConfig: `[{"queue_name":"test-queue","worker_pool_id":"test-pool","inference_objective":"obj"}]`}
	_, err = NewRedisMQFlow(opts, connOpts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when igw_base_url is missing in queue config, got nil")
	}

	// Case 4: worker_pool_id and igw_base_url specified and pool exists
	opts = PubSubFlowOptions{QueuesConfig: `[{"queue_name":"test-queue","worker_pool_id":"test-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`}
	_, err = NewRedisMQFlow(opts, connOpts, WithWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id exists: %v", err)
	}
}
