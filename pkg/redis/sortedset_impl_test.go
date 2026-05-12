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
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

// Test helper to create test flow and Redis
func setupTest(t *testing.T) (*miniredis.Miniredis, *redis.Client, context.Context, context.CancelFunc) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	return s, rdb, ctx, cancel
}

// envelopeJSON marshals a RequestMessage as the tagged envelope format.
func envelopeJSON(rm api.RequestMessage) string {
	ir := api.NewInternalRequest(api.InternalRouting{}, &rm)
	b, _ := json.Marshal(ir)
	return string(b)
}

func TestSortedSetFlow_MessageProcessing(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "test-queue"
	ch := make(chan *pipeline.EmbelishedRequestMessage)
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: ch},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		ID:       "msg-1",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  map[string]any{"test": "data"},
	}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, ch, queue)

	select {
	case received := <-ch:
		if received.PublicRequest == nil || received.PublicRequest.ReqID() != "msg-1" {
			t.Errorf("Expected msg-1, got %v", received.PublicRequest)
		}
		if received.RequestQueueName != queue {
			t.Error("Queue name not set in InternalRouting")
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
		msg := api.RequestMessage{ID: m.id, Created: time.Now().Unix(), Deadline: m.deadline}
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(m.deadline), Member: envelopeJSON(msg)})
	}

	msgChannel := make(chan *pipeline.EmbelishedRequestMessage, 10)
	go flow.requestWorker(ctx, msgChannel, queue)

	var processed []string
	for i := 0; i < 3; i++ {
		select {
		case msg := <-msgChannel:
			processed = append(processed, msg.PublicRequest.ReqID())
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
	ch := make(chan *pipeline.EmbelishedRequestMessage)
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: ch},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
	}

	pastDeadline := time.Now().Unix() - 100
	msg := api.RequestMessage{ID: "expired", Created: time.Now().Unix(), Deadline: pastDeadline}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(pastDeadline), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, ch, queue)

	select {
	case msg := <-ch:
		t.Fatalf("Should not receive expired message: %s", msg.PublicRequest.ReqID())
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
	ch := make(chan *pipeline.EmbelishedRequestMessage)
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: ch},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
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
	validMsg := api.RequestMessage{ID: "valid", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(validMsg)})

	go flow.requestWorker(ctx, ch, queue)

	// Should skip malformed and receive valid message
	select {
	case msg := <-ch:
		if msg.PublicRequest == nil || msg.PublicRequest.ReqID() != "valid" {
			t.Errorf("Expected valid message, got %v", msg.PublicRequest)
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
		retryChannel: make(chan pipeline.RetryMessage, 1),
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
	}

	go flow.retryWorker(ctx)

	retryMsg := pipeline.RetryMessage{
		EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
			InternalRequest: api.NewInternalRequest(
				api.InternalRouting{RetryCount: 1, RequestQueueName: queue},
				&api.RequestMessage{
					ID:       "retry-1",
					Created:  time.Now().Unix(),
					Deadline: 9999999999,
				},
			),
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
	}

	go flow.resultWorker(ctx)

	flow.resultChannel <- api.ResultMessage{ID: "first", Payload: "result1"}
	flow.resultChannel <- api.ResultMessage{ID: "second", Payload: "result2"}
	time.Sleep(100 * time.Millisecond)

	// RPOP should get FIFO order
	first, _ := rdb.RPop(ctx, queue).Result()
	second, _ := rdb.RPop(ctx, queue).Result()

	var msg1, msg2 api.ResultMessage
	json.Unmarshal([]byte(first), &msg1)  // nolint:errcheck
	json.Unmarshal([]byte(second), &msg2) // nolint:errcheck

	if msg1.ID != "first" || msg2.ID != "second" {
		t.Errorf("FIFO order broken: got %s, %s", msg1.ID, msg2.ID)
	}
}

func TestSortedSetFlow_ResultBatch(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "batch-result-queue"
	origQueue := *ssResultQueueName
	*ssResultQueueName = queue
	defer func() { *ssResultQueueName = origQueue }()

	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		resultChannel: make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
	}

	// Pre-fill the channel before starting the worker so all messages
	// are available for a single batch drain.
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		flow.resultChannel <- api.ResultMessage{
			ID:      "batch-" + strconv.Itoa(i),
			Payload: "data-" + strconv.Itoa(i),
		}
	}

	go flow.resultWorker(ctx)
	time.Sleep(200 * time.Millisecond)

	// All messages should be in Redis
	length, err := rdb.LLen(ctx, queue).Result()
	if err != nil {
		t.Fatalf("LLen error: %v", err)
	}
	if length != int64(numMessages) {
		t.Errorf("Expected %d results in Redis, got %d", numMessages, length)
	}

	// Verify FIFO order via RPOP
	for i := 0; i < numMessages; i++ {
		raw, _ := rdb.RPop(ctx, queue).Result()
		var msg api.ResultMessage
		json.Unmarshal([]byte(raw), &msg) // nolint:errcheck
		expected := "batch-" + strconv.Itoa(i)
		if msg.ID != expected {
			t.Errorf("Position %d: expected %s, got %s", i, expected, msg.ID)
		}
	}
}

func TestSortedSetFlow_ResultBatchMultiQueue(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	defaultQueue := "default-result-queue"
	customQueue := "custom-result-queue"
	origQueue := *ssResultQueueName
	*ssResultQueueName = defaultQueue
	defer func() { *ssResultQueueName = origQueue }()

	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		resultChannel: make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
	}

	// Send messages targeting different queues in a single batch.
	flow.resultChannel <- api.ResultMessage{ID: "default-1", Payload: "d1"}
	flow.resultChannel <- api.ResultMessage{
		ID:      "custom-1",
		Payload: "c1",
		Routing: api.InternalRouting{ResultQueueName: customQueue},
	}
	flow.resultChannel <- api.ResultMessage{ID: "default-2", Payload: "d2"}
	flow.resultChannel <- api.ResultMessage{
		ID:      "custom-2",
		Payload: "c2",
		Routing: api.InternalRouting{ResultQueueName: customQueue},
	}

	go flow.resultWorker(ctx)
	time.Sleep(200 * time.Millisecond)

	// Verify default queue
	defaultLen, _ := rdb.LLen(ctx, defaultQueue).Result()
	if defaultLen != 2 {
		t.Errorf("Expected 2 messages in default queue, got %d", defaultLen)
	}

	// Verify custom queue
	customLen, _ := rdb.LLen(ctx, customQueue).Result()
	if customLen != 2 {
		t.Errorf("Expected 2 messages in custom queue, got %d", customLen)
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
		msg := api.RequestMessage{ID: string(rune('A' + i)), Created: time.Now().Unix(), Deadline: 9999999999}
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})
	}

	var wg sync.WaitGroup
	processed := make(chan string, numMessages*2)

	for w := 0; w < 3; w++ {
		wg.Add(1)
		flow := &RedisSortedSetFlow{rdb: rdb, pollInterval: 20 * time.Millisecond, batchSize: 10}
		msgChan := make(chan *pipeline.EmbelishedRequestMessage, 10)

		go func() {
			defer wg.Done()
			workerCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			go flow.requestWorker(workerCtx, msgChan, queue)
			for {
				select {
				case msg := <-msgChan:
					processed <- msg.PublicRequest.ReqID()
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
		retryChannel:  make(chan pipeline.RetryMessage),
		resultChannel: make(chan api.ResultMessage),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
	}

	workerCtx, cancel := context.WithCancel(ctx)
	msgChan := make(chan *pipeline.EmbelishedRequestMessage)

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

	origURL := *RedisURL
	*RedisURL = "redis://" + s.Addr()
	defer func() { *RedisURL = origURL }()

	flow, err := NewRedisSortedSetFlow()
	if err != nil {
		t.Fatal(err)
	}
	flow.rdb = rdb
	flow.pollInterval = 50 * time.Millisecond

	flow.Start(ctx)

	// Add message
	msg := api.RequestMessage{ID: "integration", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	// Should be received on first request channel
	select {
	case received := <-flow.RequestChannels()[0].Channel:
		if received.PublicRequest == nil || received.PublicRequest.ReqID() != "integration" {
			t.Errorf("Expected integration, got %v", received.PublicRequest)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Integration test timeout")
	}
}

func TestSortedSetFlow_ResultRetryAfterFailure(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "retry-result-queue"
	origQueue := *ssResultQueueName
	*ssResultQueueName = queue
	defer func() { *ssResultQueueName = origQueue }()

	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		resultChannel: make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
	}

	// Inject an error so the first Exec fails.
	s.SetError("READONLY simulated failure")

	go flow.resultWorker(ctx)

	flow.resultChannel <- api.ResultMessage{ID: "retry-msg", Payload: "data"}

	// Wait long enough for the first attempt to fail.
	time.Sleep(150 * time.Millisecond)

	// No results should be in Redis yet.
	length, _ := rdb.LLen(ctx, queue).Result()
	if length != 0 {
		t.Fatalf("Expected 0 results while Redis is failing, got %d", length)
	}

	// Clear the error so subsequent retries succeed.
	s.SetError("")

	// Wait for retry to complete.
	time.Sleep(500 * time.Millisecond)

	length, _ = rdb.LLen(ctx, queue).Result()
	if length != 1 {
		t.Fatalf("Expected 1 result after retry, got %d", length)
	}

	raw, _ := rdb.RPop(ctx, queue).Result()
	var resultMsg api.ResultMessage
	json.Unmarshal([]byte(raw), &resultMsg) // nolint:errcheck
	if resultMsg.ID != "retry-msg" {
		t.Errorf("Expected retry-msg, got %s", resultMsg.ID)
	}
}

func TestSortedSetFlow_RetryWorkerDrainsOnShutdown(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "retry-drain-queue"
	const totalMessages = maxBatchSize + 10
	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		retryChannel: make(chan pipeline.RetryMessage, totalMessages),
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		flow.retryWorker(workerCtx)
		close(done)
	}()

	// Buffer more messages than maxBatchSize so the drain path
	// exercises multiple pipeline flushes.
	for i := 0; i < totalMessages; i++ {
		flow.retryChannel <- pipeline.RetryMessage{
			EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
				InternalRequest: api.NewInternalRequest(
					api.InternalRouting{RequestQueueName: queue},
					&api.RequestMessage{
						ID:       "drain-" + strconv.Itoa(i),
						Created:  time.Now().Unix(),
						Deadline: 9999999999,
					},
				),
			},
			BackoffDurationSeconds: 0,
		}
	}
	workerCancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryWorker did not stop after context cancellation")
	}

	count, err := rdb.ZCard(ctx, queue).Result()
	if err != nil {
		t.Fatalf("ZCard error: %v", err)
	}
	if int(count) != totalMessages {
		t.Fatalf("Expected %d retry messages flushed on shutdown, got %d", totalMessages, count)
	}
}

func TestSortedSetFlow_RetryBatchAfterFailure(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "retry-batch-queue"
	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		retryChannel: make(chan pipeline.RetryMessage, 10),
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
	}

	// Inject an error so the first pipeline flush fails.
	s.SetError("READONLY simulated failure")
	go flow.retryWorker(ctx)

	flow.retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
			InternalRequest: api.NewInternalRequest(
				api.InternalRouting{RequestQueueName: queue},
				&api.RequestMessage{
					ID:       "retry-batch-msg",
					Created:  time.Now().Unix(),
					Deadline: 9999999999,
				},
			),
		},
		BackoffDurationSeconds: 0,
	}

	// Keep Redis failing long enough for early attempts to fail.
	time.Sleep(150 * time.Millisecond)

	// Clear the error so the next retry attempt succeeds.
	s.SetError("")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		count, err := rdb.ZCard(ctx, queue).Result()
		if err == nil && count == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Expected retry message to be enqueued after transient Redis failure")
}

func TestSortedSetFlow_RequestWorkerRequeuesOnShutdown(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "requeue-shutdown-queue"
	// Unbuffered channel with no reader: the worker's channel send will block
	// indefinitely, so ctx.Done() is the only way to unblock the select.
	msgChan := make(chan *pipeline.EmbelishedRequestMessage)

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: msgChan},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
	}

	ir := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       "requeue-1",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  map[string]any{"key": "value"},
	})
	msgBytes, _ := json.Marshal(ir)
	score := float64(time.Now().Unix())
	rdb.ZAdd(ctx, queue, redis.Z{Score: score, Member: string(msgBytes)})

	workerCtx, workerCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		flow.requestWorker(workerCtx, msgChan, queue)
		close(done)
	}()

	// Wait until the message has been popped from Redis (queue becomes empty)
	// before cancelling. This proves re-queue, not just "message was never popped".
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cnt, _ := rdb.ZCard(ctx, queue).Result(); cnt == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cnt, _ := rdb.ZCard(ctx, queue).Result(); cnt != 0 {
		t.Fatal("Message was never popped from Redis")
	}

	workerCancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("requestWorker did not stop after context cancellation")
	}

	// The message should be back in Redis.
	count, err := rdb.ZCard(ctx, queue).Result()
	if err != nil {
		t.Fatalf("ZCard error: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected message to be re-queued on shutdown, got count=%d", count)
	}

	results, _ := rdb.ZRangeWithScores(ctx, queue, 0, -1).Result()
	if results[0].Score != score {
		t.Errorf("Expected re-queued score %f, got %f", score, results[0].Score)
	}
	var restored api.InternalRequest
	json.Unmarshal([]byte(results[0].Member.(string)), &restored) // nolint:errcheck
	if restored.PublicRequest.ReqID() != "requeue-1" {
		t.Errorf("Expected re-queued message id requeue-1, got %s", restored.PublicRequest.ReqID())
	}
}
