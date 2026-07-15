package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

// noopGate returns a gate that always returns full budget (1.0)
func noopGate() pipeline.Gate {
	return pipeline.ConstOpenGate()
}

type stubFlowCancellationChecker struct {
	cancelled bool
	err       error
	onCheck   func()
}

func (s *stubFlowCancellationChecker) IsCancelled(ctx context.Context, requestID, requestToken string) (bool, error) {
	if s.onCheck != nil {
		s.onCheck()
		s.onCheck = nil
	}
	return s.cancelled, s.err
}

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

func TestParseQueueConfigs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantErr  bool
		validate func(t *testing.T, configs []queueConfig)
	}{
		{
			name:    "single queue with string gate params",
			input:   `[{"queue_name":"q1","igw_base_url":"http://gw","gate_type":"redis","gate_params":{"address":"localhost:6379"}}]`,
			wantLen: 1,
			validate: func(t *testing.T, configs []queueConfig) {
				if configs[0].QueueName != "q1" {
					t.Errorf("expected q1, got %s", configs[0].QueueName)
				}
				if configs[0].GateParams["address"] != "localhost:6379" {
					t.Errorf("expected localhost:6379, got %v", configs[0].GateParams["address"])
				}
			},
		},
		{
			name:    "numeric gate params preserved as native types",
			input:   `[{"queue_name":"q1","igw_base_url":"http://gw","gate_type":"prometheus-saturation","gate_params":{"threshold":0.7,"pool":"p1"}}]`,
			wantLen: 1,
			validate: func(t *testing.T, configs []queueConfig) {
				if configs[0].GateParams["threshold"] != 0.7 {
					t.Errorf("expected 0.7, got '%v'", configs[0].GateParams["threshold"])
				}
				if configs[0].GateParams["pool"] != "p1" {
					t.Errorf("expected 'p1', got '%v'", configs[0].GateParams["pool"])
				}
			},
		},
		{
			name:    "multiple queues",
			input:   `[{"queue_name":"q1","igw_base_url":"http://igw:80"},{"queue_name":"q2","igw_base_url":"http://gw","gate_type":"redis","gate_params":{"address":"redis:6379"}}]`,
			wantLen: 2,
			validate: func(t *testing.T, configs []queueConfig) {
				if configs[0].QueueName != "q1" {
					t.Errorf("expected q1, got %s", configs[0].QueueName)
				}
				if configs[1].QueueName != "q2" {
					t.Errorf("expected q2, got %s", configs[1].QueueName)
				}
			},
		},
		{
			name:    "no gate params",
			input:   `[{"queue_name":"q1","igw_base_url":"http://gw","request_path_url":"/v1/completions"}]`,
			wantLen: 1,
			validate: func(t *testing.T, configs []queueConfig) {
				if len(configs[0].GateParams) != 0 {
					t.Errorf("expected empty gate params, got %v", configs[0].GateParams)
				}
			},
		},
		{
			name:    "invalid json",
			input:   `not json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configs, err := parseQueueConfigs([]byte(tt.input))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseQueueConfigs() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(configs) != tt.wantLen {
				t.Fatalf("expected %d configs, got %d", tt.wantLen, len(configs))
			}
			if tt.validate != nil {
				tt.validate(t, configs)
			}
		})
	}
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
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		ID:       "msg-1",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  map[string]any{"test": "data"},
	}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	select {
	case received := <-flow.requestChannels[0].channel.Channel:
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
		msg := api.RequestMessage{ID: m.id, Created: time.Now().Unix(), Deadline: m.deadline}
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(m.deadline), Member: envelopeJSON(msg)})
	}

	msgChannel := make(chan *api.InternalRequest, 10)
	go flow.requestWorker(ctx, msgChannel, queue, "")

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
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	pastDeadline := time.Now().Unix() - 100
	msg := api.RequestMessage{ID: "expired", Created: time.Now().Unix(), Deadline: pastDeadline}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(pastDeadline), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	select {
	case msg := <-flow.requestChannels[0].channel.Channel:
		t.Fatalf("Should not receive expired message: %s", msg.PublicRequest.ReqID())
	case <-time.After(300 * time.Millisecond):
		// Expected - message expired
	}

	// Verify message was removed
	if count, _ := rdb.ZCard(ctx, queue).Result(); count != 0 {
		t.Errorf("Expired message not removed, count=%d", count)
	}
}

func TestSortedSetFlow_ExpiredMessagesCleanupRequestState(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "expired-cleanup-queue"
	token := "expired-token"
	requestID := "expired-cleanup"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	ir := api.NewInternalRequest(api.InternalRouting{RequestToken: token}, &api.RequestMessage{
		ID:       requestID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Unix() - 100,
	})
	msgBytes, _ := json.Marshal(ir)
	rdb.Set(ctx, api.RequestActiveTokenKey(requestID), token, time.Hour)
	rdb.Set(ctx, api.RequestCancellationKey(requestID), token, time.Hour)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix() - 100), Member: string(msgBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	time.Sleep(300 * time.Millisecond)

	if count, _ := rdb.ZCard(ctx, queue).Result(); count != 0 {
		t.Fatalf("Expected expired message to be removed from queue, got count=%d", count)
	}
	if exists, _ := rdb.Exists(ctx, api.RequestActiveTokenKey(requestID)).Result(); exists != 0 {
		t.Fatalf("expected expired request active token for %q to be cleaned up", requestID)
	}
	if exists, _ := rdb.Exists(ctx, api.RequestCancellationKey(requestID)).Result(); exists != 0 {
		t.Fatalf("expected expired request cancellation marker for %q to be cleaned up", requestID)
	}
}

func TestSortedSetFlow_CancelledMessageProducesCancelledResult(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "cancelled-queue"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1)},
			queueName: queue,
		}},
		resultChannel: make(chan api.ResultMessage, 1),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
		gate:          noopGate(),
	}

	requestToken := "cancel-token"
	ir := api.NewInternalRequest(api.InternalRouting{RequestToken: requestToken}, &api.RequestMessage{
		ID: "cancelled-msg", Created: time.Now().Unix(), Deadline: 9999999999,
	})
	msgBytes, _ := json.Marshal(ir)
	rdb.Set(ctx, api.RequestCancellationKey(ir.PublicRequest.ReqID()), requestToken, time.Hour)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	select {
	case result := <-flow.resultChannel:
		if result.ID != ir.PublicRequest.ReqID() {
			t.Fatalf("Expected cancelled result for %q, got %q", ir.PublicRequest.ReqID(), result.ID)
		}
		if result.ErrorCode != api.ErrCodeCancelled {
			t.Fatalf("Expected ErrorCode %q, got %q", api.ErrCodeCancelled, result.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for cancelled result")
	}

	select {
	case msg := <-flow.requestChannels[0].channel.Channel:
		t.Fatalf("Cancelled message should not reach worker channel: %s", msg.PublicRequest.ReqID())
	default:
	}
}

func TestSortedSetFlow_CancellationCheckErrorLeavesMessageDispatchable(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "cancel-check-error-queue"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1)},
			queueName: queue,
		}},
		resultChannel:       make(chan api.ResultMessage, 1),
		pollInterval:        50 * time.Millisecond,
		batchSize:           10,
		gate:                noopGate(),
		cancellationChecker: &stubFlowCancellationChecker{err: fmt.Errorf("redis unavailable")},
	}

	requestToken := "cancel-check-error-token"
	ir := api.NewInternalRequest(api.InternalRouting{RequestToken: requestToken}, &api.RequestMessage{
		ID: "cancel-check-error-msg", Created: time.Now().Unix(), Deadline: 9999999999,
	})
	msgBytes, _ := json.Marshal(ir)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: string(msgBytes)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	select {
	case msg := <-flow.requestChannels[0].channel.Channel:
		if msg.PublicRequest.ReqID() != ir.PublicRequest.ReqID() {
			t.Fatalf("expected message %q to remain dispatchable, got %q", ir.PublicRequest.ReqID(), msg.PublicRequest.ReqID())
		}
	case result := <-flow.resultChannel:
		t.Fatalf("message should not emit a result on cancellation check error: %+v", result)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message to reach worker channel")
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
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
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
	validMsg := api.RequestMessage{ID: "valid", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(validMsg)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	// Should skip malformed and receive valid message
	select {
	case msg := <-flow.requestChannels[0].channel.Channel:
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
		gate:         noopGate(),
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
	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, 2),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
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

func TestSortedSetFlow_ResultStructuredFields(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "structured-result-queue"
	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, 4),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
	}

	messages := []api.ResultMessage{
		{ID: "success", StatusCode: 201, Payload: `{"id":"new"}`},
		{ID: "http-err", StatusCode: 502, Payload: `{"error":"bad gateway"}`},
		{ID: "deadline", Payload: `{"error":"deadline exceeded"}`, ErrorCode: api.ErrCodeDeadlineExceeded, ErrorMessage: "deadline exceeded"},
		{ID: "gate-drop", Payload: `{"error":"Pool gating dropped request"}`, ErrorCode: api.ErrCodeGateDropped, ErrorMessage: "Pool gating dropped request"},
	}
	for _, m := range messages {
		flow.resultChannel <- m
	}

	go flow.resultWorker(ctx)

	timeout := time.After(2 * time.Second)
	for {
		n, err := rdb.LLen(ctx, queue).Result()
		if err == nil && n >= int64(len(messages)) {
			break
		}
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for %d results to be pushed", len(messages))
		case <-time.After(10 * time.Millisecond):
		}
	}

	for _, want := range messages {
		raw, err := rdb.RPop(ctx, queue).Result()
		if err != nil {
			t.Fatalf("RPop error for %s: %v", want.ID, err)
		}
		var got api.ResultMessage
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Fatalf("Unmarshal error for %s: %v", want.ID, err)
		}
		if got.ID != want.ID {
			t.Errorf("ID = %q, want %q", got.ID, want.ID)
		}
		if got.StatusCode != want.StatusCode {
			t.Errorf("[%s] StatusCode = %d, want %d", want.ID, got.StatusCode, want.StatusCode)
		}
		if got.Payload != want.Payload {
			t.Errorf("[%s] Payload = %q, want %q", want.ID, got.Payload, want.Payload)
		}
		if got.ErrorCode != want.ErrorCode {
			t.Errorf("[%s] ErrorCode = %q, want %q", want.ID, got.ErrorCode, want.ErrorCode)
		}
		if got.ErrorMessage != want.ErrorMessage {
			t.Errorf("[%s] ErrorMessage = %q, want %q", want.ID, got.ErrorMessage, want.ErrorMessage)
		}
	}
}

func TestSortedSetFlow_ResultBatchClearsCancellationMarkers(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "cancel-cleanup-result-queue"
	cancelledID := "cancel-cleanup"
	requestToken := "cleanup-token"
	rdb.Set(ctx, api.RequestActiveTokenKey(cancelledID), requestToken, time.Hour)
	rdb.Set(ctx, api.RequestCancellationKey(cancelledID), requestToken, time.Hour)

	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, 1),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
	}

	go flow.resultWorker(ctx)
	flow.resultChannel <- api.NewCancelledResult(&api.RequestMessage{ID: cancelledID}, api.InternalRouting{RequestToken: requestToken})

	timeout := time.After(2 * time.Second)
	for {
		n, err := rdb.LLen(ctx, queue).Result()
		if err == nil && n == 1 {
			break
		}
		select {
		case <-timeout:
			t.Fatal("timeout waiting for cancelled result to be pushed")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if exists, _ := rdb.Exists(ctx, api.RequestCancellationKey(cancelledID)).Result(); exists != 0 {
		t.Fatalf("expected cancellation marker for %q to be cleared after result flush", cancelledID)
	}
	if exists, _ := rdb.Exists(ctx, api.RequestActiveTokenKey(cancelledID)).Result(); exists != 0 {
		t.Fatalf("expected active token for %q to be cleared after result flush", cancelledID)
	}
}

func TestSortedSetFlow_OldResultDoesNotClearNewGenerationCancellation(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "cancel-generation-result-queue"
	requestID := "reused-request-id"
	oldToken := "old-token"
	newToken := "new-token"

	rdb.Set(ctx, api.RequestActiveTokenKey(requestID), newToken, time.Hour)
	rdb.Set(ctx, api.RequestCancellationKey(requestID), newToken, time.Hour)

	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, 1),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
	}

	go flow.resultWorker(ctx)
	flow.resultChannel <- api.NewCancelledResult(&api.RequestMessage{ID: requestID}, api.InternalRouting{RequestToken: oldToken})

	timeout := time.After(2 * time.Second)
	for {
		n, err := rdb.LLen(ctx, queue).Result()
		if err == nil && n == 1 {
			break
		}
		select {
		case <-timeout:
			t.Fatal("timeout waiting for old-generation result to be pushed")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if got, _ := rdb.Get(ctx, api.RequestCancellationKey(requestID)).Result(); got != newToken {
		t.Fatalf("expected new generation cancellation marker %q to remain, got %q", newToken, got)
	}
	if got, _ := rdb.Get(ctx, api.RequestActiveTokenKey(requestID)).Result(); got != newToken {
		t.Fatalf("expected new generation active token %q to remain, got %q", newToken, got)
	}
}

func TestSortedSetFlow_ResultBatch(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "batch-result-queue"
	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
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

	flow := &RedisSortedSetFlow{
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
		defaultResultQueueName: defaultQueue,
		configMap: map[string]queueConfig{
			"queue-a": {ID: "queue-a", QueueName: "request:queue-a", ResultQueueName: "result:queue-a"},
			"queue-b": {ID: "queue-b", QueueName: "request:queue-b", ResultQueueName: "result:queue-b"},
		},
	}

	flow.resultChannel <- api.ResultMessage{ID: "a-1", Payload: "d1", Routing: api.InternalRouting{QueueID: "queue-a"}}
	flow.resultChannel <- api.ResultMessage{ID: "b-1", Payload: "c1", Routing: api.InternalRouting{QueueID: "queue-b"}}
	flow.resultChannel <- api.ResultMessage{ID: "a-2", Payload: "d2", Routing: api.InternalRouting{QueueID: "queue-a"}}
	flow.resultChannel <- api.ResultMessage{ID: "no-id", Payload: "fallback"}

	go flow.resultWorker(ctx)
	time.Sleep(200 * time.Millisecond)

	aLen, _ := rdb.LLen(ctx, "result:queue-a").Result()
	if aLen != 2 {
		t.Errorf("Expected 2 messages in result:queue-a, got %d", aLen)
	}

	bLen, _ := rdb.LLen(ctx, "result:queue-b").Result()
	if bLen != 1 {
		t.Errorf("Expected 1 message in result:queue-b, got %d", bLen)
	}

	defaultLen, _ := rdb.LLen(ctx, defaultQueue).Result()
	if defaultLen != 1 {
		t.Errorf("Expected 1 message in default queue (no queue ID), got %d", defaultLen)
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
		flow := &RedisSortedSetFlow{rdb: rdb, pollInterval: 20 * time.Millisecond, batchSize: 10, gate: noopGate()}
		msgChan := make(chan *api.InternalRequest, 10)

		go func() {
			defer wg.Done()
			workerCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			go flow.requestWorker(workerCtx, msgChan, queue, "")
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
		gate:          noopGate(),
	}

	workerCtx, cancel := context.WithCancel(ctx)
	msgChan := make(chan *api.InternalRequest)

	done := make(chan bool)
	go func() {
		flow.requestWorker(workerCtx, msgChan, queue, "")
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

	flowOpts := SortedSetFlowOptions{
		RequestQueueName: queue,
		IGWBaseURL:       "http://gw",
		ResultQueueName:  "result-list",
		PollIntervalMs:   1000,
		BatchSize:        10,
		GateParamsJSON:   "{}",
	}
	connOpts := ConnectionOptions{URL: "redis://" + s.Addr()}
	flow, err := NewRedisSortedSetFlow(flowOpts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
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

func TestSortedSetFlow_ZeroBudget(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "zero-budget-queue"

	// Create a gate with zero budget initially
	var budgetValue atomic.Uint64 // Store as bits to represent float64

	budgetValue.Store(math.Float64bits(0.0))
	gate := pipeline.DispatchGateFunc(func(ctx context.Context) float64 {
		return math.Float64frombits(budgetValue.Load())
	})

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
			queueName: queue,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         gate,
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		ID:       "test-zero-budget",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  map[string]any{"test": "data"},
	}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

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
		if received.PublicRequest == nil || received.PublicRequest.ReqID() != "test-zero-budget" {
			t.Errorf("Expected test-zero-budget, got %v", received.PublicRequest)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message should be pulled after budget increased")
	}
}

func TestSortedSetFlow_ResultRetryAfterFailure(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "retry-result-queue"
	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
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
	var msg api.ResultMessage
	json.Unmarshal([]byte(raw), &msg) // nolint:errcheck
	if msg.ID != "retry-msg" {
		t.Errorf("Expected retry-msg, got %s", msg.ID)
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
		gate:         noopGate(),
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
		gate:         noopGate(),
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

func TestSortedSetFlow_PartialBudget(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "partial-budget-queue"

	// Gate with 30% budget - should process floor(10*0.3)=3 messages per cycle
	gate := pipeline.DispatchGateFunc(func(ctx context.Context) float64 {
		return 0.3
	})

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 20)},
			queueName: queue,
		}},
		pollInterval: 200 * time.Millisecond,
		batchSize:    10,
		gate:         gate,
	}

	// Add 10 messages
	for i := 0; i < 10; i++ {
		msg := api.RequestMessage{
			ID:       "msg-" + strconv.Itoa(i),
			Created:  time.Now().Unix(),
			Deadline: 9999999999,
		}
		rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix() + int64(i)), Member: envelopeJSON(msg)})
	}

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, "")

	// Wait for one poll cycle (200ms interval + buffer)
	time.Sleep(250 * time.Millisecond)

	// With 30% budget and batchSize=10, floor(10*0.3)=3 messages should be processed per cycle
	// After one cycle, 7 messages should remain
	remaining, _ := rdb.ZCard(ctx, queue).Result()
	if remaining != 7 {
		t.Errorf("Expected 7 messages remaining with 30%% budget (3 pulled), got %d remaining", remaining)
	}
}

func TestSortedSetFlow_RequestWorkerRequeuesOnShutdown(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "requeue-shutdown-queue"
	// Unbuffered channel with no reader: the worker's channel send will block
	// indefinitely, so ctx.Done() is the only way to unblock the select.
	// This guarantees the re-queue path is exercised deterministically.
	msgChan := make(chan *api.InternalRequest)

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: msgChan},
			queueName: queue,
			gate:      noopGate(),
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
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
		flow.requestWorker(workerCtx, msgChan, queue, "")
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

func TestApplyQueueConfigDefaults_IDPreserved(t *testing.T) {
	cfg := queueConfig{ID: "my-id", QueueName: "my-queue", ResultQueueName: "my-result"}
	applyQueueConfigDefaults(&cfg)

	if cfg.ID != "my-id" {
		t.Errorf("Expected ID 'my-id', got %q", cfg.ID)
	}
	if cfg.QueueName != "my-queue" {
		t.Errorf("Expected QueueName 'my-queue', got %q", cfg.QueueName)
	}
	if cfg.ResultQueueName != "my-result" {
		t.Errorf("Expected ResultQueueName 'my-result', got %q", cfg.ResultQueueName)
	}
}

func TestApplyQueueConfigDefaults_IDInferredFromQueueName(t *testing.T) {
	cfg := queueConfig{QueueName: "my-request-sortedset"}
	applyQueueConfigDefaults(&cfg)

	if cfg.ID != "my-request-sortedset" {
		t.Errorf("Expected ID inferred as 'my-request-sortedset', got %q", cfg.ID)
	}
	if cfg.QueueName != "my-request-sortedset" {
		t.Errorf("Expected QueueName unchanged, got %q", cfg.QueueName)
	}
	if cfg.ResultQueueName != "" {
		t.Errorf("Expected empty ResultQueueName, got %q", cfg.ResultQueueName)
	}
}

func TestLoadQueueConfigs_DuplicateIDError(t *testing.T) {
	input := `[{"id":"same","igw_base_url":"http://a"},{"id":"same","igw_base_url":"http://b"}]`
	_, err := parseQueueConfigs([]byte(input))
	if err != nil {
		t.Fatalf("parseQueueConfigs should succeed: %v", err)
	}

	configs := []queueConfig{
		{ID: "same", QueueName: "q1"},
		{ID: "same", QueueName: "q2"},
	}
	seen := make(map[string]bool, len(configs))
	var dupErr error
	for i := range configs {
		applyQueueConfigDefaults(&configs[i])
		if seen[configs[i].ID] {
			dupErr = fmt.Errorf("duplicate queue id %q", configs[i].ID)
			break
		}
		seen[configs[i].ID] = true
	}
	if dupErr == nil {
		t.Fatal("Expected duplicate ID error, got nil")
	}
}

func TestLoadQueueConfigs_InferredDuplicateIDError(t *testing.T) {
	configs := []queueConfig{
		{QueueName: "same-queue"},
		{QueueName: "same-queue"},
	}
	seen := make(map[string]bool, len(configs))
	var dupErr error
	for i := range configs {
		applyQueueConfigDefaults(&configs[i])
		if seen[configs[i].ID] {
			dupErr = fmt.Errorf("duplicate queue id %q", configs[i].ID)
			break
		}
		seen[configs[i].ID] = true
	}
	if dupErr == nil {
		t.Fatal("Expected duplicate ID error for inferred IDs, got nil")
	}
}

func TestLoadQueueConfigs_DuplicateQueueNameError(t *testing.T) {
	configs := []queueConfig{
		{ID: "id-1", QueueName: "same-queue"},
		{ID: "id-2", QueueName: "same-queue"},
	}
	seenID := make(map[string]bool, len(configs))
	seenQueue := make(map[string]bool, len(configs))
	var dupErr error
	for i := range configs {
		applyQueueConfigDefaults(&configs[i])
		if seenID[configs[i].ID] {
			dupErr = fmt.Errorf("duplicate queue id %q", configs[i].ID)
			break
		}
		seenID[configs[i].ID] = true
		if seenQueue[configs[i].QueueName] {
			dupErr = fmt.Errorf("duplicate queue_name %q", configs[i].QueueName)
			break
		}
		seenQueue[configs[i].QueueName] = true
	}
	if dupErr == nil {
		t.Fatal("Expected duplicate queue_name error, got nil")
	}
}

func TestSortedSetFlow_QueueIDSetOnDequeue(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "test-qid-queue"
	queueID := "test-qid"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
			queueName: queue,
			queueID:   queueID,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	msg := api.RequestMessage{ID: "msg-qid", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, queueID)

	select {
	case received := <-flow.requestChannels[0].channel.Channel:
		if received.QueueID != queueID {
			t.Errorf("Expected QueueID %q, got %q", queueID, received.QueueID)
		}
		if received.RequestQueueName != queue {
			t.Errorf("Expected RequestQueueName %q, got %q", queue, received.RequestQueueName)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestSortedSetFlow_ResultQueueIgnoresMessagePayload(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	configResult := "config-result-queue"
	messageResult := "message-result-queue"

	flow := &RedisSortedSetFlow{
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
		defaultResultQueueName: "global-default",
		configMap: map[string]queueConfig{
			"my-queue": {ID: "my-queue", ResultQueueName: configResult},
		},
	}

	flow.resultChannel <- api.ResultMessage{
		ID:      "test-1",
		Payload: "data",
		Routing: api.InternalRouting{QueueID: "my-queue", ResultQueueName: messageResult},
	}

	go flow.resultWorker(ctx)
	time.Sleep(200 * time.Millisecond)

	configLen, _ := rdb.LLen(ctx, configResult).Result()
	if configLen != 1 {
		t.Errorf("Expected 1 message in config result queue, got %d", configLen)
	}

	messageLen, _ := rdb.LLen(ctx, messageResult).Result()
	if messageLen != 0 {
		t.Errorf("Expected 0 messages in message-level result queue (should be ignored), got %d", messageLen)
	}
}

func TestSortedSetFlow_ResultQueueFallsBackToMessageLevel(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	messageResult := "producer-result-queue"

	flow := &RedisSortedSetFlow{
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
		defaultResultQueueName: "global-default",
		configMap: map[string]queueConfig{
			"no-result-cfg": {ID: "no-result-cfg", QueueName: "req", ResultQueueName: ""},
		},
	}

	// Config exists but has no ResultQueueName → should fall back to message-level
	flow.resultChannel <- api.ResultMessage{
		ID:      "fallback-1",
		Payload: "data",
		Routing: api.InternalRouting{QueueID: "no-result-cfg", ResultQueueName: messageResult},
	}
	// No config match and no message-level → should fall back to global default
	flow.resultChannel <- api.ResultMessage{
		ID:      "global-1",
		Payload: "data",
		Routing: api.InternalRouting{},
	}

	go flow.resultWorker(ctx)
	time.Sleep(200 * time.Millisecond)

	msgLen, _ := rdb.LLen(ctx, messageResult).Result()
	if msgLen != 1 {
		t.Errorf("Expected 1 message in producer result queue (message-level fallback), got %d", msgLen)
	}

	globalLen, _ := rdb.LLen(ctx, "global-default").Result()
	if globalLen != 1 {
		t.Errorf("Expected 1 message in global default queue, got %d", globalLen)
	}
}

func TestNewRedisSortedSetFlow_PoolRequiredAndValidation(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	connOpts := ConnectionOptions{URL: "redis://" + s.Addr()}
	baseOpts := SortedSetFlowOptions{PollIntervalMs: 1000, BatchSize: 10, GateParamsJSON: "{}"}

	// Case 1: worker_pool_id is missing from configuration, and pool "default" does not exist
	opts := baseOpts
	opts.QueuesConfig = `[{"queue_name":"test-queue","inference_objective":"obj","igw_base_url":"http://gw"}]`
	_, err := NewRedisSortedSetFlow(opts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when worker_pool_id is missing and 'default' pool does not exist, got nil")
	}

	// Case 5: worker_pool_id is missing, but only a single 'default' pool is specified
	opts = baseOpts
	opts.QueuesConfig = `[{"queue_name":"test-queue","inference_objective":"obj","igw_base_url":"http://gw"}]`
	_, err = NewRedisSortedSetFlow(opts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id is missing but default pool exists: %v", err)
	}

	// Case 6: worker_pool_id is specified as custom, but only a single 'default' pool is specified
	opts = baseOpts
	opts.QueuesConfig = `[{"queue_name":"test-queue","worker_pool_id":"custom-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`
	_, err = NewRedisSortedSetFlow(opts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when worker_pool_id is custom but only default pool exists, got nil")
	}

	// Case 2: worker_pool_id is specified but pool does not exist
	opts = baseOpts
	opts.QueuesConfig = `[{"queue_name":"test-queue","worker_pool_id":"non-existent","inference_objective":"obj","igw_base_url":"http://gw"}]`
	_, err = NewRedisSortedSetFlow(opts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when specified worker_pool_id does not exist, got nil")
	}

	// Case 3: worker_pool_id specified and pool exists, but igw_base_url is missing
	opts = baseOpts
	opts.QueuesConfig = `[{"queue_name":"test-queue","worker_pool_id":"test-pool","inference_objective":"obj"}]`
	_, err = NewRedisSortedSetFlow(opts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err == nil {
		t.Error("Expected error when igw_base_url is missing in queue config, got nil")
	}

	// Case 4: worker_pool_id and igw_base_url specified and pool exists
	opts = baseOpts
	opts.QueuesConfig = `[{"queue_name":"test-queue","worker_pool_id":"test-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`
	_, err = NewRedisSortedSetFlow(opts, connOpts, WithSortedSetWorkerPools([]pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}))
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id exists: %v", err)
	}
}

func TestQueueBacklog(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{
			{queueName: "queue-a", queueID: "a"},
			{queueName: "queue-b", queueID: "b"},
		},
	}

	// queue-a gets 3 members, queue-b gets 1, an unrelated key is ignored.
	for i, m := range []string{"m1", "m2", "m3"} {
		rdb.ZAdd(ctx, "queue-a", redis.Z{Score: float64(i), Member: m})
	}
	rdb.ZAdd(ctx, "queue-b", redis.Z{Score: 0, Member: "only"})

	stats, err := flow.QueueBacklog(ctx)
	if err != nil {
		t.Fatalf("QueueBacklog returned error: %v", err)
	}

	got := make(map[string]int64, len(stats))
	for _, s := range stats {
		got[s.QueueName] = s.Depth
	}
	if got["queue-a"] != 3 {
		t.Errorf("queue-a backlog = %d, want 3", got["queue-a"])
	}
	if got["queue-b"] != 1 {
		t.Errorf("queue-b backlog = %d, want 1", got["queue-b"])
	}
}

// TestQueueBacklogReportsZeroOnError verifies that a per-queue failure reports a
// 0 sentinel for that queue (rather than skipping it) so the gauge does not
// retain a stale value, while healthy queues still report their real depth.
func TestQueueBacklogReportsZeroOnError(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{
			{queueName: "queue-ok", queueID: "ok"},
			{queueName: "queue-bad", queueID: "bad"},
		},
	}

	rdb.ZAdd(ctx, "queue-ok", redis.Z{Score: 0, Member: "m1"})
	// Make queue-bad a string so ZCard fails with WRONGTYPE.
	rdb.Set(ctx, "queue-bad", "not-a-sorted-set", 0)

	stats, err := flow.QueueBacklog(ctx)
	if err == nil {
		t.Fatal("QueueBacklog expected an error for the WRONGTYPE queue, got nil")
	}

	got := make(map[string]int64, len(stats))
	for _, s := range stats {
		got[s.QueueName] = s.Depth
	}
	if _, ok := got["queue-bad"]; !ok {
		t.Error("queue-bad missing from stats; want a 0 sentinel instead of being skipped")
	}
	if got["queue-bad"] != 0 {
		t.Errorf("queue-bad backlog = %d, want 0", got["queue-bad"])
	}
	if got["queue-ok"] != 1 {
		t.Errorf("queue-ok backlog = %d, want 1", got["queue-ok"])
	}
}

func TestSortedSetFlow_QueueLabelsSetOnDequeue(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "test-labels-queue"
	queueID := "test-labels-qid"
	labels := map[string]string{
		"foo": "bar",
		"abc": "xyz",
	}

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		requestChannels: []requestChannelData{{
			channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
			queueName: queue,
			queueID:   queueID,
		}},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
		configMap: map[string]queueConfig{
			queueID: {
				ID:     queueID,
				Labels: labels,
			},
		},
	}

	msg := api.RequestMessage{ID: "msg-labels", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.requestChannels[0].channel.Channel, queue, queueID)

	select {
	case received := <-flow.requestChannels[0].channel.Channel:
		if received.Labels == nil {
			t.Fatal("Expected labels on dequeued request, got nil")
		}
		if received.Labels["foo"] != "bar" || received.Labels["abc"] != "xyz" {
			t.Fatalf("Unexpected labels: %v", received.Labels)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}
