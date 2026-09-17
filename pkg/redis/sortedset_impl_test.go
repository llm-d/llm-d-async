package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func registerTestClaim(ctx context.Context, flow *RedisSortedSetFlow, queueName, reqID, reqToken string) {
	token := "token-" + reqID
	if reqToken != "" {
		token += "-" + reqToken
	}
	ir := api.NewInternalRequest(
		api.InternalRouting{RequestQueueName: queueName, RequestToken: reqToken},
		&api.RequestMessage{
			ID:       reqID,
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Hour).Unix(),
			Payload:  testPayload(map[string]any{"model": "test"}),
		},
	)
	payloadBytes, _ := json.Marshal(ir)
	keys := newClaimKeys(queueName)
	claimID := claimKey(reqID, reqToken)
	flow.rdb.HSet(ctx, keys.claimed, claimID, string(payloadBytes))
	flow.rdb.HSet(ctx, keys.owners, claimID, token)
	flow.rdb.ZAdd(ctx, keys.idx, redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: claimID})
	flow.claimTokens.Store(claimID, &claimHandle{
		token:        token,
		queue:        queueName,
		requestID:    reqID,
		requestToken: reqToken,
	})
}

// mkWorkerRuntime wraps a bare channel and queue identity into the runtime
// shape requestWorker consumes, for tests that drive a worker directly.
func mkWorkerRuntime(ch chan *api.InternalRequest, queue, queueID string) *queueRuntime {
	return &queueRuntime{data: requestChannelData{
		channel:   pipeline.RequestChannel{Channel: ch},
		queueName: queue,
		queueID:   queueID,
	}}
}

func TestParseSortedSetQueueConfigs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLen  int
		wantErr  bool
		validate func(t *testing.T, configs []SortedSetQueueConfig)
	}{
		{
			name:    "single queue with string gate params",
			input:   `[{"queue_name":"q1","igw_base_url":"http://gw","gate_type":"redis","gate_params":{"address":"localhost:6379"}}]`,
			wantLen: 1,
			validate: func(t *testing.T, configs []SortedSetQueueConfig) {
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
			validate: func(t *testing.T, configs []SortedSetQueueConfig) {
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
			validate: func(t *testing.T, configs []SortedSetQueueConfig) {
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
			validate: func(t *testing.T, configs []SortedSetQueueConfig) {
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
			var configs []SortedSetQueueConfig
			err := json.Unmarshal([]byte(tt.input), &configs)
			if (err != nil) != tt.wantErr {
				t.Fatalf("json.Unmarshal() error = %v, wantErr = %v", err, tt.wantErr)
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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
			}},
		},
		queueOrder:   []string{""},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		ID:       "msg-1",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  testPayload(map[string]any{"test": "data"}),
	}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	select {
	case received := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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
	go flow.requestWorker(ctx, mkWorkerRuntime(msgChannel, queue, ""))

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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
			}},
		},
		queueOrder:    []string{""},
		resultChannel: make(chan api.ResultMessage, 1),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
		gate:          noopGate(),
	}

	pastDeadline := time.Now().Unix() - 100
	msg := api.RequestMessage{ID: "expired", Created: time.Now().Unix(), Deadline: pastDeadline}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(pastDeadline), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	// The expiry is surfaced as a DEADLINE_EXCEEDED result rather than a
	// silent drop, so fetch can distinguish a queue timeout from an unknown id.
	select {
	case result := <-flow.resultChannel:
		if result.ID != "expired" {
			t.Fatalf("Expected deadline result for %q, got %q", "expired", result.ID)
		}
		if result.ErrorCode != api.ErrCodeDeadlineExceeded {
			t.Fatalf("Expected ErrorCode %q, got %q", api.ErrCodeDeadlineExceeded, result.ErrorCode)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for deadline exceeded result")
	}

	select {
	case msg := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
		t.Fatalf("Should not receive expired message: %s", msg.PublicRequest.ReqID())
	default:
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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
			}},
		},
		queueOrder:    []string{""},
		resultChannel: make(chan api.ResultMessage, 1),
		pollInterval:  50 * time.Millisecond,
		batchSize:     10,
		gate:          noopGate(),
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

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1)},
				queueName: queue,
			}},
		},
		queueOrder:    []string{""},
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

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

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
	case msg := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1)},
				queueName: queue,
			}},
		},
		queueOrder:          []string{""},
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

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	select {
	case msg := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
			}},
		},
		queueOrder:   []string{""},
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

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	// Should skip malformed and receive valid message
	select {
	case msg := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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

	// The retry parks in the retry queue scored by its due time, NOT in the
	// request queue (where the score means deadline and ZPopMin would pop a
	// future-scored retry immediately).
	if n, _ := rdb.ZCard(ctx, queue).Result(); n != 0 {
		t.Fatalf("Expected request queue empty before backoff elapses, got %d entries", n)
	}
	results, _ := rdb.ZRangeWithScores(ctx, flow.retryQueue(), 0, -1).Result()
	if len(results) != 1 {
		t.Fatalf("Expected 1 message in retry queue, got %d", len(results))
	}
	expectedScore := float64(time.Now().Unix()) + 2.0
	if results[0].Score < expectedScore-1 || results[0].Score > expectedScore+1 {
		t.Errorf("Retry due-time score incorrect: expected ~%f, got %f", expectedScore, results[0].Score)
	}

	// Once due, the mover re-enters it into the request queue with the
	// original deadline as the score. The mover compares scores against
	// wall-clock time (miniredis.FastForward cannot advance time.Now()), so
	// this waits out the 2s backoff in real time.
	go flow.retryMover(ctx)
	deadlineCheck := time.After(5 * time.Second)
	for {
		entries, _ := rdb.ZRangeWithScores(ctx, queue, 0, -1).Result()
		if len(entries) == 1 {
			if entries[0].Score != 9999999999 {
				t.Errorf("Re-entered retry should carry deadline score, got %f", entries[0].Score)
			}
			if n, _ := rdb.ZCard(ctx, flow.retryQueue()).Result(); n != 0 {
				t.Errorf("Retry queue should be empty after move, got %d", n)
			}
			break
		}
		select {
		case <-deadlineCheck:
			t.Fatal("timed out waiting for due retry to re-enter request queue")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestSortedSetFlow_RetryReleasesReservation is a regression test for #311: a
// retried request returns to the queue and is re-gated (re-reserved) on its next
// dispatch, so the retry path must release its current queue-gate reservation.
// Before the fix, only resultWorker released reservations, so every retry leaked
// one — inFlight ratcheted up until Budget() hit 0 and the queue wedged.
func TestSortedSetFlow_RetryReleasesReservation(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	const reqID = "retry-release-1"
	queue := "retry-release-queue"

	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		retryChannel: make(chan pipeline.RetryMessage, 1),
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	// Simulate a dispatched request holding a gate reservation (as the dequeue
	// loop stores after gate.Apply).
	releasedCh := make(chan struct{}, 1)
	flow.activeReleases.Store(reqID, []pipeline.GateReleaseFunc{
		func() { releasedCh <- struct{}{} },
	})

	go flow.retryWorker(ctx)

	flow.retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
			InternalRequest: api.NewInternalRequest(
				api.InternalRouting{RetryCount: 1, RequestQueueName: queue},
				&api.RequestMessage{ID: reqID, Created: time.Now().Unix(), Deadline: 9999999999},
			),
		},
		BackoffDurationSeconds: 0,
	}

	select {
	case <-releasedCh:
		// Reservation released on the retry path — correct.
	case <-time.After(2 * time.Second):
		t.Fatal("retry did not release the queue-gate reservation (#311 leak)")
	}

	if _, held := flow.activeReleases.Load(reqID); held {
		t.Error("reservation still present in activeReleases after retry")
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

	registerTestClaim(ctx, flow, queue, "first", "")
	registerTestClaim(ctx, flow, queue, "second", "")
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
		resultChannel:          make(chan api.ResultMessage, 6),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
	}

	messages := []api.ResultMessage{
		api.NewHTTPResult(&api.RequestMessage{ID: "success"}, api.InternalRouting{RequestToken: "success-token"}, 201, []byte(`{"id":"new"}`)),
		api.NewHTTPResult(&api.RequestMessage{ID: "http-err"}, api.InternalRouting{RequestToken: "http-err-token"}, 502, []byte(`{"error":"bad gateway"}`)),
		api.NewErrorResult(&api.RequestMessage{ID: "non-http"}, api.InternalRouting{RequestToken: "non-http-token"}, api.ErrCodeInferenceError, "connection reset"),
		api.NewDeadlineExceededResult(&api.RequestMessage{ID: "deadline"}, api.InternalRouting{RequestToken: "deadline-token"}),
		api.NewCancelledResult(&api.RequestMessage{ID: "cancelled"}, api.InternalRouting{RequestToken: "cancelled-token"}),
		api.NewGateDroppedResult(&api.RequestMessage{ID: "gate-drop"}, api.InternalRouting{RequestToken: "gate-drop-token"}),
	}
	for _, m := range messages {
		registerTestClaim(ctx, flow, queue, m.ID, m.Routing.RequestToken)
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
		var wire api.InternalResult
		got := &wire.ResultMessage
		if err := json.Unmarshal([]byte(raw), &wire); err != nil {
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
		if wire.RequestToken != want.Routing.RequestToken {
			t.Errorf("[%s] RequestToken = %q, want %q", want.ID, wire.RequestToken, want.Routing.RequestToken)
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

	registerTestClaim(ctx, flow, queue, cancelledID, requestToken)
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

	// Cleanup runs after the result push becomes visible, so poll for it
	// rather than asserting in the same instant the list length flips.
	deadline := time.Now().Add(2 * time.Second)
	for {
		cancelExists, _ := rdb.Exists(ctx, api.RequestCancellationKey(cancelledID)).Result()
		activeExists, _ := rdb.Exists(ctx, api.RequestActiveTokenKey(cancelledID)).Result()
		if cancelExists == 0 && activeExists == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected request state for %q cleared after result flush (cancel=%d active=%d)",
				cancelledID, cancelExists, activeExists)
		}
		time.Sleep(10 * time.Millisecond)
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

	registerTestClaim(ctx, flow, queue, requestID, oldToken)
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
		id := "batch-" + strconv.Itoa(i)
		registerTestClaim(ctx, flow, queue, id, "")
		flow.resultChannel <- api.ResultMessage{
			ID:      id,
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

func TestSortedSetFlow_ResultTTL(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		defaultResultQueueName: "default-results",
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, 4),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
		configMap: map[string]SortedSetQueueConfig{
			"ttl-queue": {ID: "ttl-queue", ResultTTLSeconds: 60},
			"no-ttl":    {ID: "no-ttl"},
		},
	}

	go flow.resultWorker(ctx)

	registerTestClaim(ctx, flow, "results:req:ttl-1", "ttl-1", "")
	registerTestClaim(ctx, flow, "belt-results", "plain-1", "")
	// Per-message result key routing from a queue with result_ttl_seconds:
	// the destination key gets an expiry.
	flow.resultChannel <- api.ResultMessage{
		ID:      "ttl-1",
		Payload: "data",
		Routing: api.InternalRouting{QueueID: "ttl-queue", ResultQueueName: "results:req:ttl-1"},
	}
	// Same flow from a queue without TTL configured: no expiry (unchanged
	// belt behavior).
	flow.resultChannel <- api.ResultMessage{
		ID:      "plain-1",
		Payload: "data",
		Routing: api.InternalRouting{QueueID: "no-ttl", ResultQueueName: "belt-results"},
	}
	time.Sleep(200 * time.Millisecond)

	if n, _ := rdb.LLen(ctx, "results:req:ttl-1").Result(); n != 1 {
		t.Fatalf("expected 1 result on per-request key, got %d", n)
	}
	ttl := s.TTL("results:req:ttl-1")
	if ttl <= 0 || ttl > 60*time.Second {
		t.Errorf("expected TTL in (0, 60s] on per-request key, got %v", ttl)
	}

	if n, _ := rdb.LLen(ctx, "belt-results").Result(); n != 1 {
		t.Fatalf("expected 1 result on belt, got %d", n)
	}
	if ttl := s.TTL("belt-results"); ttl != 0 {
		t.Errorf("expected no TTL on belt key, got %v", ttl)
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
		configMap: map[string]SortedSetQueueConfig{
			"queue-a": {ID: "queue-a", QueueName: "request:queue-a", ResultQueueName: "result:queue-a"},
			"queue-b": {ID: "queue-b", QueueName: "request:queue-b", ResultQueueName: "result:queue-b"},
		},
	}

	registerTestClaim(ctx, flow, "request:queue-a", "a-1", "")
	registerTestClaim(ctx, flow, "request:queue-b", "b-1", "")
	registerTestClaim(ctx, flow, "request:queue-a", "a-2", "")
	registerTestClaim(ctx, flow, defaultQueue, "no-id", "")
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
			go flow.requestWorker(workerCtx, mkWorkerRuntime(msgChan, queue, ""))
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
		flow.requestWorker(workerCtx, mkWorkerRuntime(msgChan, queue, ""))
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

	cfg := SortedSetConfig{
		URL:             "redis://" + s.Addr(),
		ResultQueueName: "result-list",
		PollIntervalMs:  1000,
		BatchSize:       10,
		Queues: []SortedSetQueueConfig{{
			QueueName:  queue,
			IGWBaseURL: "http://gw",
		}},
	}
	cfg.ApplyDefaults()
	flow, err := NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
			}},
		},
		queueOrder:   []string{""},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         gate,
	}

	// Add message with valid deadline
	msg := api.RequestMessage{
		ID:       "test-zero-budget",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  testPayload(map[string]any{"test": "data"}),
	}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	// Wait for several poll cycles - message should NOT be pulled (budget=0)
	select {
	case <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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
	case received := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
		if received.PublicRequest == nil || received.PublicRequest.ReqID() != "test-zero-budget" {
			t.Errorf("Expected test-zero-budget, got %v", received.PublicRequest)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Message should be pulled after budget increased")
	}
}

func TestSortedSetFlow_ClosedGateRecordsGateClosedDecision(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "gate-closed-metric-queue"
	queueID := "gate-closed-metric-id"
	gateClosed := func() float64 {
		return testutil.ToFloat64(metrics.GateDecisions.WithLabelValues(queueID, queue, "", metrics.ReasonGateClosed))
	}

	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         pipeline.DispatchGateFunc(func(ctx context.Context) float64 { return 0.0 }),
	}
	msgChannel := make(chan *api.InternalRequest, 1)

	// Nothing queued: the gate held nothing back, so nothing is counted.
	flow.processMessages(ctx, msgChannel, queue, queueID, flow.gate, logr.Discard())
	if got := gateClosed(); got != 0 {
		t.Fatalf("gate_closed on an empty queue = %v, want 0", got)
	}

	msg := api.RequestMessage{
		ID:       "gate-closed-1",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  testPayload(map[string]any{"test": "data"}),
	}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	// Work is waiting and the budget zeroes the batch: each throttled poll is a
	// gate_closed decision, even though no message is ever dequeued (#368).
	flow.processMessages(ctx, msgChannel, queue, queueID, flow.gate, logr.Discard())
	flow.processMessages(ctx, msgChannel, queue, queueID, flow.gate, logr.Discard())
	if got := gateClosed(); got != 2 {
		t.Fatalf("gate_closed with a backlogged queue = %v, want 2", got)
	}
	if len(msgChannel) != 0 {
		t.Fatalf("message dispatched while the gate was closed")
	}
	if count, _ := rdb.ZCard(ctx, queue).Result(); count != 1 {
		t.Fatalf("queue depth = %d, want the message left in place", count)
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

	registerTestClaim(ctx, flow, queue, "retry-msg", "")

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

	count, err := rdb.ZCard(ctx, flow.retryQueue()).Result()
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
		count, err := rdb.ZCard(ctx, flow.retryQueue()).Result()
		if err == nil && count == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Expected retry message to be enqueued after transient Redis failure")
}

func TestSortedSetFlow_ResultSustainedOutage_DropsClaimHandle(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "sustained-result-queue"
	flow := &RedisSortedSetFlow{
		defaultResultQueueName: queue,
		rdb:                    rdb,
		resultChannel:          make(chan api.ResultMessage, resultChannelBuffer),
		pollInterval:           50 * time.Millisecond,
		batchSize:              10,
		gate:                   noopGate(),
		queues: map[string]*queueRuntime{
			queue: {data: requestChannelData{queueName: queue, queueID: queue}},
		},
		queueOrder: []string{queue},
	}

	registerTestClaim(ctx, flow, queue, "sustained-msg", "")

	// Inject permanent error so retries are completely exhausted.
	s.SetError("READONLY simulated permanent failure")

	go flow.resultWorker(ctx)

	flow.resultChannel <- api.ResultMessage{ID: "sustained-msg", Payload: "data"}

	// Wait for retryRedisOp to exhaust retries and verify claim handle is removed.
	deadline := time.Now().Add(5 * time.Second)
	handleDropped := false
	for time.Now().Before(deadline) {
		if _, ok := flow.claimTokens.Load(claimKey("sustained-msg", "")); !ok {
			handleDropped = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !handleDropped {
		t.Fatal("expected claim handle to be deleted after sustained Redis outage on result flush")
	}

	// Redis recovers after the sustained outage.
	s.SetError("")

	// Expire the lease in Redis claim index by backdating its expiry score.
	keys := newClaimKeys(queue)
	claimID := claimKey("sustained-msg", "")
	flow.rdb.ZAdd(ctx, keys.idx, redis.Z{Score: float64(time.Now().Add(-10 * time.Second).Unix()), Member: claimID})

	// Reclaimer runs: with the local handle gone, heartbeats were stopped,
	// allowing the expired lease to be reclaimed and redelivered to pending.
	released, err := flow.reclaimExpiredClaims(ctx)
	if err != nil {
		t.Fatalf("reclaimExpiredClaims failed: %v", err)
	}
	if released != 1 {
		t.Fatalf("expected 1 released claim, got %d", released)
	}

	// Verify the request has returned to the pending sorted set.
	pendingCount, err := rdb.ZCard(ctx, queue).Result()
	if err != nil || pendingCount != 1 {
		t.Fatalf("expected 1 request in pending queue, got %d (err: %v)", pendingCount, err)
	}
	claimedExists, _ := rdb.HExists(ctx, keys.claimed, claimID).Result()
	if claimedExists {
		t.Fatal("expected claimed hash entry to be removed after reclaim")
	}
}

func TestSortedSetFlow_RetrySustainedOutage_DropsClaimHandle(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer s.Close()
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	queue := "sustained-retry-queue"
	flow := &RedisSortedSetFlow{
		rdb:          rdb,
		retryChannel: make(chan pipeline.RetryMessage, 10),
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
		queues: map[string]*queueRuntime{
			queue: {data: requestChannelData{queueName: queue, queueID: queue}},
		},
		queueOrder: []string{queue},
	}

	registerTestClaim(ctx, flow, queue, "sustained-retry-msg", "token-1")

	// Inject permanent error so retries are completely exhausted.
	s.SetError("READONLY simulated permanent failure")

	go flow.retryWorker(ctx)

	flow.retryChannel <- pipeline.RetryMessage{
		EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
			InternalRequest: api.NewInternalRequest(
				api.InternalRouting{RequestQueueName: queue, RequestToken: "token-1"},
				&api.RequestMessage{
					ID:       "sustained-retry-msg",
					Created:  time.Now().Unix(),
					Deadline: 9999999999,
				},
			),
		},
		BackoffDurationSeconds: 0,
	}

	// Wait for retryRedisOp to exhaust retries and verify claim handle is removed.
	deadline := time.Now().Add(5 * time.Second)
	handleDropped := false
	for time.Now().Before(deadline) {
		if _, ok := flow.claimTokens.Load(claimKey("sustained-retry-msg", "token-1")); !ok {
			handleDropped = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !handleDropped {
		t.Fatal("expected claim handle to be deleted after sustained Redis outage on retry flush")
	}

	// Redis recovers after the sustained outage.
	s.SetError("")

	// Expire the lease in Redis claim index by backdating its expiry score.
	keys := newClaimKeys(queue)
	retryClaimID := claimKey("sustained-retry-msg", "token-1")
	flow.rdb.ZAdd(ctx, keys.idx, redis.Z{Score: float64(time.Now().Add(-10 * time.Second).Unix()), Member: retryClaimID})

	// Reclaimer runs: with the local handle gone, heartbeats were stopped,
	// allowing the expired lease to be reclaimed and redelivered to pending.
	released, err := flow.reclaimExpiredClaims(ctx)
	if err != nil {
		t.Fatalf("reclaimExpiredClaims failed: %v", err)
	}
	if released != 1 {
		t.Fatalf("expected 1 released claim, got %d", released)
	}

	// Verify the request has returned to the pending sorted set.
	pendingCount, err := rdb.ZCard(ctx, queue).Result()
	if err != nil || pendingCount != 1 {
		t.Fatalf("expected 1 request in pending queue, got %d (err: %v)", pendingCount, err)
	}
	claimedExists, _ := rdb.HExists(ctx, keys.claimed, retryClaimID).Result()
	if claimedExists {
		t.Fatal("expected claimed hash entry to be removed after reclaim")
	}
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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 20)},
				queueName: queue,
			}},
		},
		queueOrder:   []string{""},
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

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

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
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: msgChan},
				queueName: queue,
				gate:      noopGate(),
			}},
		},
		queueOrder:   []string{""},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	ir := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       "requeue-1",
		Created:  time.Now().Unix(),
		Deadline: 9999999999,
		Payload:  testPayload(map[string]any{"key": "value"}),
	})
	msgBytes, _ := json.Marshal(ir)
	score := float64(time.Now().Unix())
	rdb.ZAdd(ctx, queue, redis.Z{Score: score, Member: string(msgBytes)})

	workerCtx, workerCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		flow.requestWorker(workerCtx, mkWorkerRuntime(msgChan, queue, ""))
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
	// Re-queue restores message to pending at its deadline score (9999999999).
	if results[0].Score != 9999999999 {
		t.Errorf("Expected re-queued score 9999999999 (deadline), got %f", results[0].Score)
	}
	var restored api.InternalRequest
	json.Unmarshal([]byte(results[0].Member.(string)), &restored) // nolint:errcheck
	if restored.PublicRequest.ReqID() != "requeue-1" {
		t.Errorf("Expected re-queued message id requeue-1, got %s", restored.PublicRequest.ReqID())
	}
}

func TestSortedSetConfigApplyDefaults_IDPreserved(t *testing.T) {
	cfg := SortedSetConfig{Queues: []SortedSetQueueConfig{
		{ID: "my-id", QueueName: "my-queue", ResultQueueName: "my-result"},
	}}
	cfg.ApplyDefaults()

	q := cfg.Queues[0]
	if q.ID != "my-id" {
		t.Errorf("Expected ID 'my-id', got %q", q.ID)
	}
	if q.QueueName != "my-queue" {
		t.Errorf("Expected QueueName 'my-queue', got %q", q.QueueName)
	}
	if q.ResultQueueName != "my-result" {
		t.Errorf("Expected ResultQueueName 'my-result', got %q", q.ResultQueueName)
	}
}

func TestSortedSetConfigApplyDefaults_IDInferredFromQueueName(t *testing.T) {
	cfg := SortedSetConfig{Queues: []SortedSetQueueConfig{
		{QueueName: "my-request-sortedset"},
	}}
	cfg.ApplyDefaults()

	q := cfg.Queues[0]
	if q.ID != "my-request-sortedset" {
		t.Errorf("Expected ID inferred as 'my-request-sortedset', got %q", q.ID)
	}
	if q.QueueName != "my-request-sortedset" {
		t.Errorf("Expected QueueName unchanged, got %q", q.QueueName)
	}
	if q.ResultQueueName != "" {
		t.Errorf("Expected empty ResultQueueName, got %q", q.ResultQueueName)
	}
}

func TestSortedSetConfigValidate_DuplicateIDError(t *testing.T) {
	cfg := SortedSetConfig{URL: "redis://localhost:6379", Queues: []SortedSetQueueConfig{
		{ID: "same", QueueName: "q1", IGWBaseURL: "http://a"},
		{ID: "same", QueueName: "q2", IGWBaseURL: "http://b"},
	}}
	cfg.ApplyDefaults()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate queue id") {
		t.Fatalf("Expected duplicate ID error, got: %v", err)
	}
}

func TestSortedSetConfigValidate_InferredDuplicateIDError(t *testing.T) {
	cfg := SortedSetConfig{URL: "redis://localhost:6379", Queues: []SortedSetQueueConfig{
		{QueueName: "same-queue", IGWBaseURL: "http://a"},
		{QueueName: "same-queue", IGWBaseURL: "http://b"},
	}}
	cfg.ApplyDefaults()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate queue id") {
		t.Fatalf("Expected duplicate ID error for inferred IDs, got: %v", err)
	}
}

func TestSortedSetConfigValidate_DuplicateQueueNameError(t *testing.T) {
	cfg := SortedSetConfig{URL: "redis://localhost:6379", Queues: []SortedSetQueueConfig{
		{ID: "id-1", QueueName: "same-queue", IGWBaseURL: "http://a"},
		{ID: "id-2", QueueName: "same-queue", IGWBaseURL: "http://b"},
	}}
	cfg.ApplyDefaults()
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate queue_name") {
		t.Fatalf("Expected duplicate queue_name error, got: %v", err)
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
		queues: map[string]*queueRuntime{
			queueID: {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
				queueID:   queueID,
			}},
		},
		queueOrder:   []string{queueID},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
	}

	msg := api.RequestMessage{ID: "msg-qid", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	select {
	case received := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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
		configMap: map[string]SortedSetQueueConfig{
			"my-queue": {ID: "my-queue", ResultQueueName: configResult},
		},
	}

	registerTestClaim(ctx, flow, configResult, "test-1", "")
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
		configMap: map[string]SortedSetQueueConfig{
			"no-result-cfg": {ID: "no-result-cfg", QueueName: "req", ResultQueueName: ""},
		},
	}

	// Config exists but has no ResultQueueName → should fall back to message-level
	registerTestClaim(ctx, flow, "req", "fallback-1", "")
	flow.resultChannel <- api.ResultMessage{
		ID:      "fallback-1",
		Payload: "data",
		Routing: api.InternalRouting{QueueID: "no-result-cfg", ResultQueueName: messageResult},
	}
	// No config match and no message-level → should fall back to global default
	registerTestClaim(ctx, flow, "global-default", "global-1", "")
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

	parseQueues := func(queuesJSON string) []SortedSetQueueConfig {
		var queues []SortedSetQueueConfig
		if err := json.Unmarshal([]byte(queuesJSON), &queues); err != nil {
			t.Fatalf("failed to parse queues: %v", err)
		}
		return queues
	}
	newCfg := func(queuesJSON string) SortedSetConfig {
		cfg := SortedSetConfig{
			URL:            "redis://" + s.Addr(),
			PollIntervalMs: 1000,
			BatchSize:      10,
			Queues:         parseQueues(queuesJSON),
		}
		cfg.ApplyDefaults()
		return cfg
	}

	// Case 1: worker_pool_id is missing from configuration, and pool "default" does not exist
	cfg := newCfg(`[{"queue_name":"test-queue","inference_objective":"obj","igw_base_url":"http://gw"}]`)
	_, err := NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}, nil)
	if err == nil {
		t.Error("Expected error when worker_pool_id is missing and 'default' pool does not exist, got nil")
	}

	// Case 5: worker_pool_id is missing, but only a single 'default' pool is specified
	cfg = newCfg(`[{"queue_name":"test-queue","inference_objective":"obj","igw_base_url":"http://gw"}]`)
	_, err = NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id is missing but default pool exists: %v", err)
	}

	// Case 6: worker_pool_id is specified as custom, but only a single 'default' pool is specified
	cfg = newCfg(`[{"queue_name":"test-queue","worker_pool_id":"custom-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`)
	_, err = NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err == nil {
		t.Error("Expected error when worker_pool_id is custom but only default pool exists, got nil")
	}

	// Case 2: worker_pool_id is specified but pool does not exist
	cfg = newCfg(`[{"queue_name":"test-queue","worker_pool_id":"non-existent","inference_objective":"obj","igw_base_url":"http://gw"}]`)
	_, err = NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}, nil)
	if err == nil {
		t.Error("Expected error when specified worker_pool_id does not exist, got nil")
	}

	// Case 3: worker_pool_id specified and pool exists, but igw_base_url is missing
	cfg = newCfg(`[{"queue_name":"test-queue","worker_pool_id":"test-pool","inference_objective":"obj"}]`)
	_, err = NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}, nil)
	if err == nil {
		t.Error("Expected error when igw_base_url is missing in queue config, got nil")
	}

	// Case 4: worker_pool_id and igw_base_url specified and pool exists
	cfg = newCfg(`[{"queue_name":"test-queue","worker_pool_id":"test-pool","inference_objective":"obj","igw_base_url":"http://gw"}]`)
	_, err = NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "test-pool", Workers: 1}}, nil)
	if err != nil {
		t.Errorf("Unexpected error when worker_pool_id exists: %v", err)
	}
}

// TestNewRedisSortedSetFlow_DefaultsWorkerPoolIDInConfigMap checks that a queue
// config with no worker_pool_id is normalized before it is stored: configMap is
// the source of the pool_name label on async_dispatch_budget and
// async_gate_decisions_total, and it used to keep the raw "" while the request
// channel — and every pool-keyed series — said "default" (issue #369).
func TestNewRedisSortedSetFlow_DefaultsWorkerPoolIDInConfigMap(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	cfg := SortedSetConfig{
		URL:            "redis://" + s.Addr(),
		PollIntervalMs: 1000,
		BatchSize:      10,
		Queues: []SortedSetQueueConfig{
			{ID: "q1", QueueName: "test-queue", InferenceObjective: "obj", IGWBaseURL: "http://gw"},
		},
	}
	cfg.ApplyDefaults()

	flow, err := NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err != nil {
		t.Fatalf("Unexpected error creating flow: %v", err)
	}

	qcfg, ok := flow.configMap["q1"]
	if !ok {
		t.Fatal("Expected queue q1 in configMap")
	}
	if qcfg.WorkerPoolID != "default" {
		t.Errorf("configMap worker pool = %q, want %q", qcfg.WorkerPoolID, "default")
	}
	if got := flow.queues[flow.queueOrder[0]].data.channel.WorkerPoolID; got != qcfg.WorkerPoolID {
		t.Errorf("request channel worker pool = %q, configMap says %q; the two must agree or the metrics do not join", got, qcfg.WorkerPoolID)
	}
}

// TestNewRedisSortedSetFlow_RetryFallsBackToFirstQueue is a regression test: a
// retry message that carries no RequestQueueName must be destined for a real
// queue key, not "". NewRedisSortedSetFlow seeds defaultRequestQueueName from
// the first configured queue; without that seed flushRetryBatch would stamp ""
// into the parked retry's envelope and the mover would re-enter it on the
// empty key, losing the message.
func TestNewRedisSortedSetFlow_RetryFallsBackToFirstQueue(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	const primaryQueue = "primary-queue"
	cfg := SortedSetConfig{
		URL:            "redis://" + s.Addr(),
		PollIntervalMs: 1000,
		BatchSize:      10,
		Queues: []SortedSetQueueConfig{
			{ID: "q1", QueueName: primaryQueue, InferenceObjective: "obj", IGWBaseURL: "http://gw"},
		},
	}
	cfg.ApplyDefaults()

	flow, err := NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err != nil {
		t.Fatalf("Unexpected error creating flow: %v", err)
	}
	defer flow.rdb.Close() // nolint:errcheck

	// RetryMessage with no RequestQueueName in its routing.
	retryMsg := pipeline.RetryMessage{
		EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{
			InternalRequest: api.NewInternalRequest(
				api.InternalRouting{RetryCount: 1},
				&api.RequestMessage{ID: "retry-no-queue", Created: time.Now().Unix(), Deadline: 9999999999},
			),
		},
		BackoffDurationSeconds: 0,
	}

	flow.flushRetryBatch(context.Background(), []pipeline.RetryMessage{retryMsg})

	// The retry parks in the retry queue with the seeded queue name stamped
	// into its envelope, so the mover re-enters it on a real key.
	members, _ := flow.rdb.ZRange(context.Background(), flow.retryQueue(), 0, -1).Result()
	if len(members) != 1 {
		t.Fatalf("Expected 1 parked retry in %q, got %d", flow.retryQueue(), len(members))
	}
	var ir api.InternalRequest
	if err := json.Unmarshal([]byte(members[0]), &ir); err != nil {
		t.Fatalf("Failed to parse parked retry: %v", err)
	}
	if ir.RequestQueueName != primaryQueue {
		t.Errorf("Parked retry envelope queue = %q, want %q", ir.RequestQueueName, primaryQueue)
	}
	if n, _ := flow.rdb.ZCard(context.Background(), "").Result(); n != 0 {
		t.Errorf("Retry landed on the empty key \"\" (ZCARD=%d); default request queue was not seeded", n)
	}
}

func TestQueueBacklog(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		queues: map[string]*queueRuntime{
			"a": {data: requestChannelData{queueName: "queue-a", queueID: "a"}},
			"b": {data: requestChannelData{queueName: "queue-b", queueID: "b"}},
		},
		queueOrder: []string{"a", "b"},
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

	got := make(map[string]pipeline.QueueBacklogStat, len(stats))
	for _, s := range stats {
		got[s.QueueName] = s
	}
	if got["queue-a"].Depth != 3 || !got["queue-a"].SourceAvailable {
		t.Errorf("queue-a backlog = %+v, want depth 3 from an available source", got["queue-a"])
	}
	if got["queue-b"].Depth != 1 || !got["queue-b"].SourceAvailable {
		t.Errorf("queue-b backlog = %+v, want depth 1 from an available source", got["queue-b"])
	}
}

func TestQueueBacklogDeadlineViews(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		queues: map[string]*queueRuntime{
			"a": {data: requestChannelData{queueName: "queue-a", queueID: "a"}},
		},
		queueOrder: []string{"a"},
	}

	now := time.Now().Unix()
	// Relative offsets (seconds): two expired (one exactly at now, which must
	// count in le="0" and therefore in every higher cumulative bucket too),
	// then one item per bucket span. Offsets keep at least 5s of margin from
	// every bucket boundary (0, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600,
	// 7200, ...) because QueueBacklog reads ZCOUNT against its own clock,
	// which may be up to a second ahead of the test's.
	offsets := []int64{-10, 0, 10, 40, 100, 400, 1000, 4000}
	for i, off := range offsets {
		rdb.ZAdd(ctx, "queue-a", redis.Z{Score: float64(now + off), Member: fmt.Sprintf("m%d", i)})
	}

	stats, err := flow.QueueBacklog(ctx)
	if err != nil {
		t.Fatalf("QueueBacklog returned error: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("len(stats) = %d, want 1", len(stats))
	}
	s := stats[0]
	if s.Depth != 8 {
		t.Errorf("Depth = %d, want 8", s.Depth)
	}

	// Bucket counts are cumulative le semantics: bucket b counts every item
	// with deadline <= now+b. The offset-0 item is expired (le="0") and so
	// appears in every bucket.
	wantLabels := metrics.DeadlineProximityBucketLabels()
	if len(s.ExpiringCounts) != len(wantLabels) {
		t.Fatalf("len(ExpiringCounts) = %d, want %d", len(s.ExpiringCounts), len(wantLabels))
	}
	wantCounts := map[string]int64{
		"0":        2, // -10, 0
		"1000":     2,
		"5000":     2,
		"15000":    3, // +10
		"30000":    3,
		"60000":    4, // +40
		"120000":   5, // +100
		"300000":   5,
		"600000":   6, // +400
		"1800000":  7, // +1000
		"3600000":  7,
		"7200000":  8, // +4000
		"21600000": 8,
		"86400000": 8,
	}
	gotCounts := make(map[string]int64, len(s.ExpiringCounts))
	for i, ec := range s.ExpiringCounts {
		if ec.Window != wantLabels[i] {
			t.Errorf("ExpiringCounts[%d].Window = %q, want %q (bucket order)", i, ec.Window, wantLabels[i])
		}
		gotCounts[ec.Window] = ec.Count
	}
	for window, want := range wantCounts {
		if gotCounts[window] != want {
			t.Errorf("ExpiringCounts[%q] = %d, want %d", window, gotCounts[window], want)
		}
	}
}

// Bucket counts are exact regardless of queue size: ZCOUNT compares only
// scores, so a large queue costs the same to read as a small one.
func TestQueueBacklogDeadlineCountsAtScale(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		queues: map[string]*queueRuntime{
			"big": {data: requestChannelData{queueName: "queue-big", queueID: "big"}},
		},
		queueOrder: []string{"big"},
	}

	now := time.Now().Unix()
	// Seed from now+100s so no item's score can collide with the poller's own
	// `now` (an item whose deadline equals the current second is expired).
	for i := 0; i < 1002; i++ {
		rdb.ZAdd(ctx, "queue-big", redis.Z{Score: float64(now + 100 + int64(i)), Member: fmt.Sprintf("m%d", i)})
	}

	stats, err := flow.QueueBacklog(ctx)
	if err != nil {
		t.Fatalf("QueueBacklog returned error: %v", err)
	}
	s := stats[0]
	if s.Depth != 1002 {
		t.Errorf("Depth = %d, want 1002", s.Depth)
	}
	// All items are within 24h of now, so the final cumulative bucket holds
	// every one of them; the expired bucket is empty.
	for _, ec := range s.ExpiringCounts {
		switch ec.Window {
		case "86400000":
			if ec.Count != 1002 {
				t.Errorf("24h bucket count = %d, want 1002 (counts must not be capped)", ec.Count)
			}
		case "0":
			if ec.Count != 0 {
				t.Errorf("expired bucket count = %d, want 0", ec.Count)
			}
		}
	}
}

// TestQueueBacklogReportsZeroOnError verifies that a per-queue failure reports a
// 0 sentinel marked unavailable (rather than skipping it) so the gauge does not
// retain a stale value or look like a trustworthy empty queue, while healthy
// queues still report their real depth.
func TestQueueBacklogReportsZeroOnError(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer rdb.Close() // nolint:errcheck
	defer cancel()

	flow := &RedisSortedSetFlow{
		rdb: rdb,
		queues: map[string]*queueRuntime{
			"ok":  {data: requestChannelData{queueName: "queue-ok", queueID: "ok"}},
			"bad": {data: requestChannelData{queueName: "queue-bad", queueID: "bad"}},
		},
		queueOrder: []string{"ok", "bad"},
	}

	rdb.ZAdd(ctx, "queue-ok", redis.Z{Score: 0, Member: "m1"})
	// Make queue-bad a string so ZCard fails with WRONGTYPE.
	rdb.Set(ctx, "queue-bad", "not-a-sorted-set", 0)

	stats, err := flow.QueueBacklog(ctx)
	if err == nil {
		t.Fatal("QueueBacklog expected an error for the WRONGTYPE queue, got nil")
	}

	got := make(map[string]pipeline.QueueBacklogStat, len(stats))
	for _, s := range stats {
		got[s.QueueName] = s
	}
	if _, ok := got["queue-bad"]; !ok {
		t.Error("queue-bad missing from stats; want a 0 sentinel instead of being skipped")
	}
	if got["queue-bad"].Depth != 0 || got["queue-bad"].SourceAvailable {
		t.Errorf("queue-bad backlog = %+v, want unavailable depth 0", got["queue-bad"])
	}
	if got["queue-ok"].Depth != 1 || !got["queue-ok"].SourceAvailable {
		t.Errorf("queue-ok backlog = %+v, want available depth 1", got["queue-ok"])
	}

	// A failed read must leave the expiring counts zeroed with every bucket
	// present, so the deadline-proximity snapshot is cleared rather than left
	// stale.
	for _, s := range stats {
		if s.QueueName == "queue-bad" {
			if s.ExpiringCounts == nil {
				t.Fatal("queue-bad expiring counts must be non-nil on ZCard failure")
			}
			for _, ec := range s.ExpiringCounts {
				if ec.Count != 0 {
					t.Errorf("queue-bad bucket %q count = %d, want 0", ec.Window, ec.Count)
				}
			}
			if got := len(s.ExpiringCounts); got != len(metrics.DeadlineProximityBucketLabels()) {
				t.Errorf("queue-bad expiring count buckets = %d, want %d", got, len(metrics.DeadlineProximityBucketLabels()))
			}
		}
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
		queues: map[string]*queueRuntime{
			queueID: {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)},
				queueName: queue,
				queueID:   queueID,
			}},
		},
		queueOrder:   []string{queueID},
		pollInterval: 50 * time.Millisecond,
		batchSize:    10,
		gate:         noopGate(),
		configMap: map[string]SortedSetQueueConfig{
			queueID: {
				ID:     queueID,
				Labels: labels,
			},
		},
	}

	msg := api.RequestMessage{ID: "msg-labels", Created: time.Now().Unix(), Deadline: 9999999999}
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: envelopeJSON(msg)})

	go flow.requestWorker(ctx, flow.queues[flow.queueOrder[0]])

	select {
	case received := <-flow.queues[flow.queueOrder[0]].data.channel.Channel:
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

func testPayload(m map[string]any) json.RawMessage {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}
