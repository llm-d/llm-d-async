package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	redislib "github.com/redis/go-redis/v9"
)

var reconfigurePools = []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}

func reconfigureTestFlow(t *testing.T, s *miniredis.Miniredis, queues ...SortedSetQueueConfig) *RedisSortedSetFlow {
	t.Helper()
	flow, err := NewRedisSortedSetFlow(SortedSetConfig{
		URL:            "redis://" + s.Addr(),
		PollIntervalMs: 10,
		BatchSize:      5,
		Queues:         queues,
	}, reconfigurePools, nil)
	if err != nil {
		t.Fatalf("failed to create flow: %v", err)
	}
	return flow
}

func testQueue(id string) SortedSetQueueConfig {
	return SortedSetQueueConfig{ID: id, QueueName: id + "-queue", IGWBaseURL: "http://gw"}
}

// pushTestMessage enqueues one request into the named sorted set with a
// future deadline, the same shape the frontend writes.
func pushTestMessage(t *testing.T, flow *RedisSortedSetFlow, ctx context.Context, queue, id string) {
	t.Helper()
	ir := api.NewInternalRequest(api.InternalRouting{RequestToken: "tok"}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: time.Now().Add(time.Hour).Unix(),
	})
	member, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := flow.rdb.ZAdd(ctx, queue, redislib.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: string(member)}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}
}

// awaitMessage waits for a request to arrive on the channel with the given ID.
func awaitMessage(t *testing.T, ch chan *api.InternalRequest, wantID string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ir := <-ch:
			if ir.PublicRequest.ReqID() == wantID {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message %q", wantID)
		}
	}
}

func TestReconfigureQueues_AddNewQueueStartsConsuming(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))

	ctx := context.Background()
	flow.Start(ctx)
	defer flow.StopConsuming()
	defer flow.Shutdown()

	result, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1"), testQueue("q2")}, nil)
	if err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}
	if len(result.Added) != 1 || len(result.Removed) != 0 {
		t.Fatalf("expected 1 added, 0 removed, got %d/%d", len(result.Added), len(result.Removed))
	}
	if result.Added[0].WorkerPoolID != "default" {
		t.Fatalf("expected normalized default pool, got %q", result.Added[0].WorkerPoolID)
	}

	pushTestMessage(t, flow, ctx, "q2-queue", "msg-on-q2")
	awaitMessage(t, result.Added[0].Channel, "msg-on-q2")

	// The original queue still consumes on its own channel.
	q1 := flow.queues["q1"].data.channel
	pushTestMessage(t, flow, ctx, "q1-queue", "msg-on-q1")
	awaitMessage(t, q1.Channel, "msg-on-q1")
}

func TestReconfigureQueues_RemoveStopsConsumingAndCloses(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"), testQueue("q2"))

	ctx := context.Background()
	flow.Start(ctx)
	defer flow.StopConsuming()
	defer flow.Shutdown()

	q1Channel := flow.queues["q1"].data.channel.Channel
	result, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q2")}, nil)
	if err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}
	if len(result.Added) != 0 || len(result.Removed) != 1 {
		t.Fatalf("expected 0 added, 1 removed, got %d/%d", len(result.Added), len(result.Removed))
	}

	// The removed queue's channel must be closed so merge policies drop it.
	select {
	case _, ok := <-q1Channel:
		if ok {
			t.Fatal("expected removed queue's channel to be closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for removed queue's channel to close")
	}

	// Backlog of the removed queue stays in Redis, unconsumed; the kept
	// queue still delivers.
	if depth, err := flow.rdb.ZCard(ctx, "q1-queue").Result(); err == nil && depth != 0 {
		t.Fatalf("unexpected q1 backlog: %d", depth)
	}
	pushTestMessage(t, flow, ctx, "q1-queue", "orphan")
	pushTestMessage(t, flow, ctx, "q2-queue", "still-served")
	q2Channel := flow.queues["q2"].data.channel.Channel
	awaitMessage(t, q2Channel, "still-served")
	if depth, err := flow.rdb.ZCard(ctx, "q1-queue").Result(); err != nil || depth != 1 {
		t.Fatalf("removed queue must hold its backlog, got depth=%d err=%v", depth, err)
	}

	// Re-adding the same ID must produce a fresh channel that consumes the
	// preserved backlog.
	result2, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1"), testQueue("q2")}, nil)
	if err != nil {
		t.Fatalf("re-add failed: %v", err)
	}
	awaitMessage(t, result2.Added[0].Channel, "orphan")
}

func TestReconfigureQueues_ModifyReplacesChannel(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))

	ctx := context.Background()
	flow.Start(ctx)
	defer flow.StopConsuming()
	defer flow.Shutdown()

	oldChannel := flow.queues["q1"].data.channel.Channel
	modified := testQueue("q1")
	modified.InferenceObjective = "new-objective"

	result, err := flow.ReconfigureQueues([]SortedSetQueueConfig{modified}, nil)
	if err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}
	if len(result.Added) != 1 || len(result.Removed) != 1 {
		t.Fatalf("expected 1 added, 1 removed, got %d/%d", len(result.Added), len(result.Removed))
	}
	if result.Added[0].InferenceObjective != "new-objective" {
		t.Fatalf("expected new objective, got %q", result.Added[0].InferenceObjective)
	}

	select {
	case _, ok := <-oldChannel:
		if ok {
			t.Fatal("expected replaced channel to be closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for replaced channel to close")
	}

	pushTestMessage(t, flow, ctx, "q1-queue", "after-modify")
	awaitMessage(t, result.Added[0].Channel, "after-modify")
}

func TestReconfigureQueues_InFlightResultKeepsOriginalRouting(t *testing.T) {
	s := miniredis.RunT(t)
	original := testQueue("q1")
	original.ResultQueueName = "results-old"
	original.ResultTTLSeconds = 60
	flow := reconfigureTestFlow(t, s, original)
	defer flow.rdb.Close() // nolint:errcheck

	ctx := context.Background()
	pushTestMessage(t, flow, ctx, original.QueueName, "in-flight")
	msgChannel := make(chan *api.InternalRequest, 1)
	flow.processMessages(ctx, msgChannel, original.QueueName, original.ID, pipeline.ConstOpenGate(), logr.Discard())
	ir := <-msgChannel

	modified := original
	modified.ResultQueueName = "results-new"
	modified.ResultTTLSeconds = 120
	if _, err := flow.ReconfigureQueues([]SortedSetQueueConfig{modified}, nil); err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}

	flow.flushResultBatch(ctx, []api.ResultMessage{{
		ID:      ir.PublicRequest.ReqID(),
		Routing: ir.InternalRouting,
	}})
	if n, _ := flow.rdb.LLen(ctx, "results-old").Result(); n != 1 {
		t.Fatalf("in-flight result was not routed to original queue, got %d", n)
	}
	if n, _ := flow.rdb.LLen(ctx, "results-new").Result(); n != 0 {
		t.Fatalf("in-flight result leaked to new queue, got %d", n)
	}
	if ttl := s.TTL("results-old"); ttl <= 0 || ttl > 60*time.Second {
		t.Fatalf("in-flight result did not keep original TTL, got %v", ttl)
	}
}

func TestReconfigureQueues_InvalidConfigKeepsLastGood(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))

	ctx := context.Background()
	flow.Start(ctx)
	defer flow.StopConsuming()
	defer flow.Shutdown()

	cases := []struct {
		name   string
		queues []SortedSetQueueConfig
	}{
		{"duplicate queue names", []SortedSetQueueConfig{
			{ID: "a", QueueName: "same", IGWBaseURL: "http://gw"},
			{ID: "b", QueueName: "same", IGWBaseURL: "http://gw"},
		}},
		{"duplicate ids", []SortedSetQueueConfig{
			{ID: "same", QueueName: "a", IGWBaseURL: "http://gw"},
			{ID: "same", QueueName: "b", IGWBaseURL: "http://gw"},
		}},
		{"missing igw_base_url", []SortedSetQueueConfig{{ID: "a", QueueName: "a"}}},
		{"unknown worker pool", []SortedSetQueueConfig{{ID: "a", QueueName: "a", IGWBaseURL: "http://gw", WorkerPoolID: "ghost"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := flow.ReconfigureQueues(tc.queues, nil); err == nil {
				t.Fatal("expected validation error")
			}
			// The live queue set must be untouched.
			channels := flow.RequestChannels()
			if len(channels) != 1 {
				t.Fatalf("last good registry mutated: %d channels", len(channels))
			}
			if cfg, ok := flow.queueConfigOf("q1"); !ok || cfg.QueueName != "q1-queue" {
				t.Fatalf("q1 config lost: %+v ok=%v", cfg, ok)
			}
		})
	}

	pushTestMessage(t, flow, ctx, "q1-queue", "last-good")
	awaitMessage(t, flow.RequestChannels()[0].Channel, "last-good")
}

func TestReconfigureQueues_CallbackErrorKeepsLiveRegistry(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))
	flow.Start(context.Background())
	defer flow.StopConsuming()
	defer flow.Shutdown()

	old := flow.RequestChannels()[0]
	wantErr := errors.New("policy unavailable")
	result, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1"), testQueue("q2")}, func(added []pipeline.RequestChannel) error {
		if len(added) != 1 || added[0].WorkerPoolID != "default" || added[0].IGWBaseURL != "http://gw" {
			t.Fatalf("callback saw wrong added channels: %+v", added)
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if len(result.Added) != 0 || len(result.Removed) != 0 {
		t.Fatalf("failed callback returned result: %+v", result)
	}
	channels := flow.RequestChannels()
	if len(channels) != 1 || channels[0].Channel != old.Channel {
		t.Fatalf("live registry changed after callback failure: %+v", channels)
	}
	pushTestMessage(t, flow, context.Background(), "q1-queue", "still-live")
	awaitMessage(t, old.Channel, "still-live")
}

func TestReconfigureQueues_CallbackSeesAddedBeforeCommit(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))
	flow.Start(context.Background())
	defer flow.StopConsuming()
	defer flow.Shutdown()

	callbackCalled := false
	result, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1"), testQueue("q2")}, func(added []pipeline.RequestChannel) error {
		callbackCalled = true
		if len(added) != 1 || added[0].WorkerPoolID != "default" {
			t.Fatalf("callback saw wrong added channels: %+v", added)
		}
		return nil
	})
	if err != nil || !callbackCalled {
		t.Fatalf("reconfigure callback result: called=%v err=%v", callbackCalled, err)
	}
	if len(result.Added) != 1 || len(flow.RequestChannels()) != 2 {
		t.Fatalf("successful callback did not commit: result=%+v channels=%d", result, len(flow.RequestChannels()))
	}
}

func TestReconfigureQueues_WaitingWorkerIsReleasedByStopConsuming(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))
	flow.Start(context.Background())
	defer flow.Shutdown()

	old := flow.queues["q1"]
	cancelCalled := make(chan struct{})
	old.cancel = func() {
		close(cancelCalled)
	}
	reconfigureDone := make(chan error, 1)
	go func() {
		_, err := flow.ReconfigureQueues(nil, nil)
		reconfigureDone <- err
	}()
	select {
	case <-cancelCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("reconfigure did not begin draining the removed worker")
	}

	stopDone := make(chan struct{})
	go func() {
		flow.StopConsuming()
		close(stopDone)
	}()
	select {
	case err := <-reconfigureDone:
		if err != nil {
			t.Fatalf("reconfigure failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reconfigure remained blocked after StopConsuming")
	}
	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("StopConsuming did not return")
	}
}

func TestReconfigureQueues_IdenticalConfigIsNoop(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))

	ctx := context.Background()
	flow.Start(ctx)
	defer flow.StopConsuming()
	defer flow.Shutdown()

	result, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1")}, nil)
	if err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}
	if len(result.Added) != 0 || len(result.Removed) != 0 {
		t.Fatalf("identical config should be a no-op, got added=%d removed=%d", len(result.Added), len(result.Removed))
	}
}

func TestReconfigureQueues_EmptyConfigDrainsAll(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))

	ctx := context.Background()
	flow.Start(ctx)
	defer flow.StopConsuming()
	defer flow.Shutdown()

	result, err := flow.ReconfigureQueues(nil, nil)
	if err != nil {
		t.Fatalf("reconfigure failed: %v", err)
	}
	if len(result.Removed) != 1 {
		t.Fatalf("expected 1 removed, got %d", len(result.Removed))
	}
	if got := len(flow.RequestChannels()); got != 0 {
		t.Fatalf("expected empty registry, got %d", got)
	}

	// Empty is not a dead end: queues can still come back.
	result2, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1")}, nil)
	if err != nil || len(result2.Added) != 1 {
		t.Fatalf("re-add after drain failed: added=%d err=%v", len(result2.Added), err)
	}
	pushTestMessage(t, flow, ctx, "q1-queue", "revived")
	awaitMessage(t, result2.Added[0].Channel, "revived")
}

func TestReconfigureQueues_AfterStopConsumingFails(t *testing.T) {
	s := miniredis.RunT(t)
	flow := reconfigureTestFlow(t, s, testQueue("q1"))

	ctx := context.Background()
	flow.Start(ctx)
	flow.StopConsuming()
	defer flow.Shutdown()

	if _, err := flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q2")}, nil); !errors.Is(err, errFlowStopped) {
		t.Fatalf("expected errFlowStopped, got %v", err)
	}
}

func TestReconfigureQueues_RaceWithStopConsuming(t *testing.T) {
	for i := range 20 {
		t.Run(fmt.Sprintf("iter-%d", i), func(t *testing.T) {
			s := miniredis.RunT(t)
			flow := reconfigureTestFlow(t, s, testQueue("q1"))

			ctx := context.Background()
			flow.Start(ctx)

			stopDone := make(chan struct{})
			go func() {
				flow.StopConsuming()
				close(stopDone)
			}()
			// Either succeeds before stop or fails with errFlowStopped —
			// both are legal; the run must not panic, deadlock or race.
			go func() {
				_, _ = flow.ReconfigureQueues([]SortedSetQueueConfig{testQueue("q1")}, nil)
			}()
			<-stopDone
			flow.Shutdown()
		})
	}
}
