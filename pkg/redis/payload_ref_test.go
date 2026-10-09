package redis

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func pointerRequest(t *testing.T, id string, deadline int64, payload string) (*api.InternalRequest, string) {
	t.Helper()
	ir := api.NewInternalRequest(api.InternalRouting{RequestToken: "gen-" + id}, &api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  json.RawMessage(payload),
	})
	ir.PayloadRef = api.RequestPayloadKey(id, ir.RequestToken)
	envelope, _, err := api.SplitPayload(ir)
	if err != nil {
		t.Fatal(err)
	}
	return ir, string(envelope)
}

func TestFetchPayloads_AttachesReferencedPayloads(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	flow := &RedisSortedSetFlow{rdb: rdb}
	future := time.Now().Add(time.Hour).Unix()

	present, _ := pointerRequest(t, "present", future, `{"prompt":"present"}`)
	missing, _ := pointerRequest(t, "missing", future, `{"prompt":"missing"}`)
	if err := rdb.Set(ctx, present.PayloadRef, `{"prompt":"present"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}

	errs, err := flow.fetchPayloads(ctx, []claimedRequest{{ir: present}, {ir: missing}})
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != "" || string(present.PublicRequest.ReqPayload()) != `{"prompt":"present"}` {
		t.Errorf("present: err=%q payload=%s", errs[0], present.PublicRequest.ReqPayload())
	}
	if errs[1] == "" {
		t.Error("missing: want a payload error")
	}
}

func TestFetchPayloads_FetchErrorFailsTheBatch(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	flow := &RedisSortedSetFlow{rdb: rdb}
	ir, _ := pointerRequest(t, "r", time.Now().Add(time.Hour).Unix(), `{}`)
	s.Close()
	if _, err := flow.fetchPayloads(ctx, []claimedRequest{{ir: ir}}); err == nil {
		t.Fatal("want an error when payloads cannot be fetched")
	}
}

type commandLog struct {
	mu    sync.Mutex
	names []string
}

func (l *commandLog) DialHook(next redis.DialHook) redis.DialHook { return next }

func (l *commandLog) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		l.record(cmd)
		return next(ctx, cmd)
	}
}

func (l *commandLog) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			l.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func (l *commandLog) record(cmd redis.Cmder) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.names = append(l.names, cmd.Name())
}

func (l *commandLog) count(name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, got := range l.names {
		if got == name {
			n++
		}
	}
	return n
}

type refuseGate struct{}

func (refuseGate) Budget(context.Context) float64 { return 1 }

func (refuseGate) Apply(context.Context, *api.InternalRequest, *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	return pipeline.Refuse(), nil
}

func queuePointerRequest(t *testing.T, ctx context.Context, rdb *redis.Client, queue, id, payload string) *api.InternalRequest {
	t.Helper()
	ir, member := pointerRequest(t, id, time.Now().Add(time.Hour).Unix(), payload)
	if err := rdb.Set(ctx, ir.PayloadRef, payload, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: member}).Err(); err != nil {
		t.Fatal(err)
	}
	return ir
}

func TestSortedSetFlow_RefusedRequestDoesNotFetchItsPayload(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	log := &commandLog{}
	rdb.AddHook(log)
	queue := "pointer-queue"
	flow := &RedisSortedSetFlow{rdb: rdb, batchSize: 10, claimLeaseTTL: time.Minute, resultChannel: make(chan api.ResultMessage, 1)}
	queuePointerRequest(t, ctx, rdb, queue, "held", `{"prompt":"held"}`)
	msgs := make(chan *api.InternalRequest, 1)

	for range 3 {
		flow.processMessagesWithConfig(ctx, msgs, queue, "q", refuseGate{}, logr.Discard(), SortedSetQueueConfig{})
	}

	if n := log.count("mget"); n != 0 {
		t.Fatalf("MGET issued %d times for a refused request", n)
	}
	if len(msgs) != 0 || len(flow.resultChannel) != 0 {
		t.Fatal("a refused request produced an outcome")
	}
	if n, _ := rdb.ZCard(ctx, queue).Result(); n != 1 {
		t.Fatalf("pending = %d, want the refused request left queued", n)
	}
}

func TestSortedSetFlow_CancelledRequestDoesNotFetchItsPayload(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	log := &commandLog{}
	rdb.AddHook(log)
	queue := "pointer-queue"
	flow := &RedisSortedSetFlow{
		rdb:                 rdb,
		batchSize:           10,
		claimLeaseTTL:       time.Minute,
		resultChannel:       make(chan api.ResultMessage, 1),
		cancellationChecker: &stubFlowCancellationChecker{cancelled: true},
	}
	queuePointerRequest(t, ctx, rdb, queue, "gone", `{"prompt":"gone"}`)
	msgs := make(chan *api.InternalRequest, 1)

	flow.processMessagesWithConfig(ctx, msgs, queue, "q", noopGate(), logr.Discard(), SortedSetQueueConfig{})

	if n := log.count("mget"); n != 0 {
		t.Fatalf("MGET issued %d times for a cancelled request", n)
	}
	select {
	case res := <-flow.resultChannel:
		if res.ErrorCode != api.ErrCodeCancelled {
			t.Fatalf("result = %+v", res)
		}
	default:
		t.Fatal("no result for the cancelled request")
	}
	if len(msgs) != 0 {
		t.Fatal("a cancelled request was dispatched")
	}
}

func TestSortedSetFlow_FetchesAcceptedPayloadsInOneMGET(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	log := &commandLog{}
	rdb.AddHook(log)
	queue := "pointer-queue"
	flow := &RedisSortedSetFlow{rdb: rdb, batchSize: 10, claimLeaseTTL: time.Minute, resultChannel: make(chan api.ResultMessage, 3)}
	want := map[string]string{}
	for _, id := range []string{"a", "b", "c"} {
		want[id] = `{"prompt":"` + id + `"}`
		queuePointerRequest(t, ctx, rdb, queue, id, want[id])
	}
	msgs := make(chan *api.InternalRequest, 3)

	flow.processMessagesWithConfig(ctx, msgs, queue, "q", noopGate(), logr.Discard(), SortedSetQueueConfig{})

	if n := log.count("mget"); n != 1 {
		t.Fatalf("MGET issued %d times, want 1 for the batch", n)
	}
	if len(msgs) != 3 {
		t.Fatalf("dispatched %d requests, want 3", len(msgs))
	}
	for range 3 {
		got := <-msgs
		id := got.PublicRequest.ReqID()
		if string(got.PublicRequest.ReqPayload()) != want[id] {
			t.Errorf("%s payload = %s, want %s", id, got.PublicRequest.ReqPayload(), want[id])
		}
	}
}

type observeGate struct {
	onApply func(id string)
}

func (observeGate) Budget(context.Context) float64 { return 1 }

func (g observeGate) Apply(_ context.Context, msg *api.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	g.onApply(msg.PublicRequest.ReqID())
	return pipeline.Continue(), nil
}

func TestSortedSetFlow_DispatchesAnInlineRequestBeforeGatingTheNext(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	queue := "inline-queue"
	flow := &RedisSortedSetFlow{rdb: rdb, batchSize: 10, claimLeaseTTL: time.Minute, resultChannel: make(chan api.ResultMessage, 2)}
	now := time.Now()
	for i, id := range []string{"first", "second"} {
		member := envelopeJSON(api.RequestMessage{ID: id, Created: now.Unix(), Deadline: now.Add(time.Hour).Unix(), Payload: json.RawMessage(`{}`)})
		if err := rdb.ZAdd(ctx, queue, redis.Z{Score: float64(now.Unix()) + float64(i), Member: member}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	msgs := make(chan *api.InternalRequest, 1)
	queuedWhenSecondGated := -1
	gate := observeGate{onApply: func(id string) {
		if id == "second" {
			queuedWhenSecondGated = len(msgs)
		}
	}}

	pollCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stop()
	flow.processMessagesWithConfig(pollCtx, msgs, queue, "q", gate, logr.Discard(), SortedSetQueueConfig{})

	if queuedWhenSecondGated != 1 {
		t.Fatalf("%d requests were downstream when the second was gated, want the first already there", queuedWhenSecondGated)
	}
	if got := (<-msgs).PublicRequest.ReqID(); got != "first" {
		t.Fatalf("dispatched %q first", got)
	}
}

func TestSortedSetFlow_ShutdownReleasesClaimsItDidNotDispatch(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	queue := "pointer-queue"
	flow := &RedisSortedSetFlow{rdb: rdb, batchSize: 10, claimLeaseTTL: time.Minute, resultChannel: make(chan api.ResultMessage, 2)}
	first := queuePointerRequest(t, ctx, rdb, queue, "first", `{"prompt":"first"}`)
	second := queuePointerRequest(t, ctx, rdb, queue, "second", `{"prompt":"second"}`)
	msgs := make(chan *api.InternalRequest)

	pollCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		flow.processMessagesWithConfig(pollCtx, msgs, queue, "q", noopGate(), logr.Discard(), SortedSetQueueConfig{})
	}()
	var dispatched *api.InternalRequest
	select {
	case dispatched = <-msgs:
	case <-ctx.Done():
		t.Fatal("nothing was dispatched")
	}
	stop()
	<-done

	undispatched := first
	if dispatched.PublicRequest.ReqID() == "first" {
		undispatched = second
	}
	keys := newClaimKeys(queue)
	if ok, _ := rdb.HExists(ctx, keys.claimed, claimKey(undispatched.PublicRequest.ReqID(), undispatched.RequestToken)).Result(); ok {
		t.Fatal("the undispatched request is still claimed")
	}
	if n, _ := rdb.ZCard(ctx, queue).Result(); n != 1 {
		t.Fatalf("pending = %d, want the undispatched request back in the queue", n)
	}
	if ok, _ := rdb.HExists(ctx, keys.claimed, claimKey(dispatched.PublicRequest.ReqID(), dispatched.RequestToken)).Result(); !ok {
		t.Fatal("the dispatched request lost its claim")
	}
}

func TestSortedSetFlow_DispatchesPointerRequestWithItsPayload(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	queue := "pointer-queue"
	flow := &RedisSortedSetFlow{
		rdb: rdb,
		queues: map[string]*queueRuntime{
			"": {data: requestChannelData{
				channel:   pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1)},
				queueName: queue,
			}},
		},
		queueOrder:    []string{""},
		pollInterval:  10 * time.Millisecond,
		batchSize:     10,
		claimLeaseTTL: time.Minute,
		gate:          noopGate(),
	}
	ir, member := pointerRequest(t, "p1", time.Now().Add(time.Hour).Unix(), `{"prompt":"big"}`)
	if strings.Contains(member, "big") {
		t.Fatalf("queued member carries the payload: %s", member)
	}
	rdb.Set(ctx, ir.PayloadRef, `{"prompt":"big"}`, 0)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: member})

	go flow.requestWorker(ctx, flow.queues[""])

	select {
	case got := <-flow.queues[""].data.channel.Channel:
		if string(got.PublicRequest.ReqPayload()) != `{"prompt":"big"}` {
			t.Fatalf("payload = %s", got.PublicRequest.ReqPayload())
		}
		if got.PayloadRef != ir.PayloadRef {
			t.Fatalf("payload ref = %q, want %q", got.PayloadRef, ir.PayloadRef)
		}
	case <-ctx.Done():
		t.Fatal("request was not dispatched")
	}
	if claimed, _ := rdb.HGet(ctx, newClaimKeys(queue).claimed, claimKey("p1", ir.RequestToken)).Result(); claimed != member {
		t.Fatalf("claimed hash holds %q, want the envelope", claimed)
	}
}

func TestSortedSetFlow_MissingPayloadEndsWithPayloadUnavailable(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	queue := "pointer-queue"
	flow := &RedisSortedSetFlow{
		rdb:           rdb,
		pollInterval:  10 * time.Millisecond,
		batchSize:     10,
		claimLeaseTTL: time.Minute,
		gate:          noopGate(),
		resultChannel: make(chan api.ResultMessage, 1),
	}
	msgs := make(chan *api.InternalRequest, 1)
	ir, member := pointerRequest(t, "gone", time.Now().Add(time.Hour).Unix(), `{"prompt":"gone"}`)
	rdb.ZAdd(ctx, queue, redis.Z{Score: float64(time.Now().Unix()), Member: member})

	flow.processMessagesWithConfig(ctx, msgs, queue, "q", noopGate(), logr.Discard(), SortedSetQueueConfig{})

	select {
	case res := <-flow.resultChannel:
		if res.ID != "gone" || res.ErrorCode != api.ErrCodePayloadUnavailable {
			t.Fatalf("result = %+v", res)
		}
		if res.Routing.PayloadRef != ir.PayloadRef {
			t.Fatalf("result routing lost the payload ref: %q", res.Routing.PayloadRef)
		}
	default:
		t.Fatal("no result for a request whose payload is missing")
	}
	if len(msgs) != 0 {
		t.Fatal("a request without its payload was dispatched")
	}
	if n, _ := rdb.ZCard(ctx, queue).Result(); n != 0 {
		t.Fatalf("pending = %d, want the request claimed", n)
	}
	if ok, _ := rdb.HExists(ctx, newClaimKeys(queue).claimed, claimKey("gone", ir.RequestToken)).Result(); !ok {
		t.Fatal("the request is not claimed, so its result could never be acked")
	}
}

func TestEncodeRequest(t *testing.T) {
	pointer, _ := pointerRequest(t, "p", time.Now().Add(time.Hour).Unix(), `{"prompt":"pointer"}`)
	if err := api.AttachPayload(pointer, json.RawMessage(`{"prompt":"pointer"}`)); err != nil {
		t.Fatal(err)
	}
	b, err := encodeRequest(pointer)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "pointer\"}") {
		t.Fatalf("pointer request encoded its payload: %s", b)
	}
	var decoded api.InternalRequest
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.PayloadRef != pointer.PayloadRef {
		t.Fatalf("payload ref = %q", decoded.PayloadRef)
	}

	inline := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "i", Deadline: 1, Payload: json.RawMessage(`{"prompt":"inline"}`)})
	b, err = encodeRequest(inline)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `{"prompt":"inline"}`) {
		t.Fatalf("inline request lost its payload: %s", b)
	}
}

func TestFlushRetryBatch_ParksOnlyTheEnvelopeOfAPointerRequest(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	flow := &RedisSortedSetFlow{rdb: rdb, retryQueueName: "retry", defaultRequestQueueName: "q", claimLeaseTTL: time.Minute}
	ir, _ := pointerRequest(t, "r1", time.Now().Add(time.Hour).Unix(), `{"prompt":"retry me"}`)
	if err := api.AttachPayload(ir, json.RawMessage(`{"prompt":"retry me"}`)); err != nil {
		t.Fatal(err)
	}
	ir.RequestQueueName = "q"
	rdb.Set(ctx, ir.PayloadRef, `{"prompt":"retry me"}`, 0)

	flow.flushRetryBatch(ctx, []pipeline.RetryMessage{{EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{InternalRequest: ir}}})

	members, err := rdb.ZRange(ctx, "retry", 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("retry members = %v, err = %v", members, err)
	}
	if strings.Contains(members[0], "retry me") {
		t.Fatalf("parked retry carries the payload: %s", members[0])
	}
	if v, _ := rdb.Get(ctx, ir.PayloadRef).Result(); v != `{"prompt":"retry me"}` {
		t.Fatalf("payload key = %q, want it kept for the retry", v)
	}
	var parked api.InternalRequest
	if err := json.Unmarshal([]byte(members[0]), &parked); err != nil {
		t.Fatal(err)
	}
	if parked.PayloadRef != ir.PayloadRef || parked.PublicRequest.ReqID() != "r1" {
		t.Fatalf("parked retry = %+v", parked.InternalRouting)
	}
}

func TestAckResult_DeletesThePayloadOnlyForTheClaimOwner(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)
	ir, member := pointerRequest(t, "c1", testDeadline, `{"prompt":"owned"}`)
	if err := rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member}).Err(); err != nil {
		t.Fatal(err)
	}
	rdb.Set(ctx, ir.PayloadRef, `{"prompt":"owned"}`, 0)
	rdb.Set(ctx, "unrelated", "keep", 0)
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	stale := &RedisSortedSetFlow{rdb: rdb}
	pushed, err := stale.ackResult(ctx, "q", "results", "c1", ir.RequestToken, ir.PayloadRef, `{"id":"c1"}`, 0)
	if err != nil || pushed {
		t.Fatalf("stale ack: pushed=%v err=%v", pushed, err)
	}
	if n, _ := rdb.Exists(ctx, ir.PayloadRef).Result(); n != 1 {
		t.Fatal("a fenced ack deleted the payload")
	}

	pushed, err = flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, ir.PayloadRef, `{"id":"c1"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("owner ack: pushed=%v err=%v", pushed, err)
	}
	if n, _ := rdb.Exists(ctx, ir.PayloadRef).Result(); n != 0 {
		t.Fatal("the owner's ack left the payload behind")
	}
	if v, _ := rdb.Get(ctx, "unrelated").Result(); v != "keep" {
		t.Fatal("ack touched an unrelated key")
	}
}

func TestAckResult_InlineRequestLeavesPayloadKeysAlone(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)
	ir, member := claimEnvelope(t, "c2", testDeadline)
	ir.RequestToken = "gen-c2"
	if err := rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member}).Err(); err != nil {
		t.Fatal(err)
	}
	lookalike := api.RequestPayloadKey("c2", ir.RequestToken)
	rdb.Set(ctx, lookalike, "keep", 0)
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	pushed, err := flow.ackResult(ctx, "q", "results", "c2", ir.RequestToken, "", `{"id":"c2"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("ack: pushed=%v err=%v", pushed, err)
	}
	if v, _ := rdb.Get(ctx, lookalike).Result(); v != "keep" {
		t.Fatal("an ack without a payload ref deleted a payload key")
	}
}
