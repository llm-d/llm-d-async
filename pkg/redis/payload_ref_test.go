package redis

import (
	"encoding/json"
	"strings"
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

func TestLoadRequests_AttachesReferencedPayloads(t *testing.T) {
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	flow := &RedisSortedSetFlow{rdb: rdb}
	future := time.Now().Add(time.Hour).Unix()
	now := float64(time.Now().Unix())

	present, presentMember := pointerRequest(t, "present", future, `{"prompt":"present"}`)
	_, missingMember := pointerRequest(t, "missing", future, `{"prompt":"missing"}`)
	expired, expiredMember := pointerRequest(t, "expired", time.Now().Add(-time.Minute).Unix(), `{"prompt":"expired"}`)
	inlineMember := envelopeJSON(api.RequestMessage{ID: "inline", Created: time.Now().Unix(), Deadline: future, Payload: json.RawMessage(`{"prompt":"inline"}`)})
	if err := rdb.Set(ctx, present.PayloadRef, `{"prompt":"present"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, expired.PayloadRef, `{"prompt":"expired"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}

	zs := []redis.Z{{Member: presentMember}, {Member: missingMember}, {Member: expiredMember}, {Member: inlineMember}, {Member: "not json"}}
	got, err := flow.loadRequests(ctx, zs, now, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(zs) {
		t.Fatalf("got %d entries, want %d", len(got), len(zs))
	}
	for i, p := range got {
		if p.member != zs[i].Member {
			t.Errorf("entry %d member = %q, want the peeked bytes", i, p.member)
		}
	}
	if !got[0].ok || got[0].payloadErr != "" || string(got[0].ir.PublicRequest.ReqPayload()) != `{"prompt":"present"}` {
		t.Errorf("present: ok=%v err=%q payload=%s", got[0].ok, got[0].payloadErr, got[0].ir.PublicRequest.ReqPayload())
	}
	if !got[1].ok || got[1].payloadErr == "" {
		t.Errorf("missing: ok=%v err=%q", got[1].ok, got[1].payloadErr)
	}
	if got[2].payloadErr != "" || string(got[2].ir.PublicRequest.ReqPayload()) == `{"prompt":"expired"}` {
		t.Errorf("expired requests are not fetched: err=%q payload=%s", got[2].payloadErr, got[2].ir.PublicRequest.ReqPayload())
	}
	if !got[3].ok || got[3].payloadErr != "" || string(got[3].ir.PublicRequest.ReqPayload()) != `{"prompt":"inline"}` {
		t.Errorf("inline: ok=%v err=%q payload=%s", got[3].ok, got[3].payloadErr, got[3].ir.PublicRequest.ReqPayload())
	}
	if got[4].ok {
		t.Error("an unparsable member is not ok")
	}
}

func TestLoadRequests_FetchErrorFailsTheBatch(t *testing.T) {
	s, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	flow := &RedisSortedSetFlow{rdb: rdb}
	_, member := pointerRequest(t, "r", time.Now().Add(time.Hour).Unix(), `{}`)
	s.Close()
	if _, err := flow.loadRequests(ctx, []redis.Z{{Member: member}}, float64(time.Now().Unix()), logr.Discard()); err == nil {
		t.Fatal("want an error when payloads cannot be fetched")
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
