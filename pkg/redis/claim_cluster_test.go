package redis

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cluster-mode claim tests run against miniredis with the hash-tagged key
// layout forced on. miniredis is a single node, so these verify the layout
// and the cross-slot ack sequence; the slot arithmetic itself is covered in
// keyslot_test.go and a real cluster is exercised by the gated integration
// test in test/integration/redis_cluster_test.go.
func newClusterClaimTestFlow(t *testing.T) (*redis.Client, *RedisSortedSetFlow) {
	t.Helper()
	_, rdb, _, flow := newClaimTestFlow(t)
	flow.clusterKeys = true
	return rdb, flow
}

func TestClusterKeys_ClaimLivesUnderTaggedKeys(t *testing.T) {
	rdb, flow := newClusterClaimTestFlow(t)
	ctx := t.Context()

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	token, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline))
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	tagged := newClaimKeys("q", true)
	if got, _ := rdb.HGet(ctx, "{q}:claimed", "c1").Result(); got != member {
		t.Fatalf("payload not under {q}:claimed (got %q)", got)
	}
	if got, _ := rdb.HGet(ctx, tagged.owners, "c1").Result(); got != token {
		t.Fatal("owner token not under the tagged owners key")
	}
	if n, _ := rdb.Exists(ctx, "q:claimed", "q:claim-owners", "q:claims-idx").Result(); n != 0 {
		t.Fatalf("standalone key names were written in cluster mode (%d present)", n)
	}

	// Release and reclaim must use the same layout, or claims would leak.
	if err := flow.releaseClaim(ctx, "q", "c1", ir.RequestToken, member, float64(testDeadline), token); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb.ZCard(ctx, "q").Result(); n != 1 {
		t.Fatalf("pending zcard after release = %d, want 1", n)
	}
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); err != nil || !ok {
		t.Fatalf("re-claim: ok=%v err=%v", ok, err)
	}
	// Expire the lease by hand and let the reclaimer redeliver it.
	rdb.ZAdd(ctx, tagged.idx, redis.Z{Score: float64(time.Now().Add(-time.Minute).Unix()), Member: "c1"})
	released, err := flow.reclaimExpiredClaims(ctx)
	if err != nil || released != 1 {
		t.Fatalf("reclaim: released=%d err=%v", released, err)
	}
	if n, _ := rdb.ZCard(ctx, "q").Result(); n != 1 {
		t.Fatalf("pending zcard after reclaim = %d, want 1", n)
	}
}

func TestClusterKeys_AckResultCrossSlot_PushesAndDropsClaim(t *testing.T) {
	rdb, flow := newClusterClaimTestFlow(t)
	ctx := t.Context()
	if sameHashSlot("{q}:claim-owners", "results") {
		t.Fatal("test fixture: result list must live in a different slot than the queue")
	}

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, "", `{"id":"c1"}`, time.Hour)
	if err != nil || !pushed {
		t.Fatalf("ack: pushed=%v err=%v", pushed, err)
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 1 {
		t.Fatalf("result list len = %d, want 1", n)
	}
	if ttl, _ := rdb.TTL(ctx, "results").Result(); ttl <= 0 {
		t.Fatalf("result list TTL = %v, want the list TTL applied", ttl)
	}
	tagged := newClaimKeys("q", true)
	for _, k := range []string{tagged.claimed, tagged.owners} {
		if exists, _ := rdb.HExists(ctx, k, "c1").Result(); exists {
			t.Fatalf("claim survived its ack in %s", k)
		}
	}
	if _, err := rdb.ZScore(ctx, tagged.idx, "c1").Result(); err != redis.Nil {
		t.Fatalf("lease index still holds the claim: %v", err)
	}
	if _, held := flow.claimTokens.Load(claimKey("c1", ir.RequestToken)); held {
		t.Fatal("handle not dropped after ack")
	}

	// A second ack is fenced exactly like the atomic path: nothing pushed.
	pushed, err = flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, "", `{"id":"c1"}`, 0)
	if err != nil || pushed {
		t.Fatalf("second ack: pushed=%v err=%v, want false/nil", pushed, err)
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 1 {
		t.Fatalf("duplicate record pushed: list len = %d", n)
	}
}

func TestClusterKeys_AckResultCrossSlot_FencesStolenLease(t *testing.T) {
	rdb, flow := newClusterClaimTestFlow(t)
	ctx := t.Context()

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// Another instance reclaimed and re-claimed the request: the owner token
	// in Redis no longer matches this flow's handle.
	tagged := newClaimKeys("q", true)
	rdb.HSet(ctx, tagged.owners, "c1", "other-owner")

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, "", `{"id":"c1"}`, 0)
	if err != nil || pushed {
		t.Fatalf("stale ack: pushed=%v err=%v, want false/nil", pushed, err)
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 0 {
		t.Fatalf("stale owner pushed a record: list len = %d", n)
	}
	if got, _ := rdb.HGet(ctx, tagged.owners, "c1").Result(); got != "other-owner" {
		t.Fatal("stale owner disturbed the new owner's claim")
	}
	if exists, _ := rdb.HExists(ctx, tagged.claimed, "c1").Result(); !exists {
		t.Fatal("stale owner dropped the new owner's payload")
	}
	if _, held := flow.claimTokens.Load(claimKey("c1", ir.RequestToken)); held {
		t.Fatal("stale handle should be dropped once fenced")
	}
}

func TestClusterKeys_AckResultSameSlotStaysAtomic(t *testing.T) {
	rdb, flow := newClusterClaimTestFlow(t)
	ctx := t.Context()
	const resultList = "{q}:results"
	if !sameHashSlot("{q}:claim-owners", resultList) {
		t.Fatal("test fixture: result list must share the queue's slot")
	}

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	pushed, err := flow.ackResult(ctx, "q", resultList, "c1", ir.RequestToken, "", `{"id":"c1"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("ack: pushed=%v err=%v", pushed, err)
	}
	if n, _ := rdb.LLen(ctx, resultList).Result(); n != 1 {
		t.Fatalf("result list len = %d, want 1", n)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q", true).claimed, "c1").Result(); exists {
		t.Fatal("claim survived its ack")
	}
}

func TestClusterKeys_AckResultDropsThePayloadKeyAfterThePush(t *testing.T) {
	for name, resultList := range map[string]string{"cross-slot list": "results", "same-slot list": "{q}:results"} {
		t.Run(name, func(t *testing.T) {
			rdb, flow := newClusterClaimTestFlow(t)
			ctx := t.Context()
			ir, member := pointerRequest(t, "c1", testDeadline, `{"prompt":"p"}`)
			rdb.Set(ctx, ir.PayloadRef, `{"prompt":"p"}`, 0)
			rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
			if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
				t.Fatalf("claim: ok=%v err=%v", ok, err)
			}

			pushed, err := flow.ackResult(ctx, "q", resultList, "c1", ir.RequestToken, ir.PayloadRef, `{"id":"c1"}`, 0)
			if err != nil || !pushed {
				t.Fatalf("ack: pushed=%v err=%v", pushed, err)
			}
			if n, _ := rdb.LLen(ctx, resultList).Result(); n != 1 {
				t.Fatalf("result list len = %d, want 1", n)
			}
			if n, _ := rdb.Exists(ctx, ir.PayloadRef).Result(); n != 0 {
				t.Fatal("payload key survived the ack")
			}
		})
	}
}

func TestClusterKeys_FencedAckKeepsThePayloadForTheNewOwner(t *testing.T) {
	rdb, flow := newClusterClaimTestFlow(t)
	ctx := t.Context()
	ir, member := pointerRequest(t, "c1", testDeadline, `{"prompt":"p"}`)
	rdb.Set(ctx, ir.PayloadRef, `{"prompt":"p"}`, 0)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	rdb.HSet(ctx, newClaimKeys("q", true).owners, claimKey("c1", ir.RequestToken), "other-owner")

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, ir.PayloadRef, `{"id":"c1"}`, 0)
	if err != nil || pushed {
		t.Fatalf("stale ack: pushed=%v err=%v, want false/nil", pushed, err)
	}
	if n, _ := rdb.Exists(ctx, ir.PayloadRef).Result(); n != 1 {
		t.Fatal("stale owner deleted the payload the new owner still needs")
	}
}

func TestClusterKeys_FetchPayloadsReadsEachKeyOnItsOwn(t *testing.T) {
	// request-payload keys are keyed by request, so in cluster mode they span
	// slots and a single MGET would be a CROSSSLOT error.
	_, rdb, ctx, cancel := setupTest(t)
	defer cancel()
	defer rdb.Close() // nolint:errcheck
	commands := &commandLog{}
	rdb.AddHook(commands)
	flow := &RedisSortedSetFlow{rdb: rdb, clusterKeys: true}
	future := time.Now().Add(time.Hour).Unix()

	present, _ := pointerRequest(t, "present", future, `{"prompt":"present"}`)
	missing, _ := pointerRequest(t, "missing", future, `{"prompt":"missing"}`)
	other, _ := pointerRequest(t, "other", future, `{"prompt":"other"}`)
	rdb.Set(ctx, present.PayloadRef, `{"prompt":"present"}`, 0)
	rdb.Set(ctx, other.PayloadRef, `{"prompt":"other"}`, 0)

	errs, err := flow.fetchPayloads(ctx, []claimedRequest{{ir: present}, {ir: missing}, {ir: other}})
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != "" || string(present.PublicRequest.ReqPayload()) != `{"prompt":"present"}` {
		t.Errorf("present: err=%q payload=%s", errs[0], present.PublicRequest.ReqPayload())
	}
	if errs[1] == "" {
		t.Error("missing: want a payload error")
	}
	if errs[2] != "" || string(other.PublicRequest.ReqPayload()) != `{"prompt":"other"}` {
		t.Errorf("other: err=%q payload=%s", errs[2], other.PublicRequest.ReqPayload())
	}
	if commands.count("mget") != 0 || commands.count("get") != 3 {
		t.Fatalf("want three single-key GETs and no MGET, got %v", commands.names)
	}
}
