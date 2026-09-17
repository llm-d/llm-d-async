package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

// newClaimTestFlow builds a bare flow wired to miniredis with a short lease so
// expiry paths can be exercised without sleeping for seconds.
func newClaimTestFlow(t *testing.T) (*miniredis.Miniredis, *redis.Client, context.Context, *RedisSortedSetFlow) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	flow := &RedisSortedSetFlow{
		rdb:                  rdb,
		gate:                 noopGate(),
		pollInterval:         50 * time.Millisecond,
		batchSize:            10,
		claimLeaseTTL:        200 * time.Millisecond,
		claimReclaimInterval: 50 * time.Millisecond,
		queues: map[string]*queueRuntime{
			"q": {data: requestChannelData{
				queueName: "q",
				queueID:   "q",
			}},
		},
		queueOrder: []string{"q"},
	}
	return s, rdb, context.Background(), flow
}

func claimEnvelope(t *testing.T, id string, deadline int64) (*api.InternalRequest, string) {
	t.Helper()
	ir := api.NewInternalRequest(api.InternalRouting{RequestQueueName: "q"}, &api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  testPayload(map[string]any{"model": "m", "prompt": "p"}),
	})
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return ir, string(b)
}

// Sort score and body deadline deliberately diverge; both sit an hour ahead
// so lease capping never pins expiry to the wall clock mid-test.
var (
	testScore    = float64(time.Now().Add(time.Hour).Unix())
	testDeadline = int64(testScore) + 3600
)

func TestClaimRequest_MovesOutOfPendingAndRejectsDoubleClaim(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	if err := rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member}).Err(); err != nil {
		t.Fatal(err)
	}

	token, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline))
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if token == "" {
		t.Fatal("empty ownership token")
	}
	if n, _ := rdb.ZCard(ctx, "q").Result(); n != 0 {
		t.Fatalf("pending zcard = %d, want 0", n)
	}
	keys := newClaimKeys("q")
	if got, _ := rdb.HGet(ctx, keys.claimed, "c1").Result(); got != member {
		t.Fatalf("claimed payload mismatch")
	}
	if got, _ := rdb.HGet(ctx, keys.owners, "c1").Result(); got != token {
		t.Fatalf("owner token mismatch")
	}

	// Double-claim by another consumer must lose the race: the member is no
	// longer pending.
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); ok || err != nil {
		t.Fatalf("double claim: ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestClaimRequest_SelfOverwriteOnRetryReturn(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}

	// The retry mover re-enters due retries into the pending set while the
	// original claim is still alive; re-claiming must overwrite, not fail.
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("self re-claim: ok=%v err=%v", ok, err)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1").Result(); !exists {
		t.Fatal("payload field missing after self-overwrite")
	}
}

func TestReleaseClaim_RestoresOriginalScoreAndHonorsToken(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	token, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline))
	if !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	// A stale owner must not drop the live claim.
	if err := flow.releaseClaim(ctx, "q", "c1", ir.RequestToken, member, float64(testDeadline), "deadbeef"); err != nil {
		t.Fatalf("stale-token release returned error: %v", err)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1").Result(); !exists {
		t.Fatal("stale token removed a foreign claim")
	}

	if err := flow.releaseClaim(ctx, "q", "c1", ir.RequestToken, member, float64(testDeadline), token); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := rdb.ZScore(ctx, "q", member).Result(); err != nil {
		t.Fatalf("member not restored to pending: %v", err)
	}
	if n, _ := rdb.HLen(ctx, newClaimKeys("q").claimed).Result(); n != 0 {
		t.Fatalf("claimed hash len = %d, want 0", n)
	}
}

func TestAckResult_PushesOnceThenFencesDuplicates(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, "", `{"id":"c1"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("first ack: pushed=%v err=%v", pushed, err)
	}
	pushed, err = flow.ackResult(ctx, "q", "results", "c1", ir.RequestToken, "", `{"id":"c1"}`, 0)
	// Second ack after handle deletion and owner removal must be fenced (returns false, nil).
	if err != nil || pushed {
		t.Fatalf("second ack should be fenced: pushed=%v err=%v, want false/nil", pushed, err)
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 1 {
		t.Fatalf("result list len = %d, want 1", n)
	}
	// Ack must drop the claim so the reclaimer never redelivers it.
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "c1").Result(); exists {
		t.Fatal("claim survived its own ack")
	}
}

func TestAckResult_StaleTokenLeavesForeignClaimIntact(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	// Simulate a claim owned by ANOTHER instance (no local token registered).
	keys := newClaimKeys("q")
	_, member := claimEnvelope(t, "c1", testDeadline)
	rdb.HSet(ctx, keys.claimed, "c1", member)
	rdb.HSet(ctx, keys.owners, "c1", "foreign-token")
	rdb.ZAdd(ctx, keys.idx, redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: "c1"})

	pushed, err := flow.ackResult(ctx, "q", "results", "c1", "", "", `{"id":"c1"}`, 0)
	if err != nil || pushed {
		t.Fatalf("stale ack should be fenced: pushed=%v err=%v", pushed, err)
	}
	if exists, _ := rdb.HExists(ctx, keys.claimed, "c1").Result(); !exists {
		t.Fatal("fenced ack dropped a foreign instance's claim")
	}
	if n, _ := rdb.LLen(ctx, "results").Result(); n != 0 {
		t.Fatalf("fenced ack pushed result, len=%d want 0", n)
	}
}

func TestReclaimExpiredClaims_RedeliversOnlyLapsedLeases(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	// Expired claim: redelivered at its original sort score. A negative lease
	// TTL puts the expiry in the past deterministically (lease scores have
	// whole-second granularity).
	flow.claimLeaseTTL = -2 * time.Second
	irE, memberE := claimEnvelope(t, "expired", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: memberE})
	if _, ok, err := flow.claimRequest(ctx, "q", irE, memberE, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim expired-case: ok=%v err=%v", ok, err)
	}
	// Live claim: untouched.
	flow.claimLeaseTTL = time.Hour
	irL, memberL := claimEnvelope(t, "live", testDeadline)
	flow.claimLeaseTTL = time.Hour
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore + 1, Member: memberL})
	if _, ok, err := flow.claimRequest(ctx, "q", irL, memberL, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim live-case: ok=%v err=%v", ok, err)
	}

	released, err := flow.reclaimExpiredClaims(ctx)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if released != 1 {
		t.Fatalf("released = %d, want 1", released)
	}
	if _, err := rdb.ZScore(ctx, "q", memberE).Result(); err != nil {
		t.Fatalf("expired request not redelivered: %v", err)
	}
	if exists, _ := rdb.HExists(ctx, newClaimKeys("q").claimed, "live").Result(); !exists {
		t.Fatal("live claim was reclaimed")
	}
}

func TestRenewClaim_ExtendsLiveAndIgnoresMissing(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	before, _ := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()
	// Grow the lease so the renewal lands in a later whole-second bucket
	// (lease scores are unix seconds; same-second rewrites would compare equal).
	flow.claimLeaseTTL = time.Hour
	var c1Token string
	claimID := claimKey("c1", ir.RequestToken)
	if v, ok := flow.claimTokens.Load(claimID); ok {
		if h, ok := v.(*claimHandle); ok {
			c1Token = h.token
		}
	}
	if _, err := flow.renewClaim(ctx, "q", "c1", ir.RequestToken, float64(testDeadline), c1Token); err != nil {
		t.Fatalf("renew: %v", err)
	}
	after, _ := rdb.ZScore(ctx, newClaimKeys("q").idx, claimID).Result()
	if after <= before {
		t.Fatalf("lease not extended: before=%f after=%f", before, after)
	}

	// Renewing an unknown request must be a clean no-op (returns 0, no error).
	if res, err := flow.renewClaim(ctx, "q", "ghost", "", float64(testDeadline), "ghost-token"); err != nil {
		t.Fatalf("renew ghost returned error: %v", err)
	} else if res != 0 {
		t.Fatalf("ghost renewal should return 0, got %d", res)
	}
	if _, err := rdb.ZScore(ctx, newClaimKeys("q").idx, "ghost").Result(); err != redis.Nil {
		t.Fatalf("ghost renewal created an index entry (err=%v)", err)
	}
}

func TestClaimExpiry_CapsAtDeadlinePlusGrace(t *testing.T) {
	_, _, _, flow := newClaimTestFlow(t)
	flow.claimLeaseTTL = time.Hour

	now := float64(time.Now().Unix())
	farFuture := now + 24*3600
	expiry := flow.claimExpiry(farFuture)
	if expiry > now+time.Hour.Seconds()+1 {
		t.Fatalf("expiry %f exceeds configured lease past now %f", expiry, now)
	}

	nearExpiry := now + 10
	expiry = flow.claimExpiry(nearExpiry)
	want := nearExpiry + reclaimGraceAfterDeadline.Seconds()
	if expiry != want {
		t.Fatalf("expiry = %f, want deadline+grace %f", expiry, want)
	}
}

func TestHeartbeatClaims_ExtendsLiveLeases(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)

	ir, member := claimEnvelope(t, "c1", testDeadline)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member})
	if _, ok, err := flow.claimRequest(ctx, "q", ir, member, float64(testDeadline)); !ok || err != nil {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	before, _ := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()

	// Grow the lease and heartbeat: the expiry must move out accordingly,
	// proving slow-but-healthy work is not treated as dead.
	flow.claimLeaseTTL = time.Hour
	flow.heartbeatClaims(ctx)
	after, err := rdb.ZScore(ctx, newClaimKeys("q").idx, "c1").Result()
	if err != nil {
		t.Fatalf("heartbeat lost the claim: %v", err)
	}
	if after-before < time.Hour.Seconds()-10 {
		t.Fatalf("lease not meaningfully extended: before=%f after=%f", before, after)
	}
}

func TestClaimRequest_MultipleGenerationsSameReqID_DoNotOverwrite(t *testing.T) {
	_, rdb, ctx, flow := newClaimTestFlow(t)
	keys := newClaimKeys("q")

	// Gen 1
	ir1 := api.NewInternalRequest(api.InternalRouting{RequestQueueName: "q", RequestToken: "token-gen1"}, &api.RequestMessage{
		ID:       "shared-id",
		Created:  time.Now().Unix(),
		Deadline: testDeadline,
	})
	b1, _ := json.Marshal(ir1)
	member1 := string(b1)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member1})

	// Gen 2
	ir2 := api.NewInternalRequest(api.InternalRouting{RequestQueueName: "q", RequestToken: "token-gen2"}, &api.RequestMessage{
		ID:       "shared-id",
		Created:  time.Now().Unix(),
		Deadline: testDeadline,
	})
	b2, _ := json.Marshal(ir2)
	member2 := string(b2)
	rdb.ZAdd(ctx, "q", redis.Z{Score: testScore, Member: member2})

	// Claim Gen 1
	tok1, ok1, err1 := flow.claimRequest(ctx, "q", ir1, member1, float64(testDeadline))
	if !ok1 || err1 != nil {
		t.Fatalf("claim gen1: ok=%v err=%v", ok1, err1)
	}

	// Claim Gen 2
	tok2, ok2, err2 := flow.claimRequest(ctx, "q", ir2, member2, float64(testDeadline))
	if !ok2 || err2 != nil {
		t.Fatalf("claim gen2: ok=%v err=%v", ok2, err2)
	}

	claimID1 := claimKey("shared-id", "token-gen1")
	claimID2 := claimKey("shared-id", "token-gen2")

	// Both must exist independently in Redis
	owner1, _ := rdb.HGet(ctx, keys.owners, claimID1).Result()
	owner2, _ := rdb.HGet(ctx, keys.owners, claimID2).Result()
	if owner1 != tok1 {
		t.Fatalf("owner1 = %q, want %q", owner1, tok1)
	}
	if owner2 != tok2 {
		t.Fatalf("owner2 = %q, want %q", owner2, tok2)
	}
	if tok1 == tok2 {
		t.Fatalf("tokens must differ: %q == %q", tok1, tok2)
	}

	// Ack Gen 1
	pushed1, err := flow.ackResult(ctx, "q", "result-list", "shared-id", "token-gen1", "", `{"id":"shared-id"}`, 0)
	if !pushed1 || err != nil {
		t.Fatalf("ack gen1: pushed=%v err=%v", pushed1, err)
	}

	// Gen 1 must be gone from Redis, but Gen 2 must still be intact
	if exists, _ := rdb.HExists(ctx, keys.claimed, claimID1).Result(); exists {
		t.Fatal("gen1 claim still exists in claimed hash")
	}
	if exists, _ := rdb.HExists(ctx, keys.claimed, claimID2).Result(); !exists {
		t.Fatal("gen2 claim was incorrectly dropped when gen1 was acked")
	}

	// Ack Gen 2
	pushed2, err := flow.ackResult(ctx, "q", "result-list", "shared-id", "token-gen2", "", `{"id":"shared-id"}`, 0)
	if !pushed2 || err != nil {
		t.Fatalf("ack gen2: pushed=%v err=%v", pushed2, err)
	}
	if exists, _ := rdb.HExists(ctx, keys.claimed, claimID2).Result(); exists {
		t.Fatal("gen2 claim still exists in claimed hash after ack")
	}
}
