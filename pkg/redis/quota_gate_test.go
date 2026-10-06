package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func TestRedisQuotaGate_Concurrency(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	gate := NewRedisQuotaGate(rdb, "userid", QuotaModeConcurrency, 2, 10*time.Second, "quota:")

	ctx := context.Background()
	attrs := map[string]string{"userid": "user1"}

	var releases1 []pipeline.GateReleaseFunc
	// 1st acquisition - Allowed
	msg1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Metadata: attrs})
	verdict1, err := gate.Apply(ctx, msg1, &releases1)
	if err != nil || verdict1.Action != pipeline.ActionContinue || msg1.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict1, msg1.GetClassification(), err)
	}

	var releases2 []pipeline.GateReleaseFunc
	// 2nd acquisition - Allowed
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	verdict2, err := gate.Apply(ctx, msg2, &releases2)
	if err != nil || verdict2.Action != pipeline.ActionContinue || msg2.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict2, msg2.GetClassification(), err)
	}

	var releases3 []pipeline.GateReleaseFunc
	// 3rd acquisition - Denied (Redeliver)
	msg3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req3", Metadata: attrs})
	verdict3, err := gate.Apply(ctx, msg3, &releases3)
	if err != nil || verdict3.Action != pipeline.ActionRefuse || msg3.GetClassification() != api.ClassificationOverflow {
		t.Fatalf("Expected redeliver overflow, got %v classification %v, err: %v", verdict3, msg3.GetClassification(), err)
	}

	// Release one
	for _, f := range releases1 {
		f()
	}

	var releases4 []pipeline.GateReleaseFunc
	// 4th acquisition - Allowed again
	msg4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req4", Metadata: attrs})
	verdict4, err := gate.Apply(ctx, msg4, &releases4)
	if err != nil || verdict4.Action != pipeline.ActionContinue || msg4.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved after release, got %v classification %v, err: %v", verdict4, msg4.GetClassification(), err)
	}

	for _, f := range releases2 {
		f()
	}
}

// TestRedisQuotaGate_Concurrency_TTLRefresh is a regression test for the #311
// sibling over-admission bug: in concurrency mode the counter's TTL was set only
// on the first acquire (new_val==1) and never refreshed, so under sustained load
// the key expired mid-flight, the counter reset to 0, and further requests were
// admitted beyond the limit. Every acquire must refresh the TTL so the counter
// survives as long as there is activity (and only expires after `window` of
// total inactivity, as crash-orphan cleanup).
func TestRedisQuotaGate_Concurrency_TTLRefresh(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	gate := NewRedisQuotaGate(rdb, "userid", QuotaModeConcurrency, 2, 10*time.Second, "quota:")
	ctx := context.Background()
	attrs := map[string]string{"userid": "user1"}

	// Acquire slot 1 (counter=1, TTL=10s); hold it.
	var rel1 []pipeline.GateReleaseFunc
	msg1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Metadata: attrs})
	if v, err := gate.Apply(ctx, msg1, &rel1); err != nil || v.Action != pipeline.ActionContinue {
		t.Fatalf("acquire 1: expected continue, got %v err %v", v, err)
	}

	// Advance below the TTL, then acquire slot 2 — this must refresh the TTL.
	s.FastForward(9 * time.Second)
	var rel2 []pipeline.GateReleaseFunc
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	if v, err := gate.Apply(ctx, msg2, &rel2); err != nil || v.Action != pipeline.ActionContinue {
		t.Fatalf("acquire 2: expected continue, got %v err %v", v, err)
	}

	// Advance past the ORIGINAL first-acquire TTL (18s total > 10s) but within the
	// refreshed TTL. Both slots are still held, so the counter must remain at the
	// limit and a 3rd acquire must be refused. Before the fix the key would have
	// expired at ~10s, resetting the counter and wrongly admitting this request.
	s.FastForward(9 * time.Second)
	var rel3 []pipeline.GateReleaseFunc
	msg3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req3", Metadata: attrs})
	v, err := gate.Apply(ctx, msg3, &rel3)
	if err != nil {
		t.Fatalf("acquire 3: unexpected error %v", err)
	}
	if v.Action != pipeline.ActionRefuse || msg3.GetClassification() != api.ClassificationOverflow {
		t.Fatalf("acquire 3: counter reset mid-flight → over-admission (TTL not refreshed): got %v classification %v",
			v, msg3.GetClassification())
	}

	for _, f := range rel1 {
		f()
	}
	for _, f := range rel2 {
		f()
	}
}

func TestRedisQuotaGate_RateLimit(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	// 2 requests per 1 second
	gate := NewRedisQuotaGate(rdb, "userid", QuotaModeRateLimit, 2, 1*time.Second, "quota:")

	ctx := context.Background()
	attrs := map[string]string{"userid": "user1"}

	var releases1 []pipeline.GateReleaseFunc
	// 1st acquisition - Allowed
	msg1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Metadata: attrs})
	verdict1, err := gate.Apply(ctx, msg1, &releases1)
	if err != nil || verdict1.Action != pipeline.ActionContinue || msg1.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict1, msg1.GetClassification(), err)
	}

	var releases2 []pipeline.GateReleaseFunc
	// 2nd acquisition - Allowed
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	verdict2, err := gate.Apply(ctx, msg2, &releases2)
	if err != nil || verdict2.Action != pipeline.ActionContinue || msg2.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict2, msg2.GetClassification(), err)
	}

	var releases3 []pipeline.GateReleaseFunc
	// 3rd acquisition - Denied
	msg3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req3", Metadata: attrs})
	verdict3, err := gate.Apply(ctx, msg3, &releases3)
	if err != nil || verdict3.Action != pipeline.ActionRefuse || msg3.GetClassification() != api.ClassificationOverflow {
		t.Fatalf("Expected redeliver overflow, got %v classification %v, err: %v", verdict3, msg3.GetClassification(), err)
	}

	// Wait for window to pass
	time.Sleep(1100 * time.Millisecond)

	var releases4 []pipeline.GateReleaseFunc
	// 4th acquisition - Allowed again
	msg4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req4", Metadata: attrs})
	verdict4, err := gate.Apply(ctx, msg4, &releases4)
	if err != nil || verdict4.Action != pipeline.ActionContinue || msg4.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved after window, got %v classification %v, err: %v", verdict4, msg4.GetClassification(), err)
	}
}

func TestRedisQuotaGate_Classifying(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	// 1 request per 10 second, but Classifying mode
	gate := NewRedisQuotaGate(rdb, "userid", QuotaModeRateLimit, 1, 10*time.Second, "quota:").
		WithGatingMode(GatingModeClassifying)

	ctx := context.Background()
	attrs := map[string]string{"userid": "user1"}

	var releases1 []pipeline.GateReleaseFunc
	// 1st acquisition - Allowed and Reserved
	msg1 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Metadata: attrs})
	verdict1, err := gate.Apply(ctx, msg1, &releases1)
	if err != nil || verdict1.Action != pipeline.ActionContinue || msg1.GetClassification() != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict1, msg1.GetClassification(), err)
	}

	var releases2 []pipeline.GateReleaseFunc
	// 2nd acquisition - Allowed but Overflow
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	verdict2, err := gate.Apply(ctx, msg2, &releases2)
	if err != nil || verdict2.Action != pipeline.ActionContinue || msg2.GetClassification() != api.ClassificationOverflow {
		t.Fatalf("Expected continue overflow in classifying mode, got %v classification %v, err: %v", verdict2, msg2.GetClassification(), err)
	}
}

func TestRedisQuotaGate_MissingAttribute(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = rdb.Close() }()

	gate := NewRedisQuotaGate(rdb, "userid", QuotaModeRateLimit, 1, 1*time.Second, "quota:")

	ctx := context.Background()
	attrs := map[string]string{"teamid": "team1"} // missing 'userid'

	var releases []pipeline.GateReleaseFunc
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req1", Metadata: attrs})
	verdict, err := gate.Apply(ctx, msg, &releases)
	if err != nil || verdict.Action != pipeline.ActionContinue || msg.GetClassification() != api.ClassificationNone {
		t.Fatalf("Expected continue none for missing attribute, got %v classification %v, err: %v", verdict, msg.GetClassification(), err)
	}
}

// Releasing a reservation must not report an error: the release script has to
// return a value (a script that falls off its end replies Null Bulk, which
// go-redis turns into redis.Nil) and the remaining count must be accurate.
func TestRedisQuotaGate_ReleaseConcurrencyReturnsRemaining(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()
	gate := NewRedisQuotaGate(rdb, "userid", QuotaModeConcurrency, 5, time.Minute, "quota:")
	key := "quota:user:alice"

	for i := 0; i < 2; i++ {
		cls, release, err := gate.acquireConcurrency(ctx, key)
		if err != nil || cls != api.ClassificationReserved || release == nil {
			t.Fatalf("acquire %d: cls=%v release=%v err=%v", i, cls, release != nil, err)
		}
	}

	remaining, err := gate.releaseConcurrency(ctx, key, 60)
	if err != nil || remaining != 1 {
		t.Fatalf("first release: remaining=%d err=%v, want 1 and nil", remaining, err)
	}
	if ttl := s.TTL(key); ttl <= 0 {
		t.Fatalf("key TTL not kept alive while a reservation remains: %v", ttl)
	}

	remaining, err = gate.releaseConcurrency(ctx, key, 60)
	if err != nil || remaining != 0 {
		t.Fatalf("second release: remaining=%d err=%v, want 0 and nil", remaining, err)
	}

	// Counter expired or never existed: not an error, reported as -1.
	s.Del(key)
	remaining, err = gate.releaseConcurrency(ctx, key, 60)
	if err != nil || remaining != -1 {
		t.Fatalf("release on missing key: remaining=%d err=%v, want -1 and nil", remaining, err)
	}
}
