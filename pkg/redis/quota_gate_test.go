package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
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
	if err != nil || verdict1.Action != pipeline.ActionContinue || msg1.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict1, msg1.Classification, err)
	}

	var releases2 []pipeline.GateReleaseFunc
	// 2nd acquisition - Allowed
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	verdict2, err := gate.Apply(ctx, msg2, &releases2)
	if err != nil || verdict2.Action != pipeline.ActionContinue || msg2.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict2, msg2.Classification, err)
	}

	var releases3 []pipeline.GateReleaseFunc
	// 3rd acquisition - Denied (Redeliver)
	msg3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req3", Metadata: attrs})
	verdict3, err := gate.Apply(ctx, msg3, &releases3)
	if err != nil || verdict3.Action != pipeline.ActionRefuse || msg3.Classification != api.ClassificationOverflow {
		t.Fatalf("Expected redeliver overflow, got %v classification %v, err: %v", verdict3, msg3.Classification, err)
	}

	// Release one
	for _, f := range releases1 {
		f()
	}

	var releases4 []pipeline.GateReleaseFunc
	// 4th acquisition - Allowed again
	msg4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req4", Metadata: attrs})
	verdict4, err := gate.Apply(ctx, msg4, &releases4)
	if err != nil || verdict4.Action != pipeline.ActionContinue || msg4.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved after release, got %v classification %v, err: %v", verdict4, msg4.Classification, err)
	}

	for _, f := range releases2 {
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
	if err != nil || verdict1.Action != pipeline.ActionContinue || msg1.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict1, msg1.Classification, err)
	}

	var releases2 []pipeline.GateReleaseFunc
	// 2nd acquisition - Allowed
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	verdict2, err := gate.Apply(ctx, msg2, &releases2)
	if err != nil || verdict2.Action != pipeline.ActionContinue || msg2.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict2, msg2.Classification, err)
	}

	var releases3 []pipeline.GateReleaseFunc
	// 3rd acquisition - Denied
	msg3 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req3", Metadata: attrs})
	verdict3, err := gate.Apply(ctx, msg3, &releases3)
	if err != nil || verdict3.Action != pipeline.ActionRefuse || msg3.Classification != api.ClassificationOverflow {
		t.Fatalf("Expected redeliver overflow, got %v classification %v, err: %v", verdict3, msg3.Classification, err)
	}

	// Wait for window to pass
	time.Sleep(1100 * time.Millisecond)

	var releases4 []pipeline.GateReleaseFunc
	// 4th acquisition - Allowed again
	msg4 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req4", Metadata: attrs})
	verdict4, err := gate.Apply(ctx, msg4, &releases4)
	if err != nil || verdict4.Action != pipeline.ActionContinue || msg4.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved after window, got %v classification %v, err: %v", verdict4, msg4.Classification, err)
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
	if err != nil || verdict1.Action != pipeline.ActionContinue || msg1.Classification != api.ClassificationReserved {
		t.Fatalf("Expected continue reserved, got %v classification %v, err: %v", verdict1, msg1.Classification, err)
	}

	var releases2 []pipeline.GateReleaseFunc
	// 2nd acquisition - Allowed but Overflow
	msg2 := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req2", Metadata: attrs})
	verdict2, err := gate.Apply(ctx, msg2, &releases2)
	if err != nil || verdict2.Action != pipeline.ActionContinue || msg2.Classification != api.ClassificationOverflow {
		t.Fatalf("Expected continue overflow in classifying mode, got %v classification %v, err: %v", verdict2, msg2.Classification, err)
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
	if err != nil || verdict.Action != pipeline.ActionContinue || msg.Classification != api.ClassificationNone {
		t.Fatalf("Expected continue none for missing attribute, got %v classification %v, err: %v", verdict, msg.Classification, err)
	}
}
