package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
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

	// 1st acquisition - Allowed
	res, err := gate.Acquire(ctx, attrs)
	if err != nil || !res.Allowed || res.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved, got %v %v, err: %v", res.Allowed, res.Classification, err)
	}

	// 2nd acquisition - Allowed
	res2, err := gate.Acquire(ctx, attrs)
	if err != nil || !res2.Allowed || res2.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved, got %v %v, err: %v", res2.Allowed, res2.Classification, err)
	}

	// 3rd acquisition - Denied
	res3, err := gate.Acquire(ctx, attrs)
	if err != nil || res3.Allowed || res3.Classification != api.ClassificationOverflow {
		t.Fatalf("Expected allowed=false overflow, got %v %v, err: %v", res3.Allowed, res3.Classification, err)
	}

	// Release one
	res.Release()

	// 4th acquisition - Allowed again
	res4, err := gate.Acquire(ctx, attrs)
	if err != nil || !res4.Allowed || res4.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved after release, got %v %v, err: %v", res4.Allowed, res4.Classification, err)
	}

	res2.Release()
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

	// 1st acquisition - Allowed
	res, err := gate.Acquire(ctx, attrs)
	if err != nil || !res.Allowed || res.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved, got %v %v, err: %v", res.Allowed, res.Classification, err)
	}

	// 2nd acquisition - Allowed
	res2, err := gate.Acquire(ctx, attrs)
	if err != nil || !res2.Allowed || res2.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved, got %v %v, err: %v", res2.Allowed, res2.Classification, err)
	}

	// 3rd acquisition - Denied
	res3, err := gate.Acquire(ctx, attrs)
	if err != nil || res3.Allowed || res3.Classification != api.ClassificationOverflow {
		t.Fatalf("Expected allowed=false overflow, got %v %v, err: %v", res3.Allowed, res3.Classification, err)
	}

	// Wait for window to pass
	time.Sleep(1100 * time.Millisecond)

	// 4th acquisition - Allowed again
	res4, err := gate.Acquire(ctx, attrs)
	if err != nil || !res4.Allowed || res4.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved after window, got %v %v, err: %v", res4.Allowed, res4.Classification, err)
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

	// 1st acquisition - Allowed and Reserved
	res, err := gate.Acquire(ctx, attrs)
	if err != nil || !res.Allowed || res.Classification != api.ClassificationReserved {
		t.Fatalf("Expected allowed=true reserved, got %v %v, err: %v", res.Allowed, res.Classification, err)
	}

	// 2nd acquisition - Allowed but Overflow
	res2, err := gate.Acquire(ctx, attrs)
	if err != nil || !res2.Allowed || res2.Classification != api.ClassificationOverflow {
		t.Fatalf("Expected allowed=true overflow, got %v %v, err: %v", res2.Allowed, res2.Classification, err)
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

	res, err := gate.Acquire(ctx, attrs)
	if err != nil || !res.Allowed || res.Classification != api.ClassificationNone {
		t.Fatalf("Expected allowed=true none for missing attribute, got %v %v, err: %v", res.Allowed, res.Classification, err)
	}
}
