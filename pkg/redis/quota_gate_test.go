package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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
	allowed, release, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed {
		t.Fatalf("Expected allowed=true, got %v, err: %v", allowed, err)
	}

	// 2nd acquisition - Allowed
	allowed2, release2, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed2 {
		t.Fatalf("Expected allowed=true, got %v, err: %v", allowed2, err)
	}

	// 3rd acquisition - Denied
	allowed3, _, err := gate.Acquire(ctx, attrs)
	if err != nil || allowed3 {
		t.Fatalf("Expected allowed=false, got %v, err: %v", allowed3, err)
	}

	// Release one
	release()

	// 4th acquisition - Allowed again
	allowed4, _, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed4 {
		t.Fatalf("Expected allowed=true after release, got %v, err: %v", allowed4, err)
	}

	release2()
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
	allowed, _, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed {
		t.Fatalf("Expected allowed=true, got %v, err: %v", allowed, err)
	}

	// 2nd acquisition - Allowed
	allowed2, _, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed2 {
		t.Fatalf("Expected allowed=true, got %v, err: %v", allowed2, err)
	}

	// 3rd acquisition - Denied
	allowed3, _, err := gate.Acquire(ctx, attrs)
	if err != nil || allowed3 {
		t.Fatalf("Expected allowed=false, got %v, err: %v", allowed3, err)
	}

	// Wait for window to pass
	time.Sleep(1100 * time.Millisecond)

	// 4th acquisition - Allowed again
	allowed4, _, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed4 {
		t.Fatalf("Expected allowed=true after window, got %v, err: %v", allowed4, err)
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

	allowed, _, err := gate.Acquire(ctx, attrs)
	if err != nil || !allowed {
		t.Fatalf("Expected allowed=true for missing attribute, got %v, err: %v", allowed, err)
	}
}
