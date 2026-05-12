/*
Copyright 2026 The llm-d Authors
*/

package flowcontrol

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func newClassifierTestFixture(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return s, rdb
}

func msgWithLabels(labels map[string]string) *pipeline.EmbelishedRequestMessage {
	return &pipeline.EmbelishedRequestMessage{Labels: pipeline.Labels(labels)}
}

func TestReservationClassifier_RequiresBucketKeys(t *testing.T) {
	_, rdb := newClassifierTestFixture(t)
	_, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{})
	if err == nil {
		t.Fatal("expected error when bucket_keys is empty")
	}
}

func TestReservationClassifier_RequiresRedis(t *testing.T) {
	_, err := NewReservationClassifierGate(nil, ReservationClassifierConfig{BucketKeys: []string{"team"}})
	if err == nil {
		t.Fatal("expected error when redis client is nil")
	}
}

func TestReservationClassifier_ReservedWithinCap(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys: []string{"team", "tier", "model"},
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}

	// Operator-set cap.
	if err := s.Set("rmp:cap:teamA:async:k2-6", "2"); err != nil {
		t.Fatalf("seed cap: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		msg := msgWithLabels(map[string]string{"team": "teamA", "tier": "async", "model": "k2-6"})
		v, err := g.Apply(ctx, msg)
		if err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
		if v.Terminate {
			t.Errorf("verdict %d should Continue, got %+v", i, v)
		}
		if got := msg.Labels.Get("class"); got != "reserved" {
			t.Errorf("classify %d: class = %q, want reserved", i, got)
		}
	}
	// Third should be overflow.
	msg := msgWithLabels(map[string]string{"team": "teamA", "tier": "async", "model": "k2-6"})
	v, err := g.Apply(ctx, msg)
	if err != nil {
		t.Fatalf("Apply 3: %v", err)
	}
	if v.Terminate {
		t.Errorf("verdict should Continue, got %+v", v)
	}
	if got := msg.Labels.Get("class"); got != "overflow" {
		t.Errorf("classify: class = %q, want overflow", got)
	}
	// In-flight stays at cap (overflow decremented back).
	if got, _ := s.Get("rmp:inflight:teamA:async:k2-6"); got != "2" {
		t.Errorf("inflight after overflow = %q, want 2", got)
	}
}

func TestReservationClassifier_ReleaseDecrements(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys: []string{"team"},
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	if err := s.Set("rmp:cap:teamA", "3"); err != nil {
		t.Fatalf("seed cap: %v", err)
	}

	ctx := context.Background()
	msg := msgWithLabels(map[string]string{"team": "teamA"})
	if _, err := g.Apply(ctx, msg); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, _ := s.Get("rmp:inflight:teamA"); got != "1" {
		t.Errorf("inflight after Apply = %q, want 1", got)
	}
	msg.FireReleases()
	if got, _ := s.Get("rmp:inflight:teamA"); got != "0" {
		t.Errorf("inflight after FireReleases = %q, want 0", got)
	}
}

func TestReservationClassifier_OverflowAttachesNoRelease(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys: []string{"team"},
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	if err := s.Set("rmp:cap:teamA", "1"); err != nil {
		t.Fatalf("seed cap: %v", err)
	}

	ctx := context.Background()
	// Saturate the bucket.
	reserved := msgWithLabels(map[string]string{"team": "teamA"})
	if _, err := g.Apply(ctx, reserved); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	// Next classify is overflow.
	overflow := msgWithLabels(map[string]string{"team": "teamA"})
	if _, err := g.Apply(ctx, overflow); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if got := overflow.Labels.Get("class"); got != "overflow" {
		t.Fatalf("expected overflow classification, got %q", got)
	}
	// Firing overflow's releases must not double-decrement the
	// counter (Lua already returned the slot).
	overflow.FireReleases()
	if got, _ := s.Get("rmp:inflight:teamA"); got != "1" {
		t.Errorf("inflight after firing overflow releases = %q, want 1 (overflow holds no slot)", got)
	}
}

func TestReservationClassifier_FallbackCapWhenCapKeyUnset(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys:  []string{"team"},
		FallbackCap: 5,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	// No cap key set; fallback should govern.
	if exists := s.Exists("rmp:cap:teamA"); exists {
		t.Fatalf("precondition failed: cap key should be unset")
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		msg := msgWithLabels(map[string]string{"team": "teamA"})
		if _, err := g.Apply(ctx, msg); err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
		if got := msg.Labels.Get("class"); got != "reserved" {
			t.Errorf("classify %d (fallback): class = %q, want reserved", i, got)
		}
	}
	// 6th should overflow.
	msg := msgWithLabels(map[string]string{"team": "teamA"})
	if _, err := g.Apply(ctx, msg); err != nil {
		t.Fatalf("Apply 6: %v", err)
	}
	if got := msg.Labels.Get("class"); got != "overflow" {
		t.Errorf("classify 6: class = %q, want overflow (cap reached via fallback)", got)
	}
}

func TestReservationClassifier_FallbackCapZeroAllOverflow(t *testing.T) {
	_, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys:  []string{"team"},
		FallbackCap: 0,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	ctx := context.Background()
	msg := msgWithLabels(map[string]string{"team": "teamA"})
	if _, err := g.Apply(ctx, msg); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := msg.Labels.Get("class"); got != "overflow" {
		t.Errorf("with fallback=0 and no cap key, class should be overflow, got %q", got)
	}
}

func TestReservationClassifier_SeparateBuckets(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys: []string{"team", "tier", "model"},
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	_ = s.Set("rmp:cap:teamA:async:k2-6", "1")
	_ = s.Set("rmp:cap:teamB:async:k2-6", "1")

	ctx := context.Background()
	a := msgWithLabels(map[string]string{"team": "teamA", "tier": "async", "model": "k2-6"})
	b := msgWithLabels(map[string]string{"team": "teamB", "tier": "async", "model": "k2-6"})
	for _, m := range []*pipeline.EmbelishedRequestMessage{a, b} {
		if _, err := g.Apply(ctx, m); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got := m.Labels.Get("class"); got != "reserved" {
			t.Errorf("expected reserved (separate buckets), got %q", got)
		}
	}
}

func TestReservationClassifier_RedisOutageFailsOpen(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys: []string{"team"},
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	// Simulate redis outage.
	s.Close()

	ctx := context.Background()
	msg := msgWithLabels(map[string]string{"team": "teamA"})
	v, err := g.Apply(ctx, msg)
	if err == nil {
		t.Fatalf("expected error from redis outage")
	}
	if v.Terminate {
		t.Errorf("expected Continue on redis outage (fail-safe), got %+v", v)
	}
	if got := msg.Labels.Get("class"); got != "overflow" {
		t.Errorf("on outage, expected overflow classification, got %q", got)
	}
}

func TestReservationClassifier_TTLAppliedOnInflightKey(t *testing.T) {
	s, rdb := newClassifierTestFixture(t)
	g, err := NewReservationClassifierGate(rdb, ReservationClassifierConfig{
		BucketKeys: []string{"team"},
		TTLSeconds: 60,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	_ = s.Set("rmp:cap:teamA", "10")

	ctx := context.Background()
	msg := msgWithLabels(map[string]string{"team": "teamA"})
	if _, err := g.Apply(ctx, msg); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	ttl := s.TTL("rmp:inflight:teamA")
	if ttl <= 0 {
		t.Errorf("expected positive TTL on inflight key, got %v", ttl)
	}
}

func TestReservationClassifierFactory(t *testing.T) {
	_, rdb := newClassifierTestFixture(t)
	f := NewGateFactory("", WithRedisClient(rdb))
	gate, err := f.CreateGate("reservation-classifier", map[string]string{
		"bucket_keys": "team,tier,model",
		"class_label": "class",
	})
	if err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	if _, ok := gate.(*ReservationClassifierGate); !ok {
		t.Errorf("factory did not return *ReservationClassifierGate")
	}
}

func TestReservationClassifierFactory_RequiresRedis(t *testing.T) {
	f := NewGateFactory("") // no redis client
	_, err := f.CreateGate("reservation-classifier", map[string]string{
		"bucket_keys": "team",
	})
	if err == nil {
		t.Errorf("expected error without redis client")
	}
}

func TestReservationClassifierFactory_RequiresBucketKeys(t *testing.T) {
	_, rdb := newClassifierTestFixture(t)
	f := NewGateFactory("", WithRedisClient(rdb))
	_, err := f.CreateGate("reservation-classifier", map[string]string{})
	if err == nil {
		t.Errorf("expected error without bucket_keys")
	}
}
