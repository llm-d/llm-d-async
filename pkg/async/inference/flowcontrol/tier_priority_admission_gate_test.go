/*
Copyright 2026 The llm-d Authors
*/

package flowcontrol

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

// newTestRedis starts a miniredis server and returns it with a client. The
// server is stopped automatically when the test ends.
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return s, rdb
}

// fakeSource lets tests drive TryAcquire deterministically without
// touching redis or Prometheus.
type fakeSource struct {
	mu       sync.Mutex
	ok       bool
	err      error
	releases atomic.Int64
	// trips counts TryAcquire calls; useful for verifying poll behavior.
	trips atomic.Int64
}

func (f *fakeSource) TryAcquire(ctx context.Context) (bool, func(), error) {
	f.trips.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ok {
		return false, nil, f.err
	}
	rel := func() { f.releases.Add(1) }
	return true, rel, f.err
}

func (f *fakeSource) set(ok bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ok = ok
	f.err = err
}

func newAdmissionGate(t *testing.T, src PoolLoadSource, mut func(*TierPriorityAdmissionConfig)) *TierPriorityAdmissionGate {
	t.Helper()
	cfg := TierPriorityAdmissionConfig{Source: src, BlockPollInterval: 5 * time.Millisecond}
	if mut != nil {
		mut(&cfg)
	}
	g, err := NewTierPriorityAdmissionGate(cfg)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	return g
}

func newReq(t *testing.T, labels map[string]string) *pipeline.EmbelishedRequestMessage {
	t.Helper()
	ir := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "test-id"})
	return &pipeline.EmbelishedRequestMessage{InternalRequest: ir, Labels: pipeline.Labels(labels)}
}

func TestAdmission_RequiresSource(t *testing.T) {
	_, err := NewTierPriorityAdmissionGate(TierPriorityAdmissionConfig{})
	if err == nil {
		t.Fatal("expected error when source is nil")
	}
}

func TestAdmission_ContinueWhenCapacityAvailable(t *testing.T) {
	src := &fakeSource{ok: true}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "interactive", "class": "overflow"})

	v, err := g.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if v.Terminate {
		t.Errorf("verdict = %+v, want Continue", v)
	}
	// Source's release should be attached.
	msg.FireReleases()
	if got := src.releases.Load(); got != 1 {
		t.Errorf("release fired %d times, want 1", got)
	}
}

func TestAdmission_ContinuePropagatesSourceError(t *testing.T) {
	// Source can return (ok=true, err=non-nil) for stale-Prometheus
	// fail-open. Gate should Continue but surface the error.
	src := &fakeSource{ok: true, err: errors.New("stale")}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "async", "class": "reserved"})

	v, err := g.Apply(context.Background(), msg)
	if err == nil {
		t.Fatal("expected source error to surface")
	}
	if v.Terminate {
		t.Errorf("verdict = %+v, want Continue (fail-open)", v)
	}
}

func TestAdmission_DropInteractiveOverflowOnSaturation(t *testing.T) {
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "interactive", "class": "overflow"})

	v, err := g.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.Terminate || v.Redeliver {
		t.Fatalf("verdict = %+v, want Drop", v)
	}
	if v.Result == nil {
		t.Fatal("expected non-nil Result on fail-fast Drop")
	}
	if v.Result.ID != "test-id" {
		t.Errorf("Result.ID = %q, want test-id", v.Result.ID)
	}
	if v.Result.Payload == "" {
		t.Error("Result.Payload should contain fail-fast body")
	}
}

func TestAdmission_RefuseAsyncOverflowOnSaturation(t *testing.T) {
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "async", "class": "overflow"})

	v, err := g.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.Terminate || !v.Redeliver {
		t.Errorf("verdict = %+v, want Refuse", v)
	}
}

func TestAdmission_RefuseBatchOverflowOnSaturation(t *testing.T) {
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "batch", "class": "overflow"})

	v, err := g.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.Terminate || !v.Redeliver {
		t.Errorf("verdict = %+v, want Refuse", v)
	}
}

func TestAdmission_BlocksReservedUntilCapacityOpens(t *testing.T) {
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "async", "class": "reserved"})

	done := make(chan pipeline.Verdict, 1)
	go func() {
		v, _ := g.Apply(context.Background(), msg)
		done <- v
	}()

	// Should still be blocking after a couple of poll cycles.
	select {
	case v := <-done:
		t.Fatalf("returned too early: %+v", v)
	case <-time.After(20 * time.Millisecond):
	}

	// Open capacity.
	src.set(true, nil)
	select {
	case v := <-done:
		if v.Terminate {
			t.Errorf("verdict = %+v, want Continue once capacity opens", v)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("gate did not unblock after capacity opened")
	}
	// Should have polled multiple times before unblocking.
	if got := src.trips.Load(); got < 2 {
		t.Errorf("expected multiple TryAcquire calls before unblock, got %d", got)
	}
}

func TestAdmission_BlockedReservedHonorsCtxCancel(t *testing.T) {
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "async", "class": "reserved"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan pipeline.Verdict, 1)
	go func() {
		v, _ := g.Apply(ctx, msg)
		done <- v
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case v := <-done:
		if !v.Terminate || !v.Redeliver {
			t.Errorf("verdict on ctx cancel = %+v, want Refuse", v)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("gate did not return after ctx cancel")
	}
}

func TestAdmission_BlocksInteractiveReservedToo(t *testing.T) {
	// Reserved at any tier blocks — even interactive. Only overflow
	// gets the fail-fast treatment.
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, nil)
	msg := newReq(t, map[string]string{"tier": "interactive", "class": "reserved"})

	done := make(chan pipeline.Verdict, 1)
	go func() {
		v, _ := g.Apply(context.Background(), msg)
		done <- v
	}()

	// Verify it's blocking, not dropping/refusing.
	select {
	case v := <-done:
		t.Fatalf("returned too early: %+v", v)
	case <-time.After(20 * time.Millisecond):
	}

	src.set(true, nil)
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("did not unblock after capacity opened")
	}
}

func TestAdmission_CustomLabelKeys(t *testing.T) {
	src := &fakeSource{ok: false}
	g := newAdmissionGate(t, src, func(c *TierPriorityAdmissionConfig) {
		c.TierLabel = "priority"
		c.ClassLabel = "kind"
		c.ReservedVal = "guaranteed"
		c.OverflowVal = "spillover"
		c.FailFastTier = "realtime"
	})
	// Should be recognized as fail-fast under the custom selector.
	msg := newReq(t, map[string]string{"priority": "realtime", "kind": "spillover"})

	v, err := g.Apply(context.Background(), msg)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.Terminate || v.Redeliver || v.Result == nil {
		t.Errorf("verdict = %+v, want Drop with result under custom labels", v)
	}
}

func TestAdmissionFactory_Prometheus(t *testing.T) {
	// Build via factory (without running PromQL — we just verify
	// construction). source=prometheus needs url and query.
	f := NewGateFactory("http://prom.example.com:9090")
	gate, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source":              "prometheus",
		"query":               "vector(0)",
		"threshold":           "10",
		"refresh_interval_ms": "100",
	})
	if err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	if _, ok := gate.(*TierPriorityAdmissionGate); !ok {
		t.Errorf("factory did not return *TierPriorityAdmissionGate")
	}
}

func TestAdmissionFactory_PrometheusRequiresURL(t *testing.T) {
	f := NewGateFactory("") // no factory-level URL
	_, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source":    "prometheus",
		"query":     "vector(0)",
		"threshold": "10",
	})
	if err == nil {
		t.Errorf("expected error when prometheus URL is unset")
	}
}

func TestAdmissionFactory_PrometheusRequiresQuery(t *testing.T) {
	f := NewGateFactory("http://prom:9090")
	_, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source":    "prometheus",
		"threshold": "10",
	})
	if err == nil {
		t.Errorf("expected error when query is missing")
	}
}

func TestAdmissionFactory_RedisCounter(t *testing.T) {
	_, rdb := newTestRedis(t)
	f := NewGateFactory("", WithRedisClient(rdb))
	gate, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source":       "redis-counter",
		"bucket":       "pool:groq-whisper",
		"fallback_cap": "16",
	})
	if err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	if _, ok := gate.(*TierPriorityAdmissionGate); !ok {
		t.Errorf("factory did not return *TierPriorityAdmissionGate")
	}
}

func TestAdmissionFactory_RedisCounterRequiresClient(t *testing.T) {
	f := NewGateFactory("") // no redis client
	_, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source": "redis-counter",
		"bucket": "pool:x",
	})
	if err == nil {
		t.Errorf("expected error without redis client")
	}
}

func TestAdmissionFactory_RedisCounterRequiresBucket(t *testing.T) {
	_, rdb := newTestRedis(t)
	f := NewGateFactory("", WithRedisClient(rdb))
	_, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source": "redis-counter",
	})
	if err == nil {
		t.Errorf("expected error without bucket")
	}
}

func TestAdmissionFactory_UnknownSource(t *testing.T) {
	f := NewGateFactory("")
	_, err := f.CreateGate("tier-priority-admission", map[string]string{
		"source": "nonsense",
	})
	if err == nil {
		t.Errorf("expected error for unknown source")
	}
}

// End-to-end with the real RedisCounterLoadSource via miniredis: verify
// the gate respects the redis-counter signal across the three branches.
func TestAdmission_E2EWithRedisCounter(t *testing.T) {
	s, rdb := newTestRedis(t)
	if err := s.Set("rmp:cap:pool:test", "2"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	src, err := NewRedisCounterLoadSource(rdb, RedisCounterLoadSourceConfig{
		Bucket: "pool:test",
	})
	if err != nil {
		t.Fatalf("construct source: %v", err)
	}
	g := newAdmissionGate(t, src, nil)

	ctx := context.Background()
	// First two reserved messages admit.
	for i := 0; i < 2; i++ {
		msg := newReq(t, map[string]string{"tier": "async", "class": "reserved"})
		v, err := g.Apply(ctx, msg)
		if err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
		if v.Terminate {
			t.Errorf("Apply %d verdict = %+v, want Continue", i, v)
		}
	}
	// Third reserved blocks. Test by running with a quick-cancel ctx.
	{
		cctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		msg := newReq(t, map[string]string{"tier": "async", "class": "reserved"})
		v, _ := g.Apply(cctx, msg)
		if !v.Terminate || !v.Redeliver {
			t.Errorf("blocked-then-canceled reserved = %+v, want Refuse", v)
		}
	}
	// Interactive overflow at saturation → Drop with result.
	{
		msg := newReq(t, map[string]string{"tier": "interactive", "class": "overflow"})
		v, err := g.Apply(ctx, msg)
		if err != nil {
			t.Fatalf("interactive overflow: %v", err)
		}
		if !v.Terminate || v.Redeliver || v.Result == nil {
			t.Errorf("interactive overflow = %+v, want Drop with result", v)
		}
	}
	// Async overflow at saturation → Refuse.
	{
		msg := newReq(t, map[string]string{"tier": "async", "class": "overflow"})
		v, err := g.Apply(ctx, msg)
		if err != nil {
			t.Fatalf("async overflow: %v", err)
		}
		if !v.Terminate || !v.Redeliver {
			t.Errorf("async overflow = %+v, want Refuse", v)
		}
	}
}
