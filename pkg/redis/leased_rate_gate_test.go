package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	goredis "github.com/redis/go-redis/v9"
)

func newLeasedRateGateTest(t *testing.T, rate float64) (*RedisLeasedRateGate, *miniredis.Miniredis, *time.Time) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	now := time.Unix(1_700_000_000, 0)
	server.SetTime(now)
	gate := NewRedisLeasedRateGate(client, "control:pool-a", "state:pool-a", "pool-a", 1)
	gate.now = func() time.Time { return now }
	writeDispatchRateLimit(t, client, "control:pool-a", api.DispatchRateLimit{
		APIVersion:           api.DispatchRateLimitAPIVersion,
		PoolID:               "pool-a",
		MaxAdmissionRPS:      rate,
		ValidUntilUnixMillis: now.Add(time.Minute).UnixMilli(),
		DecisionID:           "decision-1",
	})
	return gate, server, &now
}

func TestRedisLeasedRateGateInitializesDrainMetricsBeforeFirstRequest(t *testing.T) {
	const pool = "bootstrap-metrics-pool"
	collectors := []struct {
		collectorName string
		count         func() int
		value         func() float64
	}{
		{
			collectorName: "llm_d_async_async_drain_limit_rps",
			count: func() int {
				return testutil.CollectAndCount(metrics.DrainLimitRPS, "llm_d_async_async_drain_limit_rps")
			},
			value: func() float64 { return testutil.ToFloat64(metrics.DrainLimitRPS.WithLabelValues(pool)) },
		},
		{
			collectorName: "llm_d_async_async_drain_limit_lease_valid",
			count: func() int {
				return testutil.CollectAndCount(metrics.DrainLimitLeaseValid, "llm_d_async_async_drain_limit_lease_valid")
			},
			value: func() float64 { return testutil.ToFloat64(metrics.DrainLimitLeaseValid.WithLabelValues(pool)) },
		},
		{
			collectorName: "llm_d_async_async_drain_limit_valid_until_seconds",
			count: func() int {
				return testutil.CollectAndCount(metrics.DrainLimitValidUntil, "llm_d_async_async_drain_limit_valid_until_seconds")
			},
			value: func() float64 { return testutil.ToFloat64(metrics.DrainLimitValidUntil.WithLabelValues(pool)) },
		},
	}

	before := make([]int, len(collectors))
	for i, collector := range collectors {
		before[i] = collector.count()
	}
	gate := NewRedisLeasedRateGate(nil, "bootstrap-control", "bootstrap-state", pool, 1)
	if gate == nil {
		t.Fatal("NewRedisLeasedRateGate returned nil")
	}
	for i, collector := range collectors {
		if got := collector.count(); got != before[i]+1 {
			t.Errorf("%s series count = %d, want %d after construction", collector.collectorName, got, before[i]+1)
		}
		if got := collector.value(); got != 0 {
			t.Errorf("%s bootstrap value = %v, want 0", collector.collectorName, got)
		}
	}
}

func writeDispatchRateLimit(t *testing.T, client *goredis.Client, key string, limit api.DispatchRateLimit) {
	t.Helper()
	err := client.HSet(context.Background(), key, map[string]any{
		dispatchRateAPIVersionField: limit.APIVersion,
		dispatchRatePoolIDField:     limit.PoolID,
		dispatchRateRPSField:        strconv.FormatFloat(limit.MaxAdmissionRPS, 'f', -1, 64),
		dispatchRateValidUntilField: strconv.FormatInt(limit.ValidUntilUnixMillis, 10),
		dispatchRateDecisionIDField: limit.DecisionID,
	}).Err()
	if err != nil {
		t.Fatalf("write dispatch-rate limit: %v", err)
	}
}

func applyLeasedRateGate(t *testing.T, gate *RedisLeasedRateGate) pipeline.VerdictAction {
	t.Helper()
	verdict, err := gate.Apply(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("gate.Apply: %v", err)
	}
	return verdict.Action
}

func TestRedisLeasedRateGateMissingLeaseFailsClosed(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()
	gate := NewRedisLeasedRateGate(client, "missing", "state", "pool-a", 1)

	if got := gate.Budget(context.Background()); got != 0 {
		t.Fatalf("Budget = %v, want 0", got)
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("Apply action = %v, want refuse", action)
	}
}

func TestRedisLeasedRateGateExplicitPause(t *testing.T) {
	gate, _, _ := newLeasedRateGateTest(t, 0)
	if got := gate.Budget(context.Background()); got != 0 {
		t.Fatalf("Budget = %v, want 0 for explicit pause", got)
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("Apply action = %v, want refuse", action)
	}
}

func TestRedisLeasedRateGateEnforcesAndRefillsTokenBucket(t *testing.T) {
	gate, server, now := newLeasedRateGateTest(t, 2)
	if got := gate.Budget(context.Background()); got != 1 {
		t.Fatalf("Budget = %v, want 1 with positive valid lease", got)
	}

	for i := 0; i < 2; i++ {
		if action := applyLeasedRateGate(t, gate); action != pipeline.ActionContinue {
			t.Fatalf("initial token %d action = %v, want continue", i, action)
		}
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("third action = %v, want refuse", action)
	}

	*now = now.Add(500 * time.Millisecond)
	server.SetTime(*now)
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionContinue {
		t.Fatalf("action after refill = %v, want continue", action)
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("second action after refill = %v, want refuse", action)
	}
}

func TestRedisLeasedRateGateUsesRedisTimeAcrossSkewedInstances(t *testing.T) {
	gate, server, now := newLeasedRateGateTest(t, 1)
	other := NewRedisLeasedRateGate(gate.rdb, gate.controlKey, gate.stateKey, gate.poolID, 1)

	// Model two Async replicas whose wall clocks disagree by one second. The
	// shared Redis clock must be the only clock that advances the token bucket.
	gate.now = func() time.Time { return now.Add(500 * time.Millisecond) }
	other.now = func() time.Time { return now.Add(-500 * time.Millisecond) }

	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionContinue {
		t.Fatalf("first instance action = %v, want continue", action)
	}
	if action := applyLeasedRateGate(t, other); action != pipeline.ActionRefuse {
		t.Fatalf("skewed instance action = %v, want refuse", action)
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("fast instance action after slow instance = %v, want refuse", action)
	}

	*now = now.Add(time.Second)
	server.SetTime(*now)
	if action := applyLeasedRateGate(t, other); action != pipeline.ActionContinue {
		t.Fatalf("action after Redis clock refill = %v, want continue", action)
	}
}

func TestRedisLeasedRateGateSharesLimitAcrossInstances(t *testing.T) {
	gate, _, _ := newLeasedRateGateTest(t, 1)
	other := NewRedisLeasedRateGate(gate.rdb, gate.controlKey, gate.stateKey, gate.poolID, 1)
	other.now = gate.now

	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionContinue {
		t.Fatalf("first instance action = %v, want continue", action)
	}
	if action := applyLeasedRateGate(t, other); action != pipeline.ActionRefuse {
		t.Fatalf("second instance action = %v, want shared refusal", action)
	}
}

func TestRedisLeasedRateGateRejectsExpiredOrMismatchedLease(t *testing.T) {
	gate, server, now := newLeasedRateGateTest(t, 5)
	*now = now.Add(2 * time.Minute)
	server.SetTime(*now)
	if got := gate.Budget(context.Background()); got != 0 {
		t.Fatalf("expired Budget = %v, want 0", got)
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("expired action = %v, want refuse", action)
	}

	*now = now.Add(-2 * time.Minute)
	server.SetTime(*now)
	limit, err := gate.readLimit(context.Background(), *now)
	if err != nil {
		t.Fatalf("read valid lease: %v", err)
	}
	limit.PoolID = "pool-b"
	writeDispatchRateLimit(t, gate.rdb, gate.controlKey, limit)
	if got := gate.Budget(context.Background()); got != 0 {
		t.Fatalf("mismatched Budget = %v, want 0", got)
	}
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("mismatched action = %v, want refuse", action)
	}
}

func TestRedisLeasedRateGateMalformedNumericLeaseFailsClosed(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{name: "NaN rate", field: dispatchRateRPSField, value: "NaN"},
		{name: "positive infinite rate", field: dispatchRateRPSField, value: "+Inf"},
		{name: "negative infinite rate", field: dispatchRateRPSField, value: "-Inf"},
		{name: "fractional expiry", field: dispatchRateValidUntilField, value: "1700000060000.5"},
		{name: "NaN expiry", field: dispatchRateValidUntilField, value: "NaN"},
		{name: "infinite expiry", field: dispatchRateValidUntilField, value: "+Inf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate, server, _ := newLeasedRateGateTest(t, 5)
			if err := gate.rdb.HSet(context.Background(), gate.controlKey, tt.field, tt.value).Err(); err != nil {
				t.Fatalf("write malformed field: %v", err)
			}
			if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
				t.Fatalf("Apply action = %v, want refuse", action)
			}
			if server.Exists(gate.stateKey) {
				t.Fatal("malformed command must not create token-bucket state")
			}
		})
	}
}

func TestRedisLeasedRateGateCapacityOverflowFailsClosed(t *testing.T) {
	gate, server, _ := newLeasedRateGateTest(t, 1e308)
	gate.burstSeconds = 1e308
	if action := applyLeasedRateGate(t, gate); action != pipeline.ActionRefuse {
		t.Fatalf("Apply action = %v, want refuse", action)
	}
	if server.Exists(gate.stateKey) {
		t.Fatal("overflowing token capacity must not create token-bucket state")
	}
}
