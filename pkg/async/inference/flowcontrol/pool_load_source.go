/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package flowcontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sync/atomic"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/redis/go-redis/v9"
)

// PoolLoadSource is what the tier-priority-admission gate consults to
// decide whether the pool is saturated. Two concrete implementations:
//
//   - PromQLLoadSource: background goroutine runs a configurable PromQL
//     query at a refresh interval, atomically stores the latest value.
//     The dispatch path does an atomic load — no network, no lock.
//     Right for self-hosted IGW-fronted pools where the EPP / engine
//     emits queue-size / KV-cache metrics.
//
//   - RedisCounterLoadSource: an atomic redis INCR/DECR counter scoped
//     to one pool. Acquire (INCR with cap check via Lua) on Continue;
//     release (DECR) on terminal. Precise — no overshoot — but adds a
//     redis round-trip per dispatch decision. Right for external pools
//     (Groq) where strict cluster-wide accounting matters.
//
// Both expose the same surface: a TryAcquire that returns whether the
// pool is currently below cap, and (for counter-based sources) attaches
// a release. Metric-based sources don't need a release — they observe
// rather than mutate.
type PoolLoadSource interface {
	// TryAcquire reports whether the pool currently has capacity. For
	// counter-based sources, success means a slot was held; the caller
	// is responsible for attaching the returned Release to the message
	// so the slot frees on terminal. For metric-based sources, the
	// returned release is nil — observation only.
	//
	// ok=true:  pool has capacity. release may be non-nil (counter
	//           sources) or nil (metric sources).
	// ok=false: pool is saturated. release is always nil.
	// err != nil indicates a transient failure querying the source.
	//           ok is the fail-safe answer (true = fail-open, false =
	//           fail-closed) chosen by the source impl. Callers may
	//           log and proceed using ok.
	TryAcquire(ctx context.Context) (ok bool, release func(), err error)
}

// PromQLLoadSource keeps a single float64 value fresh in the background
// via a configurable PromQL query, and exposes it via a saturated
// threshold check.
//
// The query is expected to return a single scalar (or first-element
// vector); the source treats "result >= threshold" as saturated.
//
// Refresh runs in a goroutine started by Start. Until first successful
// refresh, the source reports not-saturated (fail-open) so a
// not-yet-warmed-up dispatcher doesn't reject everything.
//
// On query error: keeps last good value, marks unhealthy after
// staleAfter elapses without success; queries after staleAfter return
// fail-open with an error.
type PromQLLoadSource struct {
	client    promv1.API
	query     string
	threshold float64
	interval  time.Duration
	staleAfter time.Duration

	// last is the most recently observed value (float64-bits in uint64
	// for atomic.Store/Load). lastTS is unix-nano of the last successful
	// refresh.
	last   atomic.Uint64
	lastTS atomic.Int64
	// hasValue indicates whether at least one refresh has succeeded.
	hasValue atomic.Bool
}

var _ PoolLoadSource = (*PromQLLoadSource)(nil)

// PromQLLoadSourceConfig parses cleanly from gate factory params.
type PromQLLoadSourceConfig struct {
	URL         string
	Query       string
	Threshold   float64
	Interval    time.Duration
	StaleAfter  time.Duration
}

// NewPromQLLoadSource constructs the source. Start() must be called
// (with a cancellable ctx) to begin the refresh loop.
func NewPromQLLoadSource(cfg PromQLLoadSourceConfig) (*PromQLLoadSource, error) {
	if cfg.URL == "" {
		return nil, errors.New("promql-load-source: url is required")
	}
	if _, err := url.Parse(cfg.URL); err != nil {
		return nil, fmt.Errorf("promql-load-source: invalid url: %w", err)
	}
	if cfg.Query == "" {
		return nil, errors.New("promql-load-source: query is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = 30 * time.Second
	}
	cli, err := promapi.NewClient(promapi.Config{Address: cfg.URL})
	if err != nil {
		return nil, fmt.Errorf("promql-load-source: prom client: %w", err)
	}
	return &PromQLLoadSource{
		client:     promv1.NewAPI(cli),
		query:      cfg.Query,
		threshold:  cfg.Threshold,
		interval:   cfg.Interval,
		staleAfter: cfg.StaleAfter,
	}, nil
}

// Start runs the refresh loop until ctx is canceled. Idempotent — a
// second call returns immediately because Run-once is the contract;
// the gate factory wires this when first used.
func (s *PromQLLoadSource) Start(ctx context.Context) {
	go func() {
		// First refresh immediately; subsequent on ticker.
		s.refresh(ctx)
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.refresh(ctx)
			}
		}
	}()
}

func (s *PromQLLoadSource) refresh(ctx context.Context) {
	qctx, cancel := context.WithTimeout(ctx, s.interval)
	defer cancel()
	val, _, err := s.client.Query(qctx, s.query, time.Now())
	if err != nil {
		return
	}
	v, ok := extractScalar(val)
	if !ok {
		return
	}
	storeFloat(&s.last, v)
	s.lastTS.Store(time.Now().UnixNano())
	s.hasValue.Store(true)
}

// TryAcquire reads the cached value and compares to threshold. Returns
// (true, nil, nil) when below threshold, (false, nil, nil) when at or
// above. Until the first successful refresh, returns fail-open.
// After staleAfter elapses without a refresh, returns fail-open with
// an error so the caller can log.
func (s *PromQLLoadSource) TryAcquire(ctx context.Context) (bool, func(), error) {
	if !s.hasValue.Load() {
		return true, nil, nil
	}
	last := s.lastTS.Load()
	if last > 0 && time.Since(time.Unix(0, last)) > s.staleAfter {
		return true, nil, fmt.Errorf("promql-load-source: stale (no refresh in %s)", s.staleAfter)
	}
	v := loadFloat(&s.last)
	return v < s.threshold, nil, nil
}

func storeFloat(a *atomic.Uint64, v float64) {
	a.Store(math.Float64bits(v))
}
func loadFloat(a *atomic.Uint64) float64 {
	return math.Float64frombits(a.Load())
}

func extractScalar(v model.Value) (float64, bool) {
	switch t := v.(type) {
	case *model.Scalar:
		return float64(t.Value), true
	case model.Vector:
		if len(t) == 0 {
			return 0, false
		}
		return float64(t[0].Value), true
	default:
		return 0, false
	}
}

// RedisCounterLoadSource is a redis-backed atomic in-flight counter
// scoped to one pool. Same Lua-script pattern as ReservationClassifier
// (INCR with cap check, EXPIRE for leak ceiling, DECR-back on
// over-cap), bucketed on the configured pool key.
type RedisCounterLoadSource struct {
	rdb         *redis.Client
	inflightKey string
	capKey      string
	ttlSeconds  int
	fallbackCap int
	script      *redis.Script
}

// redisCounter defaults shared by RedisCounterLoadSource.
const (
	defaultRedisKeyPrefix  = "rmp"
	defaultRedisTTLSeconds = 24 * 60 * 60 // 24h leak ceiling

	// redisClassifyScript atomically checks and acquires a slot.
	// KEYS[1]=inflight key, KEYS[2]=cap key
	// ARGV[1]=TTL seconds, ARGV[2]=fallback cap
	// Returns 1 (slot held) or 0 (over cap; slot returned).
	redisClassifyScript = `
local cap = tonumber(redis.call("GET", KEYS[2])) or tonumber(ARGV[2])
local cur = redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], ARGV[1])
if cur > cap then
  redis.call("DECR", KEYS[1])
  return 0
end
return 1
`
)

var _ PoolLoadSource = (*RedisCounterLoadSource)(nil)

// RedisCounterLoadSourceConfig parses from gate factory params.
type RedisCounterLoadSourceConfig struct {
	Bucket      string // e.g. "pool:groq-whisper" (appended to key prefix)
	KeyPrefix   string // default "rmp"
	TTLSeconds  int    // default 86400 (24h)
	FallbackCap int    // cap used when no cap key set (default 0 => always saturated)
}

// NewRedisCounterLoadSource constructs the source.
func NewRedisCounterLoadSource(rdb *redis.Client, cfg RedisCounterLoadSourceConfig) (*RedisCounterLoadSource, error) {
	if rdb == nil {
		return nil, errors.New("redis-counter-load-source: redis client is required")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("redis-counter-load-source: bucket is required")
	}
	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = defaultRedisKeyPrefix
	}
	ttl := cfg.TTLSeconds
	if ttl <= 0 {
		ttl = defaultRedisTTLSeconds
	}
	return &RedisCounterLoadSource{
		rdb:         rdb,
		inflightKey: prefix + ":inflight:" + cfg.Bucket,
		capKey:      prefix + ":cap:" + cfg.Bucket,
		ttlSeconds:  ttl,
		fallbackCap: cfg.FallbackCap,
		script:      redis.NewScript(redisClassifyScript),
	}, nil
}

// TryAcquire runs the classify Lua. On res==1 returns (true, release, nil)
// where release DECRs the in-flight counter. On res==0 returns (false,
// nil, nil) — the Lua already returned the slot. On error: fail-open
// (true, nil, err) so the dispatcher keeps serving under redis outage.
func (s *RedisCounterLoadSource) TryAcquire(ctx context.Context) (bool, func(), error) {
	res, err := s.script.Run(
		ctx,
		s.rdb,
		[]string{s.inflightKey, s.capKey},
		s.ttlSeconds,
		s.fallbackCap,
	).Int()
	if err != nil {
		return true, nil, err
	}
	if res == 1 {
		key := s.inflightKey
		rdb := s.rdb
		return true, func() { rdb.Decr(context.Background(), key) }, nil
	}
	return false, nil, nil
}
