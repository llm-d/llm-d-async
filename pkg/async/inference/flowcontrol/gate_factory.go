/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package flowcontrol

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

// DefaultCacheTTL retained for CLI flag compatibility.
const DefaultCacheTTL = 5 * time.Second

var _ pipeline.GateFactory = (*GateFactory)(nil)

// GateFactory creates Gate instances based on configuration.
type GateFactory struct {
	prometheusURL string
	cacheTTL      time.Duration
	rdb           *redis.Client
	ctx           context.Context
}

// GateFactoryOption is a functional option for configuring GateFactory.
type GateFactoryOption func(*GateFactory)

// WithRedisClient wires a shared *redis.Client into the factory. Required
// for gate types that need redis (currently: tier-priority-admission with
// source=redis-counter). Caller owns the client's lifecycle.
func WithRedisClient(rdb *redis.Client) GateFactoryOption {
	return func(f *GateFactory) { f.rdb = rdb }
}

// WithBackgroundContext wires a long-lived context that the factory uses
// to drive background goroutines for gates that need them (e.g.
// tier-priority-admission with source=prometheus runs a PromQL refresh
// loop). Canceling ctx stops those loops. If unset, gates use
// context.Background().
func WithBackgroundContext(ctx context.Context) GateFactoryOption {
	return func(f *GateFactory) { f.ctx = ctx }
}

// NewGateFactory creates a new GateFactory. prometheusURL is retained for
// forward compatibility but unused by the current gate set.
func NewGateFactory(prometheusURL string, opts ...GateFactoryOption) *GateFactory {
	return NewGateFactoryWithCacheTTL(prometheusURL, DefaultCacheTTL, opts...)
}

// NewGateFactoryWithCacheTTL creates a GateFactory with a custom cache TTL.
func NewGateFactoryWithCacheTTL(prometheusURL string, cacheTTL time.Duration, opts ...GateFactoryOption) *GateFactory {
	f := &GateFactory{
		prometheusURL: prometheusURL,
		cacheTTL:      cacheTTL,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// CreateGate creates a Gate based on the gate type and parameters.
// Supported gate types:
//   - "constant": always Continue.
//   - "local-rate-limit": pod-local token bucket. Params:
//     requests_per_minute (required), burst (default 1).
//   - "local-max-concurrency": pod-local in-flight semaphore. Params:
//     max_concurrency (required, integer >= 1).
//   - "deadline-drop": drops messages whose deadline has passed.
//   - "constant-decision": always returns the configured verdict.
//     Params: decision (continue|drop|refuse).
//   - "tier-priority-admission": pool saturation gate with three-way verdict.
//     Params: source (prometheus|redis-counter, required), plus source-specific
//     params. See TierPriorityAdmissionConfig and PoolLoadSource for details.
//   - "reservation-classifier": redis-backed multi-key reservation
//     classifier. Stamps a `class` label (configurable key) with
//     reserved/overflow based on per-bucket in-flight vs cap.
//     Requires WithRedisClient. Params:
//     bucket_keys (required, comma-separated label keys),
//     class_label (default "class"),
//     reserved_value (default "reserved"),
//     overflow_value (default "overflow"),
//     key_prefix (default "rmp"),
//     ttl_seconds (default 86400),
//     fallback_cap (default 0).
//
// For unknown gate types, returns the always-Continue gate as a safe default.
func (f *GateFactory) CreateGate(gateType string, params map[string]string) (pipeline.Gate, error) {
	switch gateType {
	case "constant", "":
		return pipeline.AlwaysContinue, nil

	case "local-rate-limit":
		requestsPerMinute, err := parseRequiredFloat("requests_per_minute", params["requests_per_minute"])
		if err != nil {
			return nil, err
		}
		burst, err := parseFloat("burst", params["burst"], 1)
		if err != nil {
			return nil, err
		}
		return NewLocalRateLimitGate(requestsPerMinute, burst), nil

	case "local-max-concurrency":
		maxConcurrency, err := parseRequiredFloat("max_concurrency", params["max_concurrency"])
		if err != nil {
			return nil, err
		}
		if maxConcurrency < 1 {
			return nil, fmt.Errorf("max_concurrency must be >= 1, got %v", maxConcurrency)
		}
		return NewLocalMaxConcurrencyGate(int(maxConcurrency)), nil

	case "deadline-drop":
		return NewDeadlineDropGate(), nil

	case "constant-decision":
		raw := params["decision"]
		if raw == "" {
			raw = "continue"
		}
		v, err := parseDecision(raw)
		if err != nil {
			return nil, fmt.Errorf("constant-decision gate: %w", err)
		}
		return NewConstantDecisionGate(v), nil

	case "tier-priority-admission":
		return f.createTierPriorityAdmission(params)

	case "reservation-classifier":
		if f.rdb == nil {
			return nil, fmt.Errorf("reservation-classifier gate requires a redis client (use WithRedisClient on the gate factory)")
		}
		raw := strings.TrimSpace(params["bucket_keys"])
		if raw == "" {
			return nil, fmt.Errorf("reservation-classifier gate: bucket_keys is required")
		}
		bucketKeys := splitAndTrimCSV(raw)
		if len(bucketKeys) == 0 {
			return nil, fmt.Errorf("reservation-classifier gate: bucket_keys must contain at least one label key")
		}
		ttl, err := parseInt("ttl_seconds", params["ttl_seconds"], defaultClassifierTTLSeconds)
		if err != nil {
			return nil, err
		}
		fallbackCap, err := parseInt("fallback_cap", params["fallback_cap"], 0)
		if err != nil {
			return nil, err
		}
		return NewReservationClassifierGate(f.rdb, ReservationClassifierConfig{
			BucketKeys:    bucketKeys,
			ClassKey:      params["class_label"],
			ReservedValue: params["reserved_value"],
			OverflowValue: params["overflow_value"],
			KeyPrefix:     params["key_prefix"],
			TTLSeconds:    ttl,
			FallbackCap:   fallbackCap,
		})

	default:
		// Unknown gate types default to always-Continue gate
		return pipeline.AlwaysContinue, nil
	}
}

// createTierPriorityAdmission builds a TierPriorityAdmissionGate from
// factory params. source=prometheus starts a PromQL refresh goroutine
// using the factory's background context; source=redis-counter uses the
// factory's redis client.
func (f *GateFactory) createTierPriorityAdmission(params map[string]string) (pipeline.Gate, error) {
	source := params["source"]
	if source == "" {
		return nil, fmt.Errorf("tier-priority-admission: source is required (prometheus | redis-counter)")
	}

	var loadSource PoolLoadSource
	switch source {
	case "prometheus":
		url := params["url"]
		if url == "" {
			url = f.prometheusURL
		}
		if url == "" {
			return nil, fmt.Errorf("tier-priority-admission: source=prometheus requires url (param or factory's prometheusURL)")
		}
		query := params["query"]
		if query == "" {
			return nil, fmt.Errorf("tier-priority-admission: source=prometheus requires query")
		}
		threshold, err := parseRequiredFloat("threshold", params["threshold"])
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		intervalMS, err := parseInt("refresh_interval_ms", params["refresh_interval_ms"], 1000)
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		staleMS, err := parseInt("stale_after_ms", params["stale_after_ms"], 30000)
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		src, err := NewPromQLLoadSource(PromQLLoadSourceConfig{
			URL:        url,
			Query:      query,
			Threshold:  threshold,
			Interval:   time.Duration(intervalMS) * time.Millisecond,
			StaleAfter: time.Duration(staleMS) * time.Millisecond,
		})
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		ctx := f.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		src.Start(ctx)
		loadSource = src

	case "redis-counter":
		if f.rdb == nil {
			return nil, fmt.Errorf("tier-priority-admission: source=redis-counter requires WithRedisClient on the factory")
		}
		bucket := params["bucket"]
		if bucket == "" {
			return nil, fmt.Errorf("tier-priority-admission: source=redis-counter requires bucket")
		}
		ttl, err := parseInt("ttl_seconds", params["ttl_seconds"], defaultRedisTTLSeconds)
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		fallback, err := parseInt("fallback_cap", params["fallback_cap"], 0)
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		src, err := NewRedisCounterLoadSource(f.rdb, RedisCounterLoadSourceConfig{
			Bucket:      bucket,
			KeyPrefix:   params["key_prefix"],
			TTLSeconds:  ttl,
			FallbackCap: fallback,
		})
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission: %w", err)
		}
		loadSource = src

	default:
		return nil, fmt.Errorf("tier-priority-admission: unknown source %q (want prometheus | redis-counter)", source)
	}

	pollMS, err := parseInt("block_poll_interval_ms", params["block_poll_interval_ms"], 100)
	if err != nil {
		return nil, fmt.Errorf("tier-priority-admission: %w", err)
	}
	failFastStatus, err := parseInt("fail_fast_status", params["fail_fast_status"], 429)
	if err != nil {
		return nil, fmt.Errorf("tier-priority-admission: %w", err)
	}
	return NewTierPriorityAdmissionGate(TierPriorityAdmissionConfig{
		Source:            loadSource,
		TierLabel:         params["tier_label"],
		ClassLabel:        params["class_label"],
		ReservedVal:       params["reserved_value"],
		OverflowVal:       params["overflow_value"],
		FailFastTier:      params["fail_fast_tier"],
		BlockPollInterval: time.Duration(pollMS) * time.Millisecond,
		FailFastStatus:    failFastStatus,
		FailFastBody:      params["fail_fast_body"],
	})
}

func parseFloat(name, str string, defaultValue float64) (float64, error) {
	if str == "" {
		return defaultValue, nil
	}
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value '%s': %w", name, str, err)
	}
	return v, nil
}

func parseRequiredFloat(name, str string) (float64, error) {
	if str == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	return parseFloat(name, str, 0)
}

func parseInt(name, str string, defaultValue int) (int, error) {
	if str == "" {
		return defaultValue, nil
	}
	v, err := strconv.Atoi(str)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value '%s': %w", name, str, err)
	}
	return v, nil
}

func splitAndTrimCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
