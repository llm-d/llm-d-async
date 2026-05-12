/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package flowcontrol

import (
	"fmt"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

// DefaultCacheTTL retained for CLI flag compatibility.
const DefaultCacheTTL = 5 * time.Second

var _ pipeline.GateFactory = (*GateFactory)(nil)

// GateFactory creates Gate instances based on configuration.
type GateFactory struct {
	prometheusURL string
	cacheTTL      time.Duration
}

// GateFactoryOption is a functional option for configuring GateFactory.
type GateFactoryOption func(*GateFactory)

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

	default:
		// Unknown gate types default to always-Continue gate
		return pipeline.AlwaysContinue, nil
	}
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
