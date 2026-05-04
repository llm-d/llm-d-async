/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"fmt"
	"strconv"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	redisgate "github.com/llm-d-incubation/llm-d-async/pkg/redis"
	promapi "github.com/prometheus/client_golang/api"
	goredis "github.com/redis/go-redis/v9"
)

// DefaultCacheTTL is the default TTL for cached Prometheus metric sources.
const DefaultCacheTTL = 5 * time.Second

var _ asyncapi.GateFactory = (*GateFactory)(nil)

// GateFactory creates DispatchGate instances based on configuration.
type GateFactory struct {
	prometheusURL string
	cacheTTL      time.Duration
	redisClients  map[string]*goredis.Client
}

// NewGateFactory creates a new GateFactory with an optional Prometheus URL.
// If prometheusURL is empty, Prometheus gates will fail at creation time.
// Prometheus metric sources are cached with DefaultCacheTTL; use
// NewGateFactoryWithCacheTTL to override.
func NewGateFactory(prometheusURL string) *GateFactory {
	return NewGateFactoryWithCacheTTL(prometheusURL, DefaultCacheTTL)
}

// NewGateFactoryWithCacheTTL creates a GateFactory with a custom cache TTL
// for Prometheus metric sources. A TTL of 0 disables caching.
func NewGateFactoryWithCacheTTL(prometheusURL string, cacheTTL time.Duration) *GateFactory {
	return &GateFactory{
		prometheusURL: prometheusURL,
		cacheTTL:      cacheTTL,
		redisClients:  make(map[string]*goredis.Client),
	}
}

// CreateGate creates a DispatchGate based on the gate type and parameters.
// Supported gate types:
//   - "constant": Always returns budget 1.0 (fully open)
//   - "redis": Queries Redis for dispatch budget
//   - "prometheus-saturation": Queries Prometheus for pool saturation metric.
//     Params: pool (required), threshold (default 0.8), fallback (default 0.0)
//   - "prometheus-budget": Cascades two Prometheus metric sources to compute dispatch budget D.
//     Primary: D = 1 − (queue_size / max_sys) via inference_extension_flow_control_queue_size.
//     Secondary (fallback): D = 1 − (vllm_running / (ready_pods × max_concurrency)).
//     Gate closes when D ≤ B (baseline); returns D − B when open, so callers compute
//     N = max_SYS × (D − B). Params: pool, max_sys (required),
//     max_concurrency (default 100), baseline (default 0.05), fallback (default 0.0)
//
// For unsupported or unknown gate types, returns ConstOpenGate as a safe default.
func (f *GateFactory) CreateGate(gateType string, params map[string]string) (asyncapi.DispatchGate, error) {
	switch gateType {
	case "constant":
		return ConstOpenGate(), nil

	case "redis":
		addr := params["address"]
		if addr == "" {
			return nil, fmt.Errorf("redis gate requires an 'address' in gate_params")
		}
		client, ok := f.redisClients[addr]
		if !ok {
			client = goredis.NewClient(&goredis.Options{Addr: addr})
			f.redisClients[addr] = client
		}
		budgetKey := params["budget_key"]
		if budgetKey == "" {
			budgetKey = "dispatch-gate-budget"
		}
		return redisgate.NewRedisDispatchGate(client, budgetKey), nil

	case "prometheus-saturation":
		if f.prometheusURL == "" {
			return nil, fmt.Errorf("prometheus-saturation gate type requires --prometheus-url flag to be set")
		}

		threshold, err := parseFloat("threshold", params["threshold"], 0.8)
		if err != nil {
			return nil, err
		}
		fallback, err := parseFloat("fallback", params["fallback"], 0.0)
		if err != nil {
			return nil, err
		}

		promConfig := promapi.Config{Address: f.prometheusURL}
		source, err := NewSaturationPromQLSourceFromConfig(promConfig, params)
		if err != nil {
			return nil, err
		}
		var ms MetricSource = source
		if f.cacheTTL > 0 {
			ms = NewCachedMetricSource(source, f.cacheTTL)
		}
		return NewSaturationDispatchGate(ms, threshold, fallback), nil

	case "prometheus-budget":
		if f.prometheusURL == "" {
			return nil, fmt.Errorf("prometheus-budget gate type requires --prometheus-url flag to be set")
		}

		pool := params["pool"]
		if pool == "" {
			return nil, fmt.Errorf("inference pool name is required for prometheus-budget gate")
		}
		maxSys, err := parsePositiveFloat("max_sys", params["max_sys"])
		if err != nil {
			return nil, err
		}
		maxConcurrency, err := parseFloat("max_concurrency", params["max_concurrency"], 100.0)
		if err != nil {
			return nil, err
		}
		if maxConcurrency <= 0 {
			return nil, fmt.Errorf("max_concurrency must be positive, got %g", maxConcurrency)
		}
		baseline, err := parseFloat("baseline", params["baseline"], 0.05)
		if err != nil {
			return nil, err
		}
		fallback, err := parseFloat("fallback", params["fallback"], 0.0)
		if err != nil {
			return nil, err
		}

		promConfig := promapi.Config{Address: f.prometheusURL}

		primary, err := NewFlowControlQueueSizePromQL(promConfig, pool, maxSys)
		if err != nil {
			return nil, err
		}
		secondary, err := NewVLLMSaturationPromQL(promConfig, pool, maxConcurrency)
		if err != nil {
			return nil, err
		}

		var ms MetricSource = NewCascadeMetricSource(
			cachedSource(primary, f.cacheTTL),
			cachedSource(secondary, f.cacheTTL),
		)
		return NewBudgetDispatchGate(ms, baseline, fallback), nil

	default:
		// Unknown gate types default to open gate
		return ConstOpenGate(), nil
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

func parsePositiveFloat(name, str string) (float64, error) {
	if str == "" {
		return 0, fmt.Errorf("prometheus-budget gate requires '%s' parameter", name)
	}
	v, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value '%s': %w", name, str, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %g", name, v)
	}
	return v, nil
}

func cachedSource(s MetricSource, ttl time.Duration) MetricSource {
	if ttl > 0 {
		return NewCachedMetricSource(s, ttl)
	}
	return s
}
