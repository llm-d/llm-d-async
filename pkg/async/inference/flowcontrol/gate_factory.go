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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	redisgate "github.com/llm-d-incubation/llm-d-async/pkg/redis"
	promapi "github.com/prometheus/client_golang/api"
	goredis "github.com/redis/go-redis/v9"
)

// DefaultCacheTTL is the default TTL for cached Prometheus metric sources.
const DefaultCacheTTL = 5 * time.Second

var _ pipeline.GateFactory = (*GateFactory)(nil)

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

// Close closes all Redis clients created by this factory.
func (f *GateFactory) Close() error {
	var firstErr error
	for addr, client := range f.redisClients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close Redis client for %s: %w", addr, err)
		}
	}
	return firstErr
}

// CreateGate creates a DispatchGate based on the gate type and parameters.
// Supported gate types:
//   - "constant": Always returns budget 1.0 (fully open)
//   - "redis": Queries Redis for dispatch budget
//   - "prometheus-saturation": Queries Prometheus for pool saturation metric.
//     Params: pool (required), threshold (default 0.8), fallback (default 0.0)
//   - "composite": Combines multiple gates. Params: gates (JSON array of gate configurations)
//   - "prometheus-budget": Cascades two Prometheus metric sources to compute dispatch budget D.
//     Both sources compute max_SYS = ready_pods × max_concurrency dynamically.
//     Primary: D = 1 − (queue_size / max_SYS) via inference_extension_flow_control_queue_size.
//     Secondary (fallback): D = 1 − (vllm_running / max_SYS).
//     The primary source requires llm-d's flow control plugin to be enabled.
//     The fallback filters vLLM metrics by inference_pool label, which vLLM does not
//     emit natively — model server pods must carry this label and Prometheus must be
//     configured with metric relabeling to propagate it (see docs/guides/e2e-deploy.md).
//     Gate closes when D ≤ B (baseline); returns D − B when open, so callers compute
//     N = max_SYS × (D − B). Params: pool (required),
//     max_concurrency (default 100), baseline (default 0.05), fallback (default 0.0)
//   - "prometheus-query": Evaluates an arbitrary user-supplied PromQL expression as the dispatch
//     budget. The expression must resolve to an instant vector with a single sample whose value
//     is in [0, 1]. Unlike prometheus-saturation and prometheus-budget, this gate does not
//     construct queries internally — the user provides the complete PromQL expression.
//     Params: query (required), fallback (default 0.0)
//
// For unsupported or unknown gate types, returns ConstOpenGate as a safe default.
func (f *GateFactory) CreateGate(gateType string, params map[string]string) (pipeline.DispatchGate, error) {
	switch gateType {
	case "composite":
		gatesJSON := params["gates"]
		if gatesJSON == "" {
			return nil, fmt.Errorf("composite gate requires 'gates' parameter with JSON array of gate configurations")
		}

		type gateConfig struct {
			GateType   string            `json:"gate_type"`
			GateParams map[string]string `json:"gate_params"`
		}

		var configs []gateConfig
		if err := json.Unmarshal([]byte(gatesJSON), &configs); err != nil {
			return nil, fmt.Errorf("composite gate failed to parse 'gates' parameter: %w", err)
		}

		var innerGates []pipeline.DispatchGate
		for _, cfg := range configs {
			gate, err := f.CreateGate(cfg.GateType, cfg.GateParams)
			if err != nil {
				return nil, fmt.Errorf("composite gate failed to create inner gate %q: %w", cfg.GateType, err)
			}
			innerGates = append(innerGates, gate)
		}

		return NewCompositeGate(innerGates...), nil

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

	case "redis-quota":
		addr := params["address"]
		if addr == "" {
			return nil, fmt.Errorf("redis-quota gate requires an 'address' in gate_params")
		}
		client, ok := f.redisClients[addr]
		if !ok {
			client = goredis.NewClient(&goredis.Options{Addr: addr})
			f.redisClients[addr] = client
		}

		attr := params["attribute"]
		if attr == "" {
			attr = "userid"
		}

		mode := redisgate.QuotaMode(params["mode"])
		if mode == "" {
			mode = redisgate.QuotaModeRateLimit
		}

		limit, err := strconv.Atoi(params["limit"])
		if err != nil {
			return nil, fmt.Errorf("redis-quota gate requires a valid 'limit': %w", err)
		}

		windowStr := params["window"]
		if windowStr == "" {
			windowStr = "1m"
		}
		window, err := time.ParseDuration(windowStr)
		if err != nil {
			return nil, fmt.Errorf("redis-quota gate requires a valid 'window' duration: %w", err)
		}

		prefix := params["prefix"]
		if prefix == "" {
			prefix = "quota:"
		}

		gate := redisgate.NewRedisQuotaGate(client, attr, mode, limit, window, prefix)
		gatingMode := redisgate.GatingMode(params["gating_mode"])
		if gatingMode != "" {
			gate.WithGatingMode(gatingMode)
		}

		return gate, nil

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
		if baseline < 0 || baseline >= 1 {
			return nil, fmt.Errorf("baseline must be in [0, 1), got %g", baseline)
		}
		fallback, err := parseFloat("fallback", params["fallback"], 0.0)
		if err != nil {
			return nil, err
		}

		promConfig := promapi.Config{Address: f.prometheusURL}

		primary, err := NewFlowControlQueueSizePromQL(promConfig, pool, maxConcurrency)
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

	case "prometheus-query":
		if f.prometheusURL == "" {
			return nil, fmt.Errorf("prometheus-query gate type requires --prometheus-url flag to be set")
		}

		query := params["query"]
		if query == "" {
			return nil, fmt.Errorf("prometheus-query gate requires a 'query' parameter with a PromQL expression")
		}

		fallback, err := parseFloat("fallback", params["fallback"], 0.0)
		if err != nil {
			return nil, err
		}

		promConfig := promapi.Config{Address: f.prometheusURL}
		source, err := NewPromQLMetricSource(promConfig, query)
		if err != nil {
			return nil, err
		}
		return NewMetricDispatchGate(cachedSource(source, f.cacheTTL), 0.0, fallback), nil

	case "endpoint-scrape":
		url := params["url"]
		if url == "" {
			return nil, fmt.Errorf("endpoint-scrape gate requires a 'url' parameter")
		}
		metric := params["metric"]
		if metric == "" {
			return nil, fmt.Errorf("endpoint-scrape gate requires a 'metric' parameter")
		}

		var labels map[string]string
		if labelsJSON := params["labels"]; labelsJSON != "" {
			if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
				return nil, fmt.Errorf("endpoint-scrape gate failed to parse 'labels': %w", err)
			}
		}

		maxCountPerPod, err := parseFloat("max_count_per_pod", params["max_count_per_pod"], 0)
		if err != nil {
			return nil, err
		}
		baseline, err := parseFloat("baseline", params["baseline"], 0.0)
		if err != nil {
			return nil, err
		}
		fallback, err := parseFloat("fallback", params["fallback"], 0.0)
		if err != nil {
			return nil, err
		}

		var podsLabels map[string]string
		if podsLabelsJSON := params["pods_labels"]; podsLabelsJSON != "" {
			if err := json.Unmarshal([]byte(podsLabelsJSON), &podsLabels); err != nil {
				return nil, fmt.Errorf("endpoint-scrape gate failed to parse 'pods_labels': %w", err)
			}
		}

		cfg := ScrapeConfig{
			URL:            url,
			MetricName:     metric,
			Labels:         labels,
			MaxCountPerPod: maxCountPerPod,
			PodsURL:        params["pods_url"],
			PodsMetric:     params["pods_metric"],
			PodsLabels:     podsLabels,
		}

		var ms MetricSource = NewScrapeMetricSource(cfg)
		ms = cachedSource(ms, f.cacheTTL)
		return NewMetricDispatchGate(ms, baseline, fallback), nil

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

func cachedSource(s MetricSource, ttl time.Duration) MetricSource {
	if ttl > 0 {
		return NewCachedMetricSource(s, ttl)
	}
	return s
}
