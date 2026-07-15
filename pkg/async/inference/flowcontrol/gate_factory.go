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

	"github.com/llm-d/llm-d-async/pipeline"
	redisgate "github.com/llm-d/llm-d-async/pkg/redis"
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
func (f *GateFactory) CreateGate(cfg pipeline.GateConfig) (pipeline.Gate, error) {
	params := cfg.GateParams
	if params == nil {
		params = map[string]any{}
	}

	switch cfg.GateType {
	case "composite":
		configs, err := paramGateConfigs(params, "gates")
		if err != nil {
			return nil, err
		}

		var innerGates []pipeline.Gate
		for _, innerCfg := range configs {
			gate, err := f.CreateGate(innerCfg)
			if err != nil {
				return nil, fmt.Errorf("composite gate failed to create inner gate %q: %w", innerCfg.GateType, err)
			}
			innerGates = append(innerGates, gate)
		}

		return NewCompositeGate(innerGates...), nil

	case "wait-on-refuse":
		innerCfg, err := paramGateConfig(params, "gate")
		if err != nil {
			return nil, err
		}

		innerGate, err := f.CreateGate(innerCfg)
		if err != nil {
			return nil, fmt.Errorf("wait-on-refuse gate failed to create inner gate %q: %w", innerCfg.GateType, err)
		}

		return NewWaitOnRefuseGate(innerGate), nil

	case "tier-priority-admission":
		satGateType := paramString(params, "saturation_gate", "")
		if satGateType == "" {
			return nil, fmt.Errorf("tier-priority-admission gate requires a 'saturation_gate' parameter")
		}
		satGateParams, err := paramMapAny(params, "saturation_gate_params")
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission gate failed to parse 'saturation_gate_params': %w", err)
		}

		satGate, err := f.CreateGate(pipeline.GateConfig{
			GateType:   satGateType,
			GateParams: satGateParams,
		})
		if err != nil {
			return nil, fmt.Errorf("tier-priority-admission gate failed to create saturation gate %q: %w", satGateType, err)
		}

		tierLabel := paramString(params, "tier_label", "tier")
		return NewTierPriorityAdmissionGate(satGate, tierLabel), nil

	case "constant":
		return ConstOpenGate(), nil

	case "redis":
		addr := paramString(params, "address", "")
		if addr == "" {
			return nil, fmt.Errorf("redis gate requires an 'address' in gate_params")
		}
		client, ok := f.redisClients[addr]
		if !ok {
			client = goredis.NewClient(&goredis.Options{Addr: addr})
			f.redisClients[addr] = client
		}
		budgetKey := paramString(params, "budget_key", "dispatch-gate-budget")
		return redisgate.NewRedisDispatchGate(client, budgetKey), nil

	case "redis-quota":
		addr := paramString(params, "address", "")
		if addr == "" {
			return nil, fmt.Errorf("redis-quota gate requires an 'address' in gate_params")
		}
		client, ok := f.redisClients[addr]
		if !ok {
			client = goredis.NewClient(&goredis.Options{Addr: addr})
			f.redisClients[addr] = client
		}

		attr := paramString(params, "attribute", "userid")

		mode := redisgate.QuotaMode(paramString(params, "mode", string(redisgate.QuotaModeRateLimit)))

		limit, err := paramInt(params, "limit", 0)
		if err != nil {
			return nil, fmt.Errorf("redis-quota gate requires a valid 'limit': %w", err)
		}
		if limit <= 0 {
			return nil, fmt.Errorf("redis-quota gate requires a positive 'limit', got %d", limit)
		}

		window, err := paramDuration(params, "window", 1*time.Minute)
		if err != nil {
			return nil, fmt.Errorf("redis-quota gate requires a valid 'window' duration: %w", err)
		}

		prefix := paramString(params, "prefix", "quota:")

		gate := redisgate.NewRedisQuotaGate(client, attr, mode, limit, window, prefix)
		gatingMode := redisgate.GatingMode(paramString(params, "gating_mode", ""))
		if gatingMode != "" {
			gate.WithGatingMode(gatingMode)
		}

		return gate, nil

	case "prometheus-saturation":
		if f.prometheusURL == "" {
			return nil, fmt.Errorf("prometheus-saturation gate type requires --prometheus-url flag to be set")
		}

		threshold, err := paramFloat(params, "threshold", 0.8)
		if err != nil {
			return nil, err
		}
		fallback, err := paramFloat(params, "fallback", 0.0)
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

		pool := paramString(params, "pool", "")
		if pool == "" {
			return nil, fmt.Errorf("inference pool name is required for prometheus-budget gate")
		}
		maxConcurrency, err := paramFloat(params, "max_concurrency", 100.0)
		if err != nil {
			return nil, err
		}
		if maxConcurrency <= 0 {
			return nil, fmt.Errorf("max_concurrency must be positive, got %g", maxConcurrency)
		}
		baseline, err := paramFloat(params, "baseline", 0.05)
		if err != nil {
			return nil, err
		}
		if baseline < 0 || baseline >= 1 {
			return nil, fmt.Errorf("baseline must be in [0, 1), got %g", baseline)
		}
		fallback, err := paramFloat(params, "fallback", 0.0)
		if err != nil {
			return nil, err
		}

		promConfig := promapi.Config{Address: f.prometheusURL}
		namespace := paramString(params, "namespace", "")

		primary, err := NewFlowControlQueueSizePromQL(promConfig, pool, maxConcurrency, namespace)
		if err != nil {
			return nil, err
		}
		secondary, err := NewVLLMSaturationPromQL(promConfig, pool, maxConcurrency, namespace)
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

		query := paramString(params, "query", "")
		if query == "" {
			return nil, fmt.Errorf("prometheus-query gate requires a 'query' parameter with a PromQL expression")
		}

		fallback, err := paramFloat(params, "fallback", 0.0)
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
		url := paramString(params, "url", "")
		if url == "" {
			return nil, fmt.Errorf("endpoint-scrape gate requires a 'url' parameter")
		}
		metric := paramString(params, "metric", "")
		if metric == "" {
			return nil, fmt.Errorf("endpoint-scrape gate requires a 'metric' parameter")
		}

		labels, err := paramStringMap(params, "labels")
		if err != nil {
			return nil, fmt.Errorf("endpoint-scrape gate failed to parse 'labels': %w", err)
		}

		maxCountPerPod, err := paramFloat(params, "max_count_per_pod", 0)
		if err != nil {
			return nil, err
		}
		baseline, err := paramFloat(params, "baseline", 0.0)
		if err != nil {
			return nil, err
		}
		fallback, err := paramFloat(params, "fallback", 0.0)
		if err != nil {
			return nil, err
		}

		podsLabels, err := paramStringMap(params, "pods_labels")
		if err != nil {
			return nil, fmt.Errorf("endpoint-scrape gate failed to parse 'pods_labels': %w", err)
		}

		scrapeCfg := ScrapeConfig{
			URL:            url,
			MetricName:     metric,
			Labels:         labels,
			MaxCountPerPod: maxCountPerPod,
			PodsURL:        paramString(params, "pods_url", ""),
			PodsMetric:     paramString(params, "pods_metric", ""),
			PodsLabels:     podsLabels,
		}

		var ms MetricSource = NewScrapeMetricSource(scrapeCfg)
		ms = cachedSource(ms, f.cacheTTL)
		return NewMetricDispatchGate(ms, baseline, fallback), nil

	case "local-max-concurrency":
		limit, err := paramInt(params, "limit", 0)
		if err != nil {
			return nil, fmt.Errorf("local-max-concurrency gate requires a valid integer 'limit': %w", err)
		}
		if limit <= 0 {
			return nil, fmt.Errorf("local-max-concurrency limit must be greater than 0, got %d", limit)
		}
		gate := NewLocalConcurrencyGate(limit)
		gatingMode := paramString(params, "gating_mode", "")
		if gatingMode != "" {
			if gatingMode != string(GatingModeBlocking) && gatingMode != string(GatingModeClassifying) {
				return nil, fmt.Errorf("local-max-concurrency gating_mode must be either 'blocking' or 'classifying', got %q", gatingMode)
			}
			gate.WithGatingMode(GatingMode(gatingMode))
		}
		return gate, nil

	default:
		// Unknown gate types default to open gate
		return ConstOpenGate(), nil
	}
}

// paramString extracts a string value from params, returning defaultVal if the key is absent or nil.
// Scalar types (string, float64, bool) are accepted; non-scalar types return defaultVal.
func paramString(params map[string]any, key, defaultVal string) string {
	v, ok := params[key]
	if !ok || v == nil {
		return defaultVal
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return defaultVal
		}
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		return defaultVal
	}
}

// paramFloat extracts a float64 value from params. Accepts float64, int, or string representations.
func paramFloat(params map[string]any, key string, defaultVal float64) (float64, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return defaultVal, nil
	}
	switch val := v.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case string:
		if val == "" {
			return defaultVal, nil
		}
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %s value '%s': %w", key, val, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("invalid %s value: unsupported type %T", key, v)
	}
}

// paramInt extracts an int value from params. Accepts float64, int, or string representations.
func paramInt(params map[string]any, key string, defaultVal int) (int, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return defaultVal, nil
	}
	switch val := v.(type) {
	case float64:
		if val != float64(int(val)) {
			return 0, fmt.Errorf("invalid %s value: %g is not an integer", key, val)
		}
		return int(val), nil
	case int:
		return val, nil
	case string:
		if val == "" {
			return defaultVal, nil
		}
		i, err := strconv.Atoi(val)
		if err != nil {
			return 0, fmt.Errorf("invalid %s value '%s': %w", key, val, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("invalid %s value: unsupported type %T", key, v)
	}
}

// paramDuration extracts a time.Duration from params. Accepts a string parseable by time.ParseDuration.
func paramDuration(params map[string]any, key string, defaultVal time.Duration) (time.Duration, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return defaultVal, nil
	}
	s, ok := v.(string)
	if !ok {
		s = fmt.Sprintf("%v", v)
	}
	if s == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value '%s': %w", key, s, err)
	}
	return d, nil
}

// paramStringMap extracts a map[string]string from params. Handles both
// a JSON-encoded string value and a map[string]any value (from structured config).
func paramStringMap(params map[string]any, key string) (map[string]string, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return nil, nil
		}
		var m map[string]string
		if err := json.Unmarshal([]byte(val), &m); err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", key, err)
		}
		return m, nil
	case map[string]any:
		m := make(map[string]string, len(val))
		for k, v := range val {
			m[k] = fmt.Sprintf("%v", v)
		}
		return m, nil
	case map[string]string:
		return val, nil
	default:
		return nil, fmt.Errorf("unsupported type %T for key %q", v, key)
	}
}

// paramGateConfigs extracts a []pipeline.GateConfig from params for the composite gate.
// Handles both a JSON-encoded string and a pre-parsed []any (from structured config).
func paramGateConfigs(params map[string]any, key string) ([]pipeline.GateConfig, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return nil, fmt.Errorf("composite gate requires 'gates' parameter with JSON array of gate configurations")
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return nil, fmt.Errorf("composite gate requires 'gates' parameter with JSON array of gate configurations")
		}
		var configs []pipeline.GateConfig
		if err := json.Unmarshal([]byte(val), &configs); err != nil {
			return nil, fmt.Errorf("composite gate failed to parse 'gates' parameter: %w", err)
		}
		return configs, nil
	case []any:
		data, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("composite gate failed to marshal 'gates' parameter: %w", err)
		}
		var configs []pipeline.GateConfig
		if err := json.Unmarshal(data, &configs); err != nil {
			return nil, fmt.Errorf("composite gate failed to parse 'gates' parameter: %w", err)
		}
		return configs, nil
	case []pipeline.GateConfig:
		return val, nil
	default:
		return nil, fmt.Errorf("composite gate: unsupported type %T for 'gates' parameter", v)
	}
}

// paramGateConfig extracts a single pipeline.GateConfig from params for the wait-on-refuse gate.
// Handles both a JSON-encoded string and a pre-parsed map[string]any (from structured config).
func paramGateConfig(params map[string]any, key string) (pipeline.GateConfig, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return pipeline.GateConfig{}, fmt.Errorf("wait-on-refuse gate requires a 'gate' parameter with the inner gate configuration")
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return pipeline.GateConfig{}, fmt.Errorf("wait-on-refuse gate requires a 'gate' parameter with the inner gate configuration")
		}
		var cfg pipeline.GateConfig
		if err := json.Unmarshal([]byte(val), &cfg); err != nil {
			return pipeline.GateConfig{}, fmt.Errorf("wait-on-refuse gate failed to parse 'gate' parameter: %w", err)
		}
		return cfg, nil
	case map[string]any:
		data, err := json.Marshal(val)
		if err != nil {
			return pipeline.GateConfig{}, fmt.Errorf("wait-on-refuse gate failed to marshal 'gate' parameter: %w", err)
		}
		var cfg pipeline.GateConfig
		if err := json.Unmarshal(data, &cfg); err != nil {
			return pipeline.GateConfig{}, fmt.Errorf("wait-on-refuse gate failed to parse 'gate' parameter: %w", err)
		}
		return cfg, nil
	case pipeline.GateConfig:
		return val, nil
	default:
		return pipeline.GateConfig{}, fmt.Errorf("wait-on-refuse gate: unsupported type %T for 'gate' parameter", v)
	}
}

func cachedSource(s MetricSource, ttl time.Duration) MetricSource {
	if ttl > 0 {
		return NewCachedMetricSource(s, ttl)
	}
	return s
}

// paramMapAny extracts a map[string]any from params.
func paramMapAny(params map[string]any, key string) (map[string]any, error) {
	v, ok := params[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return nil, nil
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(val), &m); err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", key, err)
		}
		return m, nil
	case map[string]any:
		return val, nil
	case map[string]string:
		m := make(map[string]any, len(val))
		for k, valStr := range val {
			m[k] = valStr
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported type %T for key %q", v, key)
	}
}
