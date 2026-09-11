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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateFactory_WithCacheTTL(t *testing.T) {
	ttl := 10 * time.Second
	factory := NewGateFactoryWithCacheTTL("http://localhost:9090", ttl)
	assert.Equal(t, "http://localhost:9090", factory.prometheusURL)
	assert.Equal(t, ttl, factory.cacheTTL)
}

func TestGateFactory_CreateConstantGate(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "constant"})

	assert.NoError(t, err)
	assert.NotNil(t, gate)
	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "constant gate should always return 1.0")
}

func TestGateFactory_UnknownGateType(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "unknown-type"})

	assert.NoError(t, err)
	assert.NotNil(t, gate)
	budget := gate.Budget(context.Background())
	// Should fall back to ConstOpenGate
	assert.Equal(t, 1.0, budget, "unknown gate type should default to ConstOpenGate")
}

func TestGateFactory_EmptyGateType(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{})

	assert.NoError(t, err)
	assert.NotNil(t, gate)
	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "empty gate type should default to ConstOpenGate")
}

func TestGateFactory_PrometheusGateWithoutURL(t *testing.T) {
	factory := NewGateFactory("") // No Prometheus URL
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{}})
	assert.Error(t, err, "should return error when Prometheus URL is not set")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "prometheus-saturation gate type requires --prometheus-url flag to be set")
}

func TestGateFactory_PrometheusGateWithoutPoolParam(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{}})
	assert.Error(t, err, "should return error when pool parameter is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "inference pool name is required")
}

func TestGateFactory_PrometheusGateWithInvalidThreshold(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{
		"threshold": "not-a-number",
	}})
	assert.Error(t, err, "should return error when threshold is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid threshold value")
}

func TestGateFactory_PrometheusGateWithInvalidFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{
		"fallback": "not-a-number",
	}})
	assert.Error(t, err, "should return error when fallback is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid fallback value")
}

func TestGateFactory_PrometheusGateWithThresholdAndFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{
		"pool":      "my-pool",
		"threshold": 0.7,
		"fallback":  0.3,
	}})
	assert.NoError(t, err, "should create gate when threshold and fallback are valid floats")
	assert.NotNil(t, gate)
}

func TestGateFactory_RedisGateMissingAddress(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "redis", GateParams: map[string]any{}})
	assert.Error(t, err, "should return error when address is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "redis gate requires an 'address' in gate_params")
}

func TestGateFactory_RedisGateNilParams(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "redis"})
	assert.Error(t, err, "should return error when params is nil")
	assert.Nil(t, gate)
}

func TestGateFactory_RedisGateSharesClient(t *testing.T) {
	factory := NewGateFactory("")
	cfg := pipeline.GateConfig{GateType: "redis", GateParams: map[string]any{"address": "localhost:6379"}}
	gate1, err1 := factory.CreateGate(cfg)
	gate2, err2 := factory.CreateGate(cfg)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotNil(t, gate1)
	assert.NotNil(t, gate2)
	// Both gates should have been created from the same cached client
	assert.Len(t, factory.redisClients, 1, "should reuse the same Redis client for the same address")
}

func TestGateFactory_RedisGateDifferentAddresses(t *testing.T) {
	factory := NewGateFactory("")
	gate1, err1 := factory.CreateGate(pipeline.GateConfig{GateType: "redis", GateParams: map[string]any{"address": "host1:6379"}})
	gate2, err2 := factory.CreateGate(pipeline.GateConfig{GateType: "redis", GateParams: map[string]any{"address": "host2:6379"}})
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotNil(t, gate1)
	assert.NotNil(t, gate2)
	assert.Len(t, factory.redisClients, 2, "should create separate clients for different addresses")
}

func TestGateFactory_BudgetGateWithoutURL(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{"pool": "my-pool"}})
	assert.Error(t, err, "should return error when Prometheus URL is not set")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "prometheus-budget gate type requires --prometheus-url flag to be set")
}

func TestGateFactory_BudgetGateMissingPool(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"max_concurrency": 100.0,
	}})
	assert.Error(t, err, "should return error when pool is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "inference pool name is required")
}

func TestGateFactory_BudgetGateDefaultMaxConcurrency(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool": "my-pool",
	}})
	assert.NoError(t, err, "should use default max_concurrency=100 when not provided")
	assert.NotNil(t, gate)
}

func TestGateFactory_BudgetGateWithZeroMaxConcurrency(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":            "my-pool",
		"max_concurrency": 0.0,
	}})
	assert.Error(t, err)
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "max_concurrency must be positive")
}

func TestGateFactory_BudgetGateWithPoolAndMaxConcurrency(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":            "my-pool",
		"max_concurrency": 100.0,
	}})
	assert.NoError(t, err)
	assert.NotNil(t, gate)
}

func TestGateFactory_BudgetGateWithInvalidBaseline(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":     "my-pool",
		"baseline": "not-a-number",
	}})
	assert.Error(t, err, "should return error when baseline is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid baseline value")
}

func TestGateFactory_BudgetGateWithBaselineOutOfRange(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":     "my-pool",
		"baseline": 1.0,
	}})
	assert.Error(t, err, "baseline=1.0 should be rejected (gate would never open)")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "baseline must be in [0, 1)")

	gate, err = factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":     "my-pool",
		"baseline": -0.1,
	}})
	assert.Error(t, err, "negative baseline should be rejected")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "baseline must be in [0, 1)")
}

func TestGateFactory_BudgetGateWithInvalidFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":     "my-pool",
		"fallback": "not-a-number",
	}})
	assert.Error(t, err, "should return error when fallback is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid fallback value")
}

func TestGateFactory_BudgetGateWithAllParams(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":            "my-pool",
		"max_concurrency": 100.0,
		"baseline":        0.1,
		"fallback":        0.5,
	}})
	assert.NoError(t, err)
	assert.NotNil(t, gate)
}

func TestGateFactory_BudgetGateCascadeSources(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":            "my-pool",
		"max_concurrency": 100.0,
	}})
	require.NoError(t, err)

	metricGate, ok := gate.(*MetricDispatchGate)
	require.True(t, ok)
	cascade, ok := metricGate.source.(*CascadeMetricSource)
	require.True(t, ok)
	require.Len(t, cascade.sources, 3)

	exprs := make([]string, len(cascade.sources))
	for i, s := range cascade.sources {
		cached, ok := s.(*CachedMetricSource)
		require.True(t, ok)
		promSource, ok := cached.source.(*PromQLMetricSource)
		require.True(t, ok)
		exprs[i] = promSource.expr
	}

	// Flow control stays first for installs that enable the plugin; the metric a
	// stock EPP always exports is next, so the cascade resolves without it; vLLM
	// last because it needs scrape-time relabeling to carry inference_pool.
	assert.Contains(t, exprs[0], "inference_extension_flow_control_queue_size")
	assert.Contains(t, exprs[1], "inference_pool_per_pod_queue_size")
	assert.Contains(t, exprs[2], "vllm:num_requests_running")
}

func TestGateFactory_BudgetGateLogsResolvedQueries(t *testing.T) {
	var logged []string
	logger := funcr.New(func(_, args string) { logged = append(logged, args) }, funcr.Options{})

	factory := NewGateFactory("http://localhost:9090").WithLogger(logger)
	_, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool": "my-pool",
	}})
	require.NoError(t, err)

	joined := strings.Join(logged, "\n")
	assert.Contains(t, joined, "inference_extension_flow_control_queue_size")
	assert.Contains(t, joined, "inference_pool_per_pod_queue_size")
	assert.Contains(t, joined, "vllm:num_requests_running")

	// The resolved closing point goes to the same logger as the source queries.
	assert.Contains(t, joined, "prometheus-budget gate configured")
	assert.Contains(t, joined, "closesAtLoadPerReadyPod")
}

func TestGateFactory_SaturationGateLogsResolvedQuery(t *testing.T) {
	var logged []string
	logger := funcr.New(func(_, args string) { logged = append(logged, args) }, funcr.Options{})

	factory := NewGateFactory("http://localhost:9090").WithLogger(logger)
	_, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{
		"pool": "my-pool",
	}})
	require.NoError(t, err)

	assert.Contains(t, strings.Join(logged, "\n"), "llm_d_epp_flow_control_pool_saturation")
}

func TestGateFactory_PrometheusQueryGateWithoutURL(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-query", GateParams: map[string]any{
		"query": "up",
	}})
	assert.Error(t, err, "should return error when Prometheus URL is not set")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "prometheus-query gate type requires --prometheus-url flag to be set")
}

func TestGateFactory_PrometheusQueryGateMissingQuery(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-query", GateParams: map[string]any{}})
	assert.Error(t, err, "should return error when query parameter is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "requires a 'query' parameter")
}

func TestGateFactory_PrometheusQueryGateWithInvalidFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-query", GateParams: map[string]any{
		"query":    "up",
		"fallback": "not-a-number",
	}})
	assert.Error(t, err, "should return error when fallback is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid fallback value")
}

func TestGateFactory_PrometheusQueryGateWithDefaults(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-query", GateParams: map[string]any{
		"query": "up",
	}})
	assert.NoError(t, err, "should create gate with default fallback")
	assert.NotNil(t, gate)
}

func TestGateFactory_PrometheusQueryGateWithAllParams(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-query", GateParams: map[string]any{
		"query":    `1 - (sum(rate(http_requests_total[5m])) / 100)`,
		"fallback": 0.5,
	}})
	assert.NoError(t, err, "should create gate with all params specified")
	assert.NotNil(t, gate)
}

// TestGateFactory_StampsOwnerOnMetricGate checks that the owning queue and worker
// pool reach the gauges a metric gate records, and that the 'pool' param lands on
// inference_pool instead of overwriting pool_name (issue #369).
func TestGateFactory_StampsOwnerOnMetricGate(t *testing.T) {
	owner := pipeline.GateOwner{QueueID: "team-a-premium", QueueName: "queue:a", WorkerPoolID: "model-a-pool"}
	factory := NewGateFactory("http://localhost:9090")

	gate, err := factory.CreateGate(pipeline.GateConfig{
		GateType:   "prometheus-query",
		GateParams: map[string]any{"query": "up", "pool": "optimized-baseline"},
		Owner:      owner,
	})
	assert.NoError(t, err)

	metricGate, ok := gate.(*MetricDispatchGate)
	assert.True(t, ok, "prometheus-query should produce a MetricDispatchGate")
	assert.Equal(t, owner, metricGate.owner)
	assert.Equal(t, "optimized-baseline", metricGate.inferencePool)
}

// TestGateFactory_PropagatesOwnerThroughWrappers checks that the owner survives the
// factory's recursive gate types — a gate nested inside wait-on-refuse inside
// composite still labels its metrics with the queue that owns it (issue #369).
func TestGateFactory_PropagatesOwnerThroughWrappers(t *testing.T) {
	owner := pipeline.GateOwner{QueueID: "team-b-standard", QueueName: "queue:b", WorkerPoolID: "model-b-pool"}
	factory := NewGateFactory("http://localhost:9090")

	gate, err := factory.CreateGate(pipeline.GateConfig{
		GateType: "composite",
		GateParams: map[string]any{
			"gates": []any{
				map[string]any{
					"gate_type": "wait-on-refuse",
					"gate_params": map[string]any{
						"gate": map[string]any{
							"gate_type":   "prometheus-query",
							"gate_params": map[string]any{"query": "up"},
						},
					},
				},
			},
		},
		Owner: owner,
	})
	assert.NoError(t, err)

	composite, ok := gate.(*CompositeGate)
	assert.True(t, ok)
	assert.Len(t, composite.gates, 1)
	waiter, ok := composite.gates[0].(*WaitOnRefuseGate)
	assert.True(t, ok)
	metricGate, ok := waiter.inner.(*MetricDispatchGate)
	assert.True(t, ok)
	assert.Equal(t, owner, metricGate.owner)
}

// TestGateFactory_PropagatesOwnerToSaturationGate covers the third recursion site,
// tier-priority-admission's inner saturation gate (issue #369).
func TestGateFactory_PropagatesOwnerToSaturationGate(t *testing.T) {
	owner := pipeline.GateOwner{QueueID: "team-c-batch", QueueName: "queue:c", WorkerPoolID: "model-c-pool"}
	factory := NewGateFactory("http://localhost:9090")

	gate, err := factory.CreateGate(pipeline.GateConfig{
		GateType: "tier-priority-admission",
		GateParams: map[string]any{
			"saturation_gate":        "prometheus-query",
			"saturation_gate_params": map[string]any{"query": "up"},
		},
		Owner: owner,
	})
	assert.NoError(t, err)

	tierGate, ok := gate.(*TierPriorityAdmissionGate)
	assert.True(t, ok)
	metricGate, ok := tierGate.saturationGate.(*MetricDispatchGate)
	assert.True(t, ok)
	assert.Equal(t, owner, metricGate.owner)
}
