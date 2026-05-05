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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGateFactory_WithCacheTTL(t *testing.T) {
	ttl := 10 * time.Second
	factory := NewGateFactoryWithCacheTTL("http://localhost:9090", ttl)
	assert.Equal(t, "http://localhost:9090", factory.prometheusURL)
	assert.Equal(t, ttl, factory.cacheTTL)
}

func TestGateFactory_CreateConstantGate(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("constant", nil)

	assert.NoError(t, err)
	assert.NotNil(t, gate)
	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "constant gate should always return 1.0")
}

func TestGateFactory_UnknownGateType(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("unknown-type", nil)

	assert.NoError(t, err)
	assert.NotNil(t, gate)
	budget := gate.Budget(context.Background())
	// Should fall back to ConstOpenGate
	assert.Equal(t, 1.0, budget, "unknown gate type should default to ConstOpenGate")
}

func TestGateFactory_EmptyGateType(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("", nil)

	assert.NoError(t, err)
	assert.NotNil(t, gate)
	budget := gate.Budget(context.Background())
	assert.Equal(t, 1.0, budget, "empty gate type should default to ConstOpenGate")
}

func TestGateFactory_PrometheusGateWithoutURL(t *testing.T) {
	factory := NewGateFactory("") // No Prometheus URL
	gate, err := factory.CreateGate("prometheus-saturation", map[string]string{})
	assert.Error(t, err, "should return error when Prometheus URL is not set")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "prometheus-saturation gate type requires --prometheus-url flag to be set")
}

func TestGateFactory_PrometheusGateWithoutPoolParam(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-saturation", map[string]string{})
	assert.Error(t, err, "should return error when pool parameter is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "inference pool name is required")
}

func TestGateFactory_PrometheusGateWithInvalidThreshold(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-saturation", map[string]string{
		"threshold": "not-a-number",
	})
	assert.Error(t, err, "should return error when threshold is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid threshold value")
}

func TestGateFactory_PrometheusGateWithInvalidFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-saturation", map[string]string{
		"fallback": "not-a-number",
	})
	assert.Error(t, err, "should return error when fallback is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid fallback value")
}

func TestGateFactory_PrometheusGateWithThresholdAndFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-saturation", map[string]string{
		"pool":      "my-pool",
		"threshold": "0.7",
		"fallback":  "0.3",
	})
	assert.NoError(t, err, "should create gate when threshold and fallback are valid floats")
	assert.NotNil(t, gate)
}

func TestGateFactory_RedisGateMissingAddress(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("redis", map[string]string{})
	assert.Error(t, err, "should return error when address is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "redis gate requires an 'address' in gate_params")
}

func TestGateFactory_RedisGateNilParams(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("redis", nil)
	assert.Error(t, err, "should return error when params is nil")
	assert.Nil(t, gate)
}

func TestGateFactory_RedisGateSharesClient(t *testing.T) {
	factory := NewGateFactory("")
	params := map[string]string{"address": "localhost:6379"}
	gate1, err1 := factory.CreateGate("redis", params)
	gate2, err2 := factory.CreateGate("redis", params)
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotNil(t, gate1)
	assert.NotNil(t, gate2)
	// Both gates should have been created from the same cached client
	assert.Len(t, factory.redisClients, 1, "should reuse the same Redis client for the same address")
}

func TestGateFactory_RedisGateDifferentAddresses(t *testing.T) {
	factory := NewGateFactory("")
	gate1, err1 := factory.CreateGate("redis", map[string]string{"address": "host1:6379"})
	gate2, err2 := factory.CreateGate("redis", map[string]string{"address": "host2:6379"})
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotNil(t, gate1)
	assert.NotNil(t, gate2)
	assert.Len(t, factory.redisClients, 2, "should create separate clients for different addresses")
}

func TestGateFactory_BudgetGateWithoutURL(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{"pool": "my-pool"})
	assert.Error(t, err, "should return error when Prometheus URL is not set")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "prometheus-budget gate type requires --prometheus-url flag to be set")
}

func TestGateFactory_BudgetGateMissingPool(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"max_concurrency": "100",
	})
	assert.Error(t, err, "should return error when pool is missing")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "inference pool name is required")
}

func TestGateFactory_BudgetGateDefaultMaxConcurrency(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool": "my-pool",
	})
	assert.NoError(t, err, "should use default max_concurrency=100 when not provided")
	assert.NotNil(t, gate)
}

func TestGateFactory_BudgetGateWithZeroMaxConcurrency(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool":            "my-pool",
		"max_concurrency": "0",
	})
	assert.Error(t, err)
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "max_concurrency must be positive")
}

func TestGateFactory_BudgetGateWithPoolAndMaxConcurrency(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool":            "my-pool",
		"max_concurrency": "100",
	})
	assert.NoError(t, err)
	assert.NotNil(t, gate)
}

func TestGateFactory_BudgetGateWithInvalidBaseline(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool":     "my-pool",
		"baseline": "not-a-number",
	})
	assert.Error(t, err, "should return error when baseline is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid baseline value")
}

func TestGateFactory_BudgetGateWithBaselineOutOfRange(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")

	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool":     "my-pool",
		"baseline": "1.0",
	})
	assert.Error(t, err, "baseline=1.0 should be rejected (gate would never open)")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "baseline must be in [0, 1)")

	gate, err = factory.CreateGate("prometheus-budget", map[string]string{
		"pool":     "my-pool",
		"baseline": "-0.1",
	})
	assert.Error(t, err, "negative baseline should be rejected")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "baseline must be in [0, 1)")
}

func TestGateFactory_BudgetGateWithInvalidFallback(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool":     "my-pool",
		"fallback": "not-a-number",
	})
	assert.Error(t, err, "should return error when fallback is not a valid float")
	assert.Nil(t, gate)
	assert.Contains(t, err.Error(), "invalid fallback value")
}

func TestGateFactory_BudgetGateWithAllParams(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")
	gate, err := factory.CreateGate("prometheus-budget", map[string]string{
		"pool":            "my-pool",
		"max_concurrency": "100",
		"baseline":        "0.1",
		"fallback":        "0.5",
	})
	assert.NoError(t, err)
	assert.NotNil(t, gate)
}
