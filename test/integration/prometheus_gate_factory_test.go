//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGateFactory_PrometheusSaturation_EndToEnd validates the full wiring:
// GateFactory.CreateGate("prometheus-saturation", params) ->
//
//	NewSaturationPromQLSourceFromConfig -> NewPromQLMetricSource ->
//	CachedMetricSource -> SaturationDispatchGate -> Budget()
//
// This covers cross-package interaction between flowcontrol (factory, cache,
// gate) and the Prometheus HTTP client against a real httptest.Server.
func TestGateFactory_PrometheusSaturation_EndToEnd(t *testing.T) {
	var queryCount int64
	body := &atomic.Value{}
	body.Store(promVectorResponse("0.3"))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&queryCount, 1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body.Load().(string))
	}))
	defer server.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL(server.URL, 200*time.Millisecond)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{
		"pool":      "my-pool",
		"threshold": 0.8,
		"fallback":  0.0,
	}})
	require.NoError(t, err)

	ctx := context.Background()

	// Mock returns 0.3 as the result of the PromQL expression "1 - saturation".
	// So D = 0.3. Threshold in budget space = 1 - 0.8 = 0.2.
	// Gate returns D - threshold = 0.3 - 0.2 = 0.1 (gate open).
	budget := gate.Budget(ctx)
	assert.InDelta(t, 0.1, budget, 0.01, "Budget should be ~0.1 when metric returns 0.3, threshold=0.2")
	assert.Equal(t, int64(1), atomic.LoadInt64(&queryCount))

	// Second call within cache TTL should NOT trigger another HTTP request.
	budget2 := gate.Budget(ctx)
	assert.InDelta(t, 0.1, budget2, 0.01)
	assert.Equal(t, int64(1), atomic.LoadInt64(&queryCount), "Should use cached result")

	// Wait for cache TTL to expire and update response.
	time.Sleep(250 * time.Millisecond)
	body.Store(promVectorResponse("0.05"))

	// Mock returns 0.05 => D = 0.05, threshold = 0.2 => D <= threshold => gate closed.
	budget3 := gate.Budget(ctx)
	assert.Equal(t, 0.0, budget3, "Gate should be closed when metric returns 0.05 (below threshold 0.2)")
	assert.Equal(t, int64(2), atomic.LoadInt64(&queryCount), "Should have queried Prometheus again after cache expiry")
}

// TestGateFactory_PrometheusBudget_EndToEnd validates the prometheus-budget gate
// wiring through GateFactory with cascade metric sources.
func TestGateFactory_PrometheusBudget_EndToEnd(t *testing.T) {
	body := &atomic.Value{}
	body.Store(promVectorResponse("0.7"))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body.Load().(string))
	}))
	defer server.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL(server.URL, 100*time.Millisecond)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-budget", GateParams: map[string]any{
		"pool":            "my-pool",
		"max_concurrency": 100.0,
		"baseline":        0.05,
		"fallback":        0.0,
	}})
	require.NoError(t, err)

	ctx := context.Background()

	// The prometheus-budget gate uses BudgetDispatchGate with baseline=0.05.
	// Primary source query returns budget value D = 0.7 (from our mock).
	// Gate returns D - baseline = 0.7 - 0.05 = 0.65.
	budget := gate.Budget(ctx)
	assert.InDelta(t, 0.65, budget, 0.01)

	// Update to near-zero budget (below baseline).
	time.Sleep(150 * time.Millisecond)
	body.Store(promVectorResponse("0.03"))

	// D = 0.03, baseline = 0.05 => D <= baseline => gate closed.
	budget2 := gate.Budget(ctx)
	assert.Equal(t, 0.0, budget2, "Gate should close when budget drops below baseline")
}

// TestGateFactory_PrometheusSaturation_MissingParams validates error handling.
func TestGateFactory_PrometheusSaturation_MissingParams(t *testing.T) {
	factory := flowcontrol.NewGateFactory("")

	// Missing prometheusURL.
	_, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{
		"pool": "my-pool",
	}})
	assert.Error(t, err, "Should fail when prometheus URL is empty")

	factory2 := flowcontrol.NewGateFactory("http://localhost:9090")

	// Missing pool param.
	_, err = factory2.CreateGate(pipeline.GateConfig{GateType: "prometheus-saturation", GateParams: map[string]any{}})
	assert.Error(t, err, "Should fail when pool is missing")
}

// TestGateFactory_PrometheusQuery_EndToEnd validates the prometheus-query gate
// wiring: GateFactory.CreateGate("prometheus-query", params) ->
//
//	NewPromQLMetricSource (user-supplied PromQL) ->
//	CachedMetricSource -> MetricDispatchGate(threshold=0) -> Budget()
func TestGateFactory_PrometheusQuery_EndToEnd(t *testing.T) {
	var queryCount int64
	body := &atomic.Value{}
	body.Store(promVectorResponse("0.7"))

	const promQL = "my_custom_metric"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&queryCount, 1)
		assert.Equal(t, promQL, r.FormValue("query"), "gate should forward the user-supplied PromQL expression")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body.Load().(string))
	}))
	defer server.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL(server.URL, 200*time.Millisecond)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "prometheus-query", GateParams: map[string]any{
		"query":    promQL,
		"fallback": 0.0,
	}})
	require.NoError(t, err)

	ctx := context.Background()

	// threshold=0 so Budget = value - 0 = 0.7.
	budget := gate.Budget(ctx)
	assert.InDelta(t, 0.7, budget, 0.01)
	assert.Equal(t, int64(1), atomic.LoadInt64(&queryCount))

	// Second call within cache TTL should NOT trigger another HTTP request.
	budget2 := gate.Budget(ctx)
	assert.InDelta(t, 0.7, budget2, 0.01)
	assert.Equal(t, int64(1), atomic.LoadInt64(&queryCount), "Should use cached result")

	// Wait for cache TTL to expire and update response.
	time.Sleep(250 * time.Millisecond)
	body.Store(promVectorResponse("0.0"))

	// Query returns 0.0 => Budget = 0.0.
	budget3 := gate.Budget(ctx)
	assert.Equal(t, 0.0, budget3)
	assert.Equal(t, int64(2), atomic.LoadInt64(&queryCount), "Should have queried Prometheus again after cache expiry")
}

func promVectorResponse(value string) string {
	return fmt.Sprintf(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"test","inference_pool":"my-pool"},"value":[1234567890,"%s"]}]}}`, value)
}
