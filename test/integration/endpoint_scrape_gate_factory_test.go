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

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const metricsTemplate = `# HELP vllm:num_requests_waiting Number of requests waiting.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="sim-model"} %s
`

func TestGateFactory_EndpointScrape_StaticMaxCount(t *testing.T) {
	var queryCount int64
	metricValue := &atomic.Value{}
	metricValue.Store("2")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&queryCount, 1)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, metricsTemplate, metricValue.Load().(string))
	}))
	defer server.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL("", 200*time.Millisecond)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: map[string]any{
		"url":               server.URL,
		"metric":            "vllm:num_requests_waiting",
		"max_count_per_pod": 10.0,
		"baseline":          0.0,
		"fallback":          0.0,
	}})
	require.NoError(t, err)

	ctx := context.Background()

	// value=2, maxCount=10 → saturation=0.2 → budget=0.8 → gate open
	budget := gate.Budget(ctx)
	assert.InDelta(t, 0.8, budget, 0.01)
	assert.Equal(t, int64(1), atomic.LoadInt64(&queryCount))

	// Second call within cache TTL reuses cached result.
	budget2 := gate.Budget(ctx)
	assert.InDelta(t, 0.8, budget2, 0.01)
	assert.Equal(t, int64(1), atomic.LoadInt64(&queryCount), "Should use cached result")

	// Wait for cache to expire, update metric.
	time.Sleep(250 * time.Millisecond)
	metricValue.Store("10")

	// value=10, maxCount=10 → saturation=1.0 → budget=0.0 → gate closed
	budget3 := gate.Budget(ctx)
	assert.Equal(t, 0.0, budget3, "Gate should close when fully saturated")
	assert.Equal(t, int64(2), atomic.LoadInt64(&queryCount))
}

func TestGateFactory_EndpointScrape_DirectSaturation(t *testing.T) {
	const metricsBody = `# TYPE pool_saturation gauge
pool_saturation{name="pool-a"} 0.6
`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, metricsBody)
	}))
	defer server.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL("", 0)

	// No max_count_per_pod → metric value used directly as saturation
	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: map[string]any{
		"url":      server.URL,
		"metric":   "pool_saturation",
		"labels":   `{"name":"pool-a"}`,
		"baseline": 0.1,
	}})
	require.NoError(t, err)

	// saturation=0.6 → budget=1-0.6=0.4 → minus baseline 0.1 → 0.3
	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.3, budget, 0.01)
}

func TestGateFactory_EndpointScrape_DynamicPods(t *testing.T) {
	simServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, metricsTemplate, "6")
	}))
	defer simServer.Close()

	const podsBody = `# TYPE ready_pods gauge
ready_pods{name="pool-a"} 3
`
	podsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, podsBody)
	}))
	defer podsServer.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL("", 0)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: map[string]any{
		"url":               simServer.URL,
		"metric":            "vllm:num_requests_waiting",
		"max_count_per_pod": 10.0,
		"pods_url":          podsServer.URL,
		"pods_metric":       "ready_pods",
		"pods_labels":       `{"name":"pool-a"}`,
	}})
	require.NoError(t, err)

	// value=6, pods=3, maxCountPerPod=10 → maxCount=30 → saturation=0.2 → budget=0.8
	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.8, budget, 0.01)
}

func TestGateFactory_EndpointScrape_MissingParams(t *testing.T) {
	factory := flowcontrol.NewGateFactory("")

	_, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: map[string]any{
		"metric": "some_metric",
	}})
	assert.Error(t, err, "Should fail when url is missing")

	_, err = factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: map[string]any{
		"url": "http://localhost:8000/metrics",
	}})
	assert.Error(t, err, "Should fail when metric is missing")
}

func TestGateFactory_EndpointScrape_FallbackOnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	factory := flowcontrol.NewGateFactoryWithCacheTTL("", 0)

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "endpoint-scrape", GateParams: map[string]any{
		"url":      server.URL,
		"metric":   "any",
		"fallback": 0.5,
	}})
	require.NoError(t, err)

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.5, budget, "Should return fallback on scrape error")
}
