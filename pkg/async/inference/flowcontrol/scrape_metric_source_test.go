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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
)

const testMetricsBody = `# HELP vllm:num_requests_waiting Number of requests waiting.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="sim-model"} 3
vllm:num_requests_waiting{model_name="other-model"} 7
# HELP inference_pool_saturation Pool saturation ratio.
# TYPE inference_pool_saturation gauge
inference_pool_saturation{name="pool-a"} 0.75
inference_pool_saturation{name="pool-b"} 0.2
# HELP model_ready Whether the model is ready.
# TYPE model_ready gauge
model_ready{model_name="sim-model"} 1
`

const testPodsMetricsBody = `# HELP ready_pods Number of ready pods.
# TYPE ready_pods gauge
ready_pods{name="pool-a"} 4
`

func newTestMetricsServer(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
}

func TestScrapeMetricSource(t *testing.T) {
	server := newTestMetricsServer(testMetricsBody)
	defer server.Close()

	t.Run("StaticMaxCount", func(t *testing.T) {
		source := NewScrapeMetricSource(ScrapeConfig{
			URL:            server.URL,
			MetricName:     "vllm:num_requests_waiting",
			Labels:         map[string]string{"model_name": "sim-model"},
			MaxCountPerPod: 5,
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		require.Len(t, samples, 1)
		// value=3, maxCount=5 → saturation=0.6 → budget=0.4
		assert.InDelta(t, 0.4, samples[0].Value, 0.001)
	})

	t.Run("DirectSaturation", func(t *testing.T) {
		source := NewScrapeMetricSource(ScrapeConfig{
			URL:        server.URL,
			MetricName: "inference_pool_saturation",
			Labels:     map[string]string{"name": "pool-a"},
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		require.Len(t, samples, 1)
		// value=0.75, maxCount=0 → saturation=0.75 → budget=0.25
		assert.InDelta(t, 0.25, samples[0].Value, 0.001)
	})

	t.Run("DirectBudget", func(t *testing.T) {
		source := NewScrapeMetricSource(ScrapeConfig{
			URL:          server.URL,
			MetricName:   "model_ready",
			Labels:       map[string]string{"model_name": "sim-model"},
			DirectBudget: true,
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		require.Len(t, samples, 1)
		assert.InDelta(t, 1.0, samples[0].Value, 0.001)
	})

	t.Run("OverSaturationClamped", func(t *testing.T) {
		source := NewScrapeMetricSource(ScrapeConfig{
			URL:            server.URL,
			MetricName:     "vllm:num_requests_waiting",
			Labels:         map[string]string{"model_name": "other-model"},
			MaxCountPerPod: 5,
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		require.Len(t, samples, 1)
		// value=7, maxCount=5 → saturation=1.4 → clamped to 1.0 → budget=0.0
		assert.InDelta(t, 0.0, samples[0].Value, 0.001)
	})

	t.Run("NoMatch", func(t *testing.T) {
		source := NewScrapeMetricSource(ScrapeConfig{
			URL:        server.URL,
			MetricName: "nonexistent_metric",
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		assert.Empty(t, samples)
	})

	t.Run("ServerError", func(t *testing.T) {
		errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer errServer.Close()

		source := NewScrapeMetricSource(ScrapeConfig{
			URL:        errServer.URL,
			MetricName: "any",
		})
		_, err := source.Query(context.Background())
		assert.Error(t, err)
	})

	t.Run("DynamicPodsCount", func(t *testing.T) {
		podsServer := newTestMetricsServer(testPodsMetricsBody)
		defer podsServer.Close()

		source := NewScrapeMetricSource(ScrapeConfig{
			URL:            server.URL,
			MetricName:     "vllm:num_requests_waiting",
			Labels:         map[string]string{"model_name": "sim-model"},
			MaxCountPerPod: 5,
			PodsURL:        podsServer.URL,
			PodsMetric:     "ready_pods",
			PodsLabels:     map[string]string{"name": "pool-a"},
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		require.Len(t, samples, 1)
		// value=3, pods=4, maxCountPerPod=5 → maxCount=20 → saturation=0.15 → budget=0.85
		assert.InDelta(t, 0.85, samples[0].Value, 0.001)
	})

	t.Run("ZeroValue", func(t *testing.T) {
		zeroBody := `# TYPE idle_metric gauge
idle_metric 0
`
		zeroServer := newTestMetricsServer(zeroBody)
		defer zeroServer.Close()

		source := NewScrapeMetricSource(ScrapeConfig{
			URL:            zeroServer.URL,
			MetricName:     "idle_metric",
			MaxCountPerPod: 10,
		})
		samples, err := source.Query(context.Background())
		require.NoError(t, err)
		require.Len(t, samples, 1)
		// value=0, maxCount=10 → saturation=0 → budget=1.0
		assert.InDelta(t, 1.0, samples[0].Value, 0.001)
	})
}

func TestScrapeMetricSourceAbsentValue(t *testing.T) {
	server := newTestMetricsServer(testMetricsBody)
	defer server.Close()
	zero := 0.0

	absent := NewScrapeMetricSource(ScrapeConfig{
		URL:            server.URL,
		MetricName:     "llm_d_epp_flow_control_queue_size",
		Labels:         map[string]string{"priority": "-10"},
		MaxCountPerPod: 1024,
		AbsentValue:    &zero,
	})
	samples, err := absent.Query(context.Background())
	require.NoError(t, err)
	require.Len(t, samples, 1)
	assert.InDelta(t, 1.0, samples[0].Value, 0.001, "an absent queue reads as empty")

	unset := NewScrapeMetricSource(ScrapeConfig{
		URL:        server.URL,
		MetricName: "llm_d_epp_flow_control_queue_size",
	})
	samples, err = unset.Query(context.Background())
	require.NoError(t, err)
	assert.Empty(t, samples, "without absent_value a missing series stays missing")

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failing.Close()
	down := NewScrapeMetricSource(ScrapeConfig{
		URL:         failing.URL,
		MetricName:  "llm_d_epp_flow_control_queue_size",
		AbsentValue: &zero,
	})
	_, err = down.Query(context.Background())
	require.Error(t, err, "absent_value never covers a failed scrape")
}

func TestScrapeMetricSourceLogsOnceWhenAbsentValueCoversASeriesNeverSeen(t *testing.T) {
	var logged []string
	logger := funcr.New(func(_, args string) { logged = append(logged, args) }, funcr.Options{Verbosity: logutil.DEFAULT})
	ctx := log.IntoContext(context.Background(), logger)

	m, url := newEPPMetrics(t, 0, 2)
	m.absent.Store(true)
	neverSeen := headroomSourceFor(url, 8)
	for range 3 {
		_, err := neverSeen.Query(ctx)
		require.NoError(t, err)
	}
	require.Len(t, logged, 1, "one line per source, not one per scrape")
	assert.Contains(t, logged[0], "has not matched a series yet")
	assert.Contains(t, logged[0], "llm_d_epp_flow_control_queue_size")
	assert.Contains(t, logged[0], `"priority"="-10"`)

	m.absent.Store(false)
	_, err := neverSeen.Query(ctx)
	require.NoError(t, err)
	m.absent.Store(true)
	_, err = neverSeen.Query(ctx)
	require.NoError(t, err)
	assert.Len(t, logged, 1)

	logged = nil
	m.absent.Store(false)
	seenFirst := headroomSourceFor(url, 8)
	_, err = seenFirst.Query(ctx)
	require.NoError(t, err)
	m.absent.Store(true)
	_, err = seenFirst.Query(ctx)
	require.NoError(t, err)
	assert.Empty(t, logged, "a series that existed and then dropped is an idle one")

	m.status.Store(http.StatusServiceUnavailable)
	failed := headroomSourceFor(url, 8)
	_, err = failed.Query(ctx)
	require.Error(t, err)
	assert.Empty(t, logged, "a failed scrape says nothing about the series")
}
