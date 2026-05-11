//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	promapi "github.com/prometheus/client_golang/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
)

// promResponse builds a Prometheus API JSON response with a single vector sample.
func promResponse(value float64) string {
	return fmt.Sprintf(
		`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"test_metric"},"value":[1234567890,"%g"]}]}}`,
		value,
	)
}

// promEmptyResponse returns a Prometheus API JSON response with no samples.
func promEmptyResponse() string {
	return `{"status":"success","data":{"resultType":"vector","result":[]}}`
}

// newMockPrometheus creates an httptest.Server that returns configurable responses.
// Callers can change the response body between requests via setResponse.
func newMockPrometheus(t *testing.T) (*httptest.Server, func(string)) {
	t.Helper()
	var mu sync.Mutex
	body := promResponse(0.5)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		resp := body
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, resp)
	}))

	setResponse := func(newBody string) {
		mu.Lock()
		body = newBody
		mu.Unlock()
	}
	return server, setResponse
}

// TestDispatchGate_OpenWhenMetricAboveThreshold verifies that the gate returns
// a positive budget when the Prometheus metric exceeds the threshold.
func TestDispatchGate_OpenWhenMetricAboveThreshold(t *testing.T) {
	server, _ := newMockPrometheus(t)
	defer server.Close()

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	gate := flowcontrol.NewMetricDispatchGate(source, 0.1, 1.0)

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.4, budget, 0.001, "Budget should be metric(0.5) - threshold(0.1) = 0.4")
}

// TestDispatchGate_ClosedWhenMetricBelowThreshold verifies gate closure
// when the metric is at or below the threshold.
func TestDispatchGate_ClosedWhenMetricBelowThreshold(t *testing.T) {
	server, setResponse := newMockPrometheus(t)
	defer server.Close()
	setResponse(promResponse(0.1))

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	gate := flowcontrol.NewMetricDispatchGate(source, 0.1, 1.0)

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget, "Budget should be 0 when metric <= threshold")
}

// TestDispatchGate_FallbackOnEmptyResponse verifies that the gate returns
// the fallback budget when Prometheus returns no samples.
func TestDispatchGate_FallbackOnEmptyResponse(t *testing.T) {
	server, setResponse := newMockPrometheus(t)
	defer server.Close()
	setResponse(promEmptyResponse())

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	gate := flowcontrol.NewMetricDispatchGate(source, 0.1, 0.75)

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.75, budget, 0.001, "Should use fallback when no samples")
}

// TestDispatchGate_FallbackOnServerError verifies that the gate returns
// the fallback budget when Prometheus returns an error.
func TestDispatchGate_FallbackOnServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `{"status":"error","errorType":"internal","error":"test error"}`)
	}))
	defer server.Close()

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	gate := flowcontrol.NewMetricDispatchGate(source, 0.1, 0.5)

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.5, budget, 0.001, "Should use fallback on error")
}

// TestDispatchGate_TransitionOpenToClosed simulates a metric dropping below
// the threshold (e.g. system becoming saturated) and verifies the gate
// transitions from open to closed.
func TestDispatchGate_TransitionOpenToClosed(t *testing.T) {
	server, setResponse := newMockPrometheus(t)
	defer server.Close()

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	gate := flowcontrol.NewMetricDispatchGate(source, 0.2, 1.0)

	// Initially open: metric=0.5, threshold=0.2 → budget=0.3
	budget := gate.Budget(context.Background())
	assert.Greater(t, budget, 0.0, "Gate should be open initially")

	// Simulate saturation: metric drops to 0.05
	setResponse(promResponse(0.05))
	budget = gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget, "Gate should be closed after saturation")

	// Recover: metric rises to 0.8
	setResponse(promResponse(0.8))
	budget = gate.Budget(context.Background())
	assert.InDelta(t, 0.6, budget, 0.001, "Gate should reopen after recovery")
}

// TestSaturationDispatchGate_HighSaturationClosesGate tests the saturation
// convenience constructor where threshold and fallback are in saturation space.
func TestSaturationDispatchGate_HighSaturationClosesGate(t *testing.T) {
	// Saturation gate: source returns 1 - saturation. If saturation=0.95,
	// source returns 0.05. Threshold in budget space = 1 - 0.8 = 0.2.
	server, setResponse := newMockPrometheus(t)
	defer server.Close()
	setResponse(promResponse(0.05)) // budget = 0.05

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	// threshold=0.8 in saturation space → 0.2 in budget space; fallback=0.9 → 0.1
	gate := flowcontrol.NewSaturationDispatchGate(source, 0.8, 0.9)

	budget := gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget, "Gate should close when budget(0.05) <= threshold(0.2)")

	// Reduce saturation: budget goes to 0.6 (saturation=0.4)
	setResponse(promResponse(0.6))
	budget = gate.Budget(context.Background())
	assert.InDelta(t, 0.4, budget, 0.001, "Gate should open: budget(0.6) - threshold(0.2) = 0.4")
}

// TestBudgetDispatchGate_BaselineReserved tests the budget convenience
// constructor where a baseline is reserved.
func TestBudgetDispatchGate_BaselineReserved(t *testing.T) {
	server, setResponse := newMockPrometheus(t)
	defer server.Close()
	setResponse(promResponse(0.3))

	source, err := flowcontrol.NewPromQLMetricSource(
		promapi.Config{Address: server.URL},
		"test_metric",
	)
	require.NoError(t, err)

	// baseline=0.05, fallback=1.0
	gate := flowcontrol.NewBudgetDispatchGate(source, 0.05, 1.0)

	budget := gate.Budget(context.Background())
	assert.InDelta(t, 0.25, budget, 0.001, "Budget should be metric(0.3) - baseline(0.05) = 0.25")

	// Metric drops to baseline → gate closes
	setResponse(promResponse(0.05))
	budget = gate.Budget(context.Background())
	assert.Equal(t, 0.0, budget, "Gate should close at baseline")
}
