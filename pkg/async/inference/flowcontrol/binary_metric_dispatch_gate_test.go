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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/api"
	"github.com/stretchr/testify/require"
)

// mockMetricSource is a test implementation of MetricSource.
type mockMetricSource struct {
	samples []Sample
	err     error
}

func (m *mockMetricSource) Query(_ context.Context) ([]Sample, error) {
	return m.samples, m.err
}

// newTestPrometheusServer creates an httptest.Server that serves Prometheus HTTP API
// responses at /api/v1/query with the given response body and status code.
func newTestPrometheusServer(statusCode int, responseBody string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = fmt.Fprint(w, responseBody)
	}))
}

// Integration tests using a mock Prometheus HTTP server

func TestBinaryMetricDispatchGate_MetricValueZero(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"name":"test"},"value":[1234567890,"0"]}]}}`
	server := newTestPrometheusServer(http.StatusOK, body)
	defer server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, buildPromQL("test_metric", map[string]string{"name": "test"}))
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGate_MetricValueNonZero(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"name":"test"},"value":[1234567890,"5"]}]}}`
	server := newTestPrometheusServer(http.StatusOK, body)
	defer server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, buildPromQL("test_metric", map[string]string{"name": "test"}))
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGate_EmptyVector(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	server := newTestPrometheusServer(http.StatusOK, body)
	defer server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, "test_metric")
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGate_ServerError(t *testing.T) {
	body := `{"status":"error","errorType":"internal","error":"something went wrong"}`
	server := newTestPrometheusServer(http.StatusInternalServerError, body)
	defer server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, "test_metric")
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGate_ServerUnreachable(t *testing.T) {
	server := newTestPrometheusServer(http.StatusOK, "")
	serverURL := server.URL
	server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: serverURL}, "test_metric")
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGate_MultipleSamples(t *testing.T) {
	// First sample has value 0, second has value 5. Gate should use first sample only.
	body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"name":"test1"},"value":[1234567890,"0"]},{"metric":{"name":"test2"},"value":[1234567890,"5"]}]}}`
	server := newTestPrometheusServer(http.StatusOK, body)
	defer server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, "test_metric")
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGate_MultipleSamplesFirstNonZero(t *testing.T) {
	// First sample has value 5, second has value 0. Gate should use first sample only.
	body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"name":"test1"},"value":[1234567890,"5"]},{"metric":{"name":"test2"},"value":[1234567890,"0"]}]}}`
	server := newTestPrometheusServer(http.StatusOK, body)
	defer server.Close()

	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, "test_metric")
	require.NoError(t, err)
	gate := NewBinaryMetricDispatchGateWithSource(source)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

// Unit tests using the MetricSource interface directly

func TestBinaryMetricDispatchGateWithSource_ZeroValue(t *testing.T) {
	gate := NewBinaryMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.0}}},
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGateWithSource_NonZeroValue(t *testing.T) {
	gate := NewBinaryMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 42.0}}},
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGateWithSource_Error(t *testing.T) {
	gate := NewBinaryMetricDispatchGateWithSource(
		&mockMetricSource{err: errors.New("connection refused")},
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBinaryMetricDispatchGateWithSource_EmptySamples(t *testing.T) {
	gate := NewBinaryMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{}},
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}
