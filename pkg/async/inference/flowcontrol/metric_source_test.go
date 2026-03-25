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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/api"
	"github.com/stretchr/testify/require"
)

// buildPromQL tests

func TestBuildPromQL_NoLabels(t *testing.T) {
	require.Equal(t, "my_metric", buildPromQL("my_metric", nil))
}

func TestBuildPromQL_EmptyLabels(t *testing.T) {
	require.Equal(t, "my_metric", buildPromQL("my_metric", map[string]string{}))
}

func TestBuildPromQL_SingleLabel(t *testing.T) {
	require.Equal(t, `my_metric{name="foo"}`, buildPromQL("my_metric", map[string]string{"name": "foo"}))
}

func TestBuildPromQL_MultipleLabels(t *testing.T) {
	require.Equal(t, `my_metric{app="bar",name="foo"}`, buildPromQL("my_metric", map[string]string{"name": "foo", "app": "bar"}))
}

// PromQLMetricSource.Query tests

func newTestSource(t *testing.T, statusCode int, responseBody string, expr string) (*PromQLMetricSource, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = fmt.Fprint(w, responseBody)
	}))
	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, expr)
	if err != nil {
		server.Close()
	}
	require.NoError(t, err)
	return source, server
}

func TestPromQLMetricSource_SingleSample(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"my_metric","name":"foo"},"value":[1234567890,"42.5"]}]}}`
	source, server := newTestSource(t, http.StatusOK, body, buildPromQL("my_metric", map[string]string{"name": "foo"}))
	defer server.Close()

	samples, err := source.Query(context.Background())
	require.NoError(t, err)
	require.Len(t, samples, 1)
	require.Equal(t, 42.5, samples[0].Value)
	require.Equal(t, "foo", samples[0].Labels["name"])
	require.Equal(t, "my_metric", samples[0].Labels["__name__"])
}

func TestPromQLMetricSource_MultipleSamples(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"name":"a"},"value":[1234567890,"1"]},` +
		`{"metric":{"name":"b"},"value":[1234567890,"2"]},` +
		`{"metric":{"name":"c"},"value":[1234567890,"3"]}]}}`
	source, server := newTestSource(t, http.StatusOK, body, "my_metric")
	defer server.Close()

	samples, err := source.Query(context.Background())
	require.NoError(t, err)
	require.Len(t, samples, 3)
	for i, expected := range []struct {
		name  string
		value float64
	}{{"a", 1}, {"b", 2}, {"c", 3}} {
		require.Equal(t, expected.value, samples[i].Value)
		require.Equal(t, expected.name, samples[i].Labels["name"])
	}
}

func TestPromQLMetricSource_EmptyVector(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	source, server := newTestSource(t, http.StatusOK, body, "my_metric")
	defer server.Close()

	samples, err := source.Query(context.Background())
	require.NoError(t, err)
	require.Empty(t, samples)
}

func TestPromQLMetricSource_ServerError(t *testing.T) {
	body := `{"status":"error","errorType":"internal","error":"something went wrong"}`
	source, server := newTestSource(t, http.StatusInternalServerError, body, "my_metric")
	defer server.Close()

	_, err := source.Query(context.Background())
	require.Error(t, err)
}

func TestPromQLMetricSource_ServerUnreachable(t *testing.T) {
	source, server := newTestSource(t, http.StatusOK, "", "my_metric")
	server.Close()

	_, err := source.Query(context.Background())
	require.Error(t, err)
}

func TestPromQLMetricSource_QueryPassthrough(t *testing.T) {
	var receivedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()

	expr := buildPromQL("inference_pool_average_queue_size", map[string]string{"name": "my-model"})
	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, expr)
	require.NoError(t, err)

	_, err = source.Query(context.Background())
	require.NoError(t, err)
	require.Equal(t, `inference_pool_average_queue_size{name="my-model"}`, receivedQuery)
}

func TestPromQLMetricSource_CustomExprPassthrough(t *testing.T) {
	var receivedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()

	expr := `max(avg_over_time(inference_extension_flow_control_pool_saturation{inference_pool="my-pool"}[5m]))`
	source, err := NewPromQLMetricSource(api.Config{Address: server.URL}, expr)
	require.NoError(t, err)

	_, err = source.Query(context.Background())
	require.NoError(t, err)
	require.Equal(t, expr, receivedQuery)
}

func TestNewPromQLMetricSource_InvalidAddress(t *testing.T) {
	_, err := NewPromQLMetricSource(api.Config{Address: "://invalid"}, "my_metric")
	require.Error(t, err)
}
