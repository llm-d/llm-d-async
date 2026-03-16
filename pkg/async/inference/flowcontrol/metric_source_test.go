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
)

// buildPromQL tests

func TestBuildPromQL_NoLabels(t *testing.T) {
	got := buildPromQL("my_metric", nil)
	if got != "my_metric" {
		t.Errorf("expected %q, got %q", "my_metric", got)
	}
}

func TestBuildPromQL_EmptyLabels(t *testing.T) {
	got := buildPromQL("my_metric", map[string]string{})
	if got != "my_metric" {
		t.Errorf("expected %q, got %q", "my_metric", got)
	}
}

func TestBuildPromQL_SingleLabel(t *testing.T) {
	got := buildPromQL("my_metric", map[string]string{"name": "foo"})
	expected := `my_metric{name="foo"}`
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

func TestBuildPromQL_MultipleLabels(t *testing.T) {
	got := buildPromQL("my_metric", map[string]string{"name": "foo", "app": "bar"})
	expected := `my_metric{app="bar",name="foo"}`
	if got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}

// PrometheusMetricSource.Query tests

func newTestSource(t *testing.T, statusCode int, responseBody string) (*PrometheusMetricSource, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		fmt.Fprint(w, responseBody)
	}))
	source, err := NewPrometheusMetricSource(api.Config{Address: server.URL})
	if err != nil {
		server.Close()
		t.Fatalf("failed to create PrometheusMetricSource: %v", err)
	}
	return source, server
}

func TestPrometheusMetricSource_SingleSample(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"my_metric","name":"foo"},"value":[1234567890,"42.5"]}]}}`
	source, server := newTestSource(t, http.StatusOK, body)
	defer server.Close()

	samples, err := source.Query(context.Background(), "my_metric", map[string]string{"name": "foo"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	if samples[0].Value != 42.5 {
		t.Errorf("expected value 42.5, got %f", samples[0].Value)
	}
	if samples[0].Labels["name"] != "foo" {
		t.Errorf("expected label name=foo, got %q", samples[0].Labels["name"])
	}
	if samples[0].Labels["__name__"] != "my_metric" {
		t.Errorf("expected label __name__=my_metric, got %q", samples[0].Labels["__name__"])
	}
}

func TestPrometheusMetricSource_MultipleSamples(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[` +
		`{"metric":{"name":"a"},"value":[1234567890,"1"]},` +
		`{"metric":{"name":"b"},"value":[1234567890,"2"]},` +
		`{"metric":{"name":"c"},"value":[1234567890,"3"]}]}}`
	source, server := newTestSource(t, http.StatusOK, body)
	defer server.Close()

	samples, err := source.Query(context.Background(), "my_metric", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(samples))
	}
	expectedValues := []float64{1, 2, 3}
	expectedNames := []string{"a", "b", "c"}
	for i, s := range samples {
		if s.Value != expectedValues[i] {
			t.Errorf("sample[%d]: expected value %f, got %f", i, expectedValues[i], s.Value)
		}
		if s.Labels["name"] != expectedNames[i] {
			t.Errorf("sample[%d]: expected name=%q, got %q", i, expectedNames[i], s.Labels["name"])
		}
	}
}

func TestPrometheusMetricSource_EmptyVector(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	source, server := newTestSource(t, http.StatusOK, body)
	defer server.Close()

	samples, err := source.Query(context.Background(), "my_metric", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("expected 0 samples, got %d", len(samples))
	}
}

func TestPrometheusMetricSource_ServerError(t *testing.T) {
	body := `{"status":"error","errorType":"internal","error":"something went wrong"}`
	source, server := newTestSource(t, http.StatusInternalServerError, body)
	defer server.Close()

	_, err := source.Query(context.Background(), "my_metric", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestPrometheusMetricSource_ServerUnreachable(t *testing.T) {
	source, server := newTestSource(t, http.StatusOK, "")
	server.Close()

	_, err := source.Query(context.Background(), "my_metric", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestPrometheusMetricSource_QueryPassthrough(t *testing.T) {
	var receivedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.FormValue("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()

	source, err := NewPrometheusMetricSource(api.Config{Address: server.URL})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}

	_, err = source.Query(context.Background(), "inference_pool_average_queue_size", map[string]string{"name": "my-model"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `inference_pool_average_queue_size{name="my-model"}`
	if receivedQuery != expected {
		t.Errorf("expected query %q, got %q", expected, receivedQuery)
	}
}

func TestNewPrometheusMetricSource_InvalidAddress(t *testing.T) {
	_, err := NewPrometheusMetricSource(api.Config{Address: "://invalid"})
	if err == nil {
		t.Fatal("expected error for invalid address, got nil")
	}
}
