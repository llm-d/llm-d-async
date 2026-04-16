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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// Sample represents a single metric sample with its labels and value.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// MetricSource queries a metrics backend for time-series data.
// The query configuration is baked into the implementation at construction time;
// callers simply invoke Query to retrieve the current samples.
type MetricSource interface {
	// Query returns the current samples for the preconfigured query.
	Query(ctx context.Context) ([]Sample, error)
}

var _ MetricSource = (*PromQLMetricSource)(nil)

// PromQLMetricSource implements MetricSource by executing a PromQL expression
// against a Prometheus-compatible API.
type PromQLMetricSource struct {
	api  v1.API
	expr string
}

// NewPromQLMetricSource creates a MetricSource that executes the given PromQL expression.
func NewPromQLMetricSource(clientConfig api.Config, expr string) (*PromQLMetricSource, error) {
	client, err := api.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating Prometheus API client: %w", err)
	}
	return &PromQLMetricSource{
		api:  v1.NewAPI(client),
		expr: expr,
	}, nil
}

// NewPromQLMetricSourceWithURL creates a MetricSource with a simple URL (no auth).
// Use this for standard Prometheus or when the URL includes authentication details.
func NewPromQLMetricSourceWithURL(url string) (*PromQLMetricSource, error) {
	if url == "" {
		return nil, fmt.Errorf("prometheus URL cannot be empty")
	}
	return NewPromQLMetricSource(api.Config{Address: url}, "")
}

// NewGMPPromQLMetricSource creates a PromQL MetricSource for Google Managed Prometheus.
func NewGMPPromQLMetricSource(projectID string, expr string) (*PromQLMetricSource, error) {
	ctx := context.Background()
	gcpClient, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/monitoring.read")
	if err != nil {
		return nil, fmt.Errorf("failed to create authenticated GCP client: %w", err)
	}

	promURL := fmt.Sprintf("https://monitoring.googleapis.com/v1/projects/%s/location/global/prometheus", projectID)
	return NewPromQLMetricSource(api.Config{
		Address:      promURL,
		RoundTripper: gcpClient.Transport,
	}, expr)
}

// buildPromQL constructs a PromQL instant vector selector from a metric name and label matchers.
func buildPromQL(metricName string, labels map[string]string) string {
	if len(labels) == 0 {
		return metricName
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(labels))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf(`%s=%s`, k, strconv.Quote(labels[k])))
	}
	return fmt.Sprintf(`%s{%s}`, metricName, strings.Join(parts, ","))
}

// Query executes the preconfigured PromQL expression and returns the result as samples.
func (s *PromQLMetricSource) Query(ctx context.Context) ([]Sample, error) {
	logger := log.FromContext(ctx)

	result, warnings, err := s.api.Query(ctx, s.expr, time.Now())
	if err != nil {
		return nil, fmt.Errorf("error querying Prometheus: %w", err)
	}
	if len(warnings) > 0 {
		logger.V(logutil.DEFAULT).Info("Prometheus query returned warnings", "warnings", warnings)
	}

	vec, ok := result.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("expected Vector result, got %T", result)
	}

	samples := make([]Sample, len(vec))
	for i, s := range vec {
		lbls := make(map[string]string, len(s.Metric))
		for k, v := range s.Metric {
			lbls[string(k)] = string(v)
		}
		samples[i] = Sample{
			Labels: lbls,
			Value:  float64(s.Value),
		}
	}
	return samples, nil
}
