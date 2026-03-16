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
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
)

// MetricSource provides a single metric value from an external source.
type MetricSource interface {
	// GetValue returns the current metric value, or an error if the value
	// could not be retrieved.
	GetValue(ctx context.Context) (float64, error)
}

// PrometheusMetricSource implements MetricSource by querying a Prometheus-compatible API.
type PrometheusMetricSource struct {
	api   v1.API
	query string
}

// NewPrometheusMetricSource creates a MetricSource that queries a Prometheus-compatible API.
func NewPrometheusMetricSource(clientConfig api.Config, query string) (*PrometheusMetricSource, error) {
	client, err := api.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating Prometheus API client: %w", err)
	}
	return &PrometheusMetricSource{
		api:   v1.NewAPI(client),
		query: query,
	}, nil
}

// NewGMPMetricSource creates a MetricSource for Google Managed Prometheus.
func NewGMPMetricSource(projectID string, query string) (*PrometheusMetricSource, error) {
	ctx := context.Background()
	gcpClient, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/monitoring.read")
	if err != nil {
		return nil, fmt.Errorf("failed to create authenticated GCP client: %w", err)
	}

	promURL := fmt.Sprintf("https://monitoring.googleapis.com/v1/projects/%s/location/global/prometheus", projectID)
	return NewPrometheusMetricSource(api.Config{
		Address:      promURL,
		RoundTripper: gcpClient.Transport,
	}, query)
}

// GetValue queries Prometheus and returns the value of the first sample in the result vector.
func (s *PrometheusMetricSource) GetValue(ctx context.Context) (float64, error) {
	result, _, err := s.api.Query(ctx, s.query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("error querying Prometheus: %w", err)
	}

	vec, ok := result.(model.Vector)
	if !ok {
		return 0, fmt.Errorf("expected Vector result, got %T", result)
	}

	if len(vec) == 0 {
		return 0, fmt.Errorf("no metrics found for query: %s", s.query)
	}

	return float64(vec[0].Value), nil
}
