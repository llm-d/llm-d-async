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
	"io"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

var _ MetricSource = (*ScrapeMetricSource)(nil)

// ScrapeMetricSource implements MetricSource by scraping raw Prometheus /metrics
// endpoints. It reads a metric value, optionally computes saturation using a
// max capacity (static or dynamic from a pods metric), and returns budget in [0, 1].
//
// Two modes for max capacity:
//   - Static: maxCountPerPod is used directly as the total max count (single pod or precomputed).
//   - Dynamic: when podsURL/podsMetric are set, ready pods are scraped from a second
//     endpoint (e.g., EPP) and max_count = ready_pods * maxCountPerPod.
//
// When maxCountPerPod == 0, the metric value is assumed to already be saturation in [0, 1].
// Output value = 1 - saturation (available capacity / budget).
type ScrapeMetricSource struct {
	client         *http.Client
	url            string
	metricName     string
	labels         map[string]string
	maxCountPerPod float64
	podsURL        string
	podsMetric     string
	podsLabels     map[string]string
}

// ScrapeConfig holds configuration for NewScrapeMetricSource.
type ScrapeConfig struct {
	URL            string
	MetricName     string
	Labels         map[string]string
	MaxCountPerPod float64
	PodsURL        string
	PodsMetric     string
	PodsLabels     map[string]string
}

// NewScrapeMetricSource creates a MetricSource that scrapes Prometheus
// text-format /metrics endpoints and returns budget values in [0, 1].
func NewScrapeMetricSource(cfg ScrapeConfig) *ScrapeMetricSource {
	return &ScrapeMetricSource{
		client:         &http.Client{Timeout: 10 * time.Second},
		url:            cfg.URL,
		metricName:     cfg.MetricName,
		labels:         cfg.Labels,
		maxCountPerPod: cfg.MaxCountPerPod,
		podsURL:        cfg.PodsURL,
		podsMetric:     cfg.PodsMetric,
		podsLabels:     cfg.PodsLabels,
	}
}

func (s *ScrapeMetricSource) Query(ctx context.Context) ([]Sample, error) {
	samples, err := scrapeMetric(ctx, s.client, s.url, s.metricName, s.labels)
	if err != nil {
		return nil, err
	}

	maxCount := s.maxCountPerPod
	if s.podsURL != "" && s.podsMetric != "" {
		podsSamples, err := scrapeMetric(ctx, s.client, s.podsURL, s.podsMetric, s.podsLabels)
		if err != nil {
			return nil, fmt.Errorf("scrape pods metric: %w", err)
		}
		if len(podsSamples) == 0 {
			return nil, fmt.Errorf("scrape: pods metric %s not found at %s", s.podsMetric, s.podsURL)
		}
		pods := podsSamples[0].Value
		if pods <= 0 {
			return nil, fmt.Errorf("scrape: ready pods is %g, cannot compute capacity", pods)
		}
		maxCount = pods * s.maxCountPerPod
	}

	result := make([]Sample, len(samples))
	for i, sample := range samples {
		var saturation float64
		if maxCount > 0 {
			saturation = sample.Value / maxCount
		} else {
			saturation = sample.Value
		}
		saturation = clampFloat(saturation, 0, 1)
		result[i] = Sample{Labels: sample.Labels, Value: 1 - saturation}
	}

	return result, nil
}

func scrapeMetric(ctx context.Context, client *http.Client, url, metricName string, labelFilters map[string]string) ([]Sample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("scrape: build request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape: fetch %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape: %s returned %d", url, resp.StatusCode)
	}

	return parseSamples(resp.Body, metricName, labelFilters)
}

func parseSamples(body io.Reader, metricName string, labelFilters map[string]string) ([]Sample, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(body)
	if err != nil {
		return nil, fmt.Errorf("scrape: parse metrics: %w", err)
	}

	family, ok := families[metricName]
	if !ok {
		return nil, nil
	}

	var samples []Sample
	for _, m := range family.GetMetric() {
		labels := make(map[string]string)
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}

		if !matchLabels(labels, labelFilters) {
			continue
		}

		var value float64
		switch {
		case m.GetGauge() != nil:
			value = m.GetGauge().GetValue()
		case m.GetCounter() != nil:
			value = m.GetCounter().GetValue()
		case m.GetUntyped() != nil:
			value = m.GetUntyped().GetValue()
		default:
			continue
		}

		samples = append(samples, Sample{Labels: labels, Value: value})
	}

	return samples, nil
}

func matchLabels(actual, filters map[string]string) bool {
	for k, v := range filters {
		if actual[k] != v {
			return false
		}
	}
	return true
}

func clampFloat(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
