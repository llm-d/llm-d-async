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
	"math"
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
// When maxCountPerPod == 0, the metric value is assumed to already be normalized in [0, 1].
// By default the normalized value is saturation and output = 1 - saturation. When
// directBudget is set, the normalized value is returned as the budget instead.
type ScrapeMetricSource struct {
	client         *http.Client
	url            string
	metricName     string
	labels         map[string]string
	maxCountPerPod float64
	directBudget   bool
	podsURL        string
	podsMetric     string
	podsLabels     map[string]string
	absentValue    *float64
}

// ScrapeConfig holds configuration for NewScrapeMetricSource.
type ScrapeConfig struct {
	URL            string
	MetricName     string
	Labels         map[string]string
	MaxCountPerPod float64
	DirectBudget   bool
	PodsURL        string
	PodsMetric     string
	PodsLabels     map[string]string
	// AbsentValue, when set, is the raw metric value assumed when a scrape succeeds but no
	// series matches, for gauges that exist only while there is something to count. Nil
	// leaves the source with no samples, which the gate treats as an error.
	AbsentValue *float64
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
		directBudget:   cfg.DirectBudget,
		podsURL:        cfg.PodsURL,
		podsMetric:     cfg.PodsMetric,
		podsLabels:     cfg.PodsLabels,
		absentValue:    cfg.AbsentValue,
	}
}

func (s *ScrapeMetricSource) Query(ctx context.Context) ([]Sample, error) {
	samples, maxCount, err := s.read(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]Sample, len(samples))
	for i, sample := range samples {
		var normalized float64
		if maxCount > 0 {
			normalized = sample.Value / maxCount
		} else {
			normalized = sample.Value
		}
		normalized = clampFloat(normalized, 0, 1)
		budget := 1 - normalized
		if s.directBudget {
			budget = normalized
		}
		result[i] = Sample{Labels: sample.Labels, Value: budget}
	}
	return result, nil
}

// Headroom returns the metric's free capacity in its own units, maxCount minus the first
// matching value, floored at 0. It needs a count metric: max_count_per_pod set, saturation
// value type.
func (s *ScrapeMetricSource) Headroom(ctx context.Context) (float64, error) {
	if s.maxCountPerPod <= 0 || s.directBudget {
		return 0, fmt.Errorf("scrape: headroom needs max_count_per_pod and value_type saturation")
	}
	samples, maxCount, err := s.read(ctx)
	if err != nil {
		return 0, err
	}
	if len(samples) == 0 {
		return 0, fmt.Errorf("scrape: metric %s not found at %s", s.metricName, s.url)
	}
	return math.Max(0, maxCount-samples[0].Value), nil
}

// read scrapes the metric and the capacity it is measured against.
func (s *ScrapeMetricSource) read(ctx context.Context) ([]Sample, float64, error) {
	samples, err := scrapeMetric(ctx, s.client, s.url, s.metricName, s.labels)
	if err != nil {
		return nil, 0, err
	}
	if len(samples) == 0 && s.absentValue != nil {
		samples = []Sample{{Value: *s.absentValue}}
	}

	maxCount := s.maxCountPerPod
	if s.podsURL != "" && s.podsMetric != "" {
		podsSamples, err := scrapeMetric(ctx, s.client, s.podsURL, s.podsMetric, s.podsLabels)
		if err != nil {
			return nil, 0, fmt.Errorf("scrape pods metric: %w", err)
		}
		if len(podsSamples) == 0 {
			return nil, 0, fmt.Errorf("scrape: pods metric %s not found at %s", s.podsMetric, s.podsURL)
		}
		pods := podsSamples[0].Value
		if pods <= 0 {
			return nil, 0, fmt.Errorf("scrape: ready pods is %g, cannot compute capacity", pods)
		}
		maxCount = pods * s.maxCountPerPod
	}
	return samples, maxCount, nil
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
