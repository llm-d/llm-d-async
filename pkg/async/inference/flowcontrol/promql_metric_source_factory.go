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
	"fmt"
	"sort"
	"strconv"
	"strings"

	promapi "github.com/prometheus/client_golang/api"
)

// NewSaturationPromQLSourceFromConfig builds a PromQLMetricSource for the saturation use case.
// It returns a budget value (1 - saturation) by constructing a PromQL query of the form
// "1 - inference_extension_flow_control_pool_saturation{...}", filtered by the "pool" param (required).
func NewSaturationPromQLSourceFromConfig(promConfig promapi.Config, params map[string]string) (*PromQLMetricSource, error) {
	inferencePool := params["pool"]
	if inferencePool == "" {
		return nil, fmt.Errorf("inference pool name is required for saturation PromQL")
	}

	labels := map[string]string{"inference_pool": inferencePool}
	if ns := params["namespace"]; ns != "" {
		labels["namespace"] = ns
	}

	queryExpr := "1 - " + buildPromQL("inference_extension_flow_control_pool_saturation", labels)
	return NewPromQLMetricSource(promConfig, queryExpr)
}

// NewPromQLMetricSourceFromLabels constructs a PromQL instant vector selector from a metric
// name and label matchers, and returns a PromQLMetricSource for it.
func NewPromQLMetricSourceFromLabels(promConfig promapi.Config, metricName string, labels map[string]string) (*PromQLMetricSource, error) {
	queryExpr := buildPromQL(metricName, labels)
	return NewPromQLMetricSource(promConfig, queryExpr)
}

// NewFlowControlQueueSizePromQL builds a PromQLMetricSource that returns the EPP queue depth
// as a dispatch budget D = 1 − (queue_size / (ready_pods × maxConcurrency)), where queue_size is
// inference_extension_flow_control_queue_size and max_SYS = ready_pods × maxConcurrency is
// computed dynamically from the inference_pool_ready_pods metric.
// inferencePool and maxConcurrency are required.
func NewFlowControlQueueSizePromQL(promConfig promapi.Config, inferencePool string, maxConcurrency float64, namespace string) (*PromQLMetricSource, error) {
	if inferencePool == "" {
		return nil, fmt.Errorf("inference pool name is required for flow control queue size PromQL")
	}
	if maxConcurrency <= 0 {
		return nil, fmt.Errorf("maxConcurrency must be positive, got %g", maxConcurrency)
	}
	queueLabels := map[string]string{"inference_pool": inferencePool}
	podsLabels := map[string]string{"name": inferencePool}
	if namespace != "" {
		queueLabels["namespace"] = namespace
		podsLabels["namespace"] = namespace
	}
	query := fmt.Sprintf(
		`1 - (sum by(inference_pool)(%s) / on() (%s * %g))`,
		buildPromQL("inference_extension_flow_control_queue_size", queueLabels),
		buildPromQL("inference_pool_ready_pods", podsLabels),
		maxConcurrency,
	)
	source, err := NewPromQLMetricSource(promConfig, query)
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus metric source: %w", err)
	}
	return source, nil
}

// NewVLLMSaturationPromQL builds a PromQLMetricSource that estimates inference pool saturation
// from vLLM and pool metrics, returning D = 1 − (running_requests / (ready_pods × maxConcurrency)).
// This serves as a fallback when EPP flow control metrics are unavailable.
// inferencePool and maxConcurrency are required.
func NewVLLMSaturationPromQL(promConfig promapi.Config, inferencePool string, maxConcurrency float64, namespace string) (*PromQLMetricSource, error) {
	if inferencePool == "" {
		return nil, fmt.Errorf("inference pool name is required for vLLM saturation PromQL")
	}
	if maxConcurrency <= 0 {
		return nil, fmt.Errorf("maxConcurrency must be positive, got %g", maxConcurrency)
	}
	vllmLabels := map[string]string{"inference_pool": inferencePool}
	podsLabels := map[string]string{"name": inferencePool}
	if namespace != "" {
		vllmLabels["namespace"] = namespace
		podsLabels["namespace"] = namespace
	}
	query := fmt.Sprintf(
		`1 - (sum(%s) / on() (%s * %g))`,
		buildPromQL("vllm:num_requests_running", vllmLabels),
		buildPromQL("inference_pool_ready_pods", podsLabels),
		maxConcurrency,
	)
	source, err := NewPromQLMetricSource(promConfig, query)
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus metric source: %w", err)
	}
	return source, nil
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
