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

	queryExpr := "1 - " + buildPromQL("inference_extension_flow_control_pool_saturation",
		map[string]string{"inference_pool": inferencePool})
	return NewPromQLMetricSource(promConfig, queryExpr)
}

// NewPromQLMetricSourceFromLabels constructs a PromQL instant vector selector from a metric
// name and label matchers, and returns a PromQLMetricSource for it.
func NewPromQLMetricSourceFromLabels(promConfig promapi.Config, metricName string, labels map[string]string) (*PromQLMetricSource, error) {
	queryExpr := buildPromQL(metricName, labels)
	return NewPromQLMetricSource(promConfig, queryExpr)
}

// NewBudgetPromQLSourceFromConfig builds a PromQLMetricSource for the dispatch budget use case,
// parsing all budget-specific parameters.
//
// Params:
//   - inferencePool: inference pool name for PromQL label selector
//   - maxSysStr: max system capacity for normalizing F_EPP (required)
//   - baselineStr: reserved baseline B (default "0.05")
func NewBudgetPromQLSourceFromConfig(promConfig promapi.Config, params map[string]string) (*PromQLMetricSource, error) {
	inferencePoolStr, maxSysStr, baselineStr := params["pool"], params["max_sys"], params["baseline"]

	baseline := 0.05
	if baselineStr != "" {
		b, err := strconv.ParseFloat(baselineStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid baseline value '%s': %w", baselineStr, err)
		}
		if b < 0 || b > 1 {
			return nil, fmt.Errorf("baseline must be in [0, 1], got %g", b)
		}
		baseline = b
	}

	if maxSysStr == "" {
		return nil, fmt.Errorf("prometheus-budget gate requires 'max_sys' parameter")
	}
	maxSys, err := strconv.ParseFloat(maxSysStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid max_sys value '%s': %w", maxSysStr, err)
	}
	if maxSys <= 0 {
		return nil, fmt.Errorf("max_sys must be positive, got %g", maxSys)
	}

	return NewBudgetPromQL(promConfig, inferencePoolStr, maxSys, baseline)
}

// NewBudgetPromQL constructs a PromQL expression for the multiplicative dispatch budget:
//
//	(1 - F_SYS) * (1 - F_EPP) * (1 - B)
//
// where:
//   - F_SYS = inference_extension_flow_control_pool_saturation
//   - F_EPP = sum(inference_extension_flow_control_queue_size) / maxSys
//   - B = baseline
func NewBudgetPromQL(promConfig promapi.Config, inferencePool string, maxSys float64, baseline float64) (*PromQLMetricSource, error) {
	if inferencePool == "" {
		return nil, fmt.Errorf("inference pool name is required for budget PromQL")
	}
	label := strconv.Quote(inferencePool)
	query := fmt.Sprintf(`(1 - inference_extension_flow_control_pool_saturation{inference_pool=%s})
			* on(inference_pool) (1 - sum by(inference_pool)(inference_extension_flow_control_queue_size{inference_pool=%s}) / %g)
			* %g`, label, label, maxSys, 1-baseline)

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
