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
	"math"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var _ pipeline.DispatchGate = (*MetricDispatchGate)(nil)

// MetricDispatchGate implements DispatchGate by querying a MetricSource for a
// budget value and returning it clamped to [0.0, 1.0].
//
// If the value is at or below the configured threshold, the gate returns 0.0.
// On error or missing/invalid data, the gate returns the configured fallback budget.
type MetricDispatchGate struct {
	source    MetricSource
	threshold float64
	fallback  float64
}

// NewMetricDispatchGate creates a MetricDispatchGate with the given source, threshold,
// and fallback budget value. The fallback is clamped to [0.0, 1.0].
func NewMetricDispatchGate(source MetricSource, threshold float64, fallback float64) *MetricDispatchGate {
	return &MetricDispatchGate{
		source:    source,
		threshold: threshold,
		fallback:  math.Max(0.0, math.Min(1.0, fallback)),
	}
}

// NewSaturationDispatchGate creates a MetricDispatchGate for the saturation use case.
// The source should return a budget value (e.g. 1 - saturation).
// The threshold and fallback are saturation values; they are internally converted
// to budget values via 1 - value.
func NewSaturationDispatchGate(source MetricSource, threshold float64, fallback float64) *MetricDispatchGate {
	return NewMetricDispatchGate(source, 1.0-threshold, 1.0-fallback)
}

// NewBudgetDispatchGate creates a MetricDispatchGate for the dispatch budget use case.
// The source should return the budget value directly: (1 - F_SYS) * (1 - F_EPP) * (1 - B).
// The threshold is set to 0.0 (gate closes only when budget reaches zero).
// The fallback parameter is a direct budget value, clamped to [0.0, 1.0].
func NewBudgetDispatchGate(source MetricSource, fallback float64) *MetricDispatchGate {
	return NewMetricDispatchGate(source, 0.0, fallback)
}

// Budget implements DispatchGate.
// On error or missing data the gate returns the configured fallback budget.
// The output is always clamped to [0.0, 1.0].
func (g *MetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	samples, err := g.source.Query(ctx)
	if err != nil {
		logger.V(logutil.DEFAULT).Info("MetricSource error, using fallback value", "fallback", g.fallback, "error", err)
		return g.fallback
	}

	if len(samples) == 0 {
		logger.V(logutil.DEFAULT).Info("No metric samples found, using fallback value", "fallback", g.fallback)
		return g.fallback
	}

	value := samples[0].Value
	if math.IsNaN(value) || math.IsInf(value, 0) {
		logger.V(logutil.DEFAULT).Info("Invalid metric value, using fallback value", "fallback", g.fallback, "value", value)
		return g.fallback
	}
	if value <= g.threshold {
		return 0.0
	}
	return math.Min(1.0, math.Max(0.0, value))
}
