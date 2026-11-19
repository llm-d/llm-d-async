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
	"math"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ pipeline.Gate = (*MetricDispatchGate)(nil)

// MetricDispatchGate implements DispatchGate by querying a MetricSource for a
// budget value D and returning D − threshold, clamped to [0.0, 1.0].
//
// The gate closes (returns 0.0) when D ≤ threshold. This implements the doc
// formula N = max_SYS × (D − B) when threshold is set to the reserved baseline B.
// On error or missing/invalid data, the gate returns the configured fallback budget.
type MetricDispatchGate struct {
	source        MetricSource
	threshold     float64
	fallback      float64
	owner         pipeline.GateOwner
	inferencePool string
}

// WithOwner sets the queue or worker pool this gate belongs to. Its
// queue_id/queue_name/pool_name label the async_gate_metric_value and
// async_gate_metric_threshold gauges, so they join with the owner's other
// series (async_dispatch_budget, async_gate_decisions_total, ...).
func (g *MetricDispatchGate) WithOwner(owner pipeline.GateOwner) *MetricDispatchGate {
	g.owner = owner
	return g
}

// WithInferencePool sets the InferencePool this gate queries, exposed as the
// inference_pool label on the gauges above. It is what the gate measures, not
// who it throttles — pool_name is the latter.
func (g *MetricDispatchGate) WithInferencePool(pool string) *MetricDispatchGate {
	g.inferencePool = pool
	return g
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
// The source should return a budget value D (e.g. 1 − saturation).
// The threshold and fallback are given in saturation space and converted to budget
// space via 1 − value.
func NewSaturationDispatchGate(source MetricSource, threshold float64, fallback float64) *MetricDispatchGate {
	return NewMetricDispatchGate(source, 1.0-threshold, 1.0-fallback)
}

// NewBudgetDispatchGate creates a MetricDispatchGate for the prometheus-budget use case.
// The source should return the dispatch budget D (e.g. D = 1 − F, where F is EPP
// fullness, or D = 1 − S, where S is inference pool saturation).
// baseline is the reserved baseline B ∈ [0, 1]: the gate closes when D ≤ B and
// returns D − B when open, so the caller computes N = max_SYS × (D − B).
// fallback is returned on error, clamped to [0.0, 1.0].
func NewBudgetDispatchGate(source MetricSource, baseline float64, fallback float64) *MetricDispatchGate {
	return NewMetricDispatchGate(source, baseline, fallback)
}

// Budget implements DispatchGate.
// Returns 0.0 when D ≤ threshold (gate closed), otherwise D − threshold clamped to [0.0, 1.0].
// On error or missing data the gate returns the configured fallback budget.
func (g *MetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	samples, err := g.source.Query(ctx)
	if err != nil {
		return g.useFallback(logger, fmt.Errorf("MetricSource error: %w", err))
	}

	if len(samples) == 0 {
		return g.useFallback(logger, fmt.Errorf("no metric samples found"))
	}

	value := samples[0].Value
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return g.useFallback(logger, fmt.Errorf("invalid metric value: %v", value))
	}

	// Expose the raw value and threshold so operators can see why the gate is
	// open/closed (it closes when value <= threshold).
	metrics.SetGateMetricValue(value, g.threshold, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, g.inferencePool)
	metrics.SetGateMetricSourceAvailable(true, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, g.inferencePool)

	if value <= g.threshold {
		return 0.0
	}
	return math.Min(1.0, math.Max(0.0, value-g.threshold))
}

// useFallback reports that this evaluation had no usable reading and returns the
// configured fallback budget. Clearing async_gate_metric_source_available is what
// lets an operator tell a fallback budget apart from a real reading of the same
// number — most importantly a fallback of 0 from a genuinely saturated pool.
func (g *MetricDispatchGate) useFallback(logger logr.Logger, err error) float64 {
	metrics.SetGateMetricSourceAvailable(false, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, g.inferencePool)
	logger.Error(err, "using fallback value", "fallback", g.fallback)
	return g.fallback
}

// Apply implements pipeline.Gate.
func (g *MetricDispatchGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	if g.Budget(ctx) <= 0.0 {
		return pipeline.Refuse(), nil
	}
	return pipeline.Continue(), nil
}
