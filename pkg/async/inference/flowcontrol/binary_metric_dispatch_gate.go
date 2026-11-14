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

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var _ pipeline.Gate = (*BinaryMetricDispatchGate)(nil)

// BinaryMetricDispatchGate implements DispatchGate using a MetricSource.
// It returns 0.0 (no capacity) if the metric value is non-zero,
// and 1.0 (full capacity) if the metric value is zero.
type BinaryMetricDispatchGate struct {
	source MetricSource
}

// NewBinaryMetricDispatchGateWithSource creates a new gate using the provided MetricSource.
func NewBinaryMetricDispatchGateWithSource(source MetricSource) *BinaryMetricDispatchGate {
	return &BinaryMetricDispatchGate{
		source: source,
	}
}

// Budget implements DispatchGate.
func (g *BinaryMetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	samples, err := g.source.Query(ctx)
	if err != nil {
		logger.V(logutil.DEFAULT).Info("MetricSource error, failing open", "error", err)
		return 1.0
	}

	if len(samples) == 0 {
		logger.V(logutil.DEFAULT).Info("No metrics found, failing open")
		return 1.0
	}

	if samples[0].Value == 0.0 {
		return 1.0
	}
	return 0.0
}

// Apply implements pipeline.Gate.
func (g *BinaryMetricDispatchGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	if g.Budget(ctx) <= 0.0 {
		return pipeline.Refuse(), nil
	}
	return pipeline.Continue(), nil
}
