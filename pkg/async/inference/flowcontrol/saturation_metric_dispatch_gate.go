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

	"github.com/prometheus/client_golang/api"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// SaturationMetricDispatchGate implements DispatchGate based on pool saturation.
// It reads the inference_extension_flow_control_pool_saturation metric and
// returns 0.0 if saturation is at or above the configured threshold,
// otherwise returns 1 - saturation.
type SaturationMetricDispatchGate struct {
	source     MetricSource
	metricName string
	labels     map[string]string
	threshold  float64
}

// NewSaturationMetricDispatchGate creates a new gate that queries Prometheus for the pool saturation metric.
func NewSaturationMetricDispatchGate(clientConfig api.Config, inferencePool string, threshold float64) *SaturationMetricDispatchGate {
	source, err := NewPrometheusMetricSource(clientConfig)
	if err != nil {
		panic(err)
	}
	return NewSaturationMetricDispatchGateWithSource(source, inferencePool, threshold)
}

// NewSaturationMetricDispatchGateWithSource creates a new gate using the provided MetricSource.
func NewSaturationMetricDispatchGateWithSource(source MetricSource, inferencePool string, threshold float64) *SaturationMetricDispatchGate {
	return &SaturationMetricDispatchGate{
		source:     source,
		metricName: "inference_extension_flow_control_pool_saturation",
		labels:     map[string]string{"inference_pool": inferencePool},
		threshold:  threshold,
	}
}

// Budget implements DispatchGate.
func (g *SaturationMetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	samples, err := g.source.Query(ctx, g.metricName, g.labels)
	if err != nil {
		logger.V(logutil.DEFAULT).Info("MetricSource error, failing closed", "error", err)
		return 0.0
	}

	if len(samples) == 0 {
		logger.V(logutil.DEFAULT).Info("No saturation metrics found, failing closed")
		return 0.0
	}

	saturation := samples[0].Value
	if saturation >= g.threshold {
		return 0.0
	}
	return 1.0 - saturation
}
