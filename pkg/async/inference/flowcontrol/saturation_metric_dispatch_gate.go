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
	"flag"
	"math"

	"github.com/prometheus/client_golang/api"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var saturationInferencePool = flag.String("gate.saturation.inference-pool", "", "inference pool name for saturation metric")
var saturationThreshold = flag.Float64("gate.saturation.threshold", 0.8, "saturation threshold above which budget is zero")

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
// On error or missing data it fails closed (returns 0.0) because without a
// reliable saturation reading the system cannot confirm there is spare capacity,
// so it is safer to hold back requests until the metric becomes available.
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
	if math.IsNaN(saturation) || math.IsInf(saturation, 0) {
		logger.V(logutil.DEFAULT).Info("Invalid saturation value, failing closed", "value", saturation)
		return 0.0
	}
	saturation = math.Max(0.0, math.Min(1.0, saturation))
	if saturation >= g.threshold {
		return 0.0
	}
	return 1.0 - saturation
}

// SaturationGate creates a SaturationMetricDispatchGate from command-line flags.
func SaturationGate() *SaturationMetricDispatchGate {
	if *isGMP {
		source, err := NewGMPMetricSource(*gmpProjectID)
		if err != nil {
			panic(err)
		}
		return NewSaturationMetricDispatchGateWithSource(source, *saturationInferencePool, *saturationThreshold)
	}

	return NewSaturationMetricDispatchGate(api.Config{
		Address: *prometheusURL,
	}, *saturationInferencePool, *saturationThreshold)
}
