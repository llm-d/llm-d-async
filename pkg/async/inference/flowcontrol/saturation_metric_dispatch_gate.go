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
var saturationFallback = flag.Float64("gate.saturation.fallback", 0.0, "fallback saturation value on error/missing metrics; default 0.0")
var saturationQueryExpr = flag.String("gate.saturation.query-expr", "", "custom PromQL expression for saturation metric; overrides inference-pool label selector")

// SaturationMetricDispatchGate implements DispatchGate based on pool saturation.
// It queries a MetricSource for saturation samples and returns 0.0 if saturation
// is at or above the configured threshold, otherwise returns max(0, 1 - saturation),
// clamped to [0.0, 1.0].
//
// On error or missing/invalid data, the gate returns the configured fallback
// budget (derived from the fallback saturation value, also clamped to [0.0, 1.0]).
type SaturationMetricDispatchGate struct {
	source    MetricSource
	threshold float64
	fallback  float64
}

// NewSaturationMetricDispatchGateWithSource creates a new gate using the provided MetricSource.
func NewSaturationMetricDispatchGateWithSource(source MetricSource, threshold float64, fallback float64) *SaturationMetricDispatchGate {
	return &SaturationMetricDispatchGate{
		source:    source,
		threshold: threshold,
		fallback:  math.Max(0.0, math.Min(1.0, 1.0-fallback)), // fallback is a saturation value; budget is clamped to [0,1]
	}
}

// Budget implements DispatchGate.
// On error or missing data the gate returns the configured fallback budget.
// The output is always clamped to [0.0, 1.0].
func (g *SaturationMetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	samples, err := g.source.Query(ctx)
	if err != nil {
		logger.V(logutil.DEFAULT).Info("MetricSource error, using fallback value", "fallback", g.fallback, "error", err)
		return g.fallback
	}

	if len(samples) == 0 {
		logger.V(logutil.DEFAULT).Info("No saturation metrics found, using fallback value", "fallback", g.fallback)
		return g.fallback
	}

	saturation := samples[0].Value
	if math.IsNaN(saturation) || math.IsInf(saturation, 0) {
		logger.V(logutil.DEFAULT).Info("Invalid saturation value, using fallback value", "fallback", g.fallback, "value", saturation)
		return g.fallback
	}
	if saturation >= g.threshold {
		return 0.0
	}
	return math.Min(1.0, math.Max(0.0, 1.0-saturation))
}

// SaturationGate creates a SaturationMetricDispatchGate from command-line flags.
func SaturationGate() *SaturationMetricDispatchGate {
	expr := buildPromQL("inference_extension_flow_control_pool_saturation",
		map[string]string{"inference_pool": *saturationInferencePool})
	if *saturationQueryExpr != "" {
		expr = *saturationQueryExpr
	}

	var source MetricSource
	if *isGMP {
		var err error
		source, err = NewGMPPromQLMetricSource(*gmpProjectID, expr)
		if err != nil {
			panic(err)
		}
	} else {
		var err error
		source, err = NewPromQLMetricSource(api.Config{
			Address: *prometheusURL,
		}, expr)
		if err != nil {
			panic(err)
		}
	}

	return NewSaturationMetricDispatchGateWithSource(source, *saturationThreshold, *saturationFallback)
}
