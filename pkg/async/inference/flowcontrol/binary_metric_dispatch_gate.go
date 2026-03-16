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

	"github.com/prometheus/client_golang/api"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var isGMP = flag.Bool("gate.pmetric.is-gmp", false, "Is this GMP (Google Managed Prometheus).")
var prometheusURL = flag.String("gate.prometheus.url", "", "Prometheus URL for non GMP metric")
var gmpProjectID = flag.String("gate.pmetric.gmp.project-id", "", "Project ID for Google Managed Prometheus")
var prometheusQueryModelName = flag.String("gate.prometheus.model-name", "", "metrics name to use for avg_queue_size")

// BinaryMetricDispatchGate implements DispatchGate using a MetricSource.
// It returns 0.0 (no capacity) if the metric value is non-zero,
// and 1.0 (full capacity) if the metric value is zero.
type BinaryMetricDispatchGate struct {
	source MetricSource
}

// NewBinaryMetricDispatchGate creates a new gate based on the provided Prometheus metric.
func NewBinaryMetricDispatchGate(clientConfig api.Config, query string) *BinaryMetricDispatchGate {
	source, err := NewPrometheusMetricSource(clientConfig, query)
	if err != nil {
		panic(err)
	}
	return NewBinaryMetricDispatchGateWithSource(source)
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

	value, err := g.source.GetValue(ctx)
	if err != nil {
		logger.V(logutil.DEFAULT).Info("MetricSource error, failing open", "error", err)
		return 1.0
	}

	if value == 0.0 {
		return 1.0
	}
	return 0.0
}

func AverageQueueSizeGate() *BinaryMetricDispatchGate {
	query := `inference_pool_average_queue_size{name="` + *prometheusQueryModelName + `"}`

	if *isGMP {
		source, err := NewGMPMetricSource(*gmpProjectID, query)
		if err != nil {
			panic(err)
		}
		return NewBinaryMetricDispatchGateWithSource(source)
	}

	return NewBinaryMetricDispatchGate(api.Config{
		Address: *prometheusURL,
	}, query)
}
