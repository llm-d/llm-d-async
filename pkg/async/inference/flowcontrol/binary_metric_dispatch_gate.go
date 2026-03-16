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
	source     MetricSource
	metricName string
	labels     map[string]string
}

// NewBinaryMetricDispatchGate creates a new gate that queries Prometheus for the given metric.
func NewBinaryMetricDispatchGate(clientConfig api.Config, metricName string, labels map[string]string) *BinaryMetricDispatchGate {
	source, err := NewPrometheusMetricSource(clientConfig)
	if err != nil {
		panic(err)
	}
	return NewBinaryMetricDispatchGateWithSource(source, metricName, labels)
}

// NewBinaryMetricDispatchGateWithSource creates a new gate using the provided MetricSource.
func NewBinaryMetricDispatchGateWithSource(source MetricSource, metricName string, labels map[string]string) *BinaryMetricDispatchGate {
	return &BinaryMetricDispatchGate{
		source:     source,
		metricName: metricName,
		labels:     labels,
	}
}

// Budget implements DispatchGate.
func (g *BinaryMetricDispatchGate) Budget(ctx context.Context) float64 {
	logger := log.FromContext(ctx)

	samples, err := g.source.Query(ctx, g.metricName, g.labels)
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

func AverageQueueSizeGate() *BinaryMetricDispatchGate {
	metricName := "inference_pool_average_queue_size"
	labels := map[string]string{"name": *prometheusQueryModelName}

	if *isGMP {
		source, err := NewGMPMetricSource(*gmpProjectID)
		if err != nil {
			panic(err)
		}
		return NewBinaryMetricDispatchGateWithSource(source, metricName, labels)
	}

	return NewBinaryMetricDispatchGate(api.Config{
		Address: *prometheusURL,
	}, metricName, labels)
}
