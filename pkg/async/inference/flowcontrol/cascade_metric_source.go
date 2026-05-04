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

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var _ MetricSource = (*CascadeMetricSource)(nil)

// CascadeMetricSource tries each source in order and returns the first successful result.
// If a source returns an error or no samples, the next source is tried and a log message
// is emitted so operators can detect when the cascade is active.
type CascadeMetricSource struct {
	sources []MetricSource
}

// NewCascadeMetricSource creates a CascadeMetricSource from the given sources, tried in order.
// At least two sources are required.
func NewCascadeMetricSource(sources ...MetricSource) *CascadeMetricSource {
	return &CascadeMetricSource{sources: sources}
}

// Query implements MetricSource. It tries each source in order, logging a warning when
// falling back so operators can detect degraded metric availability.
func (c *CascadeMetricSource) Query(ctx context.Context) ([]Sample, error) {
	logger := log.FromContext(ctx)
	for i, s := range c.sources {
		samples, err := s.Query(ctx)
		if err == nil && len(samples) > 0 {
			if i > 0 {
				logger.V(logutil.DEFAULT).Info("primary metric source unavailable, using fallback",
					"fallbackIndex", i)
			}
			return samples, nil
		}
		if err != nil {
			logger.V(logutil.DEFAULT).Info("metric source unavailable, trying next",
				"sourceIndex", i, "error", err)
		}
	}
	return nil, fmt.Errorf("all metric sources unavailable")
}
