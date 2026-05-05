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
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var _ MetricSource = (*CascadeMetricSource)(nil)

// CascadeMetricSource tries each source in order and returns the first successful result.
// If a source returns an error or no samples, the next source is tried. Log messages are
// emitted only on transitions (entering/leaving fallback) to avoid noise during sustained outages.
type CascadeMetricSource struct {
	sources       []MetricSource
	usingFallback atomic.Bool
}

// NewCascadeMetricSource creates a CascadeMetricSource from the given sources, tried in order.
// At least two sources are required.
func NewCascadeMetricSource(sources ...MetricSource) *CascadeMetricSource {
	return &CascadeMetricSource{sources: sources}
}

// Query implements MetricSource. It tries each source in order, logging once when
// entering fallback and once when recovering to the primary.
func (c *CascadeMetricSource) Query(ctx context.Context) ([]Sample, error) {
	logger := log.FromContext(ctx)
	for i, s := range c.sources {
		samples, err := s.Query(ctx)
		if err == nil && len(samples) > 0 {
			if i > 0 {
				if c.usingFallback.CompareAndSwap(false, true) {
					logger.V(logutil.DEFAULT).Info("primary metric source unavailable, using fallback",
						"fallbackIndex", i)
				}
			} else if c.usingFallback.CompareAndSwap(true, false) {
				logger.V(logutil.DEFAULT).Info("primary metric source recovered")
			}
			return samples, nil
		}
	}
	return nil, fmt.Errorf("all metric sources unavailable")
}
