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
	"sync"
	"time"
)

// CachedMetricSource wraps a MetricSource with a TTL cache so that repeated
// queries within the TTL return the cached result instead of hitting the
// backend (e.g. Prometheus) on every call.
type CachedMetricSource struct {
	source MetricSource
	ttl    time.Duration

	mu      sync.Mutex
	samples []Sample
	err     error
	expiry  time.Time
}

// NewCachedMetricSource wraps the given source with a cache that holds results
// for the specified TTL duration.
func NewCachedMetricSource(source MetricSource, ttl time.Duration) *CachedMetricSource {
	return &CachedMetricSource{
		source: source,
		ttl:    ttl,
	}
}

// Query returns cached samples if the cache is still valid, otherwise
// delegates to the underlying source and caches the result.
func (c *CachedMetricSource) Query(ctx context.Context) ([]Sample, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if now.Before(c.expiry) {
		return c.samples, c.err
	}

	c.samples, c.err = c.source.Query(ctx)
	c.expiry = now.Add(c.ttl)
	return c.samples, c.err
}
