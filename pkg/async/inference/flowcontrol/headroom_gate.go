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
	"math"
	"sync"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var _ pipeline.Gate = (*HeadroomGate)(nil)

// headroomSource reports free capacity in the metric's own units.
type headroomSource interface {
	Headroom(ctx context.Context) (float64, error)
}

// HeadroomGate admits at most as many requests as the latest reading's free slots, taking a
// reading at most once per interval. A failed reading admits nothing.
type HeadroomGate struct {
	source   headroomSource
	interval time.Duration
	now      func() time.Time

	owner         pipeline.GateOwner
	inferencePool string

	mu        sync.Mutex
	readAt    time.Time
	haveRead  bool
	allowance float64
	admitted  float64
}

// NewHeadroomGate creates a gate that re-reads source at most once per interval.
func NewHeadroomGate(source headroomSource, interval time.Duration) *HeadroomGate {
	return &HeadroomGate{source: source, interval: interval, now: time.Now}
}

// WithOwner records the queue or pool that owns the gate for its metric labels.
func (g *HeadroomGate) WithOwner(owner pipeline.GateOwner) *HeadroomGate {
	g.owner = owner
	return g
}

// WithInferencePool records the InferencePool the metric describes.
func (g *HeadroomGate) WithInferencePool(pool string) *HeadroomGate {
	g.inferencePool = pool
	return g
}

// Budget returns the unused fraction of the current reading's allowance.
func (g *HeadroomGate) Budget(ctx context.Context) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.refreshLocked(ctx)
	if g.allowance <= 0 {
		return 0
	}
	return math.Max(0, g.allowance-g.admitted) / g.allowance
}

// Apply admits the request while the current reading still has unclaimed slots.
func (g *HeadroomGate) Apply(ctx context.Context, _ *api.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.refreshLocked(ctx)
	if g.admitted >= g.allowance {
		return pipeline.Refuse(), nil
	}
	g.admitted++
	return pipeline.Continue(), nil
}

func (g *HeadroomGate) refreshLocked(ctx context.Context) {
	now := g.now()
	if g.haveRead && now.Sub(g.readAt) < g.interval {
		return
	}
	g.readAt, g.haveRead, g.admitted = now, true, 0
	headroom, err := g.source.Headroom(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "headroom gate closed until the next reading")
		metrics.SetGateMetricSourceAvailable(false, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, g.inferencePool)
		g.allowance = 0
		return
	}
	metrics.SetGateMetricValue(headroom, 0, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, g.inferencePool)
	metrics.SetGateMetricSourceAvailable(true, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, g.inferencePool)
	g.allowance = math.Floor(headroom)
}
