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
// reading at most once per interval. A request that finishes, is retried or loses its claim
// returns its slot to the reading that granted it. A failed reading admits nothing.
type HeadroomGate struct {
	source   headroomSource
	interval time.Duration
	now      func() time.Time

	owner pipeline.GateOwner

	refreshLock chan struct{}
	readAt      time.Time
	haveRead    bool

	mu        sync.Mutex
	reading   uint64
	allowance float64
	admitted  float64
}

// NewHeadroomGate creates a gate that re-reads source at most once per interval.
func NewHeadroomGate(source headroomSource, interval time.Duration) *HeadroomGate {
	return &HeadroomGate{source: source, interval: interval, now: time.Now, refreshLock: make(chan struct{}, 1)}
}

// WithOwner records the queue or pool that owns the gate for its metric labels.
func (g *HeadroomGate) WithOwner(owner pipeline.GateOwner) *HeadroomGate {
	g.owner = owner
	return g
}

// Budget is 1 while the current reading has an unclaimed slot and 0 otherwise. Apply enforces
// the count, so a whole batch may be polled for the last slot.
func (g *HeadroomGate) Budget(ctx context.Context) float64 {
	if err := g.refresh(ctx); err != nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.admitted < g.allowance {
		return 1
	}
	return 0
}

// Apply admits the request while the current reading still has unclaimed slots.
func (g *HeadroomGate) Apply(ctx context.Context, _ *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	if err := g.refresh(ctx); err != nil {
		return pipeline.Verdict{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.admitted >= g.allowance {
		return pipeline.Refuse(), nil
	}
	g.admitted++
	if releases != nil {
		reading := g.reading
		*releases = append(*releases, func() { g.release(reading) })
	}
	return pipeline.Continue(), nil
}

func (g *HeadroomGate) release(reading uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.reading == reading {
		g.admitted--
	}
}

// refresh takes a new reading once the current one is older than interval. It returns an
// error only when ctx ended before a reading was recorded.
func (g *HeadroomGate) refresh(ctx context.Context) error {
	select {
	case g.refreshLock <- struct{}{}:
		defer func() { <-g.refreshLock }()
	case <-ctx.Done():
		return fmt.Errorf("waiting for headroom reading: %w", ctx.Err())
	}

	now := g.now()
	if g.haveRead && now.Sub(g.readAt) < g.interval {
		return nil
	}

	headroom, err := g.source.Headroom(ctx)
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("headroom reading: %w", err)
	}
	if err == nil && (math.IsNaN(headroom) || math.IsInf(headroom, 0) || headroom < 0) {
		err = fmt.Errorf("headroom reading %g is not a finite count", headroom)
	}
	allowance := 0.0
	if err != nil {
		log.FromContext(ctx).Error(err, "headroom gate closed until the next reading")
		metrics.SetGateMetricSourceAvailable(false, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, "")
	} else {
		metrics.SetGateMetricValue(headroom, 0, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, "")
		metrics.SetGateMetricSourceAvailable(true, g.owner.QueueID, g.owner.QueueName, g.owner.WorkerPoolID, "")
		allowance = math.Floor(headroom)
	}

	g.readAt, g.haveRead = now, true
	g.mu.Lock()
	g.reading++
	g.allowance, g.admitted = allowance, 0
	g.mu.Unlock()
	return nil
}
