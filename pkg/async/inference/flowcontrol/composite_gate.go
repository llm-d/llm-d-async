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

	"github.com/llm-d/llm-d-async/api"
	pipeline "github.com/llm-d/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*CompositeGate)(nil)
var _ pipeline.FeedbackGate = (*CompositeGate)(nil)
var _ pipeline.WaitNotifier = (*CompositeGate)(nil)

// CompositeGate combines multiple pipeline.Gates.
// It returns the minimum budget across all inner Gates.
// It applies all inner gates (all or nothing) to incoming requests.
// Feedback and wait signals pass through: outcomes are forwarded to every
// inner gate that consumes them, and inner wait signals are fanned in, so
// wrapping a feedback gate in a composite does not sever its signal.
type CompositeGate struct {
	gates    []pipeline.Gate
	wait     <-chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
}

// NewCompositeGate creates a CompositeGate with the given inner gates.
func NewCompositeGate(gates ...pipeline.Gate) *CompositeGate {
	c := &CompositeGate{gates: gates, stop: make(chan struct{})}
	var signals []<-chan struct{}
	for _, gate := range gates {
		if notifier, ok := gate.(pipeline.WaitNotifier); ok {
			if signal := notifier.WaitSignal(); signal != nil {
				signals = append(signals, signal)
			}
		}
	}
	switch len(signals) {
	case 0:
		// c.wait stays nil: receives block forever, same as no wake support.
	case 1:
		c.wait = signals[0]
	default:
		// Fan the inner signals into one channel. The forwarding goroutines
		// run until Close; production composites are built once at startup
		// and never closed.
		merged := make(chan struct{}, 1)
		for _, signal := range signals {
			go func(signal <-chan struct{}) {
				for {
					select {
					case <-c.stop:
						return
					case <-signal:
						select {
						case merged <- struct{}{}:
						default:
						}
					}
				}
			}(signal)
		}
		c.wait = merged
	}
	return c
}

// Close stops the wait-signal fan-in goroutines. Safe to call more than once.
func (c *CompositeGate) Close() error {
	c.stopOnce.Do(func() { close(c.stop) })
	return nil
}

// ObserveOutcome implements pipeline.FeedbackGate by forwarding the outcome
// to every inner gate that consumes feedback.
func (c *CompositeGate) ObserveOutcome(fb pipeline.DispatchFeedback) {
	for _, gate := range c.gates {
		if feedbackGate, ok := gate.(pipeline.FeedbackGate); ok {
			feedbackGate.ObserveOutcome(fb)
		}
	}
}

// WaitSignal implements pipeline.WaitNotifier with the fan-in of the inner
// gates' wait signals; nil when no inner gate notifies.
func (c *CompositeGate) WaitSignal() <-chan struct{} {
	return c.wait
}

// Budget implements pipeline.Gate.
// Returns the minimum budget across all inner gates.
// If there are no inner gates, it returns 1.0.
func (c *CompositeGate) Budget(ctx context.Context) float64 {
	if len(c.gates) == 0 {
		return 1.0
	}

	minBudget := 1.0
	for _, gate := range c.gates {
		budget := gate.Budget(ctx)
		if budget < minBudget {
			minBudget = budget
		}
	}
	return minBudget
}

func (c *CompositeGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	return pipeline.ApplyChain(ctx, msg, c.gates, releases)
}
