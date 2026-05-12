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

	"github.com/llm-d-incubation/llm-d-async/api"
	pipeline "github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.DispatchGate = (*CompositeGate)(nil)
var _ pipeline.AttributeGate = (*CompositeGate)(nil)

// CompositeGate combines multiple DispatchGates and/or AttributeGates.
// It returns the minimum budget across all inner DispatchGates.
// It acquires quota across all inner AttributeGates (all or nothing).
type CompositeGate struct {
	gates []pipeline.DispatchGate
}

// NewCompositeGate creates a CompositeGate with the given inner gates.
func NewCompositeGate(gates ...pipeline.DispatchGate) *CompositeGate {
	return &CompositeGate{gates: gates}
}

// Budget implements DispatchGate.
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

// Acquire implements AttributeGate.
// It attempts to acquire quota across all inner AttributeGates.
// If any gate denies the request or fails, it releases the quota acquired from previous gates.
func (c *CompositeGate) Acquire(ctx context.Context, attributes map[string]string) (pipeline.AcquireResult, error) {
	var releases []func()
	releaseAll := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}

	finalClassification := api.ClassificationNone
	for _, gate := range c.gates {
		if attrGate, ok := gate.(pipeline.AttributeGate); ok {
			res, err := attrGate.Acquire(ctx, attributes)
			if err != nil {
				releaseAll()
				return pipeline.AcquireResult{}, err
			}
			if !res.Allowed {
				releaseAll()
				return pipeline.AcquireResult{Allowed: false, Classification: api.ClassificationOverflow}, nil
			}
			if res.Release != nil {
				releases = append(releases, res.Release)
			}

			// Aggregate classification: Overflow dominates.
			if res.Classification == api.ClassificationOverflow {
				finalClassification = api.ClassificationOverflow
			} else if res.Classification == api.ClassificationReserved && finalClassification == api.ClassificationNone {
				finalClassification = api.ClassificationReserved
			}
		}
	}

	return pipeline.AcquireResult{
		Allowed:        true,
		Classification: finalClassification,
		Release:        releaseAll,
	}, nil
}
