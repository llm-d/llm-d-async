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

var _ pipeline.Gate = (*CompositeGate)(nil)

// CompositeGate combines multiple pipeline.Gates.
// It returns the minimum budget across all inner Gates.
// It applies all inner gates (all or nothing) to incoming requests.
type CompositeGate struct {
	gates []pipeline.Gate
}

// NewCompositeGate creates a CompositeGate with the given inner gates.
func NewCompositeGate(gates ...pipeline.Gate) *CompositeGate {
	return &CompositeGate{gates: gates}
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

// Apply implements pipeline.Gate.
// It runs ApplyChain across all inner gates.
func (c *CompositeGate) Apply(ctx context.Context, msg *api.InternalRequest) (pipeline.Verdict, error) {
	return pipeline.ApplyChain(ctx, msg, c.gates)
}
