/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package flowcontrol

import (
	"context"
	"fmt"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*ConstantDecisionGate)(nil)

// ConstantDecisionGate always returns the configured Verdict. It takes no
// state, attaches no Release, and never errors. Useful as:
//
//   - A test fixture (force fail-fast / nack paths in smoke tests)
//   - A kill-switch (set a chain to Refuse to drain in-flight work and stop
//     accepting new messages)
//   - A pool-gate stand-in when bringing up a new merge policy before the
//     real pool gate is ready
//
// Construct via the GateFactory under name "constant-decision" with
// gate_params: {"decision": "continue"|"drop"|"refuse"}.
type ConstantDecisionGate struct {
	verdict pipeline.Verdict
}

// NewConstantDecisionGate constructs a gate that returns the given Verdict
// on every Apply call.
func NewConstantDecisionGate(v pipeline.Verdict) *ConstantDecisionGate {
	return &ConstantDecisionGate{verdict: v}
}

// Apply implements pipeline.Gate.
func (g *ConstantDecisionGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	return g.verdict, nil
}

// parseDecision parses a "continue" / "drop" / "refuse" string into a
// pipeline.Verdict, case-insensitively (the typical config source is YAML).
func parseDecision(s string) (pipeline.Verdict, error) {
	switch s {
	case "continue", "Continue", "CONTINUE":
		return pipeline.Continue, nil
	case "drop", "Drop", "DROP":
		return pipeline.Drop(nil), nil
	case "refuse", "Refuse", "REFUSE":
		return pipeline.Refuse(), nil
	default:
		return pipeline.Verdict{}, fmt.Errorf("unknown decision %q (want continue|drop|refuse)", s)
	}
}
