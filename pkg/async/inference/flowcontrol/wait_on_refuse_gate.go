package flowcontrol

import (
	"context"
	"fmt"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*WaitOnRefuseGate)(nil)

// WaitOnRefuseGate wraps a single inner gate and converts any ActionRefuse
// verdict from the inner gate into ActionWait.
type WaitOnRefuseGate struct {
	inner pipeline.Gate
}

// NewWaitOnRefuseGate creates a WaitOnRefuseGate with the given inner gate.
func NewWaitOnRefuseGate(inner pipeline.Gate) *WaitOnRefuseGate {
	return &WaitOnRefuseGate{inner: inner}
}

// Budget implements pipeline.Gate.
// Returns the same budget as the inner gate.
func (w *WaitOnRefuseGate) Budget(ctx context.Context) float64 {
	return w.inner.Budget(ctx)
}

// Apply implements pipeline.Gate.
// Calls Apply on the inner gate and overrides ActionRefuse to ActionWait.
func (w *WaitOnRefuseGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	verdict, err := w.inner.Apply(ctx, msg, releases)
	if err != nil {
		return verdict, fmt.Errorf("inner gate apply failed: %w", err)
	}
	if verdict.Action == pipeline.ActionRefuse {
		verdict.Action = pipeline.ActionWait
	}
	return verdict, nil
}
