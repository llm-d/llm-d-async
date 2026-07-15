package pipeline

import (
	"context"

	"github.com/llm-d/llm-d-async/api"
)

// VerdictAction represents the action to take after evaluating admission gates.
type VerdictAction int

const (
	// ActionContinue allows the request to be dispatched.
	ActionContinue VerdictAction = iota
	// ActionDrop permanently discards the request and optionally returns a result.
	ActionDrop
	// ActionRefuse temporarily rejects the request and requests redelivery/re-enqueue.
	ActionRefuse
	// ActionWait temporarily parks the worker goroutine until capacity opens.
	ActionWait
)

// Verdict carries the outcome of a gating decision.
type Verdict struct {
	Action VerdictAction
	Result *api.ResultMessage
}

// Continue returns a verdict to proceed.
func Continue() Verdict {
	return Verdict{Action: ActionContinue}
}

// Drop returns a verdict to terminate the request permanently.
func Drop(result *api.ResultMessage) Verdict {
	return Verdict{Action: ActionDrop, Result: result}
}

// Refuse returns a verdict to temporarily reject and redeliver/re-enqueue the request.
func Refuse() Verdict {
	return Verdict{Action: ActionRefuse}
}

// Wait returns a verdict to temporarily park the worker.
func Wait() Verdict {
	return Verdict{Action: ActionWait}
}

// GateReleaseFunc defines the signature for gating resource release/cleanup closures.
type GateReleaseFunc func()

// ReleaseGateReleases executes all release functions in the slice in reverse order.
func ReleaseGateReleases(releases []GateReleaseFunc) {
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			releases[i]()
		}
	}
}

// Gate defines a unified interface for system capacity and request admission control.
type Gate interface {
	// Budget returns the system dispatch capacity budget in the range [0.0, 1.0].
	// budget represents the fraction of system capacity available for new requests.
	// A value of 0.0 indicates no available capacity (system at max allowed).
	// A value of 1.0 indicates full capacity available (system is idle).
	// The system always returns a valid value, even in case of internal error.
	Budget(ctx context.Context) float64
	// Apply applies the gating logic to a request.
	Apply(ctx context.Context, msg *api.InternalRequest, releases *[]GateReleaseFunc) (Verdict, error)
}

// ApplyChain runs a series of gates sequentially. If any gate fails or indicates a non-continue
// verdict, the chain terminates immediately and rolls back any state acquired by previous gates in the chain.
func ApplyChain(ctx context.Context, msg *api.InternalRequest, gates []Gate, releases *[]GateReleaseFunc) (Verdict, error) {
	var snapshot int
	if releases != nil {
		snapshot = len(*releases)
	}
	for _, gate := range gates {
		verdict, err := gate.Apply(ctx, msg, releases)
		if err != nil || verdict.Action != ActionContinue {
			if releases != nil {
				// Rollback releases acquired in this chain
				for i := len(*releases) - 1; i >= snapshot; i-- {
					if (*releases)[i] != nil {
						(*releases)[i]()
					}
				}
				*releases = (*releases)[:snapshot]
			}
			return verdict, err
		}
	}
	return Continue(), nil
}

// GateConfig holds the configuration for a single gate instance.
type GateConfig struct {
	GateType   string         `json:"gate_type,omitempty"`
	GateParams map[string]any `json:"gate_params,omitempty"`
}

// GateFactory defines the interface for creating Gate instances.
type GateFactory interface {
	CreateGate(cfg GateConfig) (Gate, error)
}

var _ Gate = DispatchGateFunc(nil)

// DispatchGateFunc is a function type that implements Gate.
type DispatchGateFunc func(context.Context) float64

func (f DispatchGateFunc) Budget(ctx context.Context) float64 {
	return f(ctx)
}

func (f DispatchGateFunc) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]GateReleaseFunc) (Verdict, error) {
	if f(ctx) <= 0.0 {
		return Refuse(), nil
	}
	return Continue(), nil
}

// ConstOpenGate returns a gate that is always open and has full capacity.
func ConstOpenGate() Gate {
	return DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
}
