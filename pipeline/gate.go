package pipeline

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/api"
)

// Verdict carries the outcome of a gating decision.
type Verdict struct {
	Terminate bool
	Redeliver bool
	Result    *api.ResultMessageExpand
}

// Continue returns a verdict to proceed.
func Continue() Verdict {
	return Verdict{}
}

// Drop returns a verdict to terminate the request permanently.
func Drop(result *api.ResultMessageExpand) Verdict {
	return Verdict{Terminate: true, Result: result}
}

// Refuse returns a verdict to temporarily reject and redeliver/re-enqueue the request.
func Refuse() Verdict {
	return Verdict{Redeliver: true}
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
	Apply(ctx context.Context, msg *api.InternalRequest) (Verdict, error)
}

// ApplyChain runs a series of gates sequentially. If any gate fails or indicates a non-continue
// verdict, the chain terminates immediately and rolls back any state acquired by previous gates in the chain.
func ApplyChain(ctx context.Context, msg *api.InternalRequest, gates []Gate) (Verdict, error) {
	snapshot := len(msg.Releases())
	for _, gate := range gates {
		verdict, err := gate.Apply(ctx, msg)
		if err != nil || verdict.Terminate || verdict.Redeliver {
			msg.RollbackReleases(snapshot)
			return verdict, err
		}
	}
	return Continue(), nil
}

// GateFactory defines the interface for creating Gate instances.
type GateFactory interface {
	CreateGate(gateType string, params map[string]string) (Gate, error)
}

var _ Gate = DispatchGateFunc(nil)

// DispatchGateFunc is a function type that implements Gate.
type DispatchGateFunc func(context.Context) float64

func (f DispatchGateFunc) Budget(ctx context.Context) float64 {
	return f(ctx)
}

func (f DispatchGateFunc) Apply(ctx context.Context, msg *api.InternalRequest) (Verdict, error) {
	if f(ctx) <= 0.0 {
		return Refuse(), nil
	}
	return Continue(), nil
}

// ConstOpenGate returns a gate that is always open and has full capacity.
func ConstOpenGate() Gate {
	return DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
}
