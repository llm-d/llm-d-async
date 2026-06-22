package pipeline

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/api"
)

// DispatchGate defines the interface to determine whether there is enough capacity to forward a request.
type DispatchGate interface {
	// Budget returns the Dispatch Budget in the range [0.0, 1.0], representing
	// the fraction of system capacity available for new requests.
	// A value of 0.0 indicates no available capacity (system at max allowed).
	// A value of 1.0 indicates full capacity available (system is idle).
	// The system always returns a valid value, even in case of internal error.
	Budget(ctx context.Context) float64
}

// AcquireResult contains the outcome of an AttributeGate.Acquire attempt.
type AcquireResult struct {
	// Allowed is true if the request should proceed.
	Allowed bool
	// Classification indicates the quota status of the request.
	Classification api.QuotaClassification
	// Release is a function to be called when processing is complete.
	// It may be nil if no quota was acquired.
	Release func()
}

// AttributeGate defines the interface to determine if a request is allowed based on its attributes.
type AttributeGate interface {
	// Acquire attempts to acquire quota for the given attributes.
	// If the gate does not support the given attributes or is not a quota gate,
	// it should return Allowed=true and ClassificationNone.
	Acquire(ctx context.Context, attributes map[string]string) (AcquireResult, error)
}

// GateFactory defines the interface for creating DispatchGate instances.
type GateFactory interface {
	CreateGate(gateType string, params map[string]string) (DispatchGate, error)
}

var _ DispatchGate = DispatchGateFunc(nil)

// DispatchGateFunc is a function type that implements DispatchGate.
type DispatchGateFunc func(context.Context) float64

func (f DispatchGateFunc) Budget(ctx context.Context) float64 {
	return f(ctx)
}

func ConstOpenGate() DispatchGate {
	return DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
}
