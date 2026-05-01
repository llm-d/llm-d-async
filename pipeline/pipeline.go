package pipeline

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/api"
)

type Flow interface {
	Characteristics() Characteristics
	Start(ctx context.Context)
	RequestChannels() []RequestChannel
	RetryChannel() chan RetryMessage
	ResultChannel() chan api.ResultMessage
}

type Characteristics struct {
	HasExternalBackoff     bool
	SupportsMessageLatency bool
}

// DispatchGate defines the interface to determine whether there is enough capacity to forward a request.
type DispatchGate interface {
	// Budget returns the Dispatch Budget in the range [0.0, 1.0], representing
	// the fraction of system capacity available for new requests.
	// A value of 0.0 indicates no available capacity (system at max allowed).
	// A value of 1.0 indicates full capacity available (system is idle).
	// The system always returns a valid value, even in case of internal error.
	Budget(ctx context.Context) float64
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

type RequestMergePolicy interface {
	MergeRequestChannels(channels []RequestChannel) EmbelishedRequestChannel
}

type RequestChannel struct {
	Channel            chan *api.InternalRequest
	IGWBaseURl         string
	InferenceObjective string
	RequestPathURL     string
	Gate               DispatchGate
}

type EmbelishedRequestChannel struct {
	Channel chan EmbelishedRequestMessage
}

// EmbelishedRequestMessage decorates an InternalRequest with HTTP dispatch context.
// The embedded InternalRequest must be non-nil in normal use.
// Caller-supplied metadata lives on the embedded Request (via ReqMetadata());
// there is no separate Metadata field here to avoid ambiguity.
type EmbelishedRequestMessage struct {
	*api.InternalRequest
	HttpHeaders map[string]string
	RequestURL  string
}

// RetryMessage carries an embellished request and backoff for re-queueing.
type RetryMessage struct {
	EmbelishedRequestMessage
	BackoffDurationSeconds float64
}
