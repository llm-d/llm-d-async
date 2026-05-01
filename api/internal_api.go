package api

import (
	"context"
	"encoding/json"
	"fmt"
)

type Flow interface {
	Characteristics() Characteristics
	Start(ctx context.Context)
	RequestChannels() []RequestChannel
	RetryChannel() chan RetryMessage
	ResultChannel() chan ResultMessage
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
	Channel            chan *InternalRequest
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
	*InternalRequest
	HttpHeaders map[string]string
	RequestURL  string
}

// RetryMessage carries an embellished request and backoff for re-queueing.
type RetryMessage struct {
	EmbelishedRequestMessage
	BackoffDurationSeconds float64
}

// InternalRouting holds the resolved, authoritative routing fields used by
// infrastructure (producers, workers, retry logic). These are not part of the
// caller-facing contract and should not be set by callers directly.
// Producers merge caller-supplied per-message overrides (e.g.
// RedisRequest.RequestQueueName) with producer defaults and write the final
// values here. All internal pipeline code reads routing exclusively from this
// struct rather than reaching back into the typed request.
type InternalRouting struct {
	RetryCount             int    `json:"retry_count,omitempty"`
	RequestQueueName       string `json:"request_queue_name,omitempty"`
	ResultQueueName        string `json:"result_queue_name,omitempty"`
	TransportCorrelationID string `json:"transport_correlation_id,omitempty"`
}

// InternalRequest is the internal envelope: routing data plus a concrete Request.
// It is used on channels and for persistence; JSON uses a tagged envelope.
type InternalRequest struct {
	InternalRouting `json:"-"`
	PublicRequest Request
}

// NewInternalRequest returns an InternalRequest with a non-nil PublicRequest.
// routing fields may be zero; PublicRequest must be non-nil.
func NewInternalRequest(routing InternalRouting, typedReq Request) *InternalRequest {
	return &InternalRequest{InternalRouting: routing, PublicRequest: typedReq}
}

// --- JSON wire format (Redis, etc.) ---

const (
	requestKindPlain  = "plain"
	requestKindRedis  = "redis"
	requestKindPubSub = "pubsub"
)

type internalRequestWire struct {
	Internal    InternalRouting   `json:"internal"`
	RequestKind string            `json:"request_kind"`
	Data        json.RawMessage   `json:"data"`
}

// MarshalJSON encodes InternalRequest as a tagged envelope so the concrete
// Request type round-trips.
func (r *InternalRequest) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	if r.PublicRequest == nil {
		return nil, fmt.Errorf("api: InternalRequest.PublicRequest is nil")
	}
	var data []byte
	var err error
	var kind string
	switch s := r.PublicRequest.(type) {
	case *RequestMessage:
		kind = requestKindPlain
		data, err = json.Marshal(s)
	case *RedisRequest:
		kind = requestKindRedis
		data, err = json.Marshal(s)
	case *PubSubRequest:
		kind = requestKindPubSub
		data, err = json.Marshal(s)
	default:
		return nil, fmt.Errorf("api: unsupported PublicRequest type %T", r.PublicRequest)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(internalRequestWire{
		Internal:    r.InternalRouting,
		RequestKind: kind,
		Data:        data,
	})
}

// UnmarshalJSON decodes the tagged envelope.
func (r *InternalRequest) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	var w internalRequestWire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	if w.RequestKind == "" {
		return fmt.Errorf("api: missing required field request_kind")
	}
	r.InternalRouting = w.Internal
	if len(w.Data) == 0 {
		return fmt.Errorf("api: internal request data is empty")
	}
	switch w.RequestKind {
	case requestKindPlain:
		var p RequestMessage
		if err := json.Unmarshal(w.Data, &p); err != nil {
			return err
		}
		r.PublicRequest = &p
		return nil
	case requestKindRedis:
		var x RedisRequest
		if err := json.Unmarshal(w.Data, &x); err != nil {
			return err
		}
		r.PublicRequest = &x
		return nil
	case requestKindPubSub:
		var x PubSubRequest
		if err := json.Unmarshal(w.Data, &x); err != nil {
			return err
		}
		r.PublicRequest = &x
		return nil
	default:
		return fmt.Errorf("api: unknown request_kind %q", w.RequestKind)
	}
}
