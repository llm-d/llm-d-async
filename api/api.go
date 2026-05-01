package api

import (
	"context"
)

type Flow interface {

	// Characteristic of the impl
	Characteristics() Characteristics

	// starts processing requests.
	Start(ctx context.Context)

	// returns the channels for requests. Implementation is responsible for publishing on these channels.
	RequestChannels() []RequestChannel
	// returns the channel that accepts messages to be retries with their backoff delay. Implementation is responsible
	// for consuming messages on this channel.
	RetryChannel() chan RetryMessage
	// returns the channel for storing the results. Implementation is responsible for consuming messages on this channel.
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
// This allows any function with the signature func(context.Context) float64
// to be used as a DispatchGate.
type DispatchGateFunc func(context.Context) float64

// Budget implements DispatchGate by calling the function itself.
func (f DispatchGateFunc) Budget(ctx context.Context) float64 {
	return f(ctx)
}

func ConstOpenGate() DispatchGate {
	return DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
}

type RequestMergePolicy interface {
	MergeRequestChannels(channels []RequestChannel) EmbelishedRequestChannel
}

// Request is the public interface for submitting requests to the async queue.
// It exposes only the caller-visible fields. Concrete types like RequestMessage,
// RedisRequest, and PubSubRequest satisfy this interface.
type Request interface {
	ReqID() string
	ReqCreated() int64
	ReqDeadline() int64
	ReqPayload() map[string]any
	ReqMetadata() map[string]string
}

// RequestMessage contains the caller-visible fields of a request. Metadata is reserved
// for opaque, caller-supplied pass-through data (e.g. tracing IDs, user labels).
// The system does not read or write Metadata for its own routing or correlation.
// Request interface accessors use the Req prefix (e.g. ReqPayload) to avoid
// colliding with the struct's exported field names used for JSON serialization.
type RequestMessage struct {
	ID       string            `json:"id"`
	Created  int64             `json:"created"`  // Unix seconds
	Deadline int64             `json:"deadline"` // Unix seconds
	Payload  map[string]any    `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (r *RequestMessage) ReqID() string                  { return r.ID }
func (r *RequestMessage) ReqCreated() int64               { return r.Created }
func (r *RequestMessage) ReqDeadline() int64       { return r.Deadline }
func (r *RequestMessage) ReqPayload() map[string]any      { return r.Payload }
func (r *RequestMessage) ReqMetadata() map[string]string   { return r.Metadata }

// RedisRequest is the concrete Request implementation for Redis-based flows.
// Per-message queue fields here override producer defaults; producers merge them
// into InternalRouting on InternalRequest before enqueue.
type RedisRequest struct {
	RequestMessage
	RequestQueueName string `json:"request_queue_name,omitempty"`
	ResultQueueName  string `json:"result_queue_name,omitempty"`
}

// PubSubRequest is the concrete Request implementation for GCP Pub/Sub flows.
// Optional PubSubID is merged into InternalRouting.TransportCorrelationID in producers.
type PubSubRequest struct {
	RequestMessage
	PubSubID string `json:"pubsub_id,omitempty"`
}

var (
	_ Request = (*RequestMessage)(nil)
	_ Request = (*RedisRequest)(nil)
	_ Request = (*PubSubRequest)(nil)
)

type RequestChannel struct {
	Channel            chan *InternalRequest
	IGWBaseURl         string
	InferenceObjective string
	RequestPathURL     string
	Gate               DispatchGate // Dispatch gate for this channel, nil defaults to always-open
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

// ResultMessage is the async inference result returned to callers. ID and Payload are
// JSON fields; Routing and Metadata are infrastructure pass-through (json:"-").
type ResultMessage struct {
	ID       string            `json:"id"`
	Payload  string            `json:"payload"`
	Routing  InternalRouting   `json:"-"`
	Metadata map[string]string `json:"-"`
}
