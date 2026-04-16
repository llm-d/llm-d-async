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

// TODO: Consider per-message metadata map[string]string
// add endpoint to message level.
type RequestMessage struct {
	Id              string            `json:"id"`
	CreatedUnixSec  string            `json:"created"`               // Unix seconds
	RetryCount      int               `json:"retry_count,omitempty"` // TODO: Consider
	DeadlineUnixSec string            `json:"deadline"`              // TODO: check about using int64, change name to timeout
	Payload         map[string]any    `json:"payload"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

type RequestChannel struct {
	Channel            chan RequestMessage
	IGWBaseURl         string
	InferenceObjective string
	RequestPathURL     string
	Gate               DispatchGate // Dispatch gate for this channel, nil defaults to always-open
}

type EmbelishedRequestChannel struct {
	Channel chan EmbelishedRequestMessage
}

type EmbelishedRequestMessage struct {
	RequestMessage
	HttpHeaders map[string]string
	RequestURL  string
	Metadata    map[string]string
}

type RetryMessage struct {
	EmbelishedRequestMessage
	BackoffDurationSeconds float64
}

// optional field of httpstatus, golang error?
type ResultMessage struct {
	Id       string            `json:"id"`
	Payload  string            `json:"payload"`
	Metadata map[string]string `json:"-"`
}
