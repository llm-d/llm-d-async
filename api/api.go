package api

import (
	"context"
	"encoding/json"
)

// Request is the public interface for submitting requests to the async queue.
// It exposes only the caller-visible fields. Concrete types like RequestMessage,
// RedisRequest, and PubSubRequest satisfy this interface.
type Request interface {
	ReqID() string
	ReqCreated() int64
	ReqDeadline() int64
	ReqPayload() json.RawMessage
	ReqMetadata() map[string]string
	ReqHeaders() map[string]string
	ReqEndpoint() string
	ReqModel() string
}

// FairnessIDHeader is the request header llm-d-router's flow control reads to
// arbitrate fairness between flows. The processor stamps it with the same tenant
// attribute it keys quota on, replacing any caller-supplied value, so a tenant is
// arbitrated under the identity it is accounted under. Messages carrying no such
// attribute reach the gateway with whatever the caller sent.
const FairnessIDHeader = "x-llm-d-inference-fairness-id"

// ObjectiveHeader is the request header llm-d-router reads to resolve a
// request's InferenceObjective. The tier-priority merge policy stamps it with
// the per-lane objective when lane_objectives is configured.
const ObjectiveHeader = "x-llm-d-inference-objective"

// RequestMessage contains the caller-visible fields of a request. Metadata is
// caller-supplied pass-through data (e.g. tracing IDs, user labels). The system
// never writes Metadata, and reads it only where an opt-in feature is configured
// to key on a named attribute (the redis-quota gate, request body transforms,
// and merge-policy fairness stamping); it is otherwise opaque.
// Request interface accessors use the Req prefix (e.g. ReqPayload) to avoid
// colliding with the struct's exported field names used for JSON serialization.
type RequestMessage struct {
	ID       string            `json:"id"`
	Created  int64             `json:"created"`  // Unix seconds
	Deadline int64             `json:"deadline"` // Unix seconds
	Payload  json.RawMessage   `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
	// Model is empty when the producer does not set it.
	Model string `json:"model,omitempty"`
}

func (r *RequestMessage) ReqID() string                  { return r.ID }
func (r *RequestMessage) ReqCreated() int64              { return r.Created }
func (r *RequestMessage) ReqDeadline() int64             { return r.Deadline }
func (r *RequestMessage) ReqPayload() json.RawMessage    { return r.Payload }
func (r *RequestMessage) ReqMetadata() map[string]string { return r.Metadata }
func (r *RequestMessage) ReqHeaders() map[string]string  { return r.Headers }
func (r *RequestMessage) ReqEndpoint() string            { return r.Endpoint }
func (r *RequestMessage) ReqModel() string               { return r.Model }

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

// ResultMessage is the async inference result returned to callers.
//
// Wire-format semantics:
//   - StatusCode > 0: an HTTP response was received. Payload contains the response body.
//   - StatusCode == 0: no HTTP response. ErrorCode/ErrorMessage describe the failure.
//
// Routing and Metadata are infrastructure pass-through (json:"-").
type ResultMessage struct {
	ID           string            `json:"id"`
	StatusCode   int               `json:"status_code,omitempty"`
	Payload      string            `json:"payload"`
	ErrorCode    string            `json:"error_code,omitempty"`
	ErrorMessage string            `json:"error_message,omitempty"`
	Routing      InternalRouting   `json:"-"`
	Metadata     map[string]string `json:"-"`
}

// Error codes for non-HTTP failures surfaced in ResultMessage.ErrorCode.
// These are result-level codes describing why a request could not be completed.
const (
	ErrCodeDeadlineExceeded = "DEADLINE_EXCEEDED"
	ErrCodeCancelled        = "CANCELLED"
	ErrCodeGateDropped      = "GATE_DROPPED"
	ErrCodeGateError        = "GATE_ERROR"
	ErrCodeInferenceError   = "INFERENCE_ERROR"
	ErrCodeInvalidRequest   = "INVALID_REQUEST"
)

// CancellationChecker reports whether a specific request generation has been cancelled.
type CancellationChecker interface {
	IsCancelled(ctx context.Context, requestID, requestToken string) (bool, error)
}

// RequestCancellationKey returns the Redis key used for per-request cancellation markers.
func RequestCancellationKey(requestID string) string {
	return "request-cancel:" + requestID
}

// RequestActiveTokenKey returns the Redis key tracking the currently active request generation.
func RequestActiveTokenKey(requestID string) string {
	return "request-active:" + requestID
}

// RequestPayloadKey returns the Redis key holding the payload of one request generation.
func RequestPayloadKey(requestID, requestToken string) string {
	return "request-payload:" + requestID + ":" + requestToken
}

// NewErrorResult builds a non-HTTP error ResultMessage.
// errorCode must be one of the ErrCode* constants; errMsg is a human-readable description.
// Payload is populated with a JSON error object for backward compatibility with
// consumers that only read Payload.
func NewErrorResult(req Request, routing InternalRouting, errorCode, errMsg string) ResultMessage {
	errorPayload := map[string]string{"error": errMsg}
	payloadBytes, err := json.Marshal(errorPayload)
	if err != nil {
		payloadBytes = []byte(`{"error": "internal error"}`)
	}
	return ResultMessage{
		ID:           req.ReqID(),
		Payload:      string(payloadBytes),
		ErrorCode:    errorCode,
		ErrorMessage: errMsg,
		Routing:      routing,
		Metadata:     req.ReqMetadata(),
	}
}

// NewHTTPResult builds a ResultMessage for any HTTP response (success or error).
// statusCode is the actual HTTP status code; responseBody is the raw body.
func NewHTTPResult(req Request, routing InternalRouting, statusCode int, responseBody []byte) ResultMessage {
	return ResultMessage{
		ID:         req.ReqID(),
		StatusCode: statusCode,
		Payload:    string(responseBody),
		Routing:    routing,
		Metadata:   req.ReqMetadata(),
	}
}

// NewGateDroppedResult builds a ResultMessage for a gate-dropped request.
func NewGateDroppedResult(req Request, routing InternalRouting) ResultMessage {
	return NewErrorResult(req, routing, ErrCodeGateDropped, "Pool gating dropped request")
}

// NewDeadlineExceededResult builds a ResultMessage for deadline expiry.
func NewDeadlineExceededResult(req Request, routing InternalRouting) ResultMessage {
	return NewErrorResult(req, routing, ErrCodeDeadlineExceeded, "deadline exceeded")
}

// NewCancelledResult builds a ResultMessage for a cancelled request.
func NewCancelledResult(req Request, routing InternalRouting) ResultMessage {
	return NewErrorResult(req, routing, ErrCodeCancelled, "cancelled")
}
