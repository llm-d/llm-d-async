package api

// Request is the public interface for submitting requests to the async queue.
// It exposes only the caller-visible fields. Concrete types like RequestMessage,
// RedisRequest, and PubSubRequest satisfy this interface.
type Request interface {
	ReqID() string
	ReqCreated() int64
	ReqDeadline() int64
	ReqPayload() map[string]any
	ReqMetadata() map[string]string
	ReqEndpoint() string
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
	Endpoint string            `json:"endpoint,omitempty"`
}

func (r *RequestMessage) ReqID() string                  { return r.ID }
func (r *RequestMessage) ReqCreated() int64              { return r.Created }
func (r *RequestMessage) ReqDeadline() int64             { return r.Deadline }
func (r *RequestMessage) ReqPayload() map[string]any     { return r.Payload }
func (r *RequestMessage) ReqMetadata() map[string]string { return r.Metadata }
func (r *RequestMessage) ReqEndpoint() string            { return r.Endpoint }

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

// ResultMessage is the async inference result returned to callers. ID and Payload are
// JSON fields; Routing and Metadata are infrastructure pass-through (json:"-").
type ResultMessage struct {
	ID       string            `json:"id"`
	Payload  string            `json:"payload"`
	Routing  InternalRouting   `json:"-"`
	Metadata map[string]string `json:"-"`
}
