package api

import (
	"encoding/json"
	"fmt"
	"time"
)

type QuotaClassification string

const (
	ClassificationNone     QuotaClassification = ""
	ClassificationReserved QuotaClassification = "reserved"
	ClassificationOverflow QuotaClassification = "overflow"
)

const LabelClassification = "classification"

// InternalRouting holds the resolved, authoritative routing fields used by
// infrastructure (producers, workers, retry logic). These are not part of the
// caller-facing contract and should not be set by callers directly.
// Producers merge caller-supplied per-message overrides (e.g.
// RedisRequest.RequestQueueName) with producer defaults and write the final
// values here. All internal pipeline code reads routing exclusively from this
// struct rather than reaching back into the typed request.
type InternalRouting struct {
	RetryCount             int    `json:"retry_count,omitempty"`
	QueueID                string `json:"queue_id,omitempty"`
	RequestQueueName       string `json:"request_queue_name,omitempty"`
	ResultQueueName        string `json:"result_queue_name,omitempty"`
	TransportCorrelationID string `json:"transport_correlation_id,omitempty"`
	// Labels is the framework's per-message label set. Seeded by the
	// Flow at pull time from the originating channel's effective
	// policy read and mutate this map in place. Producer-controlled
	// per-message correlation data rides on body.Metadata, not Labels.
	Labels map[string]string `json:"labels,omitempty"`
}

// SetClassification sets the quota classification inside the Labels map.
func (ir *InternalRouting) SetClassification(c QuotaClassification) {
	if ir.Labels == nil {
		ir.Labels = make(map[string]string)
	}
	if c == ClassificationNone {
		delete(ir.Labels, LabelClassification)
	} else {
		ir.Labels[LabelClassification] = string(c)
	}
}

// GetClassification retrieves the quota classification from the Labels map.
func (ir *InternalRouting) GetClassification() QuotaClassification {
	if ir.Labels == nil {
		return ClassificationNone
	}
	return QuotaClassification(ir.Labels[LabelClassification])
}

// InternalRequest is the internal envelope: routing data plus a concrete Request.
// It is used on channels and for persistence; JSON uses a tagged envelope.
type InternalRequest struct {
	InternalRouting `json:"-"`
	PublicRequest   Request
	// IngestionTime records when the message was pulled from the broker into
	// the in-process pipeline. It is in-process only: the custom JSON
	// (un)marshaling does not persist it, so a re-enqueued (retried) message is
	// re-stamped on its next delivery. Zero when not set (e.g. drained messages).
	IngestionTime time.Time `json:"-"`
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
	Internal    InternalRouting `json:"internal"`
	RequestKind string          `json:"request_kind"`
	Data        json.RawMessage `json:"data"`
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
