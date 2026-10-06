package api

import (
	"encoding/json"
	"fmt"
)

// SplitPayload serializes ir without its payload and returns the payload separately.
func SplitPayload(ir *InternalRequest) (envelope []byte, payload json.RawMessage, err error) {
	if ir == nil {
		return nil, nil, fmt.Errorf("api: split payload: request is nil")
	}
	msg, err := requestMessageOf(ir.PublicRequest)
	if err != nil {
		return nil, nil, err
	}
	payload = msg.Payload
	msg.Payload = nil
	envelope, err = json.Marshal(ir)
	msg.Payload = payload
	if err != nil {
		return nil, nil, fmt.Errorf("api: split payload: %w", err)
	}
	return envelope, payload, nil
}

// AttachPayload sets the payload of an already decoded request.
func AttachPayload(ir *InternalRequest, payload json.RawMessage) error {
	if ir == nil {
		return fmt.Errorf("api: attach payload: request is nil")
	}
	msg, err := requestMessageOf(ir.PublicRequest)
	if err != nil {
		return err
	}
	msg.Payload = payload
	return nil
}

func requestMessageOf(r Request) (*RequestMessage, error) {
	switch m := r.(type) {
	case *RequestMessage:
		if m != nil {
			return m, nil
		}
	case *RedisRequest:
		if m != nil {
			return &m.RequestMessage, nil
		}
	case *PubSubRequest:
		if m != nil {
			return &m.RequestMessage, nil
		}
	default:
		if r != nil {
			return nil, fmt.Errorf("api: unsupported PublicRequest type %T", r)
		}
	}
	return nil, fmt.Errorf("api: PublicRequest is nil")
}
