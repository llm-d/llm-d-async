package api

import (
	"encoding/json"
	"fmt"
)

// SplitPayload serializes ir without its payload and returns the payload
// separately, so a transport can store the two apart and read the envelope
// without touching the payload bytes.
func SplitPayload(ir *InternalRequest) (envelope []byte, payload json.RawMessage, err error) {
	if ir == nil || ir.PublicRequest == nil {
		return nil, nil, fmt.Errorf("api: split payload: request is nil")
	}
	header, payload, err := withoutPayload(ir.PublicRequest)
	if err != nil {
		return nil, nil, err
	}
	envelope, err = json.Marshal(&InternalRequest{InternalRouting: ir.InternalRouting, PublicRequest: header})
	if err != nil {
		return nil, nil, fmt.Errorf("api: split payload: %w", err)
	}
	return envelope, payload, nil
}

// JoinPayload decodes an envelope written by SplitPayload into ir and attaches
// payload. The payload is not copied or validated.
func JoinPayload(envelope []byte, payload json.RawMessage, ir *InternalRequest) error {
	if err := json.Unmarshal(envelope, ir); err != nil {
		return fmt.Errorf("api: join payload: %w", err)
	}
	return AttachPayload(ir, payload)
}

// AttachPayload sets the payload of an already decoded request. The payload is
// not copied or validated.
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

func withoutPayload(r Request) (Request, json.RawMessage, error) {
	switch m := r.(type) {
	case *RequestMessage:
		c := *m
		c.Payload = nil
		return &c, m.Payload, nil
	case *RedisRequest:
		c := *m
		c.Payload = nil
		return &c, m.Payload, nil
	case *PubSubRequest:
		c := *m
		c.Payload = nil
		return &c, m.Payload, nil
	}
	return nil, nil, fmt.Errorf("api: unsupported PublicRequest type %T", r)
}

func requestMessageOf(r Request) (*RequestMessage, error) {
	switch m := r.(type) {
	case *RequestMessage:
		return m, nil
	case *RedisRequest:
		return &m.RequestMessage, nil
	case *PubSubRequest:
		return &m.RequestMessage, nil
	}
	return nil, fmt.Errorf("api: unsupported PublicRequest type %T", r)
}
