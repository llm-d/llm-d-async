package api

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip_PlainRequestMessage(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{RetryCount: 2, RequestQueueName: "rq", ResultQueueName: "resq"},
		&RequestMessage{
			ID: "plain-1", Created: 1000, Deadline: 2000,
			Payload:  map[string]any{"model": "m1"},
			Metadata: map[string]string{"k": "v"},
		},
	)
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got InternalRequest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	assertRouting(t, got.InternalRouting, ir.InternalRouting)
	rm, ok := got.PublicRequest.(*RequestMessage)
	if !ok {
		t.Fatalf("expected *RequestMessage, got %T", got.PublicRequest)
	}
	if rm.ID != "plain-1" || rm.Created != 1000 || rm.Deadline != 2000 {
		t.Errorf("field mismatch: %+v", rm)
	}
	if rm.Payload["model"] != "m1" {
		t.Errorf("payload mismatch: %v", rm.Payload)
	}
	if rm.Metadata["k"] != "v" {
		t.Errorf("metadata mismatch: %v", rm.Metadata)
	}
}

func TestRoundTrip_RedisRequest(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{RetryCount: 1, RequestQueueName: "rq", ResultQueueName: "resq", TransportCorrelationID: "tc"},
		&RedisRequest{
			RequestMessage:   RequestMessage{ID: "redis-1", Created: 100, Deadline: 200, Payload: map[string]any{"p": 1}},
			RequestQueueName: "per-msg-rq",
			ResultQueueName:  "per-msg-resq",
		},
	)
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got InternalRequest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	assertRouting(t, got.InternalRouting, ir.InternalRouting)
	rr, ok := got.PublicRequest.(*RedisRequest)
	if !ok {
		t.Fatalf("expected *RedisRequest, got %T", got.PublicRequest)
	}
	if rr.ID != "redis-1" {
		t.Errorf("id mismatch: %s", rr.ID)
	}
	if rr.RequestQueueName != "per-msg-rq" || rr.ResultQueueName != "per-msg-resq" {
		t.Errorf("queue fields mismatch: %+v", rr)
	}
}

func TestRoundTrip_PubSubRequest(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{TransportCorrelationID: "corr-123"},
		&PubSubRequest{
			RequestMessage: RequestMessage{ID: "ps-1", Created: 10, Deadline: 20},
			PubSubID:       "pub-abc",
		},
	)
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got InternalRequest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	assertRouting(t, got.InternalRouting, ir.InternalRouting)
	ps, ok := got.PublicRequest.(*PubSubRequest)
	if !ok {
		t.Fatalf("expected *PubSubRequest, got %T", got.PublicRequest)
	}
	if ps.ID != "ps-1" || ps.PubSubID != "pub-abc" {
		t.Errorf("field mismatch: %+v", ps)
	}
}

func TestUnmarshal_MissingRequestKind(t *testing.T) {
	raw := `{"internal":{},"data":{"id":"x"}}`
	var ir InternalRequest
	err := json.Unmarshal([]byte(raw), &ir)
	if err == nil {
		t.Fatal("expected error for missing request_kind")
	}
}

func TestUnmarshal_Null(t *testing.T) {
	var ir InternalRequest
	if err := json.Unmarshal([]byte("null"), &ir); err != nil {
		t.Fatalf("unmarshal null: %v", err)
	}
	if ir.PublicRequest != nil {
		t.Errorf("expected nil PublicRequest after null unmarshal")
	}
}

func TestUnmarshal_Empty(t *testing.T) {
	var ir InternalRequest
	err := json.Unmarshal([]byte(""), &ir)
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestMarshal_NilPublicRequest(t *testing.T) {
	ir := &InternalRequest{}
	_, err := json.Marshal(ir)
	if err == nil {
		t.Fatal("expected error when PublicRequest is nil")
	}
}

func TestMarshal_NilReceiver(t *testing.T) {
	var ir *InternalRequest
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal nil: %v", err)
	}
	if string(b) != "null" {
		t.Errorf("expected null, got %s", b)
	}
}

func TestUnmarshal_UnknownRequestKind(t *testing.T) {
	raw := `{"internal":{},"request_kind":"alien","data":{"id":"x"}}`
	var ir InternalRequest
	err := json.Unmarshal([]byte(raw), &ir)
	if err == nil {
		t.Fatal("expected error for unknown request_kind")
	}
}

func TestUnmarshal_EmptyData(t *testing.T) {
	raw := `{"internal":{},"request_kind":"plain"}`
	var ir InternalRequest
	err := json.Unmarshal([]byte(raw), &ir)
	if err == nil {
		t.Fatal("expected error for missing data field")
	}
}

func TestRoundTrip_PublicRequestInterface(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{},
		&RequestMessage{ID: "iface-test", Created: 1, Deadline: 2, Payload: map[string]any{"k": "v"}, Metadata: map[string]string{"m": "d"}},
	)
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got InternalRequest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	r := got.PublicRequest
	if r == nil {
		t.Fatal("PublicRequest is nil")
	}
	if r.ReqID() != "iface-test" {
		t.Errorf("ReqID() = %q", r.ReqID())
	}
	if r.ReqCreated() != 1 {
		t.Errorf("ReqCreated() = %d", r.ReqCreated())
	}
	if r.ReqDeadline() != 2 {
		t.Errorf("ReqDeadlineUnixSec() = %d", r.ReqDeadline())
	}
	if r.ReqPayload()["k"] != "v" {
		t.Errorf("ReqPayload() = %v", r.ReqPayload())
	}
	if r.ReqMetadata()["m"] != "d" {
		t.Errorf("ReqMetadata() = %v", r.ReqMetadata())
	}
}

func assertRouting(t *testing.T, got, want InternalRouting) {
	t.Helper()
	if got.RetryCount != want.RetryCount {
		t.Errorf("RetryCount = %d, want %d", got.RetryCount, want.RetryCount)
	}
	if got.RequestQueueName != want.RequestQueueName {
		t.Errorf("RequestQueueName = %q, want %q", got.RequestQueueName, want.RequestQueueName)
	}
	if got.ResultQueueName != want.ResultQueueName {
		t.Errorf("ResultQueueName = %q, want %q", got.ResultQueueName, want.ResultQueueName)
	}
	if got.TransportCorrelationID != want.TransportCorrelationID {
		t.Errorf("TransportCorrelationID = %q, want %q", got.TransportCorrelationID, want.TransportCorrelationID)
	}
}
