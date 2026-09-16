package api

import (
	"encoding/json"
	"testing"
)

func TestInternalResultWireCompatibility(t *testing.T) {
	wire := InternalResult{
		ResultMessage: ResultMessage{
			ID:           "result-1",
			StatusCode:   503,
			Payload:      `{"error":"unavailable"}`,
			ErrorCode:    ErrCodeInferenceError,
			ErrorMessage: "backend unavailable",
			Routing:      InternalRouting{RequestQueueName: "must-not-leak"},
			Metadata:     map[string]string{"must": "not leak"},
		},
		RequestToken: "generation-1",
	}

	data, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("marshal InternalResult: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("unmarshal result fields: %v", err)
	}
	for _, field := range []string{"id", "status_code", "payload", "error_code", "error_message", "request_token"} {
		if _, ok := fields[field]; !ok {
			t.Errorf("missing top-level field %q in %s", field, data)
		}
	}
	if _, ok := fields["routing"]; ok {
		t.Errorf("routing leaked into result wire: %s", data)
	}
	if _, ok := fields["metadata"]; ok {
		t.Errorf("metadata leaked into result wire: %s", data)
	}

	// A legacy consumer still sees the unchanged ResultMessage fields and
	// safely ignores the additive request_token field.
	var legacy ResultMessage
	if err := json.Unmarshal(data, &legacy); err != nil {
		t.Fatalf("legacy unmarshal: %v", err)
	}
	if legacy.ID != wire.ID || legacy.StatusCode != wire.StatusCode || legacy.Payload != wire.Payload {
		t.Fatalf("legacy result mismatch: got %#v want %#v", legacy, wire.ResultMessage)
	}

	// A new reader must also accept old records without generation metadata.
	var oldRecord InternalResult
	if err := json.Unmarshal([]byte(`{"id":"legacy","payload":"done"}`), &oldRecord); err != nil {
		t.Fatalf("old record unmarshal: %v", err)
	}
	if oldRecord.ID != "legacy" || oldRecord.RequestToken != "" {
		t.Fatalf("old record mismatch: %#v", oldRecord)
	}
}

func TestRoundTrip_PlainRequestMessage(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{RetryCount: 2, RequestQueueName: "rq", ResultQueueName: "resq", ResultTTLSeconds: 60, ResultRoutingResolved: true},
		&RequestMessage{
			ID: "plain-1", Created: 1000, Deadline: 2000,
			Payload:  testPayload(map[string]any{"model": "m1"}),
			Metadata: map[string]string{"k": "v"},
			Model:    "m1",
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
	if string(rm.Payload) != `{"model":"m1"}` {
		t.Errorf("payload mismatch: %s", rm.Payload)
	}
	if rm.Metadata["k"] != "v" {
		t.Errorf("metadata mismatch: %v", rm.Metadata)
	}
	if rm.Model != "m1" {
		t.Errorf("model mismatch: %q", rm.Model)
	}
}

func TestUnmarshal_EnvelopeWithoutModel(t *testing.T) {
	b := []byte(`{"internal":{},"request_kind":"plain","data":{"id":"a","created":1,"deadline":2,"payload":{"model":"m"}}}`)
	var got InternalRequest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m := got.PublicRequest.ReqModel(); m != "" {
		t.Errorf("ReqModel() = %q, want empty for an envelope written without the field", m)
	}
}

func TestRoundTrip_RedisRequest(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{RetryCount: 1, RequestQueueName: "rq", ResultQueueName: "resq", TransportCorrelationID: "tc", EnqueueSeq: 42},
		&RedisRequest{
			RequestMessage:   RequestMessage{ID: "redis-1", Created: 100, Deadline: 200, Payload: testPayload(map[string]any{"p": 1})},
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
			RequestMessage:  RequestMessage{ID: "ps-1", Created: 10, Deadline: 20},
			PubSubID:        "pub-abc",
			ResultQueueName: "per-msg-results",
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
	if ps.ID != "ps-1" || ps.PubSubID != "pub-abc" || ps.ResultQueueName != "per-msg-results" {
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
		&RequestMessage{ID: "iface-test", Created: 1, Deadline: 2, Payload: testPayload(map[string]any{"k": "v"}), Metadata: map[string]string{"m": "d"}},
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
	if string(r.ReqPayload()) != `{"k":"v"}` {
		t.Errorf("ReqPayload() = %s", r.ReqPayload())
	}
	if r.ReqMetadata()["m"] != "d" {
		t.Errorf("ReqMetadata() = %v", r.ReqMetadata())
	}
}

func TestRoundTrip_EndpointField(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{RequestQueueName: "rq"},
		&RequestMessage{
			ID: "ep-test", Created: 1, Deadline: 2,
			Payload:  testPayload(map[string]any{"model": "m"}),
			Endpoint: "/v1/custom",
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

	rm, ok := got.PublicRequest.(*RequestMessage)
	if !ok {
		t.Fatalf("expected *RequestMessage, got %T", got.PublicRequest)
	}
	if rm.Endpoint != "/v1/custom" {
		t.Errorf("Endpoint = %q, want /v1/custom", rm.Endpoint)
	}
	if rm.ReqEndpoint() != "/v1/custom" {
		t.Errorf("ReqEndpoint() = %q, want /v1/custom", rm.ReqEndpoint())
	}
}

func TestRoundTrip_EndpointOmittedWhenEmpty(t *testing.T) {
	ir := NewInternalRequest(
		InternalRouting{},
		&RequestMessage{ID: "no-ep", Created: 1, Deadline: 2, Payload: testPayload(map[string]any{})},
	)
	b, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != "" && json.Valid(b) {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatalf("unmarshal envelope: %v", err)
		}
		var data map[string]json.RawMessage
		if err := json.Unmarshal(raw["data"], &data); err != nil {
			t.Fatalf("unmarshal data: %v", err)
		}
		if _, exists := data["endpoint"]; exists {
			t.Error("endpoint should be omitted from JSON when empty")
		}
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
	if got.RequestToken != want.RequestToken {
		t.Errorf("RequestToken = %q, want %q", got.RequestToken, want.RequestToken)
	}
	if got.ResultQueueName != want.ResultQueueName {
		t.Errorf("ResultQueueName = %q, want %q", got.ResultQueueName, want.ResultQueueName)
	}
	if got.ResultTTLSeconds != want.ResultTTLSeconds {
		t.Errorf("ResultTTLSeconds = %d, want %d", got.ResultTTLSeconds, want.ResultTTLSeconds)
	}
	if got.ResultRoutingResolved != want.ResultRoutingResolved {
		t.Errorf("ResultRoutingResolved = %v, want %v", got.ResultRoutingResolved, want.ResultRoutingResolved)
	}
	if got.TransportCorrelationID != want.TransportCorrelationID {
		t.Errorf("TransportCorrelationID = %q, want %q", got.TransportCorrelationID, want.TransportCorrelationID)
	}
	if got.EnqueueSeq != want.EnqueueSeq {
		t.Errorf("EnqueueSeq = %d, want %d", got.EnqueueSeq, want.EnqueueSeq)
	}
}

func TestQueueScore(t *testing.T) {
	const d = int64(1_790_000_000)
	const maxDeadline = int64(1<<32 - 1)
	type at struct{ deadline, seq int64 }

	tests := []struct {
		name        string
		lower, high at
		tie         bool
	}{
		{name: "unstamped sorts ahead of the first sequence", lower: at{d, 0}, high: at{d, 1}},
		{name: "negative sequence scores as unstamped", lower: at{d, -1}, high: at{d, 0}, tie: true},
		{name: "adjacent sequences order", lower: at{d, 1}, high: at{d, 2}},
		{name: "last distinct sequence stays below the cap", lower: at{d, maxQueueScoreSeq - 1}, high: at{d, maxQueueScoreSeq}},
		{name: "sequences past the cap tie", lower: at{d, maxQueueScoreSeq}, high: at{d, maxQueueScoreSeq + 1000}, tie: true},
		{name: "capped sequence stays below the next deadline", lower: at{d, 1 << 40}, high: at{d + 1, 0}},
		{name: "adjacent sequences stay distinct at the largest 32-bit deadline", lower: at{maxDeadline, maxQueueScoreSeq - 1}, high: at{maxDeadline, maxQueueScoreSeq}},
		{name: "capped sequence stays below the next deadline at the largest 32-bit deadline", lower: at{maxDeadline - 1, maxQueueScoreSeq}, high: at{maxDeadline, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lo := queueScore(tt.lower.deadline, tt.lower.seq)
			hi := queueScore(tt.high.deadline, tt.high.seq)
			if tt.tie && lo != hi {
				t.Errorf("scores %v and %v, want equal", lo, hi)
			}
			if !tt.tie && lo >= hi {
				t.Errorf("scores %v and %v, want strictly increasing", lo, hi)
			}
		})
	}

	t.Run("method scores the envelope's deadline and sequence", func(t *testing.T) {
		ir := NewInternalRequest(InternalRouting{EnqueueSeq: 7}, &RequestMessage{Deadline: d})
		if got, want := ir.QueueScore(), queueScore(d, 7); got != want {
			t.Errorf("QueueScore() = %v, want %v", got, want)
		}
		if got := (&InternalRequest{}).QueueScore(); got != 0 {
			t.Errorf("QueueScore() without a PublicRequest = %v, want 0", got)
		}
	})

	t.Run("every sequence is distinct and below the next deadline", func(t *testing.T) {
		for _, deadline := range []int64{d, maxDeadline} {
			prev := queueScore(deadline, 0)
			for seq := int64(1); seq <= maxQueueScoreSeq; seq++ {
				got := queueScore(deadline, seq)
				if got <= prev || got >= float64(deadline+1) {
					t.Fatalf("queueScore(%d, %d) = %v after %v, want increasing and below %d", deadline, seq, got, prev, deadline+1)
				}
				prev = got
			}
		}
	})
}

func testPayload(m map[string]any) json.RawMessage {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}
