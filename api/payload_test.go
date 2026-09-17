package api

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

type unsupportedRequest struct{ RequestMessage }

func splitFixtures() map[string]*InternalRequest {
	msg := RequestMessage{
		ID:       "req-1",
		Created:  1700000000,
		Deadline: 1700003600,
		Payload:  json.RawMessage(`{"model":"m","prompt":"a long prompt"}`),
		Metadata: map[string]string{"tenant": "t"},
		Headers:  map[string]string{"x-h": "v"},
		Endpoint: "/v1/chat/completions",
		Model:    "m",
	}
	routing := InternalRouting{RequestQueueName: "requests", ResultQueueName: "results"}
	return map[string]*InternalRequest{
		"plain":  NewInternalRequest(routing, &msg),
		"redis":  NewInternalRequest(routing, &RedisRequest{RequestMessage: msg, RequestQueueName: "rq", ResultQueueName: "res"}),
		"pubsub": NewInternalRequest(routing, &PubSubRequest{RequestMessage: msg, PubSubID: "ps-1"}),
	}
}

func TestSplitJoinPayload_RoundTripsEveryRequestKind(t *testing.T) {
	for name, ir := range splitFixtures() {
		t.Run(name, func(t *testing.T) {
			ir.RequestToken = "gen-1"
			envelope, payload, err := SplitPayload(ir)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, ir.PublicRequest.ReqPayload()) {
				t.Fatalf("payload = %s, want %s", payload, ir.PublicRequest.ReqPayload())
			}
			if strings.Contains(string(envelope), "a long prompt") {
				t.Fatalf("envelope carries the payload: %s", envelope)
			}

			var got InternalRequest
			if err := JoinPayload(envelope, payload, &got); err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(ir)
			if err != nil {
				t.Fatal(err)
			}
			have, err := json.Marshal(&got)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(want, have) {
				t.Fatalf("round trip changed the request:\nwant %s\nhave %s", want, have)
			}
			if got.RequestToken != "gen-1" {
				t.Fatalf("request token = %q", got.RequestToken)
			}
		})
	}
}

func TestSplitPayload_LeavesTheRequestUntouched(t *testing.T) {
	ir := splitFixtures()["redis"]
	before := string(ir.PublicRequest.ReqPayload())
	if _, _, err := SplitPayload(ir); err != nil {
		t.Fatal(err)
	}
	if got := string(ir.PublicRequest.ReqPayload()); got != before {
		t.Fatalf("payload changed to %q", got)
	}
}

func TestJoinPayload_AttachesPayloadBytesAsGiven(t *testing.T) {
	ir := splitFixtures()["plain"]
	envelope, _, err := SplitPayload(ir)
	if err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{ "b":2,  "a":1 }`)
	var got InternalRequest
	if err := JoinPayload(envelope, payload, &got); err != nil {
		t.Fatal(err)
	}
	if &got.PublicRequest.ReqPayload()[0] != &payload[0] {
		t.Fatal("payload was copied")
	}
}

func TestSplitPayload_Errors(t *testing.T) {
	if _, _, err := SplitPayload(nil); err == nil {
		t.Fatal("nil request: want error")
	}
	if _, _, err := SplitPayload(&InternalRequest{}); err == nil {
		t.Fatal("nil public request: want error")
	}
	if _, _, err := SplitPayload(NewInternalRequest(InternalRouting{}, &unsupportedRequest{})); err == nil {
		t.Fatal("unsupported request type: want error")
	}
}

func TestJoinPayload_RejectsBadEnvelopes(t *testing.T) {
	for name, envelope := range map[string]string{
		"not json":         `#lda1:3:{}`,
		"missing kind":     `{"internal":{},"data":{"id":"x"}}`,
		"unknown kind":     `{"internal":{},"request_kind":"nope","data":{"id":"x"}}`,
		"truncated object": `{"internal":{},"request_kind":"plain","data":{"id":"x"`,
	} {
		t.Run(name, func(t *testing.T) {
			var ir InternalRequest
			if err := JoinPayload([]byte(envelope), json.RawMessage(`{}`), &ir); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestAttachPayload(t *testing.T) {
	for name, ir := range splitFixtures() {
		t.Run(name, func(t *testing.T) {
			payload := json.RawMessage(`{"replaced":true}`)
			if err := AttachPayload(ir, payload); err != nil {
				t.Fatal(err)
			}
			if string(ir.PublicRequest.ReqPayload()) != string(payload) {
				t.Fatalf("payload = %s", ir.PublicRequest.ReqPayload())
			}
		})
	}
	if err := AttachPayload(nil, nil); err == nil {
		t.Fatal("nil request: want error")
	}
	if err := AttachPayload(&InternalRequest{}, nil); err == nil {
		t.Fatal("nil public request: want error")
	}
	if err := AttachPayload(NewInternalRequest(InternalRouting{}, &unsupportedRequest{}), nil); err == nil {
		t.Fatal("unsupported request type: want error")
	}
}
