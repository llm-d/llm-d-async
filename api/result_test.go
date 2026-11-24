package api

import (
	"bytes"
	"encoding/json"
	"maps"
	"testing"
)

func TestNewResponseResult(t *testing.T) {
	req := &RequestMessage{ID: "speech-1", Metadata: map[string]string{"tenant": "a"}}
	routing := InternalRouting{RequestToken: "gen-1"}
	audio := []byte("RIFF\x2e\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00\xc0\x5d\x00\x00\x80\xbb\x00\x00\x02\x00\x10\x00data\x0a\x00\x00\x00\x00\x00\xff\x7f\x00\x80\xd2\x04\xff\xff")

	tests := []struct {
		name         string
		resp         InferenceResponse
		wantEncoding PayloadEncoding
	}{
		{
			name: "json body stays inline",
			resp: InferenceResponse{StatusCode: 200, Body: []byte(`{"text":"héllo"}`), ContentType: "application/json"},
		},
		{
			name: "audio body is base64 encoded",
			resp: InferenceResponse{
				StatusCode:  200,
				Body:        audio,
				ContentType: "audio/wav",
				Headers:     map[string]string{"x-vllm-omni-output-tokens": "77", "x-audio-sample-rate": "24000"},
			},
			wantEncoding: PayloadEncodingBase64,
		},
		{
			name: "all-NUL body is valid UTF-8 and stays inline",
			resp: InferenceResponse{StatusCode: 200, Body: make([]byte, 64), ContentType: "audio/pcm"},
		},
		{
			name: "empty body",
			resp: InferenceResponse{StatusCode: 204},
		},
		{
			name:         "error status with binary body",
			resp:         InferenceResponse{StatusCode: 502, Body: []byte{0xff, 0xfe, 0x00}},
			wantEncoding: PayloadEncodingBase64,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := NewResponseResult(req, routing, tc.resp)
			if msg.ID != "speech-1" || msg.StatusCode != tc.resp.StatusCode || msg.Routing.RequestToken != routing.RequestToken || msg.Metadata["tenant"] != "a" {
				t.Fatalf("result lost request identity: %+v", msg)
			}
			if msg.PayloadEncoding != tc.wantEncoding {
				t.Errorf("PayloadEncoding = %q, want %q", msg.PayloadEncoding, tc.wantEncoding)
			}

			wire, err := json.Marshal(InternalResult{ResultMessage: msg, RequestToken: routing.RequestToken})
			if err != nil {
				t.Fatal(err)
			}
			var got InternalResult
			if err := json.Unmarshal(wire, &got); err != nil {
				t.Fatal(err)
			}
			body, err := got.Body()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(body, tc.resp.Body) {
				t.Errorf("body changed across the wire:\n got %x\nwant %x", body, tc.resp.Body)
			}
			if got.ContentType != tc.resp.ContentType {
				t.Errorf("ContentType = %q, want %q", got.ContentType, tc.resp.ContentType)
			}
			if !maps.Equal(got.Headers, tc.resp.Headers) {
				t.Errorf("Headers = %v, want %v", got.Headers, tc.resp.Headers)
			}
		})
	}
}

func TestResultMessageBody(t *testing.T) {
	tests := []struct {
		name    string
		msg     ResultMessage
		want    []byte
		wantErr bool
	}{
		{name: "raw", msg: ResultMessage{Payload: `{"a":1}`}, want: []byte(`{"a":1}`)},
		{name: "base64", msg: ResultMessage{Payload: "/wA=", PayloadEncoding: PayloadEncodingBase64}, want: []byte{0xff, 0x00}},
		{name: "malformed base64", msg: ResultMessage{Payload: "!!", PayloadEncoding: PayloadEncodingBase64}, wantErr: true},
		{name: "unknown encoding", msg: ResultMessage{Payload: "x", PayloadEncoding: "gzip"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.msg.Body()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Body() = %x, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("Body() = %x, want %x", got, tc.want)
			}
		})
	}
}

func TestResultMessageOmitsUnsetBinaryFields(t *testing.T) {
	wire, err := json.Marshal(NewHTTPResult(&RequestMessage{ID: "r"}, InternalRouting{}, 200, []byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(wire, &fields); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"payload_encoding", "content_type", "headers"} {
		if _, ok := fields[k]; ok {
			t.Errorf("text result carries %q: %s", k, wire)
		}
	}
}
