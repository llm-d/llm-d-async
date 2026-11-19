package fairness

import (
	"strings"
	"testing"

	"github.com/llm-d/llm-d-async/api"
)

func TestStamp(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		attribute string
		metadata  map[string]string
		// headers is the header map as it reaches Stamp, after the caller's own
		// headers have already been merged in.
		headers map[string]string
		want    map[string]string
	}{
		{
			name:      "stamps the identity under the configured header",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": "tenant-a"},
			want:      map[string]string{api.FairnessIDHeader: "tenant-a"},
		},
		{
			name:      "reads the configured attribute, not the default",
			header:    "x-custom-fairness",
			attribute: "team",
			metadata:  map[string]string{"team": "team-b", "userid": "ignored"},
			want:      map[string]string{"x-custom-fairness": "team-b"},
		},
		{
			name:      "empty attribute falls back to the default",
			header:    api.FairnessIDHeader,
			attribute: "",
			metadata:  map[string]string{DefaultAttribute: "tenant-a"},
			want:      map[string]string{api.FairnessIDHeader: "tenant-a"},
		},
		{
			name:      "empty header disables stamping",
			header:    "",
			attribute: "userid",
			metadata:  map[string]string{"userid": "tenant-a"},
			headers:   map[string]string{api.FairnessIDHeader: "caller-set"},
			want:      map[string]string{api.FairnessIDHeader: "caller-set"},
		},
		{
			name:      "attribute absent from metadata",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"other": "x"},
			want:      map[string]string{},
		},
		{
			name:      "attribute present but empty",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": ""},
			want:      map[string]string{},
		},
		{
			// The quota gate accounts on the metadata attribute, so the gateway
			// has to arbitrate on that same identity; a caller cannot present a
			// different one.
			name:      "identity replaces a caller-supplied header",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": "tenant-a"},
			headers:   map[string]string{api.FairnessIDHeader: "caller-set"},
			want:      map[string]string{api.FairnessIDHeader: "tenant-a"},
		},
		{
			// Both keys would collapse onto one wire header, so the caller's
			// variant is dropped rather than left beside the stamp. A want of
			// exactly one entry is what pins the delete.
			name:      "caller case variant is replaced, not duplicated",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": "tenant-a"},
			headers:   map[string]string{"X-LLM-D-Inference-Fairness-ID": "caller-set"},
			want:      map[string]string{api.FairnessIDHeader: "tenant-a"},
		},
		{
			// net/http rejects control characters at write time, which would fail
			// the whole request rather than just lose the fairness hint. With no
			// identity to assert, the caller's header is left as it was.
			name:      "identity that is not a legal header value is skipped",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": "tenant-a\r\nX-Injected: 1"},
			headers:   map[string]string{api.FairnessIDHeader: "caller-set"},
			want:      map[string]string{api.FairnessIDHeader: "caller-set"},
		},
		{
			// An oversized header is a 431 at the gateway, so skip rather than
			// stamp it.
			name:      "identity longer than the cap is skipped",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": strings.Repeat("a", maxIdentityLen+1)},
			want:      map[string]string{},
		},
		{
			name:      "identity at exactly the cap is stamped",
			header:    api.FairnessIDHeader,
			attribute: "userid",
			metadata:  map[string]string{"userid": strings.Repeat("a", maxIdentityLen)},
			want:      map[string]string{api.FairnessIDHeader: strings.Repeat("a", maxIdentityLen)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := map[string]string{}
			for k, v := range tt.headers {
				headers[k] = v
			}
			req := &api.RequestMessage{Metadata: tt.metadata}

			New(tt.header, tt.attribute).Stamp(headers, req)

			if len(headers) != len(tt.want) {
				t.Fatalf("headers = %v, want %v", headers, tt.want)
			}
			for k, want := range tt.want {
				if got := headers[k]; got != want {
					t.Errorf("headers[%q] = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestZeroStamperIsDisabled(t *testing.T) {
	headers := map[string]string{}
	req := &api.RequestMessage{Metadata: map[string]string{DefaultAttribute: "tenant-a"}}

	Stamper{}.Stamp(headers, req)

	if len(headers) != 0 {
		t.Errorf("zero Stamper wrote %v, want nothing", headers)
	}
}

func TestParamsResolveHeader(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	tests := []struct {
		name    string
		params  Params
		want    string
		wantErr bool
	}{
		{name: "absent parameter uses the default header", params: Params{}, want: api.FairnessIDHeader},
		{name: "explicit empty disables stamping", params: Params{Header: strPtr("")}, want: ""},
		{name: "explicit header is used verbatim", params: Params{Header: strPtr("x-custom-fairness")}, want: "x-custom-fairness"},
		// net/http refuses to write these, which would fail every dispatched
		// request permanently, so the factory has to reject them at startup.
		{name: "header name with a space is rejected", params: Params{Header: strPtr("x-fair id")}, wantErr: true},
		{name: "header name with a colon is rejected", params: Params{Header: strPtr("x-fair:id")}, wantErr: true},
		{name: "header name with a newline is rejected", params: Params{Header: strPtr("x-fair\nid")}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.params.ResolveHeader()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ResolveHeader() = %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveHeader() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("ResolveHeader() = %q, want %q", got, tt.want)
			}
		})
	}
}
