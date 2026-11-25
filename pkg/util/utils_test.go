package util

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestNormalizeURLPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{"empty string", "", "/"},
		{"no leading slash", "foo", "/foo"},
		{"has leading slash", "/foo", "/foo"},
		{"multiple segments no leading slash", "foo/bar", "/foo/bar"},
		{"multiple segments with leading slash", "/foo/bar", "/foo/bar"},
		{"just a slash", "/", "/"},
		{"double leading slash", "//foo", "//foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeURLPath(tt.path)
			if got != tt.want {
				t.Errorf("NormalizeURLPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestNormalizeBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    string
	}{
		{"empty string", "", ""},
		{"no trailing slash", "http://example.com", "http://example.com"},
		{"single trailing slash", "http://example.com/", "http://example.com"},
		{"multiple trailing slashes", "http://example.com///", "http://example.com"},
		{"path with trailing slash", "http://example.com/v1/", "http://example.com/v1"},
		{"just a scheme and host", "https://host", "https://host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeBaseURL(tt.baseURL)
			if got != tt.want {
				t.Errorf("NormalizeBaseURL(%q) = %q, want %q", tt.baseURL, got, tt.want)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantZero bool
	}{
		{name: "integer seconds", value: "120"},
		{name: "zero seconds", value: "0", wantZero: true},
		{name: "HTTP-date future", value: time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)},
		{name: "HTTP-date past", value: time.Now().Add(-10 * time.Second).UTC().Format(http.TimeFormat), wantZero: true},
		{name: "negative integer", value: "-5", wantZero: true},
		{name: "invalid value", value: "abc", wantZero: true},
		{name: "empty string", value: "", wantZero: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := ParseRetryAfter(tt.value)
			if tt.wantZero && d != 0 {
				t.Errorf("expected zero duration, got %v", d)
			}
			if !tt.wantZero && d <= 0 {
				t.Errorf("expected positive duration, got %v", d)
			}
		})
	}
}

func TestIsAbsoluteHTTPSURL(t *testing.T) {
	tests := []struct {
		name string
		url  *url.URL
		want bool
	}{
		{name: "nil"},
		{name: "absolute HTTPS", url: &url.URL{Scheme: "https", Host: "example.com"}, want: true},
		{name: "missing host", url: &url.URL{Scheme: "https"}},
		{name: "HTTP", url: &url.URL{Scheme: "http", Host: "example.com"}},
		{name: "relative", url: &url.URL{Path: "/audio.wav"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAbsoluteHTTPSURL(tt.url); got != tt.want {
				t.Errorf("IsAbsoluteHTTPSURL(%v) = %v, want %v", tt.url, got, tt.want)
			}
		})
	}
}

func TestDecodeJSONDisallowUnknownFields(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		want    string
		wantErr bool
	}{
		{name: "valid fields", data: `{"name":"audio"}`, want: "audio"},
		{name: "unknown field", data: `{"name":"audio","other":true}`, wantErr: true},
		{name: "invalid JSON", data: `{"name":`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dst struct {
				Name string `json:"name"`
			}
			err := DecodeJSONDisallowUnknownFields([]byte(tt.data), &dst)
			if tt.wantErr {
				if err == nil {
					t.Fatal("DecodeJSONDisallowUnknownFields returned nil error, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeJSONDisallowUnknownFields: %v", err)
			}
			if dst.Name != tt.want {
				t.Errorf("decoded name = %q, want %q", dst.Name, tt.want)
			}
		})
	}
}

func TestNewProviderSet(t *testing.T) {
	tests := []struct {
		name      string
		providers []string
		want      map[string]struct{}
		wantErr   bool
	}{
		{name: "empty list", wantErr: true},
		{name: "empty entry", providers: []string{"gcs", ""}, wantErr: true},
		{name: "providers", providers: []string{"gcs", "whisper"}, want: map[string]struct{}{"gcs": {}, "whisper": {}}},
		{name: "duplicate provider", providers: []string{"gcs", "gcs"}, want: map[string]struct{}{"gcs": {}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProviderSet(tt.providers)
			if tt.wantErr {
				if err == nil {
					t.Fatal("NewProviderSet returned nil error, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewProviderSet: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewProviderSet(%v) = %v, want %v", tt.providers, got, tt.want)
			}
		})
	}
}

func TestWriteJSONField(t *testing.T) {
	tests := []struct {
		name    string
		value   json.RawMessage
		want    map[string][]string
		wantErr bool
	}{
		{name: "string", value: json.RawMessage(`"whisper"`), want: map[string][]string{"field": {"whisper"}}},
		{name: "number", value: json.RawMessage(`0.5`), want: map[string][]string{"field": {"0.5"}}},
		{name: "object", value: json.RawMessage(`{"enabled":true}`), want: map[string][]string{"field": {`{"enabled":true}`}}},
		{name: "array", value: json.RawMessage(`["word","segment"]`), want: map[string][]string{"field[]": {"word", "segment"}}},
		{name: "empty array", value: json.RawMessage(`[]`), want: map[string][]string{}},
		{name: "invalid array", value: json.RawMessage(`[`), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			if err := WriteJSONField(writer, "field", tt.value); err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("WriteJSONField: %v", err)
			}
			if tt.wantErr {
				t.Fatal("WriteJSONField returned nil error, want error")
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close multipart writer: %v", err)
			}
			form, err := multipart.NewReader(&body, writer.Boundary()).ReadForm(1 << 20)
			if err != nil {
				t.Fatalf("read multipart form: %v", err)
			}
			defer func() {
				if err := form.RemoveAll(); err != nil {
					t.Errorf("remove multipart temp files: %v", err)
				}
			}()
			if !reflect.DeepEqual(form.Value, tt.want) {
				t.Errorf("multipart fields = %v, want %v", form.Value, tt.want)
			}
		})
	}
}
