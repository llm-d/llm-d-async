package gcsmultipart

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// newPlugin builds a plugin instance for the given providers via the factory.
func newPlugin(t *testing.T, providers ...string) *Plugin {
	t.Helper()
	raw, err := json.Marshal(params{Providers: providers})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New("test", raw, plugins.NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p.(*Plugin)
}

// v4SignedURL returns a GCS V4 signed URL that expires at the given time.
func v4SignedURL(expiry time.Time) string {
	start := expiry.Add(-time.Hour).UTC()
	return fmt.Sprintf(
		"https://storage.googleapis.com/bucket/object?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Date=%s&X-Goog-Expires=%d&X-Goog-Signature=deadbeef",
		start.Format("20060102T150405Z"), int(expiry.Sub(start).Seconds()),
	)
}

// parseParts parses a multipart body into a name->values map and records which
// parts were sent as file uploads (i.e. carry a filename).
func parseParts(t *testing.T, contentType string, body []byte) (map[string][]string, map[string]string) {
	t.Helper()
	mediaType, parsms, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("ParseMediaType(%q): %v", contentType, err)
	}
	if mediaType != "multipart/form-data" {
		t.Fatalf("media type = %q, want multipart/form-data", mediaType)
	}
	if parsms["boundary"] == "" {
		t.Fatal("multipart Content-Type has no boundary")
	}

	fields := map[string][]string{}
	files := map[string]string{}
	r := multipart.NewReader(strings.NewReader(string(body)), parsms["boundary"])
	for {
		part, err := r.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextPart: %v", err)
		}
		buf, err := io.ReadAll(part)
		if err != nil {
			t.Fatalf("read part %q: %v", part.FormName(), err)
		}
		fields[part.FormName()] = append(fields[part.FormName()], string(buf))
		if fn := part.FileName(); fn != "" {
			files[part.FormName()] = fn
		}
	}
	return fields, files
}

func TestTransform_BuildsMultipart(t *testing.T) {
	p := newPlugin(t, "whisper")
	payload := []byte(`{"model":"whisper-1","gcs_uri":"https://signed/url?sig=x","language":"en"}`)

	body, contentType, handled, err := p.Transform(payload, map[string]string{"provider": "whisper"})
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	if !handled {
		t.Fatal("handled = false, want true")
	}
	if !strings.HasPrefix(contentType, "multipart/form-data; boundary=") {
		t.Fatalf("contentType = %q, want multipart/form-data with boundary", contentType)
	}

	fields, files := parseParts(t, contentType, body)
	if got := fields["url"]; len(got) != 1 || got[0] != "https://signed/url?sig=x" {
		t.Errorf("url field = %v, want the signed URL", got)
	}
	if got := fields["model"]; len(got) != 1 || got[0] != "whisper-1" {
		t.Errorf("model field = %v, want [whisper-1]", got)
	}
	if got := fields["language"]; len(got) != 1 || got[0] != "en" {
		t.Errorf("language field = %v, want [en]", got)
	}
	if _, ok := fields["gcs_uri"]; ok {
		t.Error("gcs_uri must be dropped from the multipart body")
	}
	if len(files) != 0 {
		t.Errorf("expected no file-upload parts, got %v", files)
	}
}

func TestTransform_PassesThroughNonStringFields(t *testing.T) {
	p := newPlugin(t, "whisper")
	payload := []byte(`{"gcs_uri":"https://signed/url","temperature":0.5,"stream":true}`)

	body, contentType, handled, err := p.Transform(payload, map[string]string{"provider": "whisper"})
	if err != nil || !handled {
		t.Fatalf("Transform: handled=%v err=%v", handled, err)
	}
	fields, _ := parseParts(t, contentType, body)
	if got := fields["temperature"]; len(got) != 1 || got[0] != "0.5" {
		t.Errorf("temperature field = %v, want [0.5]", got)
	}
	if got := fields["stream"]; len(got) != 1 || got[0] != "true" {
		t.Errorf("stream field = %v, want [true]", got)
	}
}

func TestTransform_NotHandled(t *testing.T) {
	p := newPlugin(t, "whisper")
	tests := []struct {
		name     string
		payload  string
		metadata map[string]string
	}{
		{"provider mismatch", `{"gcs_uri":"https://signed/url"}`, map[string]string{"provider": "other"}},
		{"no provider", `{"gcs_uri":"https://signed/url"}`, nil},
		{"no gcs_uri", `{"model":"whisper-1"}`, map[string]string{"provider": "whisper"}},
		{"empty gcs_uri", `{"gcs_uri":""}`, map[string]string{"provider": "whisper"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, _, handled, err := p.Transform([]byte(tc.payload), tc.metadata)
			if err != nil {
				t.Fatalf("Transform: unexpected error %v", err)
			}
			if handled {
				t.Error("handled = true, want false")
			}
			if body != nil {
				t.Errorf("body = %q, want nil", body)
			}
		})
	}
}

func TestTransform_RejectsFileBase64(t *testing.T) {
	p := newPlugin(t, "whisper")
	payload := []byte(`{"gcs_uri":"https://signed/url","file_base64":"QUJD"}`)

	_, _, handled, err := p.Transform(payload, map[string]string{"provider": "whisper"})
	if handled {
		t.Error("handled = true, want false on rejection")
	}
	assertFatal(t, err)
}

func TestValidate(t *testing.T) {
	p := newPlugin(t, "whisper")
	whisper := map[string]string{"provider": "whisper"}
	deadline := time.Now().Add(time.Hour)

	tests := []struct {
		name      string
		payload   string
		metadata  map[string]string
		wantError bool
	}{
		{"future expiry ok", fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(deadline.Add(time.Hour))), whisper, false},
		{"expired before deadline", fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(deadline.Add(-time.Minute))), whisper, true},
		{"no gcs_uri", `{"model":"whisper-1"}`, whisper, false},
		{"provider mismatch", fmt.Sprintf(`{"gcs_uri":%q}`, v4SignedURL(deadline.Add(-time.Minute))), map[string]string{"provider": "other"}, false},
		{"unparsable expiry", `{"gcs_uri":"https://signed/url?sig=x"}`, whisper, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := p.Validate([]byte(tc.payload), tc.metadata, deadline.Unix())
			if tc.wantError {
				assertFatal(t, err)
				return
			}
			if err != nil {
				t.Errorf("Validate: unexpected error %v", err)
			}
		})
	}
}

func TestNew_ParamValidation(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantError bool
	}{
		{"valid", `{"providers":["whisper"]}`, false},
		{"valid multiple", `{"providers":["whisper","ocr"]}`, false},
		{"empty providers", `{"providers":[]}`, true},
		{"missing providers", `{}`, true},
		{"no parameters", ``, true},
		{"empty provider string", `{"providers":[""]}`, true},
		{"unknown field", `{"providers":["whisper"],"foo":1}`, true},
		{"malformed json", `{"providers":`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New("test", json.RawMessage(tc.raw), plugins.NewHandle(context.Background()))
			if tc.wantError && err == nil {
				t.Error("New = nil error, want error")
			}
			if !tc.wantError && err != nil {
				t.Errorf("New = %v, want nil error", err)
			}
		})
	}
}

func TestSignedURLExpiry(t *testing.T) {
	ref := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		url  string
		want time.Time
		ok   bool
	}{
		{"v4", "https://x/o?X-Goog-Date=20260618T110000Z&X-Goog-Expires=3600", ref, true},
		{"v2", fmt.Sprintf("https://x/o?Expires=%d", ref.Unix()), ref, true},
		{"none", "https://x/o?sig=abc", time.Time{}, false},
		{"v4 bad date", "https://x/o?X-Goog-Date=nope&X-Goog-Expires=3600", time.Time{}, false},
		{"v4 bad expires", "https://x/o?X-Goog-Date=20260618T110000Z&X-Goog-Expires=abc", time.Time{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := signedURLExpiry(tc.url)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && !got.Equal(tc.want) {
				t.Errorf("expiry = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestPluginRegistered(t *testing.T) {
	if _, ok := plugins.Lookup(PluginType); !ok {
		t.Errorf("plugin type %q not registered via init()", PluginType)
	}
}

// assertFatal asserts err is a non-retryable invalid-request ClientError.
func assertFatal(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	var ce *asyncapi.ClientError
	if !errors.As(err, &ce) {
		t.Fatalf("error %v is not a *ClientError", err)
	}
	if !ce.Category().Fatal() {
		t.Errorf("error category %q is not fatal", ce.Category())
	}
}
