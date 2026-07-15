// Package gcsmultipart provides a RequestTransform plugin that rewrites the
// OpenAI-style JSON body into multipart/form-data for multi-modal inference
// endpoints (e.g. Whisper transcription, OCR) that expect a `url` form field
// pointing at a signed object URL rather than a JSON payload.
//
// Producers can't put raw media bytes on the broker (size limits), so the queued
// JSON carries a signed object URL (e.g. a GCS V4 signed URL) in a `gcs_uri`
// field. This plugin, at dispatch time, emits multipart/form-data with that URL
// as the `url` field and the remaining request fields as form fields. It also
// performs a deadline-aware preflight: if the signed URL expires before the
// message deadline, the request is rejected fatally so the broker doesn't retry
// a request that cannot succeed.
//
// The plugin is opt-in: it owns a message only when the per-message `provider`
// metadata matches one of its configured providers AND the body carries a
// non-empty `gcs_uri`. Otherwise it leaves the default JSON path untouched.
package gcsmultipart

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/url"
	"strconv"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// PluginType is the registry key under which this plugin's factory is registered.
const PluginType = "gcs_uri_multipart"

const (
	// providerMetadataKey is the per-message metadata key whose value is matched
	// against the plugin's configured providers.
	providerMetadataKey = "provider"

	// gcsURIField is the JSON field carrying the signed object URL.
	gcsURIField = "gcs_uri"
	// fileBase64Field carries inline media bytes, which this path does not support.
	fileBase64Field = "file_base64"
	// urlFormField is the multipart form field the endpoint expects.
	urlFormField = "url"
)

var _ transform.RequestTransform = (*Plugin)(nil)

// params is the plugin's configuration, parsed from the factory parameters.
type params struct {
	// Providers are the `provider` metadata values this instance handles.
	Providers []string `json:"providers"`
}

// Plugin rewrites a JSON body with a gcs_uri into multipart/form-data.
type Plugin struct {
	tn        plugins.TypedName
	providers map[string]struct{}
}

func init() {
	plugins.MustRegister(PluginType, New)
}

// New is the FactoryFunc for the plugin. It requires a non-empty `providers`
// list in its parameters.
func New(name string, parameters json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	var p params
	if len(parameters) > 0 {
		dec := json.NewDecoder(bytes.NewReader(parameters))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("invalid parameters: %w", err)
		}
	}
	if len(p.Providers) == 0 {
		return nil, fmt.Errorf("at least one entry in %q is required", "providers")
	}

	providers := make(map[string]struct{}, len(p.Providers))
	for _, provider := range p.Providers {
		if provider == "" {
			return nil, fmt.Errorf("%q entries must be non-empty", "providers")
		}
		providers[provider] = struct{}{}
	}

	return &Plugin{
		tn:        plugins.TypedName{Type: PluginType, Name: name},
		providers: providers,
	}, nil
}

// TypedName returns the plugin's type and instance name.
func (p *Plugin) TypedName() plugins.TypedName { return p.tn }

// owns reports whether this plugin handles a message with the given metadata.
func (p *Plugin) owns(metadata map[string]string) bool {
	_, ok := p.providers[metadata[providerMetadataKey]]
	return ok
}

// Validate performs a deadline-aware preflight. When the plugin owns the message
// and the signed URL's expiry can be parsed, it rejects the request fatally if
// the URL expires at or before the request deadline. Messages the plugin does
// not own, or whose URL has no parseable expiry, validate successfully.
func (p *Plugin) Validate(payload []byte, metadata map[string]string, reqDeadline int64) error {
	if !p.owns(metadata) {
		return nil
	}
	fields, err := decodeFields(payload)
	if err != nil {
		return fatalf("failed to parse request body: %v", err)
	}
	signedURL := stringField(fields, gcsURIField)
	if signedURL == "" {
		return nil
	}

	expiry, ok := signedURLExpiry(signedURL)
	if !ok {
		// No parseable expiry: nothing to preflight, leave it to dispatch.
		return nil
	}
	if !expiry.After(time.Unix(reqDeadline, 0)) {
		return fatalf("signed URL expires at %s, at or before the request deadline %s",
			expiry.UTC().Format(time.RFC3339), time.Unix(reqDeadline, 0).UTC().Format(time.RFC3339))
	}
	return nil
}

// Transform rewrites the JSON body into multipart/form-data when the plugin owns
// the message and a non-empty gcs_uri is present: the gcs_uri value becomes the
// `url` form field, the remaining fields pass through as form fields, and the
// gcs_uri/file_base64 fields are dropped. A non-empty file_base64 is rejected
// fatally (inline media is not supported on this path). Messages the plugin does
// not own, or that carry no gcs_uri, return handled=false unchanged.
func (p *Plugin) Transform(payload []byte, metadata map[string]string) (body []byte, contentType string, handled bool, err error) {
	if !p.owns(metadata) {
		return nil, "", false, nil
	}
	fields, err := decodeFields(payload)
	if err != nil {
		return nil, "", false, fatalf("failed to parse request body: %v", err)
	}
	signedURL := stringField(fields, gcsURIField)
	if signedURL == "" {
		return nil, "", false, nil
	}
	if stringField(fields, fileBase64Field) != "" {
		return nil, "", false, fatalf("%q is not supported on the multipart path", fileBase64Field)
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// The signed URL is written as a plain form field, not a file upload.
	if err := w.WriteField(urlFormField, signedURL); err != nil {
		return nil, "", false, fatalf("failed to write %q field: %v", urlFormField, err)
	}
	for name, raw := range fields {
		if name == gcsURIField || name == fileBase64Field {
			continue
		}
		if err := w.WriteField(name, asFormValue(raw)); err != nil {
			return nil, "", false, fatalf("failed to write %q field: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, "", false, fatalf("failed to finalize multipart body: %v", err)
	}

	return buf.Bytes(), w.FormDataContentType(), true, nil
}

// decodeFields parses the JSON body into its top-level fields, preserving each
// value's raw JSON so non-string fields pass through faithfully.
func decodeFields(payload []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

// stringField returns the value of a JSON string field, or "" if the field is
// absent or not a string.
func stringField(fields map[string]json.RawMessage, name string) string {
	raw, ok := fields[name]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// asFormValue renders a raw JSON value as a multipart form field value: JSON
// strings are unquoted, everything else (numbers, booleans, objects, arrays) is
// passed through as its JSON encoding.
func asFormValue(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// signedURLExpiry extracts the absolute expiry time of a signed object URL.
// It supports V4 signing (X-Goog-Date + X-Goog-Expires seconds) and V2 signing
// (Expires as Unix seconds). It returns ok=false if no expiry can be determined.
func signedURLExpiry(signedURL string) (time.Time, bool) {
	u, err := url.Parse(signedURL)
	if err != nil {
		return time.Time{}, false
	}
	q := u.Query()

	// V4: X-Goog-Date (yyyymmddThhmmssZ) + X-Goog-Expires (seconds).
	if date := q.Get("X-Goog-Date"); date != "" {
		if expires := q.Get("X-Goog-Expires"); expires != "" {
			start, err := time.Parse("20060102T150405Z", date)
			if err != nil {
				return time.Time{}, false
			}
			secs, err := strconv.Atoi(expires)
			if err != nil {
				return time.Time{}, false
			}
			return start.Add(time.Duration(secs) * time.Second), true
		}
	}

	// V2: Expires as absolute Unix seconds.
	if expires := q.Get("Expires"); expires != "" {
		secs, err := strconv.ParseInt(expires, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		return time.Unix(secs, 0), true
	}

	return time.Time{}, false
}

// fatalf builds a non-retryable invalid-request error. The worker treats any
// transform error as fatal; the explicit category documents intent and is robust
// to future changes in error handling.
func fatalf(format string, args ...any) error {
	return &asyncapi.ClientError{
		ErrorCategory: asyncapi.ErrCategoryInvalidReq,
		Message:       fmt.Sprintf(format, args...),
	}
}
