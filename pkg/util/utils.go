package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// NormalizeURLPath makes path absolute by adding a leading slash when needed.
func NormalizeURLPath(path string) string {
	if len(path) == 0 {
		return "/"
	}
	if path[0] != '/' {
		path = "/" + path
	}
	return path
}

// NormalizeBaseURL removes trailing slashes from a base URL.
func NormalizeBaseURL(baseURL string) string {
	if len(baseURL) == 0 {
		return ""
	}
	for baseURL[len(baseURL)-1] == '/' {
		baseURL = baseURL[:len(baseURL)-1]
	}
	return baseURL
}

// IsAbsoluteHTTPSURL reports whether u is an absolute HTTPS URL with a host.
func IsAbsoluteHTTPSURL(u *url.URL) bool {
	return u != nil && u.Scheme == "https" && u.Host != ""
}

// ParseRetryAfter parses a Retry-After header value, which can be either a number of seconds or an HTTP-date.
// Empty or invalid values return zero.
func ParseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil {
		if delay := time.Until(deadline); delay > 0 {
			return delay
		}
	}
	return 0
}

// DecodeJSONDisallowUnknownFields decodes one JSON value and rejects unknown object fields.
func DecodeJSONDisallowUnknownFields(data []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	return nil
}

// NewProviderSet validates a provider selector list and builds a lookup set.
func NewProviderSet(providers []string) (map[string]struct{}, error) {
	if len(providers) == 0 {
		return nil, errors.New(`at least one entry in "providers" is required`)
	}
	set := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		if provider == "" {
			return nil, errors.New(`"providers" entries must be non-empty`)
		}
		set[provider] = struct{}{}
	}
	return set, nil
}

// WriteJSONField writes a JSON value as a multipart form field. Arrays become
// repeated name[] fields, as expected by OpenAI-compatible media endpoints.
func WriteJSONField(writer *multipart.Writer, name string, raw json.RawMessage) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '[' {
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return fmt.Errorf("parse array: %w", err)
		}
		for _, value := range values {
			if err := writer.WriteField(name+"[]", formValue(value)); err != nil {
				return fmt.Errorf("write multipart field: %w", err)
			}
		}
		return nil
	}
	if err := writer.WriteField(name, formValue(raw)); err != nil {
		return fmt.Errorf("write multipart field: %w", err)
	}
	return nil
}

func formValue(raw json.RawMessage) string {
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	return string(raw)
}
