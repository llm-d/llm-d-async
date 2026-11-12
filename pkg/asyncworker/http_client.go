package asyncworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

var _ asyncapi.InferenceClient = (*HTTPInferenceClient)(nil)

// HTTPInferenceClient is the default HTTP implementation of InferenceClient.
type HTTPInferenceClient struct {
	client               *http.Client
	resultHeaderPrefixes []string
	maxResponseBytes     int64
}

// HTTPClientOption configures an HTTPInferenceClient.
type HTTPClientOption func(*HTTPInferenceClient)

// WithResultHeaderPrefixes forwards every response header whose name starts with one of
// prefixes, compared case-insensitively, in InferenceResponse.Headers.
func WithResultHeaderPrefixes(prefixes ...string) HTTPClientOption {
	return func(c *HTTPInferenceClient) {
		for _, p := range prefixes {
			if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
				c.resultHeaderPrefixes = append(c.resultHeaderPrefixes, p)
			}
		}
	}
}

// WithMaxResponseBytes fails a request whose response body is larger than n bytes with a
// non-retryable error instead of reading it. n <= 0 leaves the size unlimited.
func WithMaxResponseBytes(n int64) HTTPClientOption {
	return func(c *HTTPInferenceClient) {
		c.maxResponseBytes = max(n, 0)
	}
}

// NewHTTPInferenceClient creates a new HTTPInferenceClient with the given HTTP client.
func NewHTTPInferenceClient(client *http.Client, opts ...HTTPClientOption) *HTTPInferenceClient {
	c := &HTTPInferenceClient{client: client}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (h *HTTPInferenceClient) forwardedHeaders(header http.Header) map[string]string {
	if len(h.resultHeaderPrefixes) == 0 {
		return nil
	}
	var out map[string]string
	for name, values := range header {
		lower := strings.ToLower(name)
		for _, p := range h.resultHeaderPrefixes {
			if strings.HasPrefix(lower, p) {
				if out == nil {
					out = make(map[string]string)
				}
				out[lower] = strings.Join(values, ", ")
				break
			}
		}
	}
	return out
}

// SendRequest implements InferenceClient for HTTP-based inference requests.
func (h *HTTPInferenceClient) SendRequest(ctx context.Context, url string, headers map[string]string, payload []byte) (*asyncapi.InferenceResponse, error) {
	request, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryInvalidReq,
			Message:       "failed to create request",
			RawError:      err,
		}
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	result, err := h.client.Do(request)
	if err != nil {
		return nil, &asyncapi.ClientError{
			ErrorCategory: transportErrorCategory(err),
			Message:       "failed to send request",
			RawError:      err,
		}
	}
	defer result.Body.Close() // nolint:errcheck

	// Read the rejection context once: the headers are available on every path
	// below, including the one where the body read fails.
	droppedReason := result.Header.Get(asyncapi.DroppedReasonHeader)
	retryAfter, _ := parseRetryAfter(result.Header.Get("Retry-After"))

	if h.maxResponseBytes > 0 && result.ContentLength > h.maxResponseBytes {
		return nil, h.responseTooLarge(result.StatusCode)
	}
	body, err := h.readBody(result.Body)
	if errors.Is(err, errResponseTooLarge) {
		return nil, h.responseTooLarge(result.StatusCode)
	}
	resp := &asyncapi.InferenceResponse{
		StatusCode:  result.StatusCode,
		Body:        body,
		ContentType: result.Header.Get("Content-Type"),
		Headers:     h.forwardedHeaders(result.Header),
	}
	if err != nil {
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       "failed to read response",
			RawError:      err,
			RetryAfter:    retryAfter,
			StatusCode:    result.StatusCode,
			DroppedReason: droppedReason,
		}
	}

	if result.StatusCode == 429 {
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryRateLimit,
			Message:       fmt.Sprintf("rate limited: status code %d", result.StatusCode),
			RetryAfter:    retryAfter,
			StatusCode:    result.StatusCode,
			DroppedReason: droppedReason,
		}
	}

	if result.StatusCode >= 400 && result.StatusCode < 500 {
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryInvalidReq,
			Message:       fmt.Sprintf("client error: status code %d", result.StatusCode),
			StatusCode:    result.StatusCode,
			DroppedReason: droppedReason,
		}
	}

	if result.StatusCode >= 500 && result.StatusCode < 600 {
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       fmt.Sprintf("server error: status code %d", result.StatusCode),
			RetryAfter:    retryAfter,
			StatusCode:    result.StatusCode,
			DroppedReason: droppedReason,
		}
	}

	return resp, nil
}

// transportErrorCategory classifies an http.Client.Do failure. Timeouts and
// connections that were refused, reset, or closed before a response are
// ErrCategoryServer. Everything else is ErrCategoryUnknown.
func transportErrorCategory(err error) asyncapi.ErrorCategory {
	var netErr net.Error
	switch {
	case errors.As(err, &netErr) && netErr.Timeout(),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNABORTED),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.EHOSTUNREACH):
		return asyncapi.ErrCategoryServer
	default:
		return asyncapi.ErrCategoryUnknown
	}
}

var errResponseTooLarge = errors.New("response body too large")

func (h *HTTPInferenceClient) readBody(r io.Reader) ([]byte, error) {
	if h.maxResponseBytes > 0 {
		r = io.LimitReader(r, h.maxResponseBytes+1)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return body, fmt.Errorf("read response body: %w", err)
	}
	if h.maxResponseBytes > 0 && int64(len(body)) > h.maxResponseBytes {
		return nil, errResponseTooLarge
	}
	return body, nil
}

func (h *HTTPInferenceClient) responseTooLarge(statusCode int) error {
	return &asyncapi.ClientError{
		ErrorCategory: asyncapi.ErrCategoryInvalidReq,
		Message:       fmt.Sprintf("status code %d response body exceeds %d bytes", statusCode, h.maxResponseBytes),
		RawError:      errResponseTooLarge,
	}
}

// parseRetryAfter parses a Retry-After header value, which can be either
// a number of seconds (e.g. "120") or an HTTP-date (e.g. "Thu, 01 Dec 1994 16:00:00 GMT").
// Returns the parsed duration and true if successful, or (0, false) if empty or unparsable.
func parseRetryAfter(value string) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	// Try integer seconds first
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	// Try HTTP-date formats (IMF-fixdate, RFC 850, asctime)
	if t, err := http.ParseTime(value); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
