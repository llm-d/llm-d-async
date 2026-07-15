package asyncworker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
)

var _ asyncapi.InferenceClient = (*HTTPInferenceClient)(nil)

// HTTPInferenceClient is the default HTTP implementation of InferenceClient.
type HTTPInferenceClient struct {
	client *http.Client
}

// NewHTTPInferenceClient creates a new HTTPInferenceClient with the given HTTP client.
func NewHTTPInferenceClient(client *http.Client) *HTTPInferenceClient {
	return &HTTPInferenceClient{client: client}
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
			ErrorCategory: asyncapi.ErrCategoryUnknown,
			Message:       "failed to send request",
			RawError:      err,
		}
	}
	defer result.Body.Close() // nolint:errcheck

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return &asyncapi.InferenceResponse{StatusCode: result.StatusCode, Body: body}, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       "failed to read response",
			RawError:      err,
			StatusCode:    result.StatusCode,
		}
	}

	resp := &asyncapi.InferenceResponse{StatusCode: result.StatusCode, Body: body}

	if result.StatusCode == 429 {
		retryAfter, _ := parseRetryAfter(result.Header.Get("Retry-After"))
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryRateLimit,
			Message:       fmt.Sprintf("rate limited: status code %d", result.StatusCode),
			RetryAfter:    retryAfter,
			StatusCode:    result.StatusCode,
		}
	}

	if result.StatusCode >= 400 && result.StatusCode < 500 {
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryInvalidReq,
			Message:       fmt.Sprintf("client error: status code %d", result.StatusCode),
			StatusCode:    result.StatusCode,
		}
	}

	if result.StatusCode >= 500 && result.StatusCode < 600 {
		return resp, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       fmt.Sprintf("server error: status code %d", result.StatusCode),
			StatusCode:    result.StatusCode,
		}
	}

	return resp, nil
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
