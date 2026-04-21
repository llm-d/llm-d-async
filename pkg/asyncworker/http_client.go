package asyncworker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
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
func (h *HTTPInferenceClient) SendRequest(ctx context.Context, url string, headers map[string]string, payload []byte) ([]byte, error) {
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
		// Response read errors are retryable as the request may have succeeded
		return nil, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       "failed to read response",
			RawError:      err,
		}
	}

	// Check for rate limiting / load shedding (429)
	if result.StatusCode == 429 {
		return body, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryRateLimit,
			Message:       fmt.Sprintf("rate limited: status code %d", result.StatusCode),
			RawError:      nil,
		}
	}

	// Check for client errors (4xx, non-429)
	if result.StatusCode >= 400 && result.StatusCode < 500 {
		return body, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryInvalidReq,
			Message:       fmt.Sprintf("client error: status code %d", result.StatusCode),
			RawError:      nil,
		}
	}

	// Check for server errors (5xx)
	if result.StatusCode >= 500 && result.StatusCode < 600 {
		return body, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       fmt.Sprintf("server error: status code %d", result.StatusCode),
			RawError:      nil,
		}
	}

	return body, nil
}
