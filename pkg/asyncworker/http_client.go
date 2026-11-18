package asyncworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pkg/asyncworker/transform"
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
			ErrorCategory: transportErrorCategory(err),
			Message:       "failed to send request",
			RawError:      err,
		}
	}
	defer result.Body.Close() // nolint:errcheck

	// Read the rejection context once: the headers are available on every path
	// below, including the one where the body read fails.
	droppedReason := result.Header.Get(asyncapi.DroppedReasonHeader)
	retryAfter, _ := transform.ParseRetryAfter(result.Header.Get("Retry-After"))

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return &asyncapi.InferenceResponse{StatusCode: result.StatusCode, Body: body}, &asyncapi.ClientError{
			ErrorCategory: asyncapi.ErrCategoryServer,
			Message:       "failed to read response",
			RawError:      err,
			RetryAfter:    retryAfter,
			StatusCode:    result.StatusCode,
			DroppedReason: droppedReason,
		}
	}

	resp := &asyncapi.InferenceResponse{StatusCode: result.StatusCode, Body: body}

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
