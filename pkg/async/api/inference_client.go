package api

import "context"

// InferenceClient defines the interface for sending inference requests.
// This interface allows for pluggable implementations beyond the default HTTP client.
type InferenceClient interface {
	// SendRequest sends an inference request to the specified URL with the given headers and payload.
	// Returns the response body and any error that occurred.
	// Errors should implement InferenceError to indicate retry behavior via Category(), Fatal(), and Sheddable():
	// - ErrCategoryRateLimit: rate limiting (429) - retryable and sheddable
	// - ErrCategoryServer: server errors (5xx) - retryable but not sheddable
	// - ErrCategoryNetwork: network/transport errors - fatal (not retryable)
	// - ErrCategoryInvalidReq: invalid request errors - fatal (not retryable)
	// - nil: successful response (2xx, 3xx, 4xx - any non-retryable response)
	SendRequest(ctx context.Context, url string, headers map[string]string, payload []byte) (responseBody []byte, err error)
}
