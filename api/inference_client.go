package api

import "context"

// InferenceClient defines the interface for sending inference requests.
// This interface allows for pluggable implementations beyond the default HTTP client.
type InferenceClient interface {
	// SendRequest sends an inference request to the specified URL with the given headers and payload.
	// Returns the response body, the HTTP status code, and any error that occurred.
	//
	// statusCode is the actual HTTP status code from the upstream response. It is non-zero
	// whenever an HTTP response was received (including error responses like 4xx/5xx).
	// A zero statusCode means no HTTP response was obtained (e.g. transport/network failure).
	//
	// Errors should implement InferenceError to provide an ErrorCategory via Category().
	// ErrorCategory determines retry and shedding behavior through its Fatal() and Sheddable() methods:
	//   - ErrCategoryRateLimit:  retryable, sheddable (e.g. 429)
	//   - ErrCategoryServer:     retryable (e.g. 5xx)
	//   - ErrCategoryInvalidReq: not retryable (e.g. 4xx)
	//   - ErrCategoryAuth:       not retryable
	//   - ErrCategoryParse:      not retryable
	//   - ErrCategoryUnknown:    not retryable
	// A nil error indicates a successful response.
	SendRequest(ctx context.Context, url string, headers map[string]string, payload []byte) (responseBody []byte, statusCode int, err error)
}
