package api

import "context"

// InferenceResponse holds the HTTP response from an upstream inference request.
// StatusCode is non-zero whenever an HTTP response was received (including 4xx/5xx).
// A zero StatusCode means no HTTP response was obtained (e.g. transport/network failure).
// Body contains the response body; it may be partial if a read error occurred.
type InferenceResponse struct {
	StatusCode int
	Body       []byte
}

// InferenceClient defines the interface for sending inference requests.
// This interface allows for pluggable implementations beyond the default HTTP client.
type InferenceClient interface {
	// SendRequest sends an inference request to the specified URL with the given headers and payload.
	//
	// On success (nil error), the returned *InferenceResponse is always non-nil.
	// On error, *InferenceResponse may still be non-nil if the upstream sent an HTTP
	// response (e.g. 4xx/5xx); callers should check resp.StatusCode > 0 to determine
	// whether an HTTP response was received.
	//
	// Errors should implement InferenceError to provide an ErrorCategory via Category().
	// ErrorCategory determines retry and shedding behavior through its Fatal() and Sheddable() methods:
	//   - ErrCategoryRateLimit:  retryable, sheddable (e.g. 429)
	//   - ErrCategoryServer:     retryable (e.g. 5xx)
	//   - ErrCategoryInvalidReq: not retryable (e.g. 4xx)
	//   - ErrCategoryAuth:       not retryable
	//   - ErrCategoryParse:      not retryable
	//   - ErrCategoryUnknown:    not retryable
	SendRequest(ctx context.Context, url string, headers map[string]string, payload []byte) (*InferenceResponse, error)
}
