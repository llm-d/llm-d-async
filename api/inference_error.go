package api

import (
	"fmt"
	"time"
)

// ErrorCategory defines the category of an inference client error.
type ErrorCategory string

const (
	ErrCategoryRateLimit  ErrorCategory = "RATE_LIMIT"   // retryable
	ErrCategoryServer     ErrorCategory = "SERVER_ERROR" // retryable
	ErrCategoryInvalidReq ErrorCategory = "INVALID_REQ"  // not retryable
	ErrCategoryAuth       ErrorCategory = "AUTH_ERROR"   // not retryable
	ErrCategoryParse      ErrorCategory = "PARSE_ERROR"  // not retryable
	ErrCategoryUnknown    ErrorCategory = "UNKNOWN"      // not retryable
)

// Fatal returns true if errors in this category should not be retried.
func (c ErrorCategory) Fatal() bool {
	return c != ErrCategoryRateLimit && c != ErrCategoryServer
}

// Sheddable returns true if errors in this category represent rate limiting or load shedding.
func (c ErrorCategory) Sheddable() bool {
	return c == ErrCategoryRateLimit
}

// InferenceError represents an error that occurred during inference request processing.
type InferenceError interface {
	error
	// Category returns the error category, which determines retry and shedding behavior.
	Category() ErrorCategory
}

var _ InferenceError = (*ClientError)(nil)

// DroppedReasonHeader is the response header llm-d-router's flow control uses
// to communicate the machine-readable reason a request was rejected before
// dispatch or evicted in flight. The router sets it on both 429 (rejected for
// capacity or queue TTL, evicted after dispatch) and 503 (no endpoints, client
// disconnected, shutting down) responses.
const DroppedReasonHeader = "x-llm-d-request-dropped-reason"

// ClientError represents an inference client error with category and context.
type ClientError struct {
	ErrorCategory ErrorCategory
	Message       string
	RawError      error         // original error if available
	RetryAfter    time.Duration // server-specified retry delay from Retry-After header (0 means not set)
	StatusCode    int           // HTTP status code; 0 means no HTTP response was received
	DroppedReason string        // machine-readable drop reason from DroppedReasonHeader (empty means not set)
}

func (e *ClientError) Error() string {
	msg := e.Message
	if e.DroppedReason != "" {
		msg = fmt.Sprintf("%s (dropped: %s)", msg, e.DroppedReason)
	}
	if e.RawError != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.ErrorCategory, msg, e.RawError)
	}
	return fmt.Sprintf("%s: %s", e.ErrorCategory, msg)
}

func (e *ClientError) Unwrap() error {
	return e.RawError
}

func (e *ClientError) Category() ErrorCategory {
	return e.ErrorCategory
}
