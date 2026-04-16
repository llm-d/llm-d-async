package api

import "fmt"

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

// ClientError represents an inference client error with category and context.
type ClientError struct {
	ErrorCategory ErrorCategory
	Message       string
	RawError      error // original error if available
}

func (e *ClientError) Error() string {
	if e.RawError != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.ErrorCategory, e.Message, e.RawError)
	}
	return fmt.Sprintf("%s: %s", e.ErrorCategory, e.Message)
}

func (e *ClientError) Unwrap() error {
	return e.RawError
}

func (e *ClientError) Category() ErrorCategory {
	return e.ErrorCategory
}
