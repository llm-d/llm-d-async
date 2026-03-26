package api

import "fmt"

// ErrorCategory defines the category of an inference client error.
type ErrorCategory string

const (
	// ErrCategoryRateLimit represents rate limiting errors (429) - retryable and sheddable
	ErrCategoryRateLimit ErrorCategory = "RATE_LIMIT"
	// ErrCategoryServer represents server errors (5xx) - retryable but not sheddable
	ErrCategoryServer ErrorCategory = "SERVER_ERROR"
	// ErrCategoryNetwork represents network/transport errors - fatal (not retryable)
	ErrCategoryNetwork ErrorCategory = "NETWORK"
	// ErrCategoryInvalidReq represents invalid request errors - fatal (not retryable)
	ErrCategoryInvalidReq ErrorCategory = "INVALID_REQ"
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
