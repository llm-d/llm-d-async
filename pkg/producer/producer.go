package producer

import (
	"context"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
)

// Producer is the abstract interface for submitting requests to the async queue
// and retrieving results. Implementations handle the underlying queue mechanics.
type Producer interface {
	// SubmitRequest adds a request to the processing queue.
	// Returns error if submission fails.
	SubmitRequest(ctx context.Context, req api.RequestMessage) error

	// GetResult retrieves a result from the result queue.
	// Blocks until a result is available or context is cancelled.
	// Returns error if retrieval fails or context is cancelled.
	GetResult(ctx context.Context) (*api.ResultMessage, error)

	// GetResultWithTimeout retrieves a result with a timeout.
	// Returns nil result if timeout expires without a result.
	GetResultWithTimeout(ctx context.Context, timeout time.Duration) (*api.ResultMessage, error)

	// Close releases any resources held by the producer.
	Close() error
}
