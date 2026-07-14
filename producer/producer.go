package producer

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/api"
)

// Producer is the abstract interface for submitting requests to the async queue
// and retrieving results. Implementations handle the underlying queue mechanics.
type Producer interface {
	// SubmitRequest adds a request to the processing queue.
	// Returns error if submission fails.
	SubmitRequest(ctx context.Context, req api.Request) error

	// CancelRequests marks previously submitted requests as cancelled.
	// Implementations guarantee best-effort cancellation before dispatch
	// (during dequeue and worker pre-dispatch checks), but do not guarantee
	// aborting requests that are already in flight to the inference backend
	// or forcing already-dispatched requests to return a CANCELLED result.
	// Cancellation is idempotent: unknown or already-completed request IDs are a no-op.
	CancelRequests(ctx context.Context, requestIDs []string) error

	// GetResult retrieves a result from the result queue.
	// Blocks until a result is available or context is cancelled.
	// Use context.WithTimeout for timeout-based retrieval.
	GetResult(ctx context.Context) (*api.ResultMessage, error)

	// Close releases any resources held by the producer.
	Close() error
}
