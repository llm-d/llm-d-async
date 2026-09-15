package api

import (
	"context"
	"errors"
)

// ResultRouteAttribute is the Pub/Sub message attribute that routes a result
// to a producer. The producer sets it on request publications; the processor
// stamps the same value on result publications. Producers attach a filtered
// subscription on this attribute so they only receive their own results.
const ResultRouteAttribute = "result_route"

// ErrNotSupported means the producer implementation does not provide the
// requested operation. Pub/Sub CancelRequests returns this because the
// consume path has no cancellation check.
var ErrNotSupported = errors.New("operation not supported by this producer")

// Producer is the abstract interface for submitting requests to the async
// queue and retrieving results. Implementations handle the underlying queue
// mechanics.
type Producer interface {
	// SubmitRequest adds a request to the processing queue.
	// Returns error if submission fails.
	SubmitRequest(ctx context.Context, req Request) error

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
	GetResult(ctx context.Context) (*ResultMessage, error)

	// Close releases any resources held by the producer.
	Close() error
}
