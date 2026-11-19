package producer

import (
	"context"
	"errors"
	"time"

	"github.com/llm-d/llm-d-async/api"
)

var (
	// ErrResultDeliveryOwnershipLost means a durable result delivery's lease is
	// no longer owned by this consumer. A stale consumer must stop renewing or
	// acknowledging that delivery.
	ErrResultDeliveryOwnershipLost = errors.New("result delivery ownership lost")

	// ErrUnparsableResult means ReceiveResult encountered a malformed result.
	// The payload remains in leased claim state for redelivery after expiry;
	// consumers should report the error and continue receiving the route.
	ErrUnparsableResult = errors.New("unparsable result")
)

// ResultDelivery is a leased, non-destructive result delivery. Result is safe
// to checkpoint before AckResult is called. The ownership proof is deliberately
// private and only populated by a DurableResultProducer implementation.
type ResultDelivery struct {
	Result *api.ResultMessage

	claimID    string
	ownerToken string
}

// ResultDeliveryConfig reports the effective durable result recovery settings.
type ResultDeliveryConfig struct {
	LeaseTTL        time.Duration
	ReclaimInterval time.Duration
}

// DurableResultProducer is an additive result-delivery capability. Consumers
// must durably checkpoint Result, then acknowledge it. Until acknowledgement,
// lease expiry makes the result eligible for redelivery.
//
// Durable and destructive GetResult consumers must not share a result route.
type DurableResultProducer interface {
	ReceiveResult(ctx context.Context) (*ResultDelivery, error)
	RenewResult(ctx context.Context, delivery *ResultDelivery) error
	AckResult(ctx context.Context, delivery *ResultDelivery) error
	ResultDeliveryConfig() ResultDeliveryConfig
}

// Producer is the abstract interface for submitting requests to the async queue
// and retrieving results. Implementations handle the underlying queue mechanics.
type Producer interface {
	// SubmitRequest adds a request to the processing queue.
	// Returns error if submission fails.
	SubmitRequest(ctx context.Context, req api.Request) error

	// CancelRequests marks previously submitted requests as cancelled.
	// Implementations guarantee best-effort cancellation before dispatch
	// (during dequeue and worker pre-dispatch checks). Workers also poll the
	// marker while inference is executing and abort the in-flight request,
	// returning a CANCELLED result unless a terminal response arrived first.
	// Cancellation is idempotent: unknown or already-completed request IDs are a no-op.
	CancelRequests(ctx context.Context, requestIDs []string) error

	// GetResult retrieves a result from the result queue.
	// Blocks until a result is available or context is cancelled.
	// Use context.WithTimeout for timeout-based retrieval.
	GetResult(ctx context.Context) (*api.ResultMessage, error)

	// Close releases any resources held by the producer.
	Close() error
}
