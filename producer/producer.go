package producer

import (
	"context"
	"errors"
	"time"

	"github.com/llm-d/llm-d-async/api"
)

// Producer is an alias of api.Producer so existing Redis callers keep compiling
// after the interface moved to api. A GCP-only consumer can depend on api (and
// producer-gcp) without importing this Redis-backed module.
type Producer = api.Producer

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
