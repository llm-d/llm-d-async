package pipeline

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/api"
)

type Flow interface {
	Characteristics() Characteristics
	Start(ctx context.Context)
	StopConsuming()
	Shutdown()
	RequestChannels() []RequestChannel
	RetryChannel() chan RetryMessage
	ResultChannel() chan api.ResultMessage
}

// HealthChecker is an optional interface that Flow implementations can
// satisfy to report backend-specific health.
type HealthChecker interface {
	HealthCheck(ctx context.Context) error
}

type Characteristics struct {
	HasExternalBackoff     bool
	SupportsMessageLatency bool
}

// DispatchGate defines the interface to determine whether there is enough capacity to forward a request.
type DispatchGate interface {
	// Budget returns the Dispatch Budget in the range [0.0, 1.0], representing
	// the fraction of system capacity available for new requests.
	// A value of 0.0 indicates no available capacity (system at max allowed).
	// A value of 1.0 indicates full capacity available (system is idle).
	// The system always returns a valid value, even in case of internal error.
	Budget(ctx context.Context) float64
}

// AcquireResult contains the outcome of an AttributeGate.Acquire attempt.
type AcquireResult struct {
	// Allowed is true if the request should proceed.
	Allowed bool
	// Classification indicates the quota status of the request.
	Classification api.QuotaClassification
	// Release is a function to be called when processing is complete.
	// It may be nil if no quota was acquired.
	Release func()
}

// AttributeGate defines the interface to determine if a request is allowed based on its attributes.
type AttributeGate interface {
	// Acquire attempts to acquire quota for the given attributes.
	// If the gate does not support the given attributes or is not a quota gate,
	// it should return Allowed=true and ClassificationNone.
	Acquire(ctx context.Context, attributes map[string]string) (AcquireResult, error)
}

// GateFactory defines the interface for creating DispatchGate instances.
type GateFactory interface {
	CreateGate(gateType string, params map[string]string) (DispatchGate, error)
}

var _ DispatchGate = DispatchGateFunc(nil)

// DispatchGateFunc is a function type that implements DispatchGate.
type DispatchGateFunc func(context.Context) float64

func (f DispatchGateFunc) Budget(ctx context.Context) float64 {
	return f(ctx)
}

func ConstOpenGate() DispatchGate {
	return DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
}

type RequestMergePolicy interface {
	MergeRequestChannels(channels []RequestChannel, pools map[string]WorkerPoolConfig) PoolDispatch
}

// QueueBacklogStat reports the broker-side backlog for a single queue.
type QueueBacklogStat struct {
	QueueID   string
	QueueName string
	PoolName  string
	Depth     int64
}

// BacklogReporter is an optional capability for flows backed by a broker that
// exposes a queryable backlog (e.g. Redis sorted sets, GCP PubSub). Flows whose
// transport has no persisted queue (e.g. Redis Pub/Sub) do not implement it.
type BacklogReporter interface {
	// QueueBacklog returns the current backlog for each configured queue.
	QueueBacklog(ctx context.Context) ([]QueueBacklogStat, error)
}

type RequestChannel struct {
	Channel            chan *api.InternalRequest
	IGWBaseURL         string
	InferenceObjective string
	RequestPathURL     string
	Gate               DispatchGate
	WorkerPoolID       string
}

// PoolDispatch is the merge policy's output: one buffered channel per
// inference pool. Each channel carries fully-embellished messages
// destined for that pool's worker pool. Backpressure on one pool's
// channel does not affect other pools' channels — that isolation is
// the whole reason for the per-pool topology.
type PoolDispatch struct {
	Channels map[string]chan EmbelishedRequestMessage
}

type EmbelishedRequestChannel struct {
	Channel chan EmbelishedRequestMessage
}

// EmbelishedRequestMessage decorates an InternalRequest with HTTP dispatch context.
// The embedded InternalRequest must be non-nil in normal use.
// Caller-supplied metadata lives on the embedded Request (via ReqMetadata());
// there is no separate Metadata field here to avoid ambiguity.
type EmbelishedRequestMessage struct {
	*api.InternalRequest
	HttpHeaders  map[string]string
	RequestURL   string
	WorkerPoolID string
}

// RetryMessage carries an embellished request and backoff for re-queueing.
type RetryMessage struct {
	EmbelishedRequestMessage
	BackoffDurationSeconds float64
}
