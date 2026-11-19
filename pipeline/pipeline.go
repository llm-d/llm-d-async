package pipeline

import (
	"context"

	"github.com/llm-d/llm-d-async/api"
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

type RequestMergePolicy interface {
	MergeRequestChannels(channels []RequestChannel, pools map[string]WorkerPoolConfig) PoolDispatch
}

// DynamicRequestMergePolicy is an optional capability of RequestMergePolicy
// implementations that support taking on additional fan-in source channels at
// runtime, e.g. after a hot reload of a queue configuration. The per-pool
// merged channels returned by MergeRequestChannels stay stable for the
// lifetime of the policy; only the set of source channels feeding each pool
// changes. A source channel is removed from a pool by closing it: both
// policies treat a closed source channel as a removal and stop reading it.
type DynamicRequestMergePolicy interface {
	RequestMergePolicy
	// AddRequestChannels registers additional source channels, grouped by
	// each channel's WorkerPoolID. A channel referencing a pool unknown to
	// the policy is an error, in which case no channel is added. Adding a
	// channel already registered is a no-op.
	AddRequestChannels(channels []RequestChannel, pools map[string]WorkerPoolConfig) error
}

// ExpiringCount pairs a deadline-proximity bucket with the exact number of
// queued items whose deadline falls within it. Window is the histogram le
// bucket label (e.g. "0" for expired items, "60000" for up to 60s away) and
// counts are cumulative across buckets, matching le semantics.
type ExpiringCount struct {
	Window string
	Count  int64
}

// QueueBacklogStat reports the broker-side backlog for a single queue.
type QueueBacklogStat struct {
	QueueID         string
	QueueName       string
	PoolName        string
	Depth           int64
	SourceAvailable bool
	// ExpiringCounts holds exact cumulative per-bucket counts of queued items
	// nearing their deadline, ordered as the metric's bucket boundaries (most
	// urgent first). Nil when the broker cannot expose per-item deadlines
	// (e.g. Pub/Sub) or the read failed.
	ExpiringCounts []ExpiringCount
}

// BacklogReporter is an optional capability for flows backed by a broker that
// exposes a queryable backlog (e.g. Redis sorted sets, GCP PubSub). Flows whose
// transport has no persisted queue (e.g. Redis Pub/Sub) do not implement it.
type BacklogReporter interface {
	// QueueBacklog returns the current backlog for each configured queue.
	QueueBacklog(ctx context.Context) ([]QueueBacklogStat, error)
}

// CancellationCheckerProvider is an optional capability for flows that can
// surface per-request cancellation state to workers.
type CancellationCheckerProvider interface {
	CancellationChecker() api.CancellationChecker
}

type RequestChannel struct {
	Channel            chan *api.InternalRequest
	IGWBaseURL         string
	InferenceObjective string
	RequestPathURL     string
	Gate               Gate
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
