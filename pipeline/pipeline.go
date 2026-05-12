package pipeline

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/api"
)

type Flow interface {
	Characteristics() Characteristics
	Start(ctx context.Context)
	RequestChannels() []RequestChannel
	// Pools returns the inference-pool definitions the Flow knows about.
	// One Pool per distinct destination. Flows that don't model pools
	// natively (e.g. simple single-endpoint redis flows) may return a
	// single synthesized pool. main.go uses this to spawn per-pool
	// worker pools at startup.
	Pools() []Pool
	RetryChannel() chan RetryMessage
	ResultChannel() chan api.ResultMessage
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

// AttributeGate defines the interface to determine if a request is allowed based on its attributes.
type AttributeGate interface {
	// Acquire attempts to acquire quota for the given attributes.
	// Returns allowed=true if successful, and a release function to be called when processing is complete.
	// If the gate does not support the given attributes or is not a quota gate, it should return true, nil, nil.
	Acquire(ctx context.Context, attributes map[string]string) (allowed bool, release func(), err error)
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
	// MergeRequestChannels fans incoming per-subscription channels into
	// one channel per inference pool. Returned PoolDispatch.Channels is
	// keyed by Pool.ID; the merge policy guarantees that every message
	// emitted on Channels[id] has msg.PoolID == id, so consumers can
	// dispatch without re-routing. Pools with no incoming subscriptions
	// may be absent from the map; main.go can spawn pool-pool workers
	// either by walking the dispatch keys or by walking the Flow's
	// declared pools (and skipping ones with no subscribers).
	MergeRequestChannels(channels []RequestChannel) PoolDispatch
}

// PoolDispatch is the merge policy's output: one buffered channel per
// inference pool. Each channel carries fully-embellished messages
// destined for that pool's worker pool. Backpressure on one pool's
// channel does not affect other pools' channels — that isolation is
// the whole reason for the per-pool topology.
type PoolDispatch struct {
	Channels map[string]chan EmbelishedRequestMessage
}

type RequestChannel struct {
	// Channel carries fully-embellished messages from the Flow's pull
	// callback to the merge policy. Messages arrive with all HTTP
	// dispatch fields set and subscription-gate releases attached so
	// those releases survive to message terminal in the worker.
	Channel            chan *EmbelishedRequestMessage
	IGWBaseURL         string
	InferenceObjective string
	RequestPathURL     string
	HTTPHeaders        map[string]string
	ModelNameOverride  string
	Gate               DispatchGate
	// Labels is the static label set declared by the subscription's
	// TopicConfig. It is the operator's source of truth for per-subscription
	// classification (e.g. tier, team, model). Flow impls seed each pulled
	// message's Labels by merging this map over the transport's per-message
	// kv with these labels winning on key collision.
	Labels Labels
	// PoolID identifies which inference pool this subscription routes to.
	// The merge policy uses PoolID to fan out into per-pool channels;
	// the per-pool worker pool consumes from its pool's channel and
	// runs the pool's gate chain. Empty means "default pool" — a
	// synthesized single-pool topology preserves the pre-pool-topology
	// behavior.
	PoolID string
}

// EmbelishedRequestMessage decorates an InternalRequest with HTTP dispatch context.
// The embedded InternalRequest must be non-nil in normal use.
// Caller-supplied metadata lives on the embedded Request (via ReqMetadata());
// there is no separate Metadata field here to avoid ambiguity.
type EmbelishedRequestMessage struct {
	*api.InternalRequest
	HttpHeaders       map[string]string
	RequestURL        string
	Gate              DispatchGate
	ModelNameOverride string
	// Labels is the message's working label set. Seeded by the Flow at pull
	// time from a merge of the originating channel's static labels and the
	// transport's per-message kv (subscription wins on key collision).
	// Gates may mutate this map in place; the merge policy reads it to drive
	// dispatch, header injection, and routing decisions.
	Labels Labels
	// PoolID identifies the inference pool this message routes to. Set by
	// the Flow at pull time from the originating RequestChannel.PoolID.
	// The merge policy uses this to route the message to the right
	// per-pool channel; the per-pool worker pool inherits the pool's
	// gate chain.
	PoolID string
	// releases accumulates Release callbacks attached by gates and the merge
	// policy as the message moves through the pipeline. They are fired in
	// LIFO order by FireReleases when the message terminates (worker
	// completion, fail-fast in the merge policy, or Drop/Refuse in the Flow).
	releases []Release
}

// Release is a callback invoked when a message terminates. It returns any
// state taken by a gate (e.g. an in-flight slot in a reservation counter).
type Release func()

// AttachRelease appends r to the message's release stack. A nil r is a
// no-op so call sites need not branch on whether their gate emitted state.
func (m *EmbelishedRequestMessage) AttachRelease(r Release) {
	if r == nil {
		return
	}
	m.releases = append(m.releases, r)
}

// FireReleases invokes every accumulated Release in LIFO order and clears
// the stack. Safe to call exactly once at message termination; subsequent
// calls are no-ops. The releases must therefore terminate the message's
// hold on any external state (counters, slots, leases) without further
// reference to m.
func (m *EmbelishedRequestMessage) FireReleases() {
	for i := len(m.releases) - 1; i >= 0; i-- {
		if r := m.releases[i]; r != nil {
			r()
		}
	}
	m.releases = nil
}

// RetryMessage carries an embellished request and backoff for re-queueing.
type RetryMessage struct {
	EmbelishedRequestMessage
	BackoffDurationSeconds float64
	RetryReason            string
}
