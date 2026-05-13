package pipeline

import (
	"context"
	"errors"

	"github.com/llm-d-incubation/llm-d-async/api"
)

// Verdict is what a Gate's Apply returns. It encodes what the transport-side
// translator (Flow callback for subscription gates, worker pool for pool
// gates) should do with the message:
//
//   - Terminate == false: forward the message to the next stage (next gate,
//     or downstream channel). Result and Redeliver are ignored.
//   - Terminate && !Redeliver: the message is acked-and-consumed. If
//     Result is non-nil, the translator publishes it on the result topic
//     before acking. Use for stateless drops (deadline expired) or for
//     fail-fast (publish a 429-shaped result and ack).
//   - Terminate && Redeliver: the message is nacked. The transport's
//     redelivery policy (PubSub exponential backoff, redis-sortedset
//     retry-with-backoff) decides when it comes back. Use for backpressure /
//     over-cap cases.
//
// Verdict carries no Release. Gates that take state call msg.AttachRelease(r)
// directly inside Apply; ApplyChain manages release lifecycle via a snapshot
// of len(msg.releases) on entry.
//
// Construct Verdicts via the named helpers (Continue, Drop, Refuse) — they
// keep call sites readable at the intent level.
type Verdict struct {
	Terminate bool
	Redeliver bool
	Result    *api.ResultMessage
}

// Continue is the zero-value Verdict: forward to the next stage.
var Continue = Verdict{}

// Drop terminates the message without redelivery. If r is non-nil the
// transport-side translator publishes it on the result topic before acking.
// Use nil for silent drops (deadline expired); use a typed payload for
// fail-fast (429-shaped result).
func Drop(r *api.ResultMessage) Verdict {
	return Verdict{Terminate: true, Result: r}
}

// Refuse terminates the message and requests redelivery. The transport's
// redelivery policy decides timing — gates do not specify backoff.
func Refuse() Verdict {
	return Verdict{Terminate: true, Redeliver: true}
}

// Release returns any state taken by a gate (e.g. an in-flight slot in a
// reservation counter). Gates attach releases by calling
// msg.AttachRelease(r) directly inside Apply; releases fire when the message
// terminates (worker completion, Terminate verdict from a chain). A gate
// that takes no state attaches no release.
type Release func()

// Gate is the framework's per-message admission and labeling primitive.
// Gates are evaluated in chains, in configured order. Each gate may:
//
//   - read msg.Labels to make a decision
//   - mutate msg.Labels in place to attach classification or other metadata
//   - take state in some external counter, attaching a Release via
//     msg.AttachRelease
//   - return a Verdict (Continue / Drop / Refuse)
//
// On Continue, the chain advances to the next gate. On a Terminate verdict
// (Drop or Refuse), the chain runner fires any releases the chain itself
// attached (snapshot-truncate, LIFO), then returns the verdict to the
// transport-side translator.
//
// Apply must be safe to call concurrently for distinct messages.
type Gate interface {
	Apply(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error)
}

// GateFunc is a function adapter so a plain function can satisfy Gate.
type GateFunc func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error)

// Apply implements Gate.
func (f GateFunc) Apply(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
	return f(ctx, msg)
}

// AlwaysContinue is a Gate that admits every message without taking state.
// Useful as a default and in tests.
var AlwaysContinue Gate = GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
	return Continue, nil
})

// ApplyChain runs gates in order. Each gate's Verdict is authoritative
// for control flow: the chain short-circuits only on a Terminate verdict
// (Drop or Refuse); a Continue verdict — even alongside a non-nil error —
// advances to the next gate.
//
// Errors from gates are informational, not control signals. A gate that
// fails open (e.g. reservation-classifier on redis outage, tier-priority
// admission on stale Prometheus) returns Continue with an error so the
// caller can log/meter the degradation without diverting traffic. A gate
// that wants to nack on error returns Refuse explicitly. Errors from all
// gates that ran are joined via errors.Join and returned alongside the
// final Verdict; the caller is responsible for logging.
//
// Releases that gates attached during this chain run fire in LIFO order
// before a Terminate verdict returns; on Continue across all gates,
// chain-attached releases stay on the message and fire at terminal via
// FireReleases.
//
// On nil or empty gates, returns (Continue, nil) — equivalent to the
// always-open gate.
func ApplyChain(ctx context.Context, msg *EmbelishedRequestMessage, gates []Gate) (Verdict, error) {
	if len(gates) == 0 {
		return Continue, nil
	}
	// Snapshot the message's release stack so we can fire-and-truncate
	// only the releases this chain attached on short-circuit. Releases
	// attached upstream of this chain stay intact.
	snapshot := len(msg.releases)
	fireChainReleases := func() {
		for i := len(msg.releases) - 1; i >= snapshot; i-- {
			if r := msg.releases[i]; r != nil {
				r()
			}
		}
		msg.releases = msg.releases[:snapshot]
	}
	var errs []error
	for _, g := range gates {
		v, err := g.Apply(ctx, msg)
		if err != nil {
			errs = append(errs, err)
		}
		if v.Terminate {
			fireChainReleases()
			return v, errors.Join(errs...)
		}
	}
	return Continue, errors.Join(errs...)
}
