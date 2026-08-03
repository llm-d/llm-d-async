package flowcontrol

import (
	"context"
	"sync"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
)

var _ pipeline.FeedbackGate = (*AIMDGate)(nil)
var _ pipeline.WaitNotifier = (*AIMDGate)(nil)

// gentleDecreaseFactor is applied on the soft congestion signals (queue
// duration past twice the target, queue-TTL expiry), which call for backing
// off well short of a full multiplicative decrease.
const gentleDecreaseFactor = 0.9

// AIMDGate sizes dispatch windows from gateway response feedback instead of
// scraped metrics. It keeps one congestion window per (tier, classification)
// band. The bands are coupled by the gateway's strict-priority dispatch
// order: a capacity rejection decreases its own band and every band below
// it, and holds growth in the bands above it, since saturation reaches lower
// priorities first and climbs.
//
// Each band's controller is TCP-shaped: slow start (window += increase per
// accept) below ssthresh, additive increase (window += increase/window) above
// it, multiplicative decrease on capacity rejections coalesced per flight of
// sends so one congestion event is counted once, and a full close for the
// duration of a server-specified Retry-After — after which the window reopens
// from the minimum in slow start. Advisory views refine this when present: advertised
// band headroom caps the window from above, and queue duration past the
// configured target holds or gently shrinks the window before rejections
// start. Evictions and non-capacity errors leave the window unchanged.
//
// The window bounds in-flight requests. When a band is full or closed, Apply
// parks the worker (ActionWait) for reserved requests — WaitSignal wakes a
// parked worker as soon as a slot frees or a window grows — and refuses
// overflow requests back to the broker (ActionRefuse), so sheddable backlog
// never captures pool workers that open bands need.
type AIMDGate struct {
	mu     sync.Mutex
	cfg    AIMDConfig
	bands  map[string]*aimdBand
	notify chan struct{}
	now    func() time.Time
}

type aimdBand struct {
	key string
	// rank orders bands the way the gateway dispatches them: 0 is highest
	// priority, larger is lower. Strict-priority dispatch couples the bands:
	// saturation reaches lower-ranked bands first, so their rejections lead
	// higher bands' rejections.
	rank         int
	window       float64
	ssthresh     float64
	inFlight     int
	lastDecrease time.Time
	closedUntil  time.Time
	// holdUntil suppresses window growth after a lower-priority band takes a
	// capacity rejection or TTL expiry: a leading congestion signal, not yet
	// this band's. An accept at any lower-priority band clears the hold
	// early, since that accept proves there is still margin below this band.
	// holdSetAt records when the evidence arrived: outcomes are unordered
	// with respect to sends, so only accepts sent after the evidence count
	// (an older in-flight request completing does not clear a newer hold).
	// lastDecrease doubles as the same recovery point for growth: accepts
	// sent before this band's latest decrease do not grow the window.
	holdUntil time.Time
	holdSetAt time.Time
}

// AIMDConfig holds the tunables for an AIMDGate.
type AIMDConfig struct {
	// MinWindow is the floor the window never decreases below. Keeping it at
	// least 1 means the gate always probes with one in-flight request rather
	// than freezing, so recovery is observed from responses.
	MinWindow float64
	// MaxWindow caps the window.
	MaxWindow float64
	// Increase is the additive step: per accepted response in slow start, and
	// scaled by the current window (window += Increase/window) above ssthresh.
	Increase float64
	// Decrease is the multiplicative factor applied on a capacity rejection.
	// Decreases are coalesced per flight of sends (one per congestion event),
	// not per wall-clock interval.
	Decrease float64
	// HoldDuration is how long a band suppresses growth after congestion
	// evidence from a lower-priority band, unless an accept below clears the
	// hold earlier.
	HoldDuration time.Duration
	// TierLabel is the message label carrying the tier; defaults to "tier".
	TierLabel string
	// QueueDurationTarget enables the delay-based early-warning signal: an
	// accepted response whose queue duration exceeds the target holds the
	// window instead of growing it, and one past twice the target gently
	// shrinks it. Zero disables the signal.
	QueueDurationTarget time.Duration
	// PoolLabel labels this gate's band-state gauges. The factory stamps it
	// from GateConfig.Owner.WorkerPoolID so the series carry the same
	// pool_name as the owning pool's other metrics; it is not user config.
	PoolLabel string
}

// NewAIMDGate creates an AIMDGate with the given configuration. Each band's
// window starts at MinWindow and opens in slow start.
func NewAIMDGate(cfg AIMDConfig) *AIMDGate {
	if cfg.TierLabel == "" {
		cfg.TierLabel = "tier"
	}
	return &AIMDGate{
		cfg:    cfg,
		bands:  make(map[string]*aimdBand),
		notify: make(chan struct{}, 1),
		now:    time.Now,
	}
}

// bandKey keys bands on the same (classification, tier) lane order the
// tier-priority merge policy dispatches in, via the shared api.LaneRank and
// api.LaneLabel mapping: unknown tiers default to batch, and messages
// without a reserved classification count as overflow.
func (g *AIMDGate) bandKey(msg *api.InternalRequest) (string, int) {
	var tier api.PriorityTier
	class := api.ClassificationOverflow
	if msg != nil {
		tier = api.PriorityTier(msg.Labels[g.cfg.TierLabel])
		class = msg.GetClassification()
	}
	rank := api.LaneRank(tier, class)
	return api.LaneLabel(rank), rank
}

// band returns the controller for the message's band, creating it at the
// minimum window in slow start. Callers must hold g.mu.
func (g *AIMDGate) band(msg *api.InternalRequest) *aimdBand {
	key, rank := g.bandKey(msg)
	b, ok := g.bands[key]
	if !ok {
		b = &aimdBand{key: key, rank: rank, window: g.cfg.MinWindow, ssthresh: g.cfg.MaxWindow}
		g.bands[key] = b
	}
	return b
}

// export publishes a band's controller state as gauges. Callers must hold
// g.mu.
func (g *AIMDGate) export(b *aimdBand) {
	metrics.SetAIMDBandState(g.cfg.PoolLabel, b.key, b.window, b.ssthresh, b.inFlight)
}

// Budget implements pipeline.Gate. It reports the widest open fraction across
// bands, since a request for any band can still dispatch through it.
func (g *AIMDGate) Budget(ctx context.Context) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.bands) == 0 {
		return 1.0
	}
	now := g.now()
	budget := 0.0
	for _, b := range g.bands {
		if now.Before(b.closedUntil) || float64(b.inFlight) >= b.window {
			continue
		}
		if f := (b.window - float64(b.inFlight)) / b.window; f > budget {
			budget = f
		}
	}
	return budget
}

// Apply implements pipeline.Gate. It admits the request if its band's window
// has room, registering a release that frees the slot when the dispatch
// attempt completes. When the band is full or closed, reserved requests park
// the worker (ActionWait) and overflow requests yield it (ActionRefuse):
// parking captures shared pool capacity, so it is a privilege of
// non-sheddable traffic, while sheddable requests survive in the broker and
// retry — the rejection cost they were admitted to pay.
func (g *AIMDGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	g.mu.Lock()
	b := g.band(msg)

	if g.now().Before(b.closedUntil) || float64(b.inFlight) >= b.window {
		g.mu.Unlock()
		if msg != nil && msg.GetClassification() == api.ClassificationReserved {
			return pipeline.Wait(), nil
		}
		return pipeline.Refuse(), nil
	}

	b.inFlight++
	g.export(b)
	g.mu.Unlock()

	if releases != nil {
		*releases = append(*releases, func() {
			g.mu.Lock()
			b.inFlight--
			g.export(b)
			g.mu.Unlock()
			g.wake()
		})
	}

	return pipeline.Continue(), nil
}

// ObserveOutcome implements pipeline.FeedbackGate. Feedback without a Msg is
// dropped: it carries no band attribution, and defaulting it to the lowest
// band would decrease that band and hold every band above on a signal that
// belongs to none of them.
func (g *AIMDGate) ObserveOutcome(fb pipeline.DispatchFeedback) {
	if fb.Msg == nil {
		return
	}
	g.mu.Lock()
	b := g.band(fb.Msg)
	now := g.now()

	// Advertised headroom caps the window from above regardless of outcome:
	// the band cannot hold more than our in-flight plus what it reports free.
	if fb.HasBandHeadroom {
		if ceiling := max(g.cfg.MinWindow, float64(b.inFlight)+fb.BandHeadroom); b.window > ceiling {
			b.window = ceiling
			b.ssthresh = min(b.ssthresh, ceiling)
		}
	}

	switch fb.Outcome {
	case pipeline.OutcomeAccepted:
		// This accept proves the pool still had margin below every band
		// above this one when it dispatched: clear their holds rather than
		// waiting out the timer, but only where the request was sent after
		// the hold's evidence — an older in-flight request completing says
		// nothing about conditions after the congestion event. In stable
		// partial saturation (low bands rejecting, a middle band flowing),
		// the flowing band keeps the bands above it growing.
		for _, other := range g.bands {
			if other.rank < b.rank && now.Before(other.holdUntil) && sentAfter(fb.SentAt, other.holdSetAt) {
				other.holdUntil = time.Time{}
			}
		}
		target := g.cfg.QueueDurationTarget
		switch {
		case target > 0 && fb.QueueDuration >= 2*target:
			// Queue time far past target: congestion is building ahead of
			// any rejection; back off gently.
			g.decrease(b, now, gentleDecreaseFactor, fb.SentAt)
		case target > 0 && fb.QueueDuration >= target:
			// Queue time past target: hold the window.
		case now.Before(b.holdUntil):
			// A lower-priority band was recently rejected: saturation is
			// climbing the priority ladder, so hold rather than probe upward.
		case !sentAfter(fb.SentAt, b.lastDecrease):
			// Sent before this band's latest decrease: pre-congestion
			// evidence, which must not reinflate the window (TCP's recovery
			// point). Growth resumes with the first post-decrease send.
		case b.window < b.ssthresh:
			// Slow start: additive per accept compounds to doubling per
			// window's worth of accepts.
			b.window = min(g.cfg.MaxWindow, b.window+g.cfg.Increase)
		default:
			// Congestion avoidance: roughly +Increase per full window.
			b.window = min(g.cfg.MaxWindow, b.window+g.cfg.Increase/b.window)
		}
	case pipeline.OutcomeRejectedCapacity:
		// Evaluated before the decrease moves the recovery point: a
		// rejection sent before the previous decrease belongs to the
		// congestion event already acted on, and must not re-arm holds or
		// re-propagate — otherwise a flight of stale rejections keeps
		// extending higher bands' holds past the event.
		newEvent := sentAfter(fb.SentAt, b.lastDecrease)
		g.decrease(b, now, g.cfg.Decrease, fb.SentAt)
		if fb.RetryAfter > 0 {
			// A full close is an explicit server instruction, so it is not
			// coalesced: the window reopens from the floor in slow start
			// toward ssthresh.
			if reopen := now.Add(fb.RetryAfter); reopen.After(b.closedUntil) {
				b.closedUntil = reopen
			}
			b.window = g.cfg.MinWindow
		}
		if !newEvent {
			break
		}
		// Strict-priority dispatch orders the bands' fates. A rejection here
		// means every lower-priority band is at least as saturated, so they
		// decrease (and inherit a full close) without waiting to observe
		// their own rejections. For higher-priority bands the same rejection
		// is a leading signal: their margin is shrinking but not gone, so
		// they hold growth for the hold duration instead of decreasing.
		for _, other := range g.bands {
			switch {
			case other == b:
			case other.rank > b.rank:
				g.decrease(other, now, g.cfg.Decrease, fb.SentAt)
				if b.closedUntil.After(now) && b.closedUntil.After(other.closedUntil) {
					other.closedUntil = b.closedUntil
					other.window = g.cfg.MinWindow
				}
			default:
				g.hold(other, now)
			}
		}
	case pipeline.OutcomeTTLExpired:
		// Requests expiring in the gateway queue mean the window outruns the
		// deadline budgets behind it. Starvation reaches the lowest bands
		// first under priority-descending dispatch, so the expiry is also a
		// leading signal for the bands above this one — subject to the same
		// per-flight coalescing as rejections.
		newEvent := sentAfter(fb.SentAt, b.lastDecrease)
		g.decrease(b, now, gentleDecreaseFactor, fb.SentAt)
		if newEvent {
			for _, other := range g.bands {
				if other != b && other.rank < b.rank {
					g.hold(other, now)
				}
			}
		}
	case pipeline.OutcomeEvicted, pipeline.OutcomeRejectedOther, pipeline.OutcomeError:
		// Evictions are a cost signal, not a saturation signal; non-capacity
		// rejections and errors carry no capacity information.
		// TODO(aimd): sustained eviction rate in a band means overcommitted
		// accepts overstate capacity there; damp the hold-clearing inference
		// from that band's accepts once eviction traffic exists to measure.
	}
	g.export(b)
	g.mu.Unlock()
	g.wake()
}

// hold suppresses growth on a higher-priority band after congestion evidence
// below it, until the cooldown elapses or an accept at a lower-priority band
// clears it. Callers must hold g.mu.
func (g *AIMDGate) hold(o *aimdBand, now time.Time) {
	if until := now.Add(g.cfg.HoldDuration); until.After(o.holdUntil) {
		o.holdUntil = until
	}
	o.holdSetAt = now
}

// sentAfter reports whether a request sent at sentAt postdates the evidence
// recorded at eventAt. A zero sentAt means the send time is unknown and is
// treated as current.
func sentAfter(sentAt, eventAt time.Time) bool {
	return sentAt.IsZero() || sentAt.After(eventAt)
}

// decrease applies a multiplicative decrease and exits slow start, at most
// once per flight of sends: evidence from a request sent before the previous
// decrease belongs to the congestion event already acted on. The flight is
// the dimensionless coalescing unit — it scales with both concurrency and
// generation time, where a wall-clock interval cannot. Callers must hold
// g.mu.
//
// TODO(aimd): a single rejection still costs the full factor regardless of
// window size, so one marginal rejection discards half of a large window.
// DCTCP's proportional decrease (window *= 1 - alpha/2, with alpha an EWMA of
// the rejected fraction per flight) weights the evidence by sample size;
// adopt it if benchmarks show over-reaction at high concurrency.
func (g *AIMDGate) decrease(b *aimdBand, now time.Time, factor float64, sentAt time.Time) {
	if !sentAfter(sentAt, b.lastDecrease) {
		return
	}
	b.ssthresh = max(g.cfg.MinWindow, b.window*factor)
	b.window = b.ssthresh
	b.lastDecrease = now
}

// WaitSignal implements pipeline.WaitNotifier.
func (g *AIMDGate) WaitSignal() <-chan struct{} {
	return g.notify
}

// wake offers one wake token, shared across all bands: a freed slot in one
// band can wake a worker parked on another, which re-applies and parks again.
// Wakes are hints that shorten the poll interval, not per-band delivery;
// workers rely on their fallback poll timer for the rest.
func (g *AIMDGate) wake() {
	select {
	case g.notify <- struct{}{}:
	default:
	}
}
