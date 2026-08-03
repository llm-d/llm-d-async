package pipeline

import (
	"time"

	"github.com/llm-d/llm-d-async/api"
)

// DispatchOutcome classifies the gateway's response to a dispatched request
// for gates that adjust their budget from response feedback.
type DispatchOutcome int

const (
	// OutcomeAccepted indicates the gateway accepted and served the request.
	OutcomeAccepted DispatchOutcome = iota
	// OutcomeRejectedCapacity indicates the gateway rejected the request
	// because the pool lacked capacity (a congestion signal).
	OutcomeRejectedCapacity
	// OutcomeTTLExpired indicates the request expired in the gateway's queue
	// before dispatch (a sign the sender's window exceeds the deadline
	// budgets behind it).
	OutcomeTTLExpired
	// OutcomeEvicted indicates the gateway admitted the request and then
	// revoked it in flight (a cost signal, not a saturation signal).
	OutcomeEvicted
	// OutcomeRejectedOther indicates the gateway rejected the request for a
	// reason unrelated to pool capacity.
	OutcomeRejectedOther
	// OutcomeError indicates a transport or server error carrying no
	// capacity signal.
	OutcomeError
)

// DispatchFeedback carries one response's outcome and any advisory capacity
// signals back to a feedback gate. The zero value of every advisory field
// means "no signal".
type DispatchFeedback struct {
	// Msg is the dispatched request; gates use it to key per-band state.
	// Required: feedback with a nil Msg has no band attribution and gates
	// drop it.
	Msg *api.InternalRequest
	// Outcome classifies the response.
	Outcome DispatchOutcome
	// SentAt is when the request was sent. Outcomes arrive out of order with
	// respect to sends, and an accept only evidences conditions at or after
	// its send time; gates use SentAt to ignore accepts that predate newer
	// congestion evidence. Zero means unknown and is treated as current.
	// Producers must stamp SentAt: with a zero SentAt the gate stays live,
	// but rejection bursts are not coalesced — each rejection counts as a
	// separate congestion event and collapses the window multiplicatively.
	SentAt time.Time
	// RetryAfter is the server-specified retry delay, if the response carried one.
	RetryAfter time.Duration
	// QueueDuration is the time the request spent queued in the gateway
	// before dispatch; zero means no signal.
	QueueDuration time.Duration
	// BandHeadroom is the remaining queue capacity, in requests, of the band
	// the request occupied. Valid only when HasBandHeadroom is true.
	BandHeadroom float64
	// HasBandHeadroom reports whether the response carried a headroom signal.
	HasBandHeadroom bool
}

// FeedbackGate is a Gate that adjusts its dispatch budget from per-response
// outcomes reported by the worker. Gates that wrap other gates must forward
// outcomes to feedback-consuming inner gates (and likewise expose their
// WaitNotifier signal), or wrapping silently severs the feedback loop.
type FeedbackGate interface {
	Gate
	// ObserveOutcome reports the outcome of one dispatched request.
	ObserveOutcome(fb DispatchFeedback)
}

// WaitNotifier is optionally implemented by gates whose Wait verdicts can be
// woken by an event (a freed slot, a window increase) instead of only a timer.
type WaitNotifier interface {
	// WaitSignal returns a channel that receives when the gate may admit a
	// previously parked request. One receive wakes one waiter; wakes are
	// hints shared across the gate (a wake may belong to a different band's
	// slot), may be spurious, and waiters keep a fallback poll timer.
	//
	// WaitSignal may return nil when the gate has no wake support (e.g. a
	// wrapper whose inner gates do not notify). A receive from a nil channel
	// blocks forever, so callers must treat nil as poll-only rather than
	// selecting on it exclusively.
	WaitSignal() <-chan struct{}
}
