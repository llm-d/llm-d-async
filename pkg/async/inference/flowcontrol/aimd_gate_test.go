package flowcontrol

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestAIMDGate(now *time.Time) *AIMDGate {
	gate := NewAIMDGate(AIMDConfig{
		MinWindow:           1,
		MaxWindow:           64,
		Increase:            1.0,
		Decrease:            0.5,
		HoldDuration:        time.Second,
		QueueDurationTarget: 100 * time.Millisecond,
	})
	gate.now = func() time.Time { return *now }
	return gate
}

func bandRequest(tier string, classification api.QuotaClassification) *api.InternalRequest {
	labels := map[string]string{}
	if tier != "" {
		labels["tier"] = tier
	}
	ir := api.NewInternalRequest(api.InternalRouting{Labels: labels}, &api.RequestMessage{})
	ir.SetClassification(classification)
	return ir
}

func feedbackN(gate *AIMDGate, n int, fb pipeline.DispatchFeedback) {
	for range n {
		gate.ObserveOutcome(fb)
	}
}

func bandWindow(t *testing.T, gate *AIMDGate, key string) float64 {
	t.Helper()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	b, ok := gate.bands[key]
	require.True(t, ok, "band %s not found", key)
	return b.window
}

func TestAIMDGate_WindowLimitsInflightAndWakes(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	ctx := context.Background()
	msg := bandRequest("batch", api.ClassificationReserved)

	// Window starts at MinWindow=1: one slot, then park (reserved traffic
	// waits; overflow would be refused back to the broker).
	var releases []pipeline.GateReleaseFunc
	verdict, err := gate.Apply(ctx, msg, &releases)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)
	verdict, err = gate.Apply(ctx, msg, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionWait, verdict.Action)
	assert.Equal(t, 0.0, gate.Budget(ctx))

	// Releasing the slot reopens the window and wakes a parked worker.
	pipeline.ReleaseGateReleases(releases)
	select {
	case <-gate.WaitSignal():
	default:
		t.Fatal("expected a wake signal after release")
	}
	assert.Equal(t, 1.0, gate.Budget(ctx))
	verdict, err = gate.Apply(ctx, msg, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)
}

func TestAIMDGate_FullBandParksReservedRefusesOverflow(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	ctx := context.Background()
	reserved := bandRequest("batch", api.ClassificationReserved)
	overflow := bandRequest("batch", api.ClassificationOverflow)

	// Fill both bands' single MinWindow slot.
	var releases []pipeline.GateReleaseFunc
	for _, msg := range []*api.InternalRequest{reserved, overflow} {
		verdict, err := gate.Apply(ctx, msg, &releases)
		require.NoError(t, err)
		require.Equal(t, pipeline.ActionContinue, verdict.Action)
	}

	// Parking captures a shared pool worker, so it is reserved traffic's
	// privilege; sheddable traffic yields the worker and retries through
	// the broker.
	verdict, err := gate.Apply(ctx, reserved, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionWait, verdict.Action)
	verdict, err = gate.Apply(ctx, overflow, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)

	pipeline.ReleaseGateReleases(releases)
}

func TestAIMDGate_LowerBandRejectionIsLeadingSignalForHigher(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	batch := bandRequest("batch", api.ClassificationOverflow)
	interactive := bandRequest("interactive", api.ClassificationReserved)

	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
	assert.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)

	// A capacity rejection on the lowest band decreases only that band; the
	// higher band's window is untouched but held: accepts stop growing it.
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeRejectedCapacity})
	assert.InDelta(t, 4.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
	assert.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
	feedbackN(gate, 3, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)

	// Once the hold expires, growth resumes.
	now = now.Add(2 * time.Second)
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 9.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
}

func TestAIMDGate_HoldClearedByLowerBandAccept(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	batch := bandRequest("batch", api.ClassificationOverflow)
	interactive := bandRequest("interactive", api.ClassificationReserved)

	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeRejectedCapacity})

	// Held: an interactive accept inside the cooldown does not grow the window.
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	require.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)

	// A batch accept proves margin below interactive again: the hold clears
	// before the cooldown, and the next interactive accept grows the window.
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 9.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
}

func TestAIMDGate_StaleAcceptsDoNotClearHoldsOrGrow(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	batch := bandRequest("batch", api.ClassificationOverflow)
	interactive := bandRequest("interactive", api.ClassificationReserved)

	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeAccepted})

	// Congestion evidence at t=now: batch rejection decreases batch and
	// holds interactive.
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeRejectedCapacity})
	require.InDelta(t, 4.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// A batch accept whose request was sent BEFORE the rejection is
	// pre-congestion evidence: it must not clear interactive's hold, and it
	// must not regrow batch's window.
	stale := pipeline.DispatchFeedback{
		Msg:     batch,
		Outcome: pipeline.OutcomeAccepted,
		SentAt:  now.Add(-10 * time.Second),
	}
	gate.ObserveOutcome(stale)
	assert.InDelta(t, 4.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)

	// The same accept sent AFTER the rejection is post-congestion evidence:
	// it grows batch (congestion avoidance: 4 + 1/4) and clears
	// interactive's hold.
	fresh := stale
	fresh.SentAt = now.Add(10 * time.Millisecond)
	gate.ObserveOutcome(fresh)
	assert.InDelta(t, 4.25, bandWindow(t, gate, "overflow/batch"), 1e-9)
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 9.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
}

func TestAIMDGate_StaleRejectionsDoNotExtendHolds(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	batch := bandRequest("batch", api.ClassificationOverflow)
	interactive := bandRequest("interactive", api.ClassificationReserved)
	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeAccepted})

	// Fresh rejection at t0: batch decreases, interactive held until t0+1s.
	t0 := now
	feedbackN(gate, 1, pipeline.DispatchFeedback{
		Msg: batch, Outcome: pipeline.OutcomeRejectedCapacity, SentAt: now.Add(-time.Second),
	})

	// A trailing stale rejection 600ms later (sent before the decrease) is
	// the same congestion event: it must not re-arm interactive's hold.
	now = t0.Add(600 * time.Millisecond)
	feedbackN(gate, 1, pipeline.DispatchFeedback{
		Msg: batch, Outcome: pipeline.OutcomeRejectedCapacity, SentAt: t0.Add(-time.Second),
	})

	// Just past the ORIGINAL hold expiry, interactive grows again. If the
	// stale rejection had re-armed the hold (t0+1.6s), growth would still
	// be suppressed here.
	now = t0.Add(1100 * time.Millisecond)
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 9.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
}

func TestAIMDGate_TTLExpiryHoldsHigherBands(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	batch := bandRequest("batch", api.ClassificationOverflow)
	interactive := bandRequest("interactive", api.ClassificationReserved)

	feedbackN(gate, 9, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})

	// A queue-TTL expiry on batch gently shrinks batch and holds interactive.
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeTTLExpired})
	assert.InDelta(t, 9.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
}

func TestAIMDGate_HigherBandRejectionDecreasesLowerBands(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	ctx := context.Background()
	batch := bandRequest("batch", api.ClassificationOverflow)
	interactive := bandRequest("interactive", api.ClassificationReserved)

	feedbackN(gate, 15, pipeline.DispatchFeedback{Msg: batch, Outcome: pipeline.OutcomeAccepted})
	feedbackN(gate, 15, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeAccepted})

	// A rejection on the highest band means every band below it is at least
	// as saturated: both decrease.
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: interactive, Outcome: pipeline.OutcomeRejectedCapacity})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "reserved/interactive"), 1e-9)
	assert.InDelta(t, 8.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// A Retry-After close on the highest band closes the lower band too;
	// closed overflow traffic is refused back to the broker, not parked.
	now = now.Add(2 * time.Second)
	gate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg:        interactive,
		Outcome:    pipeline.OutcomeRejectedCapacity,
		RetryAfter: 5 * time.Second,
	})
	verdict, err := gate.Apply(ctx, batch, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)
	assert.InDelta(t, 1.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
}

func TestAIMDGate_SlowStartThenCongestionAvoidance(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	msg := bandRequest("async", api.ClassificationReserved)

	// Slow start: +Increase per accept.
	feedbackN(gate, 3, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 4.0, bandWindow(t, gate, "reserved/async"), 1e-9)

	// A congestion event sets ssthresh: window 4 -> 2, and growth switches
	// to additive increase (2 + 1/2 = 2.5).
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeRejectedCapacity})
	assert.InDelta(t, 2.0, bandWindow(t, gate, "reserved/async"), 1e-9)
	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 2.5, bandWindow(t, gate, "reserved/async"), 1e-9)

	// Growth caps at MaxWindow.
	feedbackN(gate, 100000, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 64.0, bandWindow(t, gate, "reserved/async"), 1e-9)
}

func TestAIMDGate_DecreasesCoalescePerFlight(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	msg := bandRequest("batch", api.ClassificationOverflow)
	feedbackN(gate, 15, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	require.InDelta(t, 16.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// A whole flight of rejections, all sent before the first decrease
	// lands, halves once: they are one congestion event.
	flightSentAt := now.Add(-time.Second)
	feedbackN(gate, 5, pipeline.DispatchFeedback{
		Msg:     msg,
		Outcome: pipeline.OutcomeRejectedCapacity,
		SentAt:  flightSentAt,
	})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// A rejection of a request sent after the decrease is a new event: the
	// shrunken window is still too big.
	feedbackN(gate, 1, pipeline.DispatchFeedback{
		Msg:     msg,
		Outcome: pipeline.OutcomeRejectedCapacity,
		SentAt:  now.Add(time.Millisecond),
	})
	assert.InDelta(t, 4.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
}

func TestAIMDGate_RetryAfterClosesAndReopensInSlowStart(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	ctx := context.Background()
	msg := bandRequest("batch", api.ClassificationOverflow)
	feedbackN(gate, 15, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})

	gate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg:        msg,
		Outcome:    pipeline.OutcomeRejectedCapacity,
		RetryAfter: 5 * time.Second,
	})

	// Fully closed, window back at the floor; overflow is refused, not parked.
	verdict, err := gate.Apply(ctx, msg, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionRefuse, verdict.Action)
	assert.Equal(t, 0.0, gate.Budget(ctx))
	assert.InDelta(t, 1.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// After Retry-After elapses the window reopens and grows in slow start
	// toward ssthresh (8 = 16 * 0.5), not from the old 16.
	now = now.Add(6 * time.Second)
	verdict, err = gate.Apply(ctx, msg, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionContinue, verdict.Action)
	feedbackN(gate, 4, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	assert.InDelta(t, 5.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
}

func TestAIMDGate_HeadroomCapsWindow(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	msg := bandRequest("batch", api.ClassificationOverflow)
	feedbackN(gate, 31, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	require.InDelta(t, 32.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// Advertised headroom of 3 with no in-flight caps the window at 3.
	gate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg:             msg,
		Outcome:         pipeline.OutcomeAccepted,
		BandHeadroom:    3,
		HasBandHeadroom: true,
	})
	assert.InDelta(t, 3.0+1.0/3.0, bandWindow(t, gate, "overflow/batch"), 1e-2)
}

func TestAIMDGate_QueueDurationHoldsThenShrinks(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	msg := bandRequest("batch", api.ClassificationOverflow)
	feedbackN(gate, 7, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	require.InDelta(t, 8.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// Queue duration at the target holds the window: no growth.
	gate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg:           msg,
		Outcome:       pipeline.OutcomeAccepted,
		QueueDuration: 150 * time.Millisecond,
	})
	assert.InDelta(t, 8.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	// Queue duration past twice the target gently shrinks it.
	gate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg:           msg,
		Outcome:       pipeline.OutcomeAccepted,
		QueueDuration: 250 * time.Millisecond,
	})
	assert.InDelta(t, 7.2, bandWindow(t, gate, "overflow/batch"), 1e-9)
}

func TestAIMDGate_TTLExpiryGentlyShrinks(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	msg := bandRequest("batch", api.ClassificationOverflow)
	feedbackN(gate, 9, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	require.InDelta(t, 10.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeTTLExpired})
	assert.InDelta(t, 9.0, bandWindow(t, gate, "overflow/batch"), 1e-9)
}

func TestAIMDGate_NonCapacityOutcomesLeaveWindowUnchanged(t *testing.T) {
	now := time.Unix(1000, 0)
	gate := newTestAIMDGate(&now)
	msg := bandRequest("batch", api.ClassificationOverflow)
	feedbackN(gate, 3, pipeline.DispatchFeedback{Msg: msg, Outcome: pipeline.OutcomeAccepted})
	require.InDelta(t, 4.0, bandWindow(t, gate, "overflow/batch"), 1e-9)

	for _, outcome := range []pipeline.DispatchOutcome{
		pipeline.OutcomeEvicted,
		pipeline.OutcomeRejectedOther,
		pipeline.OutcomeError,
	} {
		feedbackN(gate, 1, pipeline.DispatchFeedback{Msg: msg, Outcome: outcome})
		assert.InDelta(t, 4.0, bandWindow(t, gate, "overflow/batch"), 1e-9, "outcome %v", outcome)
	}
}

func TestGateFactory_AIMD(t *testing.T) {
	factory := NewGateFactory("")

	gate, err := factory.CreateGate(pipeline.GateConfig{
		GateType: "aimd",
		GateParams: map[string]any{
			"min_window":            2,
			"max_window":            16,
			"increase":              0.5,
			"decrease_factor":       0.25,
			"hold_duration":         "500ms",
			"tier_label":            "my_tier",
			"queue_duration_target": "200ms",
		},
		Owner: pipeline.GateOwner{WorkerPoolID: "pool-a"},
	})
	require.NoError(t, err)
	aimd, ok := gate.(*AIMDGate)
	require.True(t, ok)
	assert.Equal(t, 2.0, aimd.cfg.MinWindow)
	assert.Equal(t, 16.0, aimd.cfg.MaxWindow)
	assert.Equal(t, 0.5, aimd.cfg.Increase)
	assert.Equal(t, 0.25, aimd.cfg.Decrease)
	assert.Equal(t, 500*time.Millisecond, aimd.cfg.HoldDuration)
	assert.Equal(t, "my_tier", aimd.cfg.TierLabel)
	assert.Equal(t, 200*time.Millisecond, aimd.cfg.QueueDurationTarget)
	assert.Equal(t, "pool-a", aimd.cfg.PoolLabel)

	for _, params := range []map[string]any{
		{"min_window": 0},
		{"max_window": 0.5},
		{"increase": 0},
		{"decrease_factor": 1.0},
		{"queue_duration_target": "-1s"},
		{"hold_duration": "-1s"},
	} {
		_, err := factory.CreateGate(pipeline.GateConfig{GateType: "aimd", GateParams: params})
		assert.Error(t, err, "params %v", params)
	}
}
