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

func newForwardingAIMDGate() *AIMDGate {
	return NewAIMDGate(AIMDConfig{
		MinWindow:    1,
		MaxWindow:    8,
		Increase:     1.0,
		Decrease:     0.5,
		HoldDuration: time.Second,
	})
}

func aimdWindow(t *testing.T, gate *AIMDGate, msg *api.InternalRequest) float64 {
	t.Helper()
	key, _ := gate.bandKey(msg)
	gate.mu.Lock()
	defer gate.mu.Unlock()
	b, ok := gate.bands[key]
	require.True(t, ok, "band %s not found", key)
	return b.window
}

func TestCompositeGate_ForwardsFeedbackAndWait(t *testing.T) {
	aimd := newForwardingAIMDGate()
	var composite pipeline.Gate = NewCompositeGate(ConstOpenGate(), aimd)
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})

	// Wrapping must not sever feedback: outcomes reach the inner aimd gate.
	feedbackGate, ok := composite.(pipeline.FeedbackGate)
	require.True(t, ok, "composite must forward feedback")
	feedbackGate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg: msg, Outcome: pipeline.OutcomeAccepted,
	})
	assert.InDelta(t, 2.0, aimdWindow(t, aimd, msg), 1e-9)

	// And the wait signal passes through: the same ObserveOutcome woke it.
	notifier, ok := composite.(pipeline.WaitNotifier)
	require.True(t, ok, "composite must forward wait signals")
	require.NotNil(t, notifier.WaitSignal())
	select {
	case <-notifier.WaitSignal():
	default:
		t.Fatal("expected a wake signal through the composite")
	}
}

func TestCompositeGate_WaitSignalFanIn(t *testing.T) {
	first := newForwardingAIMDGate()
	second := newForwardingAIMDGate()
	composite := NewCompositeGate(first, second)
	t.Cleanup(func() { _ = composite.Close() })
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})

	signal := composite.WaitSignal()
	require.NotNil(t, signal)

	// A wake from either inner gate is observable on the fan-in.
	second.ObserveOutcome(pipeline.DispatchFeedback{
		Msg: msg, Outcome: pipeline.OutcomeAccepted,
	})
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("expected the second gate's wake through the fan-in")
	}
}

func TestCompositeGate_NoNotifiersMeansNilSignal(t *testing.T) {
	composite := NewCompositeGate(ConstOpenGate(), &mockGate{budget: 1.0})
	assert.Nil(t, composite.WaitSignal())
	// Forwarding to non-feedback inner gates is a no-op, not a panic.
	composite.ObserveOutcome(pipeline.DispatchFeedback{Outcome: pipeline.OutcomeAccepted})
}

func TestWaitOnRefuseGate_ForwardsFeedbackAndWait(t *testing.T) {
	aimd := newForwardingAIMDGate()
	var wrapped pipeline.Gate = NewWaitOnRefuseGate(aimd)
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{})

	feedbackGate, ok := wrapped.(pipeline.FeedbackGate)
	require.True(t, ok)
	feedbackGate.ObserveOutcome(pipeline.DispatchFeedback{
		Msg: msg, Outcome: pipeline.OutcomeAccepted,
	})
	assert.InDelta(t, 2.0, aimdWindow(t, aimd, msg), 1e-9)

	notifier, ok := wrapped.(pipeline.WaitNotifier)
	require.True(t, ok)
	assert.NotNil(t, notifier.WaitSignal())

	// A non-notifying inner gate degrades to no wake support, no panics.
	plain := NewWaitOnRefuseGate(&mockGate{budget: 1.0})
	assert.Nil(t, plain.WaitSignal())
	plain.ObserveOutcome(pipeline.DispatchFeedback{Outcome: pipeline.OutcomeAccepted})
}

func TestAIMDGate_NilMsgFeedbackIsDropped(t *testing.T) {
	gate := newForwardingAIMDGate()

	// Feedback without a Msg has no band attribution: it must not create or
	// mutate any band (defaulting it to the lowest band would hold every
	// band above on a signal that belongs to none of them).
	gate.ObserveOutcome(pipeline.DispatchFeedback{Outcome: pipeline.OutcomeRejectedCapacity})
	gate.mu.Lock()
	defer gate.mu.Unlock()
	assert.Empty(t, gate.bands)
}

func TestTierPriorityAdmissionGate_WaitingSaturationGateIsSaturated(t *testing.T) {
	// A feedback gate signals fullness with Wait, not Refuse; the admission
	// gate must read both as saturation instead of admitting unbounded.
	waiting := &mockGate{budget: 0.0, verdict: pipeline.Wait()}
	gate := NewTierPriorityAdmissionGate(waiting, "tier")

	reserved := api.NewInternalRequest(api.InternalRouting{
		Labels: map[string]string{"tier": "interactive"},
	}, &api.RequestMessage{ID: "r"})
	reserved.SetClassification(api.ClassificationReserved)
	verdict, err := gate.Apply(context.Background(), reserved, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionWait, verdict.Action)

	overflow := api.NewInternalRequest(api.InternalRouting{
		Labels: map[string]string{"tier": "interactive"},
	}, &api.RequestMessage{ID: "o"})
	overflow.SetClassification(api.ClassificationOverflow)
	verdict, err = gate.Apply(context.Background(), overflow, nil)
	require.NoError(t, err)
	assert.Equal(t, pipeline.ActionDrop, verdict.Action)
}
