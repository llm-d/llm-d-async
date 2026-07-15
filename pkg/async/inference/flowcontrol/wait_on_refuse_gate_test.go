package flowcontrol

import (
	"context"
	"errors"
	"testing"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestWaitOnRefuseGate_Budget(t *testing.T) {
	inner := &mockGate{budget: 0.7}
	gate := NewWaitOnRefuseGate(inner)
	assert.Equal(t, 0.7, gate.Budget(context.Background()))
}

func TestWaitOnRefuseGate_Apply(t *testing.T) {
	t.Run("Override Refuse to Wait", func(t *testing.T) {
		inner := &mockGate{verdict: pipeline.Refuse()}
		gate := NewWaitOnRefuseGate(inner)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionWait, res.Action)
	})

	t.Run("Keep Continue", func(t *testing.T) {
		inner := &mockGate{verdict: pipeline.Continue()}
		gate := NewWaitOnRefuseGate(inner)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionContinue, res.Action)
	})

	t.Run("Keep Wait", func(t *testing.T) {
		inner := &mockGate{verdict: pipeline.Wait()}
		gate := NewWaitOnRefuseGate(inner)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionWait, res.Action)
	})

	t.Run("Keep Drop", func(t *testing.T) {
		inner := &mockGate{verdict: pipeline.Verdict{Action: pipeline.ActionDrop}}
		gate := NewWaitOnRefuseGate(inner)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionDrop, res.Action)
	})

	t.Run("Error propagation", func(t *testing.T) {
		inner := &mockGate{err: errors.New("underlying failure")}
		gate := NewWaitOnRefuseGate(inner)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		var releases []pipeline.GateReleaseFunc
		_, err := gate.Apply(context.Background(), msg, &releases)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "underlying failure")
	})
}
