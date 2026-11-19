package flowcontrol

import (
	"context"
	"testing"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestTierPriorityAdmissionGate_Budget(t *testing.T) {
	satGate := DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
	gate := NewTierPriorityAdmissionGate(satGate, "tier")
	assert.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestTierPriorityAdmissionGate_Apply(t *testing.T) {
	satGate := DispatchGateFunc(func(ctx context.Context) float64 { return 0.0 })   // Always returns Refuse (Saturated)
	unsatGate := DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 }) // Always returns Continue (Unsaturated)

	t.Run("Not saturated - Continue", func(t *testing.T) {
		gate := NewTierPriorityAdmissionGate(unsatGate, "tier")
		msg := api.NewInternalRequest(api.InternalRouting{
			Labels: map[string]string{
				"tier": "interactive",
			},
		}, &api.RequestMessage{ID: "req"})
		msg.SetClassification(api.ClassificationReserved)

		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionContinue, res.Action)
	})

	t.Run("Saturated - Reserved - Wait", func(t *testing.T) {
		gate := NewTierPriorityAdmissionGate(satGate, "tier")
		msg := api.NewInternalRequest(api.InternalRouting{
			Labels: map[string]string{
				"tier": "interactive",
			},
		}, &api.RequestMessage{ID: "req"})
		msg.SetClassification(api.ClassificationReserved)

		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionWait, res.Action)
	})

	t.Run("Saturated - Overflow Interactive - Drop 429", func(t *testing.T) {
		gate := NewTierPriorityAdmissionGate(satGate, "tier")
		msg := api.NewInternalRequest(api.InternalRouting{
			Labels: map[string]string{
				"tier": "interactive",
			},
		}, &api.RequestMessage{ID: "req"})
		msg.SetClassification(api.ClassificationOverflow)

		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionDrop, res.Action)
		assert.NotNil(t, res.Result)
		assert.Contains(t, res.Result.Payload, `"code": 429`)
	})

	t.Run("Saturated - Overflow Async - Refuse", func(t *testing.T) {
		gate := NewTierPriorityAdmissionGate(satGate, "tier")
		msg := api.NewInternalRequest(api.InternalRouting{
			Labels: map[string]string{
				"tier": "async",
			},
		}, &api.RequestMessage{ID: "req"})
		msg.SetClassification(api.ClassificationOverflow)

		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionRefuse, res.Action)
	})

	t.Run("Saturated - Overflow Batch - Refuse", func(t *testing.T) {
		gate := NewTierPriorityAdmissionGate(satGate, "tier")
		msg := api.NewInternalRequest(api.InternalRouting{
			Labels: map[string]string{
				"tier": "batch",
			},
		}, &api.RequestMessage{ID: "req"})
		msg.SetClassification(api.ClassificationOverflow)

		var releases []pipeline.GateReleaseFunc
		res, err := gate.Apply(context.Background(), msg, &releases)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionRefuse, res.Action)
	})
}
