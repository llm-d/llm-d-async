/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"context"
	"errors"
	"testing"

	"github.com/llm-d-incubation/llm-d-async/api"
	pipeline "github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
)

type mockGate struct {
	budget         float64
	verdict        pipeline.Verdict
	err            error
	calls          int
	releaseCounter int
	classification api.QuotaClassification
}

func (m *mockGate) Budget(ctx context.Context) float64 {
	return m.budget
}

func (m *mockGate) Apply(ctx context.Context, msg *api.InternalRequest) (pipeline.Verdict, error) {
	m.calls++
	if m.err != nil {
		return pipeline.Verdict{}, m.err
	}
	if m.classification != "" {
		msg.Classification = m.classification
	}
	if m.verdict.Action != pipeline.ActionContinue {
		return m.verdict, nil
	}
	msg.AttachRelease(func() {
		m.releaseCounter++
	})
	return m.verdict, nil
}

func TestCompositeGate_Budget(t *testing.T) {
	t.Run("Empty gates", func(t *testing.T) {
		gate := NewCompositeGate()
		assert.Equal(t, 1.0, gate.Budget(context.Background()))
	})

	t.Run("Minimum budget", func(t *testing.T) {
		gate := NewCompositeGate(
			&mockGate{budget: 0.8},
			&mockGate{budget: 0.3},
			&mockGate{budget: 0.9},
		)
		assert.Equal(t, 0.3, gate.Budget(context.Background()))
	})

	t.Run("Single gate", func(t *testing.T) {
		gate := NewCompositeGate(
			&mockGate{budget: 0.5},
		)
		assert.Equal(t, 0.5, gate.Budget(context.Background()))
	})
}

func TestCompositeGate_Apply(t *testing.T) {
	t.Run("No inner gates", func(t *testing.T) {
		gate := NewCompositeGate()
		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		res, err := gate.Apply(context.Background(), msg)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionContinue, res.Action)
	})

	t.Run("All continue", func(t *testing.T) {
		gate1 := &mockGate{verdict: pipeline.Continue()}
		gate2 := &mockGate{verdict: pipeline.Continue()}
		gate := NewCompositeGate(gate1, gate2)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		res, err := gate.Apply(context.Background(), msg)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionContinue, res.Action)

		msg.Release()
		assert.Equal(t, 1, gate1.releaseCounter)
		assert.Equal(t, 1, gate2.releaseCounter)
	})

	t.Run("One redeliver", func(t *testing.T) {
		gate1 := &mockGate{verdict: pipeline.Continue()}
		gate2 := &mockGate{verdict: pipeline.Refuse()}
		gate3 := &mockGate{verdict: pipeline.Continue()}
		gate := NewCompositeGate(gate1, gate2, gate3)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		res, err := gate.Apply(context.Background(), msg)
		assert.NoError(t, err)
		assert.Equal(t, pipeline.ActionRefuse, res.Action)

		assert.Equal(t, 1, gate1.calls)
		assert.Equal(t, 1, gate2.calls)
		assert.Equal(t, 0, gate3.calls)          // Short circuit
		assert.Equal(t, 1, gate1.releaseCounter) // Rollback called for gate1
	})

	t.Run("Error in gate", func(t *testing.T) {
		gate1 := &mockGate{verdict: pipeline.Continue()}
		gate2 := &mockGate{err: errors.New("test error")}
		gate := NewCompositeGate(gate1, gate2)

		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req"})
		_, err := gate.Apply(context.Background(), msg)
		assert.Error(t, err)

		assert.Equal(t, 1, gate1.calls)
		assert.Equal(t, 1, gate2.calls)
		assert.Equal(t, 1, gate1.releaseCounter) // Rollback called for gate1
	})
}
