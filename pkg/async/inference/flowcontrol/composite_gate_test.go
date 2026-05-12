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

type mockDispatchGate struct {
	budget float64
}

func (m *mockDispatchGate) Budget(ctx context.Context) float64 {
	return m.budget
}

type mockAttributeGate struct {
	mockDispatchGate
	allowed        bool
	classification api.QuotaClassification
	err            error
	calls          int
	release        bool
}

func (m *mockAttributeGate) Acquire(ctx context.Context, attributes map[string]string) (pipeline.AcquireResult, error) {
	m.calls++
	if m.err != nil {
		return pipeline.AcquireResult{}, m.err
	}
	if !m.allowed {
		return pipeline.AcquireResult{Allowed: false, Classification: api.ClassificationOverflow}, nil
	}
	class := m.classification
	if class == "" {
		class = api.ClassificationReserved
	}
	return pipeline.AcquireResult{
		Allowed:        true,
		Classification: class,
		Release:        func() { m.release = true },
	}, nil
}

func TestCompositeGate_Budget(t *testing.T) {
	t.Run("Empty gates", func(t *testing.T) {
		gate := NewCompositeGate()
		assert.Equal(t, 1.0, gate.Budget(context.Background()))
	})

	t.Run("Minimum budget", func(t *testing.T) {
		gate := NewCompositeGate(
			&mockDispatchGate{budget: 0.8},
			&mockDispatchGate{budget: 0.3},
			&mockDispatchGate{budget: 0.9},
		)
		assert.Equal(t, 0.3, gate.Budget(context.Background()))
	})

	t.Run("Single gate", func(t *testing.T) {
		gate := NewCompositeGate(
			&mockDispatchGate{budget: 0.5},
		)
		assert.Equal(t, 0.5, gate.Budget(context.Background()))
	})
}

func TestCompositeGate_Acquire(t *testing.T) {
	t.Run("No attribute gates", func(t *testing.T) {
		gate := NewCompositeGate(
			&mockDispatchGate{budget: 0.5},
		)
		res, err := gate.Acquire(context.Background(), nil)
		assert.True(t, res.Allowed)
		assert.NoError(t, err)
		assert.NotNil(t, res.Release)
		res.Release()
	})

	t.Run("All allowed", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true}
		gate2 := &mockAttributeGate{allowed: true}
		gate := NewCompositeGate(gate1, gate2)

		res, err := gate.Acquire(context.Background(), nil)
		assert.True(t, res.Allowed)
		assert.NoError(t, err)
		assert.NotNil(t, res.Release)

		res.Release()
		assert.True(t, gate1.release)
		assert.True(t, gate2.release)
	})

	t.Run("One denied", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true}
		gate2 := &mockAttributeGate{allowed: false}
		gate3 := &mockAttributeGate{allowed: true}
		gate := NewCompositeGate(gate1, gate2, gate3)

		res, err := gate.Acquire(context.Background(), nil)
		assert.False(t, res.Allowed)
		assert.NoError(t, err)
		assert.Nil(t, res.Release)

		assert.Equal(t, 1, gate1.calls)
		assert.Equal(t, 1, gate2.calls)
		assert.Equal(t, 0, gate3.calls) // Short circuits
		assert.True(t, gate1.release)   // gate1 was released because gate2 denied
	})

	t.Run("Error in gate", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true}
		gate2 := &mockAttributeGate{err: errors.New("test error")}
		gate := NewCompositeGate(gate1, gate2)

		res, err := gate.Acquire(context.Background(), nil)
		assert.False(t, res.Allowed)
		assert.Error(t, err)
		assert.Nil(t, res.Release)

		assert.Equal(t, 1, gate1.calls)
		assert.Equal(t, 1, gate2.calls)
		assert.True(t, gate1.release) // gate1 was released because gate2 errored
	})
}

func TestCompositeGate_Classification(t *testing.T) {
	t.Run("Reserved aggregation", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true, classification: api.ClassificationReserved}
		gate2 := &mockAttributeGate{allowed: true, classification: api.ClassificationNone}
		gate := NewCompositeGate(gate1, gate2)

		res, err := gate.Acquire(context.Background(), nil)
		assert.NoError(t, err)
		assert.Equal(t, api.ClassificationReserved, res.Classification)
	})

	t.Run("Overflow aggregation", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true, classification: api.ClassificationReserved}
		gate2 := &mockAttributeGate{allowed: true, classification: api.ClassificationOverflow}
		gate := NewCompositeGate(gate1, gate2)

		res, err := gate.Acquire(context.Background(), nil)
		assert.NoError(t, err)
		assert.Equal(t, api.ClassificationOverflow, res.Classification)
	})
}
