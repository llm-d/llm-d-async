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
	allowed bool
	err     error
	calls   int
	release bool
}

func (m *mockAttributeGate) Acquire(ctx context.Context, attributes map[string]string) (bool, func(), error) {
	m.calls++
	if m.err != nil {
		return false, nil, m.err
	}
	if !m.allowed {
		return false, nil, nil
	}
	return true, func() { m.release = true }, nil
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
		allowed, release, err := gate.Acquire(context.Background(), nil)
		assert.True(t, allowed)
		assert.NoError(t, err)
		assert.NotNil(t, release)
		release()
	})

	t.Run("All allowed", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true}
		gate2 := &mockAttributeGate{allowed: true}
		gate := NewCompositeGate(gate1, gate2)

		allowed, release, err := gate.Acquire(context.Background(), nil)
		assert.True(t, allowed)
		assert.NoError(t, err)
		assert.NotNil(t, release)

		release()
		assert.True(t, gate1.release)
		assert.True(t, gate2.release)
	})

	t.Run("One denied", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true}
		gate2 := &mockAttributeGate{allowed: false}
		gate3 := &mockAttributeGate{allowed: true}
		gate := NewCompositeGate(gate1, gate2, gate3)

		allowed, release, err := gate.Acquire(context.Background(), nil)
		assert.False(t, allowed)
		assert.NoError(t, err)
		assert.Nil(t, release)

		assert.Equal(t, 1, gate1.calls)
		assert.Equal(t, 1, gate2.calls)
		assert.Equal(t, 0, gate3.calls) // Short circuits
		assert.True(t, gate1.release)   // gate1 was released because gate2 denied
	})

	t.Run("Error in gate", func(t *testing.T) {
		gate1 := &mockAttributeGate{allowed: true}
		gate2 := &mockAttributeGate{err: errors.New("test error")}
		gate := NewCompositeGate(gate1, gate2)

		allowed, release, err := gate.Acquire(context.Background(), nil)
		assert.False(t, allowed)
		assert.Error(t, err)
		assert.Nil(t, release)

		assert.Equal(t, 1, gate1.calls)
		assert.Equal(t, 1, gate2.calls)
		assert.True(t, gate1.release) // gate1 was released because gate2 errored
	})
}
