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
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSaturationDispatchGate(t *testing.T) {
	tests := []struct {
		name       string
		saturation float64
		err        error
		threshold  float64
		fallback   float64
		expected   float64
	}{
		// source returns D = 1-saturation; threshold is converted to budget space as B = 1-threshold.
		// Budget() returns D-B = (1-saturation)-(1-threshold) when D > B, else 0.
		{"zero saturation", 0.0, nil, 0.8, 1.0, 0.8},
		{"partial saturation", 0.3, nil, 0.8, 1.0, 0.8 - 0.3},
		{"at threshold", 0.8, nil, 0.8, 1.0, 0.0},
		{"above threshold", 0.95, nil, 0.8, 1.0, 0.0},
		{"full saturation", 1.0, nil, 0.8, 1.0, 0.0},
		{"just below threshold", 0.79, nil, 0.8, 1.0, 0.8 - 0.79},
		{"threshold one", 0.99, nil, 1.0, 1.0, 1.0 - 0.99},
		{"saturation above one", 1.5, nil, 0.8, 1.0, 0.0},
		{"negative saturation", -0.5, nil, 0.8, 1.0, 1.0},
		{"error fail closed", 0, errors.New("conn refused"), 0.8, 1.0, 0.0},
		{"error fail open", 0, errors.New("conn refused"), 0.8, 0.0, 1.0},
		{"NaN fail open", math.NaN(), nil, 0.8, 0.0, 1.0},
		{"Inf fail open", math.Inf(-1), nil, 0.8, 0.0, 1.0},
		{"NaN fail closed", math.NaN(), nil, 0.8, 1.0, 0.0},
		{"Inf fail closed", math.Inf(-1), nil, 0.8, 1.0, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var source *mockMetricSource
			if tt.err != nil {
				source = &mockMetricSource{err: tt.err}
			} else {
				source = &mockMetricSource{samples: []Sample{{Value: 1 - tt.saturation}}}
			}
			gate := NewSaturationDispatchGate(source, tt.threshold, tt.fallback)
			require.InDelta(t, tt.expected, gate.Budget(context.Background()), 1e-9)
		})
	}
}

func TestSaturationDispatchGate_EmptySamples(t *testing.T) {
	source := &mockMetricSource{samples: []Sample{}}
	gate := NewSaturationDispatchGate(source, 0.8, 1.0)
	require.Equal(t, 0.0, gate.Budget(context.Background()))

	gate = NewSaturationDispatchGate(source, 0.8, 0.0)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate(t *testing.T) {
	tests := []struct {
		name     string
		budget   float64
		err      error
		baseline float64
		fallback float64
		expected float64
	}{
		// Budget() returns D-B when D > B (baseline), else 0.
		{"D above baseline", 0.7, nil, 0.1, 0.0, 0.6},
		{"D at baseline, gate closed", 0.1, nil, 0.1, 0.0, 0.0},
		{"D below baseline, gate closed", 0.05, nil, 0.1, 0.0, 0.0},
		{"zero baseline, returns D", 0.5, nil, 0.0, 0.0, 0.5},
		{"zero baseline, full capacity", 1.0, nil, 0.0, 0.0, 1.0},
		{"fully saturated", 0.0, nil, 0.05, 0.0, 0.0},
		{"NaN", math.NaN(), nil, 0.05, 0.0, 0.0},
		{"error fail closed", 0, errors.New("conn refused"), 0.05, 0.0, 0.0},
		{"error fail open", 0, errors.New("conn refused"), 0.05, 1.0, 1.0},
		{"fallback clamped above one", 0, errors.New("error"), 0.05, 2.0, 1.0},
		{"fallback clamped below zero", 0, errors.New("error"), 0.05, -1.0, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var source *mockMetricSource
			if tt.err != nil {
				source = &mockMetricSource{err: tt.err}
			} else {
				source = &mockMetricSource{samples: []Sample{{Value: tt.budget}}}
			}
			gate := NewBudgetDispatchGate(source, tt.baseline, tt.fallback)
			require.InDelta(t, tt.expected, gate.Budget(context.Background()), 1e-9)
		})
	}
}

// switchableMetricSource allows changing what Query returns between calls.
type switchableMetricSource struct {
	mu      sync.Mutex
	samples []Sample
	err     error
}

func (s *switchableMetricSource) Query(_ context.Context) ([]Sample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.samples, s.err
}

func (s *switchableMetricSource) set(samples []Sample, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = samples
	s.err = err
}

func TestMetricDispatchGate(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		err       error
		threshold float64
		fallback  float64
		expected  float64
	}{
		{"above threshold", 0.5, nil, 0.3, 0.0, 0.2},
		{"at threshold", 0.3, nil, 0.3, 0.0, 0.0},
		{"below threshold", 0.2, nil, 0.3, 0.0, 0.0},
		{"fallback on error", 0, errors.New("error"), 0.8, 0.5, 0.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var source *mockMetricSource
			if tt.err != nil {
				source = &mockMetricSource{err: tt.err}
			} else {
				source = &mockMetricSource{samples: []Sample{{Value: tt.value}}}
			}
			gate := NewMetricDispatchGate(source, tt.threshold, tt.fallback)
			require.InDelta(t, tt.expected, gate.Budget(context.Background()), 1e-9)
		})
	}
}
