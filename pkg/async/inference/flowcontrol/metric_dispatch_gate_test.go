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
	"testing"

	"github.com/stretchr/testify/require"
)

// --- NewSaturationDispatchGate tests (fallback is a saturation value, inverted) ---

func TestSaturationDispatchGate_ZeroSaturation(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.0}}},
		0.8, 1.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_PartialSaturation(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.3}}},
		0.8, 1.0,
	)
	require.InDelta(t, 0.7, gate.Budget(context.Background()), 1e-9)
}

func TestSaturationDispatchGate_AtThreshold(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.8}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_AboveThreshold(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.95}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_FullSaturation(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 1.0}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_JustBelowThreshold(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.79}}},
		0.8, 1.0,
	)
	require.InDelta(t, 0.21, gate.Budget(context.Background()), 1e-9)
}

func TestSaturationDispatchGate_Error(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{err: errors.New("connection refused")},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_EmptySamples(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_ThresholdOne(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.99}}},
		1.0, 1.0,
	)
	require.InDelta(t, 0.01, gate.Budget(context.Background()), 1e-9)
}

func TestSaturationDispatchGate_Error_FailOpen(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{err: errors.New("connection refused")},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_EmptySamples_FailOpen(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{}},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_NaN_FailOpen(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: math.NaN()}}},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_Inf_FailOpen(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: math.Inf(1)}}},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_NaN_FailClosed(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: math.NaN()}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_Inf_FailClosed(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: math.Inf(1)}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_SaturationAboveOne_AboveThreshold(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 1.5}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_SaturationAboveOne_HighThreshold(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 1.5}}},
		2.0, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationDispatchGate_NegativeSaturation(t *testing.T) {
	gate := NewSaturationDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: -0.5}}},
		0.8, 1.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

// --- NewBudgetDispatchGate tests (threshold=1.0, fallback is direct budget) ---

func TestBudgetDispatchGate_CoreFormula(t *testing.T) {
	// PromQL returns combined saturation: F_SYS=0.5 + F_EPP=0.1 + B=0.05 = 0.65
	// Budget = 1 - 0.65 = 0.35
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.65}}},
		0.0,
	)
	require.InDelta(t, 0.35, gate.Budget(context.Background()), 1e-9)
}

func TestBudgetDispatchGate_ZeroLoad(t *testing.T) {
	// Combined saturation: 0 + 0 + 0.05 = 0.05 → budget 0.95
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.05}}},
		0.0,
	)
	require.InDelta(t, 0.95, gate.Budget(context.Background()), 1e-9)
}

func TestBudgetDispatchGate_Overloaded(t *testing.T) {
	// Combined saturation: 0.8 + 0.2 + 0.05 = 1.05 → budget clamped to 0
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 1.05}}},
		0.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_Error_FailClosed(t *testing.T) {
	gate := NewBudgetDispatchGate(
		&mockMetricSource{err: errors.New("connection refused")},
		0.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_Error_FailOpen(t *testing.T) {
	gate := NewBudgetDispatchGate(
		&mockMetricSource{err: errors.New("connection refused")},
		1.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_EmptySamples(t *testing.T) {
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{}},
		0.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_NaN(t *testing.T) {
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: math.NaN()}}},
		0.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_ExactlyOne(t *testing.T) {
	// Combined saturation = 1.0 → at threshold → budget 0.0
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 1.0}}},
		0.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_JustBelowOne(t *testing.T) {
	// Combined saturation = 0.99 → budget = 0.01
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.99}}},
		0.0,
	)
	require.InDelta(t, 0.01, gate.Budget(context.Background()), 1e-9)
}

func TestBudgetDispatchGate_NegativeClamped(t *testing.T) {
	// Negative combined saturation → budget clamped to 1.0
	gate := NewBudgetDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: -0.5}}},
		0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_FallbackClampedAboveOne(t *testing.T) {
	gate := NewBudgetDispatchGate(
		&mockMetricSource{err: errors.New("error")},
		2.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestBudgetDispatchGate_FallbackClampedBelowZero(t *testing.T) {
	gate := NewBudgetDispatchGate(
		&mockMetricSource{err: errors.New("error")},
		-1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

// --- NewMetricDispatchGate tests (generic, fallback is direct budget) ---

func TestMetricDispatchGate_CustomThreshold(t *testing.T) {
	gate := NewMetricDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.5}}},
		0.6, 0.0,
	)
	// 0.5 < 0.6 threshold → budget = 1 - 0.5 = 0.5
	require.InDelta(t, 0.5, gate.Budget(context.Background()), 1e-9)
}

func TestMetricDispatchGate_AtCustomThreshold(t *testing.T) {
	gate := NewMetricDispatchGate(
		&mockMetricSource{samples: []Sample{{Value: 0.6}}},
		0.6, 0.0,
	)
	// 0.6 >= 0.6 threshold → budget = 0.0
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestMetricDispatchGate_FallbackDirect(t *testing.T) {
	gate := NewMetricDispatchGate(
		&mockMetricSource{err: errors.New("error")},
		0.8, 0.5,
	)
	// Error → returns fallback budget directly (0.5)
	require.Equal(t, 0.5, gate.Budget(context.Background()))
}
