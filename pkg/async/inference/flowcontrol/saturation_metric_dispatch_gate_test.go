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

func TestSaturationMetricDispatchGate_ZeroSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.0}}},
		0.8, 1.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_PartialSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.3}}},
		0.8, 1.0,
	)
	require.InDelta(t, 0.7, gate.Budget(context.Background()), 1e-9)
}

func TestSaturationMetricDispatchGate_AtThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.8}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_AboveThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.95}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_FullSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 1.0}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_JustBelowThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.79}}},
		0.8, 1.0,
	)
	require.InDelta(t, 0.21, gate.Budget(context.Background()), 1e-9)
}

func TestSaturationMetricDispatchGate_Error(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{err: errors.New("connection refused")},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_EmptySamples(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_ThresholdOne(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.99}}},
		1.0, 1.0,
	)
	require.InDelta(t, 0.01, gate.Budget(context.Background()), 1e-9)
}

func TestSaturationMetricDispatchGate_Error_FailOpen(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{err: errors.New("connection refused")},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_EmptySamples_FailOpen(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{}},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_NaN_FailOpen(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: math.NaN()}}},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_Inf_FailOpen(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: math.Inf(1)}}},
		0.8, 0.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_NaN_FailClosed(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: math.NaN()}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_Inf_FailClosed(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: math.Inf(1)}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_SaturationAboveOne_AboveThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 1.5}}},
		0.8, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_SaturationAboveOne_HighThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 1.5}}},
		2.0, 1.0,
	)
	require.Equal(t, 0.0, gate.Budget(context.Background()))
}

func TestSaturationMetricDispatchGate_NegativeSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: -0.5}}},
		0.8, 1.0,
	)
	require.Equal(t, 1.0, gate.Budget(context.Background()))
}
