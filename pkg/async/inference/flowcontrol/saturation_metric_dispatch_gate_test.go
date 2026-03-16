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
)

func TestSaturationMetricDispatchGate_ZeroSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.0}}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	if budget != 1.0 {
		t.Errorf("expected budget 1.0, got %f", budget)
	}
}

func TestSaturationMetricDispatchGate_PartialSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.3}}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	expected := 0.7
	if !floatEquals(budget, expected) {
		t.Errorf("expected budget %f, got %f", expected, budget)
	}
}

func TestSaturationMetricDispatchGate_AtThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.8}}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	if budget != 0.0 {
		t.Errorf("expected budget 0.0 at threshold, got %f", budget)
	}
}

func TestSaturationMetricDispatchGate_AboveThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.95}}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	if budget != 0.0 {
		t.Errorf("expected budget 0.0 above threshold, got %f", budget)
	}
}

func TestSaturationMetricDispatchGate_FullSaturation(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 1.0}}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	if budget != 0.0 {
		t.Errorf("expected budget 0.0 at full saturation, got %f", budget)
	}
}

func TestSaturationMetricDispatchGate_JustBelowThreshold(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.79}}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	expected := 0.21
	if !floatEquals(budget, expected) {
		t.Errorf("expected budget %f, got %f", expected, budget)
	}
}

func TestSaturationMetricDispatchGate_Error(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{err: errors.New("connection refused")},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	if budget != 0.0 {
		t.Errorf("expected budget 0.0 for error (fail-closed), got %f", budget)
	}
}

func TestSaturationMetricDispatchGate_EmptySamples(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{}},
		"my-pool", 0.8,
	)
	budget := gate.Budget(context.Background())
	if budget != 0.0 {
		t.Errorf("expected budget 0.0 for empty samples (fail-closed), got %f", budget)
	}
}

func TestSaturationMetricDispatchGate_ThresholdOne(t *testing.T) {
	gate := NewSaturationMetricDispatchGateWithSource(
		&mockMetricSource{samples: []Sample{{Value: 0.99}}},
		"my-pool", 1.0,
	)
	budget := gate.Budget(context.Background())
	expected := 0.01
	if !floatEquals(budget, expected) {
		t.Errorf("expected budget %f, got %f", expected, budget)
	}
}

const floatTolerance = 1e-9

func floatEquals(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < floatTolerance
}
