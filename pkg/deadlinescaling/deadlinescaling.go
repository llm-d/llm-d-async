// Package deadlinescaling turns a queue's deadline-proximity bucket counts into
// the number of replicas needed to finish its queued items before their
// deadlines, for an autoscaler to consume as a gauge.
package deadlinescaling

import (
	"fmt"
	"math"
	"time"
)

// Config is a queue's deadline_scaling block.
type Config struct {
	// ThroughputRPS is the number of items one replica finishes per second (R).
	ThroughputRPS float64 `json:"throughput_rps"`
	// ColdStartSeconds is how long a new replica takes to start serving (C).
	ColdStartSeconds float64 `json:"cold_start_seconds"`
	// MaxReplicas caps the result. Items due before a new replica could start
	// pin the result to it.
	MaxReplicas int `json:"max_replicas"`
	// SafetyFactor, in (0, 1], shrinks the time a replica has for the items due
	// within a horizon, to absorb the coarseness of the buckets. 0 means 1.
	SafetyFactor float64 `json:"safety_factor,omitempty"`
}

// Validate reports a setting that would make RequiredReplicas meaningless.
func (c Config) Validate() error {
	switch {
	case !(c.ThroughputRPS > 0) || math.IsInf(c.ThroughputRPS, 0):
		return fmt.Errorf("deadline_scaling.throughput_rps must be a positive number")
	case !(c.ColdStartSeconds >= 0) || math.IsInf(c.ColdStartSeconds, 0):
		return fmt.Errorf("deadline_scaling.cold_start_seconds must be zero or a positive number")
	case c.MaxReplicas < 1:
		return fmt.Errorf("deadline_scaling.max_replicas must be at least 1")
	case !(c.SafetyFactor >= 0 && c.SafetyFactor <= 1):
		return fmt.Errorf("deadline_scaling.safety_factor must be in (0, 1]")
	}
	return nil
}

// RequiredReplicas is the maximum, over the horizons t whose cumulative count
// B(t) is positive, of B(t) / (R * (t - C) * SafetyFactor), rounded up and
// capped at MaxReplicas. A horizon t <= C with items in it returns
// MaxReplicas. boundaries and cumulative are aligned, most urgent first, as
// the deadline-proximity metric orders them; an empty queue needs 0.
func (c Config) RequiredReplicas(boundaries []time.Duration, cumulative []int64) int {
	safety := c.SafetyFactor
	if safety == 0 {
		safety = 1
	}
	coldStart := time.Duration(c.ColdStartSeconds * float64(time.Second))
	need := 0.0
	for i, horizon := range boundaries {
		if i >= len(cumulative) || cumulative[i] <= 0 {
			continue
		}
		if horizon <= coldStart {
			return c.MaxReplicas
		}
		budget := (horizon - coldStart).Seconds() * safety
		need = math.Max(need, float64(cumulative[i])/(c.ThroughputRPS*budget))
	}
	// Absorb float error so that exactly n replicas' worth is not rounded to n+1.
	return min(int(math.Ceil(need-1e-9)), c.MaxReplicas)
}
