// Package deadlinescaling turns the deadline-proximity bucket counts of a queue,
// or of all the queues of a worker pool, into the number of replicas needed to
// finish their items before their deadlines, for an autoscaler to consume as a
// gauge.
package deadlinescaling

import (
	"fmt"
	"math"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
)

// Config is a deadline_scaling block, on a worker pool or a queue.
type Config pipeline.DeadlineScaling

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

// AddWork adds a queue's items due within each horizon to work, in
// replica-seconds: items divided by rps, the items one replica finishes per
// second. It leaves out items already past their deadline (the t = 0 bucket):
// no replica count makes them on time. boundaries, cumulative and work are
// aligned, most urgent first, as the deadline-proximity metric orders them.
// Adding each queue of a pool gives the pool's work.
func AddWork(work []float64, boundaries []time.Duration, cumulative []int64, rps float64) {
	var expired int64
	for i, horizon := range boundaries {
		if i >= len(cumulative) || i >= len(work) {
			break
		}
		if horizon <= 0 {
			expired = cumulative[i]
			continue
		}
		if due := cumulative[i] - expired; due > 0 {
			work[i] += float64(due) / rps
		}
	}
}

// RequiredReplicas is the maximum, over the horizons t > 0 with work due in
// them, of W(t) / ((t - C) * SafetyFactor), rounded up and capped at
// MaxReplicas, where W(t) is the work due within t (see AddWork). Work due
// within 0 < t <= C returns MaxReplicas; no work needs 0.
func (c Config) RequiredReplicas(boundaries []time.Duration, work []float64) int {
	safety := c.SafetyFactor
	if safety == 0 {
		safety = 1
	}
	coldStart := time.Duration(c.ColdStartSeconds * float64(time.Second))
	need := 0.0
	for i, horizon := range boundaries {
		if i >= len(work) {
			break
		}
		if horizon <= 0 || work[i] <= 0 {
			continue
		}
		if horizon <= coldStart {
			return c.MaxReplicas
		}
		need = math.Max(need, work[i]/((horizon-coldStart).Seconds()*safety))
	}
	if need == 0 {
		return 0
	}
	// Cap before converting: need can exceed the int range or be +Inf.
	if need >= float64(c.MaxReplicas) {
		return c.MaxReplicas
	}
	// Absorb float error so that exactly n replicas' worth is not rounded to
	// n+1, but keep one replica while there are items to finish.
	return max(int(math.Ceil(need-1e-9)), 1)
}
