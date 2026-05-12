/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package flowcontrol

import (
	"context"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*LocalMaxConcurrencyGate)(nil)

// LocalMaxConcurrencyGate caps the number of concurrent in-flight
// dispatches within a single async-processor pod via a buffered-channel
// semaphore. Unlike LocalRateLimitGate, the cap is on simultaneous
// requests rather than requests-per-minute — useful when the upstream
// constraint is parallelism (e.g. groq's per-pool concurrency budget)
// and request latency varies.
type LocalMaxConcurrencyGate struct {
	sem chan struct{}
}

// NewLocalMaxConcurrencyGate returns a gate that admits up to maxConcurrency
// in-flight dispatches at a time. maxConcurrency must be > 0.
func NewLocalMaxConcurrencyGate(maxConcurrency int) *LocalMaxConcurrencyGate {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	return &LocalMaxConcurrencyGate{sem: make(chan struct{}, maxConcurrency)}
}

// Apply implements pipeline.Gate. Blocks the calling goroutine until a
// slot becomes available, then attaches a release that frees the slot and
// returns Continue. The block is intentional: callers (typically the
// worker) hold the message buffered in-memory and want it to dispatch the
// instant a slot frees — much cheaper than nack-and-redeliver round-trips
// through the transport. Returns Refuse only on ctx cancellation.
func (g *LocalMaxConcurrencyGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	select {
	case g.sem <- struct{}{}:
		msg.AttachRelease(func() {
			select {
			case <-g.sem:
			default:
			}
		})
		return pipeline.Continue, nil
	case <-ctx.Done():
		return pipeline.Refuse(), ctx.Err()
	}
}
