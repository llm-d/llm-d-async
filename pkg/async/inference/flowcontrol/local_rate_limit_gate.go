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
	"sync"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*LocalRateLimitGate)(nil)

// LocalRateLimitGate limits dispatches within a single async-processor pod.
type LocalRateLimitGate struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	refill   float64
	last     time.Time
}

// NewLocalRateLimitGate returns a pod-local token-bucket dispatch gate.
func NewLocalRateLimitGate(requestsPerMinute float64, burst float64) *LocalRateLimitGate {
	if burst <= 0 {
		burst = 1
	}
	if requestsPerMinute <= 0 {
		requestsPerMinute = 1
	}
	return &LocalRateLimitGate{
		capacity: burst,
		tokens:   burst,
		refill:   requestsPerMinute / 60.0,
		last:     time.Now(),
	}
}

// Apply implements pipeline.Gate. Blocks until a token is available
// (computed from current bucket state plus refill rate), then returns
// Continue. Like LocalMaxConcurrencyGate, the block is intentional:
// keeps the message hot rather than nack-redelivering it. No release
// is emitted — token consumption is permanent until refilled.
func (g *LocalRateLimitGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	for {
		select {
		case <-ctx.Done():
			return pipeline.Refuse(), ctx.Err()
		default:
		}
		g.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(g.last).Seconds()
		g.last = now
		g.tokens += elapsed * g.refill
		if g.tokens > g.capacity {
			g.tokens = g.capacity
		}
		if g.tokens >= 1 {
			g.tokens -= 1
			g.mu.Unlock()
			return pipeline.Continue, nil
		}
		need := 1 - g.tokens
		var wait time.Duration
		if g.refill > 0 {
			wait = time.Duration(need / g.refill * float64(time.Second))
		} else {
			wait = time.Second
		}
		if wait < 10*time.Millisecond {
			wait = 10 * time.Millisecond
		}
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return pipeline.Refuse(), ctx.Err()
		case <-time.After(wait):
		}
	}
}
