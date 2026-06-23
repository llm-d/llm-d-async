package flowcontrol

import (
	"context"
	"sync"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*LocalConcurrencyGate)(nil)

// LocalConcurrencyGate limits the number of concurrent in-flight requests
// processed from a single queue locally.
type LocalConcurrencyGate struct {
	mu       sync.Mutex
	limit    int
	inFlight int
}

// NewLocalConcurrencyGate creates a new LocalConcurrencyGate with the specified limit.
func NewLocalConcurrencyGate(limit int) *LocalConcurrencyGate {
	return &LocalConcurrencyGate{
		limit: limit,
	}
}

// Budget implements pipeline.Gate.
// Returns the fraction of available capacity in [0.0, 1.0].
func (g *LocalConcurrencyGate) Budget(ctx context.Context) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.limit <= 0 {
		return 0.0
	}
	if g.inFlight >= g.limit {
		return 0.0
	}
	return float64(g.limit-g.inFlight) / float64(g.limit)
}

// Apply implements pipeline.Gate.
// Returns VerdictContinue if request fits in budget, VerdictRefuse with redeliver otherwise.
func (g *LocalConcurrencyGate) Apply(ctx context.Context, msg *api.InternalRequest) (pipeline.Verdict, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.limit <= 0 {
		return pipeline.Refuse(), nil
	}

	if g.inFlight >= g.limit {
		return pipeline.Refuse(), nil
	}

	g.inFlight++
	msg.AttachRelease(func() {
		g.mu.Lock()
		g.inFlight--
		g.mu.Unlock()
	})

	return pipeline.Continue(), nil
}
