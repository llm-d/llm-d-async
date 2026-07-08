package flowcontrol

import (
	"context"
	"fmt"
	"sync"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*LocalConcurrencyGate)(nil)

type GatingMode string

const (
	GatingModeBlocking    GatingMode = "blocking"
	GatingModeClassifying GatingMode = "classifying"
)

// LocalConcurrencyGate limits the number of concurrent in-flight requests
// processed from a single queue locally.
type LocalConcurrencyGate struct {
	mu         sync.Mutex
	limit      int
	inFlight   int
	gatingMode GatingMode
	sem        chan struct{}
}

// NewLocalConcurrencyGate creates a new LocalConcurrencyGate with the specified limit.
func NewLocalConcurrencyGate(limit int) *LocalConcurrencyGate {
	return &LocalConcurrencyGate{
		limit:      limit,
		gatingMode: GatingModeClassifying,
	}
}

// WithGatingMode configures the gating mode (blocking or classifying).
func (g *LocalConcurrencyGate) WithGatingMode(mode GatingMode) *LocalConcurrencyGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.gatingMode = mode
	if mode == GatingModeBlocking && g.limit > 0 {
		g.sem = make(chan struct{}, g.limit)
	}
	return g
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
func (g *LocalConcurrencyGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	g.mu.Lock()

	if g.limit <= 0 {
		g.mu.Unlock()
		return pipeline.Refuse(), nil
	}

	if g.gatingMode == GatingModeBlocking {
		sem := g.sem
		g.mu.Unlock()

		select {
		case sem <- struct{}{}:
			g.mu.Lock()
			g.inFlight++
			g.mu.Unlock()

			if releases != nil {
				*releases = append(*releases, func() {
					<-sem
					g.mu.Lock()
					g.inFlight--
					g.mu.Unlock()
				})
			}
			return pipeline.Continue(), nil
		case <-ctx.Done():
			return pipeline.Verdict{}, fmt.Errorf("context canceled: %w", ctx.Err())
		}
	}

	// Classifying/non-blocking mode
	if g.inFlight >= g.limit {
		g.mu.Unlock()
		return pipeline.Refuse(), nil
	}

	g.inFlight++
	g.mu.Unlock()

	if releases != nil {
		*releases = append(*releases, func() {
			g.mu.Lock()
			g.inFlight--
			g.mu.Unlock()
		})
	}

	return pipeline.Continue(), nil
}
