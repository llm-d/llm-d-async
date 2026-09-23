package flowcontrol

import (
	"context"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*QuotaGate)(nil)

type QuotaMode string

const (
	QuotaModeRateLimit   QuotaMode = "rate-limit"
	QuotaModeConcurrency QuotaMode = "concurrency"
)

// QuotaStore counts quota usage shared by every dispatcher that uses the same store.
type QuotaStore interface {
	// AcquireSlot takes one of limit concurrent slots for key. ok is false when
	// every slot is held; release returns the slot.
	AcquireSlot(ctx context.Context, key string, limit int) (release func(), ok bool, err error)
	// Admit counts one request for key unless limit requests were admitted in
	// the trailing window.
	Admit(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}

// QuotaGate limits requests per value of a request metadata attribute.
type QuotaGate struct {
	store      QuotaStore
	attribute  string
	mode       QuotaMode
	gatingMode GatingMode
	limit      int
	window     time.Duration
	prefix     string
}

func NewQuotaGate(store QuotaStore, attribute string, mode QuotaMode, limit int, window time.Duration, prefix string) *QuotaGate {
	return &QuotaGate{
		store:      store,
		attribute:  attribute,
		mode:       mode,
		gatingMode: GatingModeBlocking,
		limit:      limit,
		window:     window,
		prefix:     prefix,
	}
}

func (g *QuotaGate) WithGatingMode(mode GatingMode) *QuotaGate {
	g.gatingMode = mode
	return g
}

// Budget implements pipeline.Gate. Quota is enforced per request in Apply, so
// the gate never closes the whole queue.
func (g *QuotaGate) Budget(ctx context.Context) float64 {
	return 1.0
}

// Apply implements pipeline.Gate. A request without the attribute is not limited.
func (g *QuotaGate) Apply(ctx context.Context, msg *api.InternalRequest, releases *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	val, ok := msg.PublicRequest.ReqMetadata()[g.attribute]
	if !ok {
		return pipeline.Continue(), nil
	}
	key := fmt.Sprintf("%s%s:%s", g.prefix, g.attribute, val)

	var admitted bool
	var release func()
	var err error
	switch g.mode {
	case QuotaModeConcurrency:
		release, admitted, err = g.store.AcquireSlot(ctx, key, g.limit)
	case QuotaModeRateLimit:
		admitted, err = g.store.Admit(ctx, key, g.limit, g.window)
	default:
		return pipeline.Verdict{}, fmt.Errorf("quota gate: unknown mode %q", g.mode)
	}
	if err != nil {
		return pipeline.Verdict{}, fmt.Errorf("quota gate %s: %w", key, err)
	}

	if !admitted {
		msg.SetClassification(api.ClassificationOverflow)
		if g.gatingMode == GatingModeBlocking {
			return pipeline.Refuse(), nil
		}
		return pipeline.Continue(), nil
	}
	msg.SetClassification(api.ClassificationReserved)
	if release != nil && releases != nil {
		*releases = append(*releases, release)
	}
	return pipeline.Continue(), nil
}
