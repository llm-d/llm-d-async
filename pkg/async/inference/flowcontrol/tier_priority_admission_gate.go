/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package flowcontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

// TierPriorityAdmissionGate is the runtime counterpart to the
// tier-priority RMP. The RMP orders messages by (tier, class); this
// gate admits or sheds them based on pool load and the same labels.
//
// Three-way verdict at saturation:
//
//	| Saturated | class    | tier        | Verdict                       |
//	|-----------|----------|-------------|-------------------------------|
//	| No        | *        | *           | Continue                      |
//	| Yes       | reserved | *           | Block until capacity OR ctx   |
//	| Yes       | overflow | interactive | Drop(&ResultMessage{429})     |
//	| Yes       | overflow | async/batch | Refuse (nack-redeliver)       |
//
// Reserved-blocking is correct because the message is already prefetched
// into this pod (c69d291). Blocking dispatches the moment capacity
// opens; the alternative (Refuse) would force a PubSub redelivery
// round-trip and exponential backoff.
//
// Overflow-Refuse for async/batch is critical: blocking overflow at the
// gate ties up workers on lower-priority work, and since the per-pool
// worker pool consumes FIFO from a shared channel, parked-overflow
// workers head-of-line-block reserved messages arriving behind them.
// Refusing overflow keeps workers cycling — they drain overflow back
// to PubSub and only park when they find reserved work that's
// legitimately worth waiting for. The RMP's strict-priority ordering at
// channel send is preserved at consumption.
//
// Overflow-Drop for interactive is the RFC's fail-fast property: the
// SLA is sub-second, so publishing a 429 result and acking is strictly
// better than nack-redeliver-loop.
//
// The saturation signal is a PoolLoadSource — pluggable: prometheus
// snapshot (background refresh) for self-hosted IGW pools, redis
// counter for external pools with strict accounting.
type TierPriorityAdmissionGate struct {
	source PoolLoadSource

	tierLabel    string
	classLabel   string
	reservedVal  string
	overflowVal  string
	failFastTier string

	blockPollInterval time.Duration

	failFastStatus int
	failFastBody   string
}

var _ pipeline.Gate = (*TierPriorityAdmissionGate)(nil)

// TierPriorityAdmissionConfig is the parsed gate config.
type TierPriorityAdmissionConfig struct {
	// Source is the pool-load signal. Required.
	Source PoolLoadSource

	// Label-key configuration. Defaults match the tier-priority RMP.
	TierLabel    string // default "tier"
	ClassLabel   string // default "class"
	ReservedVal  string // default "reserved"
	OverflowVal  string // default "overflow"
	FailFastTier string // tier that gets fail-fast-on-overflow (default "interactive")

	// Block tuning. The block branch polls TryAcquire at this interval.
	BlockPollInterval time.Duration // default 100ms

	// Fail-fast result payload shape. Body is opaque to the gate; it's
	// emitted as the Payload of the Drop result. Status is informational
	// (operator-readable) — there's no actual HTTP status code on the
	// result envelope.
	FailFastStatus int    // default 429
	FailFastBody   string // default `{"error":"pool saturated","status":429}`
}

// NewTierPriorityAdmissionGate constructs the gate. The PoolLoadSource
// must be wired (and, for PromQL-backed sources, Started) by the caller.
func NewTierPriorityAdmissionGate(cfg TierPriorityAdmissionConfig) (*TierPriorityAdmissionGate, error) {
	if cfg.Source == nil {
		return nil, fmt.Errorf("tier-priority-admission: source is required")
	}
	g := &TierPriorityAdmissionGate{
		source:            cfg.Source,
		tierLabel:         cfg.TierLabel,
		classLabel:        cfg.ClassLabel,
		reservedVal:       cfg.ReservedVal,
		overflowVal:       cfg.OverflowVal,
		failFastTier:      cfg.FailFastTier,
		blockPollInterval: cfg.BlockPollInterval,
		failFastStatus:    cfg.FailFastStatus,
		failFastBody:      cfg.FailFastBody,
	}
	if g.tierLabel == "" {
		g.tierLabel = "tier"
	}
	if g.classLabel == "" {
		g.classLabel = "class"
	}
	if g.reservedVal == "" {
		g.reservedVal = "reserved"
	}
	if g.overflowVal == "" {
		g.overflowVal = "overflow"
	}
	if g.failFastTier == "" {
		g.failFastTier = "interactive"
	}
	if g.blockPollInterval <= 0 {
		g.blockPollInterval = 100 * time.Millisecond
	}
	if g.failFastStatus == 0 {
		g.failFastStatus = 429
	}
	if g.failFastBody == "" {
		body, _ := json.Marshal(map[string]any{"error": "pool saturated", "status": g.failFastStatus})
		g.failFastBody = string(body)
	}
	return g, nil
}

// Apply implements pipeline.Gate.
//
// The block branch (reserved at saturation) is deadline-bounded. It
// derives a ctx from the message's declared deadline so reserved workers
// don't park indefinitely if the pool stays saturated past the SLA
// window. On deadline expiry the gate returns Refuse — the worker's
// existing deadline-exceeded path reaps the message cleanly.
func (g *TierPriorityAdmissionGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	if msg == nil {
		return pipeline.Continue, nil
	}
	class := msg.Labels.Get(g.classLabel)
	tier := msg.Labels.Get(g.tierLabel)

	// Derive a deadline-bounded ctx for the block branch.
	blockCtx := ctx
	var blockCancel context.CancelFunc
	if msg.InternalRequest != nil && msg.PublicRequest != nil {
		if dl := msg.PublicRequest.ReqDeadline(); dl > 0 {
			blockCtx, blockCancel = context.WithDeadline(ctx, time.Unix(dl, 0))
			defer blockCancel()
		}
	}

	for {
		ok, release, err := g.source.TryAcquire(blockCtx)
		if ok {
			if release != nil {
				msg.AttachRelease(release)
			}
			// Return any err the source emitted (e.g. stale Prometheus)
			// alongside Continue so the caller can log without breaking
			// the dispatch.
			return pipeline.Continue, err
		}

		// Saturated. Branch on labels.
		switch {
		case class == g.reservedVal:
			// Block until capacity opens or message deadline expires.
			select {
			case <-time.After(g.blockPollInterval):
				// Re-check saturation.
			case <-blockCtx.Done():
				return pipeline.Refuse(), blockCtx.Err()
			}
		case tier == g.failFastTier:
			// Urgent/interactive overflow: fail-fast.
			return pipeline.Drop(g.buildFailFastResult(msg)), nil
		default:
			// Async / batch overflow: nack-redeliver.
			return pipeline.Refuse(), nil
		}
	}
}

func (g *TierPriorityAdmissionGate) buildFailFastResult(msg *pipeline.EmbelishedRequestMessage) *api.ResultMessage {
	var id string
	var metadata map[string]string
	var routing api.InternalRouting
	if msg.PublicRequest != nil {
		id = msg.PublicRequest.ReqID()
		metadata = msg.PublicRequest.ReqMetadata()
	}
	if msg.InternalRequest != nil {
		routing = msg.InternalRouting
	}
	return &api.ResultMessage{
		ID:       id,
		Payload:  g.failFastBody,
		Routing:  routing,
		Metadata: metadata,
	}
}
