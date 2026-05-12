/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package flowcontrol

import (
	"context"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.Gate = (*DeadlineDropGate)(nil)

// DeadlineDropGate is a stateless Gate that returns Drop when the message's
// deadline (msg.PublicRequest.ReqDeadline(), Unix seconds) has passed, and
// Continue otherwise. It is intended to run early in the gate chain so
// expired messages are shed before they consume any reservation slot or
// pool capacity. The gate emits no Release (it takes no state) and does
// not produce a result message — drop semantics are ack-and-discard.
//
// To preserve the producer-visible deadline-exceeded result the worker
// emits today, leave the worker's deadline check in place; this gate adds
// a pull-time pre-filter rather than replacing the worker's check.
type DeadlineDropGate struct {
	// now is the time source; nil means time.Now. Override for tests.
	now func() time.Time
}

// NewDeadlineDropGate constructs a DeadlineDropGate using time.Now.
func NewDeadlineDropGate() *DeadlineDropGate {
	return &DeadlineDropGate{}
}

// Apply implements pipeline.Gate.
func (g *DeadlineDropGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	// EmbelishedRequestMessage embeds *api.InternalRequest as a pointer,
	// so accessing msg.PublicRequest panics when the embedded pointer is
	// nil. Check both before dereferencing.
	if msg == nil || msg.InternalRequest == nil || msg.PublicRequest == nil {
		return pipeline.Continue, nil
	}
	deadline := msg.PublicRequest.ReqDeadline()
	if deadline <= 0 {
		return pipeline.Continue, nil
	}
	now := time.Now()
	if g.now != nil {
		now = g.now()
	}
	if now.Unix() > deadline {
		return pipeline.Drop(nil), nil
	}
	return pipeline.Continue, nil
}
