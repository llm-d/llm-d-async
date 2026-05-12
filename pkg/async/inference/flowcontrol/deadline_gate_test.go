/*
Copyright 2026 The llm-d Authors
Licensed under the Apache License, Version 2.0 (the "License");
*/

package flowcontrol

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func msgWithDeadline(deadline int64) *pipeline.EmbelishedRequestMessage {
	ir := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID: "x", Created: 1, Deadline: deadline,
	})
	return &pipeline.EmbelishedRequestMessage{InternalRequest: ir}
}

func TestDeadlineDropGate_Continues(t *testing.T) {
	g := NewDeadlineDropGate()
	g.now = func() time.Time { return time.Unix(100, 0) }

	v, err := g.Apply(context.Background(), msgWithDeadline(200))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if v.Terminate {
		t.Errorf("verdict = %+v, want Continue", v)
	}
}

func TestDeadlineDropGate_Drops(t *testing.T) {
	g := NewDeadlineDropGate()
	g.now = func() time.Time { return time.Unix(300, 0) }

	v, err := g.Apply(context.Background(), msgWithDeadline(200))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.Terminate || v.Redeliver {
		t.Errorf("verdict = %+v, want Drop", v)
	}
	if v.Result != nil {
		t.Errorf("deadline drop should be silent (no result), got %+v", v.Result)
	}
}

func TestDeadlineDropGate_NoDeadlinePassesThrough(t *testing.T) {
	g := NewDeadlineDropGate()
	v, _ := g.Apply(context.Background(), msgWithDeadline(0))
	if v.Terminate {
		t.Errorf("zero-deadline message should Continue, got %+v", v)
	}
}

func TestDeadlineDropGate_FactoryRegistration(t *testing.T) {
	f := NewGateFactory("")
	gate, err := f.CreateGate("deadline-drop", nil)
	if err != nil {
		t.Fatalf("CreateGate: %v", err)
	}
	if _, ok := gate.(pipeline.Gate); !ok {
		t.Errorf("deadline-drop factory result does not implement pipeline.Gate")
	}
}
