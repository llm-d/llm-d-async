/*
Copyright 2026 The llm-d Authors
Licensed under the Apache License, Version 2.0 (the "License");
*/

package flowcontrol

import (
	"context"
	"testing"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func TestConstantDecisionGate(t *testing.T) {
	cases := []struct {
		name string
		v    pipeline.Verdict
	}{
		{"continue", pipeline.Continue},
		{"drop", pipeline.Drop(nil)},
		{"refuse", pipeline.Refuse()},
	}
	for _, c := range cases {
		g := NewConstantDecisionGate(c.v)
		got, err := g.Apply(context.Background(), &pipeline.EmbelishedRequestMessage{})
		if err != nil {
			t.Errorf("%s err: %v", c.name, err)
		}
		if got != c.v {
			t.Errorf("%s: Apply returned %+v, want %+v", c.name, got, c.v)
		}
	}
}

func TestParseDecision(t *testing.T) {
	cases := map[string]pipeline.Verdict{
		"continue": pipeline.Continue,
		"Drop":     pipeline.Drop(nil),
		"REFUSE":   pipeline.Refuse(),
	}
	for in, want := range cases {
		got, err := parseDecision(in)
		if err != nil || got != want {
			t.Errorf("parseDecision(%q) = (%+v, %v), want (%+v, nil)", in, got, err, want)
		}
	}
	if _, err := parseDecision("nope"); err == nil {
		t.Errorf("parseDecision should reject unknown values")
	}
}

func TestConstantDecisionGateFactory(t *testing.T) {
	f := NewGateFactory("")
	for _, d := range []string{"continue", "drop", "refuse"} {
		gate, err := f.CreateGate("constant-decision", map[string]string{"decision": d})
		if err != nil {
			t.Fatalf("decision=%q: %v", d, err)
		}
		if _, ok := gate.(pipeline.Gate); !ok {
			t.Errorf("decision=%q: factory result does not implement pipeline.Gate", d)
		}
	}
	if _, err := f.CreateGate("constant-decision", map[string]string{"decision": "nope"}); err == nil {
		t.Errorf("factory should reject unknown decisions")
	}
}
