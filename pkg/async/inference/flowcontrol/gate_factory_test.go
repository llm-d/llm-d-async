/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package flowcontrol

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestGateFactory_WithCacheTTL(t *testing.T) {
	ttl := 10 * time.Second
	factory := NewGateFactoryWithCacheTTL("http://localhost:9090", ttl)
	if factory.prometheusURL != "http://localhost:9090" {
		t.Errorf("expected prometheusURL http://localhost:9090, got %s", factory.prometheusURL)
	}
	if factory.cacheTTL != ttl {
		t.Errorf("expected cacheTTL %v, got %v", ttl, factory.cacheTTL)
	}
}

func TestGateFactory_CreateConstantGate(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("constant", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gate == nil {
		t.Fatal("expected non-nil gate")
	}
	v, err := gate.Apply(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Terminate {
		t.Errorf("constant gate should Continue, got Terminate=true")
	}
}

func TestGateFactory_UnknownGateType(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("unknown-type", nil)
	if err == nil {
		t.Fatal("expected error for unknown gate type")
	}
	if gate != nil {
		t.Fatalf("expected nil gate, got %v", gate)
	}
	if !strings.Contains(err.Error(), "unknown gate type") {
		t.Fatalf("error %q does not mention %q", err.Error(), "unknown gate type")
	}
}

func TestGateFactory_EmptyGateType(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gate == nil {
		t.Fatal("expected non-nil gate")
	}
}

func TestGateFactory_LocalRateLimitMissingParam(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("local-rate-limit", map[string]string{})
	if err == nil {
		t.Fatal("expected error for missing requests_per_minute")
	}
	if gate != nil {
		t.Error("expected nil gate on error")
	}
}

func TestGateFactory_LocalRateLimitValid(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("local-rate-limit", map[string]string{
		"requests_per_minute": "60",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gate == nil {
		t.Fatal("expected non-nil gate")
	}
}

func TestGateFactory_LocalMaxConcurrencyMissingParam(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("local-max-concurrency", map[string]string{})
	if err == nil {
		t.Fatal("expected error for missing max_concurrency")
	}
	if gate != nil {
		t.Error("expected nil gate on error")
	}
}

func TestGateFactory_LocalMaxConcurrencyValid(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("local-max-concurrency", map[string]string{
		"max_concurrency": "10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gate == nil {
		t.Fatal("expected non-nil gate")
	}
}

func TestGateFactory_DeadlineDrop(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("deadline-drop", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gate == nil {
		t.Fatal("expected non-nil gate")
	}
}

func TestGateFactory_ConstantDecisionContinue(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("constant-decision", map[string]string{
		"decision": "continue",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gate == nil {
		t.Fatal("expected non-nil gate")
	}
}

func TestGateFactory_ConstantDecisionInvalid(t *testing.T) {
	factory := NewGateFactory("")
	gate, err := factory.CreateGate("constant-decision", map[string]string{
		"decision": "invalid",
	})
	if err == nil {
		t.Fatal("expected error for invalid decision")
	}
	if gate != nil {
		t.Error("expected nil gate on error")
	}
}
