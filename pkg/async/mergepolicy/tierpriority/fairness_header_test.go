package tierpriority

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/plugins"
)

// Stamping behavior itself is covered by the shared fairness package. These
// tests only pin that the policy wires the stamper into its dispatch path and
// resolves its parameters.

func mergeOne(t *testing.T, policy *TierPriorityPolicy, callerHeaders map[string]string) pipeline.EmbelishedRequestMessage {
	t.Helper()
	ch := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 1),
		WorkerPoolID: "pool-f",
		IGWBaseURL:   "http://gw",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-f": {ID: "pool-f", Workers: 1},
	}
	ch.Channel <- api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       "m1",
		Created:  1,
		Deadline: 9999999999,
		Metadata: map[string]string{"userid": "tenant-a"},
		Headers:  callerHeaders,
	})
	close(ch.Channel)

	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	select {
	case msg := <-dispatch.Channels["pool-f"]:
		return msg
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for merged message")
		return pipeline.EmbelishedRequestMessage{}
	}
}

func TestFairnessHeaderStamped(t *testing.T) {
	policy := NewTierPriorityPolicy("test-policy", Config{
		PriorityHeader: "x-gateway-priority",
		TierLabel:      "tier",
		FairnessHeader: api.FairnessIDHeader,
	})

	msg := mergeOne(t, policy, nil)
	if got := msg.HttpHeaders[api.FairnessIDHeader]; got != "tenant-a" {
		t.Errorf("fairness header = %q, want %q", got, "tenant-a")
	}
}

func TestFairnessHeaderDefaultsAndDisableViaConfig(t *testing.T) {
	factory, ok := plugins.Lookup("tier-priority")
	if !ok {
		t.Fatal("tier-priority plugin not registered")
	}

	// Absent parameters stamp the default header from the default attribute.
	plugin, err := factory("test", nil, nil)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	msg := mergeOne(t, plugin.(*TierPriorityPolicy), nil)
	if got := msg.HttpHeaders[api.FairnessIDHeader]; got != "tenant-a" {
		t.Errorf("fairness header = %q, want %q", got, "tenant-a")
	}

	// An explicit empty header disables stamping.
	plugin, err = factory("test", json.RawMessage(`{"fairness_header": ""}`), nil)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	msg = mergeOne(t, plugin.(*TierPriorityPolicy), nil)
	if _, stamped := msg.HttpHeaders[api.FairnessIDHeader]; stamped {
		t.Error("fairness header should be absent when disabled")
	}
}

// The stamp has to land after the caller's headers are merged in, or a caller
// could present a fairness ID that differs from the one quota accounts on. Only
// a dispatch-path test pins that ordering.
func TestFairnessHeaderOverridesCallerSuppliedValue(t *testing.T) {
	policy := NewTierPriorityPolicy("test-policy", Config{
		PriorityHeader: "x-gateway-priority",
		TierLabel:      "tier",
		FairnessHeader: api.FairnessIDHeader,
	})

	msg := mergeOne(t, policy, map[string]string{"X-LLM-D-Inference-Fairness-ID": "spoofed"})

	if got := msg.HttpHeaders[api.FairnessIDHeader]; got != "tenant-a" {
		t.Errorf("fairness header = %q, want %q", got, "tenant-a")
	}
	for k := range msg.HttpHeaders {
		if k != api.FairnessIDHeader && strings.EqualFold(k, api.FairnessIDHeader) {
			t.Errorf("caller case variant %q survived alongside the stamp", k)
		}
	}
}

// An illegal header name is one net/http refuses to write, which would fail
// every dispatched request permanently rather than just losing the header.
func TestIllegalHeaderNamesRejectedAtStartup(t *testing.T) {
	factory, ok := plugins.Lookup("tier-priority")
	if !ok {
		t.Fatal("tier-priority plugin not registered")
	}

	tests := []struct {
		name   string
		params string
	}{
		{name: "fairness_header", params: `{"fairness_header": "x-fair id"}`},
		{name: "priority_header", params: `{"priority_header": "x-pri id"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := factory("test", json.RawMessage(tt.params), nil); err == nil {
				t.Errorf("expected an error for an illegal %s name, got nil", tt.name)
			}
		})
	}
}

// The numeric priority header has no consumer in upstream llm-d gateways, so it
// must stay unstamped unless the operator explicitly opts in.
func TestPriorityHeaderNotStampedByDefault(t *testing.T) {
	factory, ok := plugins.Lookup("tier-priority")
	if !ok {
		t.Fatal("tier-priority plugin not registered")
	}

	// Absent parameters leave the priority header unset and unstamped.
	plugin, err := factory("test", nil, nil)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	msg := mergeOne(t, plugin.(*TierPriorityPolicy), nil)
	if _, stamped := msg.HttpHeaders["x-gateway-priority"]; stamped {
		t.Error("priority header should be absent when unspecified")
	}

	// An explicit header opts back in.
	plugin, err = factory("test", json.RawMessage(`{"priority_header": "x-gateway-priority"}`), nil)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	msg = mergeOne(t, plugin.(*TierPriorityPolicy), nil)
	if _, stamped := msg.HttpHeaders["x-gateway-priority"]; !stamped {
		t.Error("priority header should be stamped when explicitly configured")
	}
}
