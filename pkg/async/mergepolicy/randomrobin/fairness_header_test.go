package randomrobin

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

func mergeOne(t *testing.T, policy *RandomRobinPolicy, callerHeaders map[string]string) pipeline.EmbelishedRequestMessage {
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
	policy := NewRandomRobinPolicy("test", Config{FairnessHeader: api.FairnessIDHeader})

	msg := mergeOne(t, policy, nil)
	if got := msg.HttpHeaders[api.FairnessIDHeader]; got != "tenant-a" {
		t.Errorf("fairness header = %q, want %q", got, "tenant-a")
	}
}

// The stamp has to land after the caller's headers are merged in, or a caller
// could present a fairness ID that differs from the one quota accounts on. Only
// a dispatch-path test pins that ordering.
func TestFairnessHeaderOverridesCallerSuppliedValue(t *testing.T) {
	policy := NewRandomRobinPolicy("test", Config{FairnessHeader: api.FairnessIDHeader})

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

func TestFairnessHeaderInvalidNameRejectedAtStartup(t *testing.T) {
	factory, ok := plugins.Lookup("random-robin")
	if !ok {
		t.Fatal("random-robin plugin not registered")
	}

	if _, err := factory("test", json.RawMessage(`{"fairness_header": "x-fair id"}`), nil); err == nil {
		t.Error("expected an error for an illegal fairness_header name, got nil")
	}
}

func TestFairnessHeaderDefaultsAndDisableViaConfig(t *testing.T) {
	factory, ok := plugins.Lookup("random-robin")
	if !ok {
		t.Fatal("random-robin plugin not registered")
	}

	// Absent parameters stamp the default header from the default attribute.
	plugin, err := factory("test", nil, nil)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	msg := mergeOne(t, plugin.(*RandomRobinPolicy), nil)
	if got := msg.HttpHeaders[api.FairnessIDHeader]; got != "tenant-a" {
		t.Errorf("fairness header = %q, want %q", got, "tenant-a")
	}

	// An explicit empty header disables stamping.
	plugin, err = factory("test", json.RawMessage(`{"fairness_header": ""}`), nil)
	if err != nil {
		t.Fatalf("factory error: %v", err)
	}
	msg = mergeOne(t, plugin.(*RandomRobinPolicy), nil)
	if _, stamped := msg.HttpHeaders[api.FairnessIDHeader]; stamped {
		t.Error("fairness header should be absent when disabled")
	}
}
