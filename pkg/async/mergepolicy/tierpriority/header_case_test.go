package tierpriority

import (
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

// onlyHeader asserts that exactly one key of headers names name in any casing
// and returns its spelling and value.
func onlyHeader(t *testing.T, headers map[string]string, name string) (string, string) {
	t.Helper()
	var key, value string
	for k, v := range headers {
		if strings.EqualFold(k, name) {
			if key != "" {
				t.Fatalf("header %q present twice: %q and %q (%v)", name, key, k, headers)
			}
			key, value = k, v
		}
	}
	if key == "" {
		t.Fatalf("header %q missing from %v", name, headers)
	}
	return key, value
}

// The worker applies the header map with net/http's Header.Set, which
// canonicalizes names, so two spellings of one header would resolve to
// whichever key map iteration visited last. Precedence must not depend on that.
func TestHeaderPrecedenceHoldsInAnyCasing(t *testing.T) {
	policy := NewTierPriorityPolicy("test-policy", Config{
		PriorityHeader: "x-gateway-priority",
		TierLabel:      "tier",
		FairnessHeader: api.FairnessIDHeader,
		LaneObjectives: map[string]string{"overflow-batch": "lane-objective"},
	})
	ch := pipeline.RequestChannel{
		Channel:            make(chan *api.InternalRequest, 1),
		WorkerPoolID:       "pool-f",
		IGWBaseURL:         "http://gw",
		InferenceObjective: "queue-objective",
	}
	pools := map[string]pipeline.WorkerPoolConfig{"pool-f": {ID: "pool-f", Workers: 1}}
	ch.Channel <- api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID: "m1", Created: 1, Deadline: 9999999999,
		Metadata: map[string]string{"userid": "tenant-a"},
		Headers: map[string]string{
			"content-type":                  "text/plain",
			"X-Gateway-Inference-Objective": "caller-objective",
			"X-Gateway-Priority":            "99",
			"X-LLM-D-Inference-Objective":   "caller-lane",
			"x-llm-d-inference-fairness-id": "spoofed",
			"X-Custom":                      "kept",
		},
	})
	close(ch.Channel)

	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	var msg pipeline.EmbelishedRequestMessage
	select {
	case msg = <-dispatch.Channels["pool-f"]:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for merged message")
	}

	// Caller wins over the stamped defaults, keeping the caller's spelling.
	if k, v := onlyHeader(t, msg.HttpHeaders, "Content-Type"); k != "content-type" || v != "text/plain" {
		t.Errorf("Content-Type = %q: %q, want caller's content-type: text/plain", k, v)
	}
	if k, v := onlyHeader(t, msg.HttpHeaders, "x-gateway-inference-objective"); k != "X-Gateway-Inference-Objective" || v != "caller-objective" {
		t.Errorf("queue objective = %q: %q, want the caller's", k, v)
	}
	// Stamps after the caller win over the caller.
	if k, v := onlyHeader(t, msg.HttpHeaders, "x-gateway-priority"); k != "x-gateway-priority" || v == "99" {
		t.Errorf("priority = %q: %q, want the policy's value under its own spelling", k, v)
	}
	if k, v := onlyHeader(t, msg.HttpHeaders, api.ObjectiveHeader); k != api.ObjectiveHeader || v != "lane-objective" {
		t.Errorf("lane objective = %q: %q, want the lane's", k, v)
	}
	if k, v := onlyHeader(t, msg.HttpHeaders, api.FairnessIDHeader); k != api.FairnessIDHeader || v != "tenant-a" {
		t.Errorf("fairness = %q: %q, want the stamped tenant", k, v)
	}
	if msg.HttpHeaders["X-Custom"] != "kept" {
		t.Errorf("unrelated caller header lost: %v", msg.HttpHeaders)
	}
}
