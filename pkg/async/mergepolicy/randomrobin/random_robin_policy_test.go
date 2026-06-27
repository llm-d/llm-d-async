package randomrobin

import (
	"fmt"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func irID(id string) *api.InternalRequest {
	return api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
	})
}

func irWithEndpoint(id, endpoint string) *api.InternalRequest {
	return api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
		Endpoint: endpoint,
	})
}

func TestProcessAllChannels(t *testing.T) {
	msgsPerChannel := 5
	channels := []pipeline.RequestChannel{
		{Channel: make(chan *api.InternalRequest, msgsPerChannel), IGWBaseURL: "http://gw", RequestPathURL: "/v1"},
		{Channel: make(chan *api.InternalRequest, msgsPerChannel), IGWBaseURL: "http://gw", RequestPathURL: "/v1"},
		{Channel: make(chan *api.InternalRequest, msgsPerChannel), IGWBaseURL: "http://gw", RequestPathURL: "/v1"},
	}
	policy := NewRandomRobinPolicy("test")

	// Send messages to each channel
	for i, ch := range channels {
		for range msgsPerChannel {
			ch.Channel <- irID(string(rune('A' + i)))
		}
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"default": {ID: "default", Workers: 1},
	}
	dispatch := policy.MergeRequestChannels(channels, pools)
	mergedChannel := dispatch.Channels["default"]
	close(channels[0].Channel)
	close(channels[1].Channel)
	close(channels[2].Channel)

	counts := map[string]int{}
	totalMessages := msgsPerChannel * 3
	for range totalMessages {
		msg := <-mergedChannel
		if msg.PublicRequest == nil {
			t.Fatal("expected PublicRequest")
		}
		counts[msg.PublicRequest.ReqID()]++

	}

	for i := range 3 {
		id := string(rune('A' + i))
		if counts[id] != msgsPerChannel {
			t.Errorf("Expected %d messages from channel %s, got %d", msgsPerChannel, id, counts[id])
		}
	}
}

func TestEmptyChannelsReturnsClosed(t *testing.T) {
	policy := NewRandomRobinPolicy("test")
	merged := policy.MergeRequestChannels(nil, nil)
	if len(merged.Channels) != 0 {
		t.Fatalf("expected 0 channels in dispatch, got %d", len(merged.Channels))
	}
}

func TestMetaAlignmentAfterChannelClosure(t *testing.T) {
	// Three channels, each with distinct metadata.
	channels := []pipeline.RequestChannel{
		{Channel: make(chan *api.InternalRequest, 1), InferenceObjective: "obj-a", WorkerPoolID: "pool-a", IGWBaseURL: "http://a", RequestPathURL: "/a"},
		{Channel: make(chan *api.InternalRequest, 1), InferenceObjective: "obj-b", WorkerPoolID: "pool-a", IGWBaseURL: "http://a", RequestPathURL: "/a"},
		{Channel: make(chan *api.InternalRequest, 1), InferenceObjective: "obj-c", WorkerPoolID: "pool-a", IGWBaseURL: "http://a", RequestPathURL: "/a"},
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-a": {ID: "pool-a", Workers: 1},
	}
	policy := NewRandomRobinPolicy("test")
	merged := policy.MergeRequestChannels(channels, pools)
	mergedChannel := merged.Channels["pool-a"]

	// Close the middle channel to shift indices.
	close(channels[1].Channel)

	// Wait until the merge goroutine observes the closure and realigns
	// channel metadata. This avoids timing flakes from fixed sleeps.
	realigned := false
	realignDeadline := time.After(2 * time.Second)
	for !realigned {
		select {
		case <-realignDeadline:
			t.Fatal("timed out waiting for channel metadata realignment")
		case channels[2].Channel <- irID("probe-c"):
		}

		select {
		case <-realignDeadline:
			t.Fatal("timed out waiting for channel metadata realignment")
		case msg := <-mergedChannel:
			if msg.PublicRequest == nil {
				t.Fatal("nil request")
			}
			if msg.PublicRequest.ReqID() != "probe-c" {
				t.Fatalf("unexpected message id while waiting for realignment: %s", msg.PublicRequest.ReqID())
			}
			realigned = msg.RequestURL == "http://a/a" &&
				msg.HttpHeaders["x-gateway-inference-objective"] == "obj-c"
		}
	}

	// Send one message on each remaining channel.
	channels[0].Channel <- irID("from-a")
	channels[2].Channel <- irID("from-c")

	deadline := time.After(2 * time.Second)
	for range 2 {
		select {
		case msg := <-mergedChannel:
			if msg.PublicRequest == nil {
				t.Fatal("nil request")
			}
			switch msg.PublicRequest.ReqID() {
			case "from-a":
				if msg.RequestURL != "http://a/a" {
					t.Errorf("expected RequestURL http://a/a, got %s", msg.RequestURL)
				}
				if msg.HttpHeaders["x-gateway-inference-objective"] != "obj-a" {
					t.Errorf("expected InferenceObjective obj-a, got %s", msg.HttpHeaders["x-gateway-inference-objective"])
				}
			case "from-c":
				if msg.RequestURL != "http://a/a" {
					t.Errorf("expected RequestURL http://a/a, got %s", msg.RequestURL)
				}
				if msg.HttpHeaders["x-gateway-inference-objective"] != "obj-c" {
					t.Errorf("expected InferenceObjective obj-c, got %s", msg.HttpHeaders["x-gateway-inference-objective"])
				}
			default:
				t.Fatalf("unexpected message id: %s", msg.PublicRequest.ReqID())
			}
		case <-deadline:
			t.Fatal("timed out waiting for messages")
		}
	}
}

func TestPerMessageEndpointOverridesChannelURL(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:            make(chan *api.InternalRequest, 2),
		InferenceObjective: "obj",
		WorkerPoolID:       "my-pool",
		IGWBaseURL:         "http://gateway",
		RequestPathURL:     "/default/path",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"my-pool": {ID: "my-pool", Workers: 1},
	}
	policy := NewRandomRobinPolicy("test")

	// One message with endpoint, one without.
	ch.Channel <- irWithEndpoint("with-ep", "/v1/custom")
	ch.Channel <- irID("without-ep")
	close(ch.Channel)

	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	mergedChannel := merged.Channels["my-pool"]

	deadline := time.After(2 * time.Second)
	results := map[string]string{}
	for range 2 {
		select {
		case msg := <-mergedChannel:
			results[msg.PublicRequest.ReqID()] = msg.RequestURL
		case <-deadline:
			t.Fatal("timed out waiting for messages")
		}
	}

	if url := results["with-ep"]; url != "http://gateway/v1/custom" {
		t.Errorf("expected http://gateway/v1/custom, got %s", url)
	}
	if url := results["without-ep"]; url != "http://gateway/default/path" {
		t.Errorf("expected http://gateway/default/path, got %s", url)
	}
}

func TestURLJoinPathHandlesSlashes(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		path     string
		endpoint string
		wantURL  string
	}{
		{"trailing slash on base", "http://gateway/", "/v1/completions", "", "http://gateway/v1/completions"},
		{"no leading slash on path", "http://gateway", "v1/completions", "", "http://gateway/v1/completions"},
		{"base with subpath", "http://gateway/api", "v1/completions", "", "http://gateway/api/v1/completions"},
		{"endpoint overrides with trailing slash base", "http://gateway/", "/default", "/v1/custom", "http://gateway/v1/custom"},
		{"no slashes at all", "http://gateway", "v1/completions", "", "http://gateway/v1/completions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := pipeline.RequestChannel{
				Channel:            make(chan *api.InternalRequest, 1),
				InferenceObjective: "obj",
				WorkerPoolID:       "test-pool",
				IGWBaseURL:         tt.base,
				RequestPathURL:     tt.path,
			}
			pools := map[string]pipeline.WorkerPoolConfig{
				"test-pool": {ID: "test-pool", Workers: 1},
			}

			if tt.endpoint != "" {
				ch.Channel <- irWithEndpoint("test", tt.endpoint)
			} else {
				ch.Channel <- irID("test")
			}
			close(ch.Channel)

			policy := NewRandomRobinPolicy("test")
			merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
			mergedChannel := merged.Channels["test-pool"]

			select {
			case msg := <-mergedChannel:
				if msg.RequestURL != tt.wantURL {
					t.Errorf("expected %s, got %s", tt.wantURL, msg.RequestURL)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out")
			}
		})
	}
}

func irWithHeaders(id string, headers map[string]string) *api.InternalRequest {
	return api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
		Headers:  headers,
	})
}

func TestPerRequestHeadersMerged(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:            make(chan *api.InternalRequest, 3),
		InferenceObjective: "obj",
		WorkerPoolID:       "test-pool",
		IGWBaseURL:         "http://gw",
		RequestPathURL:     "/v1/completions",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"test-pool": {ID: "test-pool", Workers: 1},
	}
	policy := NewRandomRobinPolicy("test")

	ch.Channel <- irWithHeaders("custom", map[string]string{
		"Authorization": "Bearer tok",
		"X-Trace-ID":    "abc",
	})
	ch.Channel <- irWithHeaders("override-objective", map[string]string{
		"x-gateway-inference-objective": "my-obj",
	})
	ch.Channel <- irID("no-headers")
	close(ch.Channel)

	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	mergedChannel := merged.Channels["test-pool"]

	deadline := time.After(2 * time.Second)
	results := map[string]map[string]string{}
	for range 3 {
		select {
		case msg := <-mergedChannel:
			results[msg.PublicRequest.ReqID()] = msg.HttpHeaders
		case <-deadline:
			t.Fatal("timed out")
		}
	}

	// Custom headers are merged in.
	if h := results["custom"]; h["Authorization"] != "Bearer tok" || h["X-Trace-ID"] != "abc" {
		t.Errorf("custom headers not merged: %v", h)
	}
	// Default headers still present.
	if h := results["custom"]; h["Content-Type"] != "application/json" {
		t.Errorf("Content-Type missing: %v", h)
	}

	// User can override inference objective.
	if h := results["override-objective"]; h["x-gateway-inference-objective"] != "my-obj" {
		t.Errorf("expected overridden objective, got %v", h)
	}

	// No headers: defaults only.
	if h := results["no-headers"]; h["Content-Type"] != "application/json" || h["x-gateway-inference-objective"] != "obj" {
		t.Errorf("default headers wrong: %v", h)
	}
}

func TestMergedChannelIsBuffered(t *testing.T) {
	numChannels := 3
	channels := make([]pipeline.RequestChannel, numChannels)
	for i := range numChannels {
		channels[i] = pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1), IGWBaseURL: "http://gw", RequestPathURL: "/v1"}
	}
	policy := NewRandomRobinPolicy("test")
	pools := map[string]pipeline.WorkerPoolConfig{
		"default": {ID: "default", Workers: 1},
	}
	merged := policy.MergeRequestChannels(channels, pools)
	mergedChannel := merged.Channels["default"]

	// Send one message per input channel.
	for i, ch := range channels {
		ch.Channel <- irID(string(rune('A' + i)))
	}

	// The merge goroutine should be able to forward all messages into the
	// buffered merged channel without a consumer draining it. With an
	// unbuffered channel this would deadlock because the goroutine blocks
	// on the first send.
	deadline := time.After(2 * time.Second)
	received := 0
	for received < numChannels {
		select {
		case <-mergedChannel:
			received++
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d messages — merged channel may be unbuffered", received, numChannels)
		}
	}
}

func TestMergeRequestChannels_PanicOnMissingPool(t *testing.T) {
	channels := []pipeline.RequestChannel{
		{WorkerPoolID: "non-existent-pool"},
	}
	policy := NewRandomRobinPolicy("test")

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MergeRequestChannels to panic when pool ID is missing in pools map")
		} else {
			expectedMsg := `worker pool "non-existent-pool" not found in pools map`
			actualMsg := fmt.Sprintf("%v", r)
			if actualMsg != expectedMsg {
				t.Errorf("Expected panic message %q, got %q", expectedMsg, actualMsg)
			}
		}
	}()

	policy.MergeRequestChannels(channels, map[string]pipeline.WorkerPoolConfig{})
}
