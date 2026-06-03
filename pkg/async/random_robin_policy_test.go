package async

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
		{Channel: make(chan *api.InternalRequest, msgsPerChannel)},
		{Channel: make(chan *api.InternalRequest, msgsPerChannel)},
		{Channel: make(chan *api.InternalRequest, msgsPerChannel)},
	}
	policy := NewRandomRobinPolicy()

	// Send messages to each channel
	for i, ch := range channels {
		for range msgsPerChannel {
			ch.Channel <- irID(string(rune('A' + i)))
		}
	}
	pools := map[string]pipeline.PoolConfig{
		"default": {ID: "default"},
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
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(nil, nil)
	if len(merged.Channels) != 0 {
		t.Fatalf("expected 0 channels in dispatch, got %d", len(merged.Channels))
	}
}

func TestMetaAlignmentAfterChannelClosure(t *testing.T) {
	// Three channels, each with distinct metadata.
	channels := []pipeline.RequestChannel{
		{Channel: make(chan *api.InternalRequest, 1), InferenceObjective: "obj-a", PoolID: "pool-a"},
		{Channel: make(chan *api.InternalRequest, 1), InferenceObjective: "obj-b", PoolID: "pool-a"},
		{Channel: make(chan *api.InternalRequest, 1), InferenceObjective: "obj-c", PoolID: "pool-a"},
	}
	pools := map[string]pipeline.PoolConfig{
		"pool-a": {ID: "pool-a", IGWBaseURL: "http://a", RequestPathURL: "/a"},
	}
	policy := NewRandomRobinPolicy()
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
		PoolID:             "my-pool",
	}
	pools := map[string]pipeline.PoolConfig{
		"my-pool": {ID: "my-pool", IGWBaseURL: "http://gateway", RequestPathURL: "/default/path"},
	}
	policy := NewRandomRobinPolicy()

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
				PoolID:             "test-pool",
			}
			pools := map[string]pipeline.PoolConfig{
				"test-pool": {ID: "test-pool", IGWBaseURL: tt.base, RequestPathURL: tt.path},
			}

			if tt.endpoint != "" {
				ch.Channel <- irWithEndpoint("test", tt.endpoint)
			} else {
				ch.Channel <- irID("test")
			}
			close(ch.Channel)

			policy := NewRandomRobinPolicy()
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
		PoolID:             "test-pool",
	}
	pools := map[string]pipeline.PoolConfig{
		"test-pool": {ID: "test-pool", IGWBaseURL: "http://gw", RequestPathURL: "/v1/completions"},
	}
	policy := NewRandomRobinPolicy()

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

func TestPoolHTTPHeadersMerged(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:            make(chan *api.InternalRequest, 3),
		InferenceObjective: "obj",
		PoolID:             "test-pool",
	}
	pools := map[string]pipeline.PoolConfig{
		"test-pool": {
			ID:             "test-pool",
			IGWBaseURL:     "http://gw",
			RequestPathURL: "/v1/completions",
			HTTPHeaders: map[string]string{
				"Authorization":                 "Bearer pool-tok",
				"X-Custom-Pool":                 "pool-val",
				"Content-Type":                  "application/custom", // overrides default
				"x-gateway-inference-objective": "pool-obj",           // should be overridden by channel objective
			},
		},
	}
	policy := NewRandomRobinPolicy()

	ch.Channel <- irID("only-pool-headers")
	ch.Channel <- irWithHeaders("request-override", map[string]string{
		"Authorization": "Bearer req-tok", // overrides pool
		"X-Custom-Req":  "req-val",
	})
	close(ch.Channel)

	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	mergedChannel := merged.Channels["test-pool"]

	deadline := time.After(2 * time.Second)
	results := map[string]map[string]string{}
	for range 2 {
		select {
		case msg := <-mergedChannel:
			results[msg.PublicRequest.ReqID()] = msg.HttpHeaders
		case <-deadline:
			t.Fatal("timed out")
		}
	}

	// 1. Check message with only pool headers
	h1 := results["only-pool-headers"]
	if h1["Authorization"] != "Bearer pool-tok" {
		t.Errorf("expected Bearer pool-tok, got %s", h1["Authorization"])
	}
	if h1["X-Custom-Pool"] != "pool-val" {
		t.Errorf("expected pool-val, got %s", h1["X-Custom-Pool"])
	}
	if h1["Content-Type"] != "application/custom" {
		t.Errorf("expected application/custom, got %s", h1["Content-Type"])
	}
	if h1["x-gateway-inference-objective"] != "obj" {
		t.Errorf("expected obj, got %s", h1["x-gateway-inference-objective"])
	}

	// 2. Check message with request headers overriding pool headers
	h2 := results["request-override"]
	if h2["Authorization"] != "Bearer req-tok" {
		t.Errorf("expected Bearer req-tok, got %s", h2["Authorization"])
	}
	if h2["X-Custom-Pool"] != "pool-val" {
		t.Errorf("expected pool-val, got %s", h2["X-Custom-Pool"])
	}
	if h2["X-Custom-Req"] != "req-val" {
		t.Errorf("expected req-val, got %s", h2["X-Custom-Req"])
	}
	if h2["Content-Type"] != "application/custom" {
		t.Errorf("expected application/custom, got %s", h2["Content-Type"])
	}
}

func TestMergedChannelIsBuffered(t *testing.T) {
	numChannels := 3
	channels := make([]pipeline.RequestChannel, numChannels)
	for i := range numChannels {
		channels[i] = pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1)}
	}
	policy := NewRandomRobinPolicy()
	pools := map[string]pipeline.PoolConfig{
		"default": {ID: "default"},
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
		{PoolID: "non-existent-pool"},
	}
	policy := NewRandomRobinPolicy()

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected MergeRequestChannels to panic when pool ID is missing in pools map")
		} else {
			expectedMsg := `pool "non-existent-pool" not found in pools map`
			actualMsg := fmt.Sprintf("%v", r)
			if actualMsg != expectedMsg {
				t.Errorf("Expected panic message %q, got %q", expectedMsg, actualMsg)
			}
		}
	}()

	policy.MergeRequestChannels(channels, map[string]pipeline.PoolConfig{})
}
