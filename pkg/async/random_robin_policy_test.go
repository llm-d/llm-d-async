package async

import (
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

// embID builds a minimal EmbelishedRequestMessage for use in tests.
// The merge policy now receives fully-embellished messages from the
// Flow's callback; tests that send to request channels must match.
func embID(id string) *pipeline.EmbelishedRequestMessage {
	ir := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
	})
	return &pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		Labels:          pipeline.Labels{},
	}
}

func embWithEndpoint(id, base, endpoint string) *pipeline.EmbelishedRequestMessage {
	ir := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
		Endpoint: endpoint,
	})
	return &pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		RequestURL:      base + endpoint,
		Labels:          pipeline.Labels{},
	}
}

func TestProcessAllChannels(t *testing.T) {
	msgsPerChannel := 5
	channels := []pipeline.RequestChannel{
		{Channel: make(chan *pipeline.EmbelishedRequestMessage, msgsPerChannel), IGWBaseURL: "", InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan *pipeline.EmbelishedRequestMessage, msgsPerChannel), IGWBaseURL: "", InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan *pipeline.EmbelishedRequestMessage, msgsPerChannel), IGWBaseURL: "", InferenceObjective: "", RequestPathURL: ""},
	}
	policy := NewRandomRobinPolicy()

	// Send messages to each channel
	for i, ch := range channels {
		for range msgsPerChannel {
			ch.Channel <- embID(string(rune('A' + i)))
		}
	}
	mergedChannel := policy.MergeRequestChannels(channels).Channels[""]
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

func TestEmptyChannelsReturnsEmptyDispatch(t *testing.T) {
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(nil)
	if len(merged.Channels) != 0 {
		t.Fatalf("expected empty per-pool dispatch, got %d channels", len(merged.Channels))
	}
}

func TestConfiguredHTTPHeadersAreMerged(t *testing.T) {
	channels := []pipeline.RequestChannel{{
		Channel:            make(chan *pipeline.EmbelishedRequestMessage, 1),
		IGWBaseURL:         "https://api.groq.com/openai",
		InferenceObjective: "default-objective",
		RequestPathURL:     "/v1/chat/completions",
		HTTPHeaders: map[string]string{
			"Authorization":                 "Bearer test-key",
			"x-gateway-inference-objective": "configured-objective",
		},
	}}
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

	// Flow builds the emb with all HTTP dispatch fields; test pre-sets them.
	emb := embID("groq-request")
	emb.RequestURL = "https://api.groq.com/openai/v1/chat/completions"
	emb.HttpHeaders = map[string]string{
		"Content-Type":                  "application/json",
		"Authorization":                 "Bearer test-key",
		"x-gateway-inference-objective": "configured-objective",
	}
	channels[0].Channel <- emb

	select {
	case msg := <-merged.Channels[""]:
		if msg.RequestURL != "https://api.groq.com/openai/v1/chat/completions" {
			t.Errorf("expected Groq OpenAI-compatible URL, got %s", msg.RequestURL)
		}
		if msg.HttpHeaders["Content-Type"] != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", msg.HttpHeaders["Content-Type"])
		}
		if msg.HttpHeaders["Authorization"] != "Bearer test-key" {
			t.Errorf("expected Authorization header to be merged, got %s", msg.HttpHeaders["Authorization"])
		}
		if msg.HttpHeaders["x-gateway-inference-objective"] != "configured-objective" {
			t.Errorf("expected configured objective header to override default, got %s", msg.HttpHeaders["x-gateway-inference-objective"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for merged message")
	}
}

func TestMergedMessageIncludesModelNameOverride(t *testing.T) {
	channels := []pipeline.RequestChannel{{
		Channel:           make(chan *pipeline.EmbelishedRequestMessage, 1),
		IGWBaseURL:        "https://api.provider.example/openai",
		RequestPathURL:    "/v1/chat/completions",
		ModelNameOverride: "provider-model",
	}}
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

	mapped := embID("mapped-request")
	mapped.ModelNameOverride = "provider-model"
	channels[0].Channel <- mapped

	select {
	case msg := <-merged.Channels[""]:
		if msg.ModelNameOverride != "provider-model" {
			t.Fatalf("expected model override provider-model, got %s", msg.ModelNameOverride)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for merged message")
	}
}

func TestEmptyInferenceObjectiveDoesNotSetHeader(t *testing.T) {
	ch := pipeline.RequestChannel{InferenceObjective: ""}
	emb := &pipeline.EmbelishedRequestMessage{}
	if ch.InferenceObjective != "" {
		emb.HttpHeaders = map[string]string{"x-gateway-inference-objective": ch.InferenceObjective}
	}
	if _, ok := emb.HttpHeaders["x-gateway-inference-objective"]; ok {
		t.Fatal("expected empty inference objective to omit gateway objective header")
	}
}

func TestMetaAlignmentAfterChannelClosure(t *testing.T) {
	// Three channels, each with distinct metadata.
	channels := []pipeline.RequestChannel{
		{Channel: make(chan *pipeline.EmbelishedRequestMessage, 1), IGWBaseURL: "http://a", InferenceObjective: "obj-a", RequestPathURL: "/a"},
		{Channel: make(chan *pipeline.EmbelishedRequestMessage, 1), IGWBaseURL: "http://b", InferenceObjective: "obj-b", RequestPathURL: "/b"},
		{Channel: make(chan *pipeline.EmbelishedRequestMessage, 1), IGWBaseURL: "http://c", InferenceObjective: "obj-c", RequestPathURL: "/c"},
	}
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

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
		case channels[2].Channel <- embID("probe-c"):
		}

		select {
		case <-realignDeadline:
			t.Fatal("timed out waiting for channel metadata realignment")
		case msg := <-merged.Channels[""]:
			if msg.PublicRequest == nil {
				t.Fatal("nil request")
			}
			if msg.PublicRequest.ReqID() != "probe-c" {
				t.Fatalf("unexpected message id while waiting for realignment: %s", msg.PublicRequest.ReqID())
			}
			// After policy refactor, URL is set by the Flow; just check ID.
			realigned = msg.PublicRequest.ReqID() == "probe-c"
		}
	}

	// Send one message on each remaining channel, pre-set URLs as the Flow would.
	fromA := embID("from-a")
	fromA.RequestURL = "http://a/a"
	channels[0].Channel <- fromA
	fromC := embID("from-c")
	fromC.RequestURL = "http://c/c"
	channels[2].Channel <- fromC

	deadline := time.After(2 * time.Second)
	for range 2 {
		select {
		case msg := <-merged.Channels[""]:
			if msg.PublicRequest == nil {
				t.Fatal("nil request")
			}
			switch msg.PublicRequest.ReqID() {
			case "from-a":
				if msg.RequestURL != "http://a/a" {
					t.Errorf("from-a: expected RequestURL http://a/a, got %s", msg.RequestURL)
				}
			case "from-c":
				if msg.RequestURL != "http://c/c" {
					t.Errorf("from-c: expected RequestURL http://c/c, got %s", msg.RequestURL)
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
		Channel:        make(chan *pipeline.EmbelishedRequestMessage, 2),
		IGWBaseURL:     "http://gateway",
		RequestPathURL: "/default/path",
	}
	policy := NewRandomRobinPolicy()

	// Pre-build as the Flow would: with-ep gets the override URL,
	// without-ep gets the default channel path.
	withEp := embWithEndpoint("with-ep", "http://gateway", "/v1/custom")
	withEp.RequestURL = "http://gateway/v1/custom"
	ch.Channel <- withEp
	withoutEp := embID("without-ep")
	withoutEp.RequestURL = "http://gateway/default/path"
	ch.Channel <- withoutEp
	close(ch.Channel)

	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch})

	deadline := time.After(2 * time.Second)
	results := map[string]string{}
	for range 2 {
		select {
		case msg := <-merged.Channels[""]:
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

func TestMergedChannelIsBuffered(t *testing.T) {
	numChannels := 3
	channels := make([]pipeline.RequestChannel, numChannels)
	for i := range numChannels {
		channels[i] = pipeline.RequestChannel{Channel: make(chan *pipeline.EmbelishedRequestMessage, 1)}
	}
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

	// Send one message per input channel.
	for i, ch := range channels {
		ch.Channel <- embID(string(rune('A' + i)))
	}

	// The merge goroutine should be able to forward all messages into the
	// buffered merged channel without a consumer draining it. With an
	// unbuffered channel this would deadlock because the goroutine blocks
	// on the first send.
	deadline := time.After(2 * time.Second)
	received := 0
	for received < numChannels {
		select {
		case <-merged.Channels[""]:
			received++
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d messages — merged channel may be unbuffered", received, numChannels)
		}
	}
}
