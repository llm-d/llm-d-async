//go:build integration

package integration_test

import (
	"sync"
	"testing"
	"time"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"
	ap "github.com/llm-d-incubation/llm-d-async/pkg/async"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRandomRobinPolicy_ConcurrentProducers validates that RandomRobinPolicy
// correctly merges messages from multiple channels when multiple goroutines
// are producing concurrently. Every message sent must appear exactly once
// on the merged output channel.
func TestRandomRobinPolicy_ConcurrentProducers(t *testing.T) {
	const numChannels = 4
	const msgsPerChannel = 25

	channels := make([]pipeline.RequestChannel, numChannels)
	for i := range numChannels {
		channels[i] = pipeline.RequestChannel{
			Channel:            make(chan *asyncapi.InternalRequest, msgsPerChannel),
			IGWBaseURL:         "http://localhost:8080",
			InferenceObjective: "latency",
			RequestPathURL:     "/v1/completions",
		}
	}

	policy := ap.NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

	// Produce messages concurrently on all channels.
	var wg sync.WaitGroup
	for chIdx := range numChannels {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for m := range msgsPerChannel {
				ir := asyncapi.NewInternalRequest(
					asyncapi.InternalRouting{},
					&asyncapi.RequestMessage{
						ID:       msgID(idx, m),
						Created:  time.Now().Unix(),
						Deadline: time.Now().Add(time.Minute).Unix(),
						Payload:  map[string]any{"model": "test"},
					},
				)
				channels[idx].Channel <- ir
			}
			close(channels[idx].Channel)
		}(chIdx)
	}

	// Consume all messages from the merged channel.
	received := make(map[string]bool)
	done := make(chan struct{})
	go func() {
		for msg := range merged.Channel {
			received[msg.InternalRequest.PublicRequest.ReqID()] = true
		}
		close(done)
	}()

	wg.Wait()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for merged channel to close")
	}

	expected := numChannels * msgsPerChannel
	assert.Equal(t, expected, len(received), "Expected %d unique messages, got %d", expected, len(received))

	// Verify every expected message was received.
	for chIdx := range numChannels {
		for m := range msgsPerChannel {
			id := msgID(chIdx, m)
			assert.True(t, received[id], "Missing message %s", id)
		}
	}
}

// TestRandomRobinPolicy_HeaderAndURLAssembly verifies that the merge policy
// correctly assembles request URLs and sets inference-objective headers for
// channels with different configurations.
func TestRandomRobinPolicy_HeaderAndURLAssembly(t *testing.T) {
	ch1 := pipeline.RequestChannel{
		Channel:            make(chan *asyncapi.InternalRequest, 1),
		IGWBaseURL:         "http://host-a:8080",
		InferenceObjective: "latency",
		RequestPathURL:     "/v1/completions",
	}
	ch2 := pipeline.RequestChannel{
		Channel:            make(chan *asyncapi.InternalRequest, 1),
		IGWBaseURL:         "http://host-b:9090",
		InferenceObjective: "throughput",
		RequestPathURL:     "/v1/chat/completions",
	}

	policy := ap.NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch1, ch2})

	ch1.Channel <- asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &asyncapi.RequestMessage{
		ID: "ch1-msg", Created: time.Now().Unix(), Deadline: time.Now().Add(time.Minute).Unix(),
		Payload: map[string]any{"model": "a"},
	})
	ch2.Channel <- asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &asyncapi.RequestMessage{
		ID: "ch2-msg", Created: time.Now().Unix(), Deadline: time.Now().Add(time.Minute).Unix(),
		Payload: map[string]any{"model": "b"},
	})
	close(ch1.Channel)
	close(ch2.Channel)

	results := map[string]pipeline.EmbelishedRequestMessage{}
	timeout := time.After(5 * time.Second)
	for range 2 {
		select {
		case msg := <-merged.Channel:
			results[msg.InternalRequest.PublicRequest.ReqID()] = msg
		case <-timeout:
			t.Fatal("Timed out waiting for merged messages")
		}
	}

	require.Contains(t, results, "ch1-msg")
	require.Contains(t, results, "ch2-msg")

	msg1 := results["ch1-msg"]
	assert.Equal(t, "http://host-a:8080/v1/completions", msg1.RequestURL)
	assert.Equal(t, "latency", msg1.HttpHeaders["x-gateway-inference-objective"])

	msg2 := results["ch2-msg"]
	assert.Equal(t, "http://host-b:9090/v1/chat/completions", msg2.RequestURL)
	assert.Equal(t, "throughput", msg2.HttpHeaders["x-gateway-inference-objective"])
}

// TestRandomRobinPolicy_PerMessageEndpointOverride verifies that when a
// message carries a per-message endpoint, it overrides the channel's default.
func TestRandomRobinPolicy_PerMessageEndpointOverride(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:            make(chan *asyncapi.InternalRequest, 1),
		IGWBaseURL:         "http://igw:8080",
		InferenceObjective: "latency",
		RequestPathURL:     "/v1/completions",
	}

	policy := ap.NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{ch})

	ch.Channel <- asyncapi.NewInternalRequest(asyncapi.InternalRouting{}, &asyncapi.RequestMessage{
		ID: "override-msg", Created: time.Now().Unix(), Deadline: time.Now().Add(time.Minute).Unix(),
		Payload:  map[string]any{"model": "test"},
		Endpoint: "/v1/embeddings",
	})
	close(ch.Channel)

	select {
	case msg := <-merged.Channel:
		assert.Equal(t, "http://igw:8080/v1/embeddings", msg.RequestURL)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for merged message")
	}
}

// TestRandomRobinPolicy_EmptyChannels verifies that MergeRequestChannels
// with an empty slice returns a closed channel immediately.
func TestRandomRobinPolicy_EmptyChannels(t *testing.T) {
	policy := ap.NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels([]pipeline.RequestChannel{})

	select {
	case _, ok := <-merged.Channel:
		assert.False(t, ok, "Channel should be closed for empty input")
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out — channel should have been closed immediately")
	}
}

func msgID(channelIdx, msgIdx int) string {
	return "ch" + itoa(channelIdx) + "-msg" + itoa(msgIdx)
}

func itoa(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return string(digits[i])
	}
	return itoa(i/10) + string(digits[i%10])
}
