//go:build integration

package integration_test

import (
	"sync"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	"github.com/stretchr/testify/assert"
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
			InferenceObjective: "latency",
			WorkerPoolID:       "test-pool",
			IGWBaseURL:         "http://localhost:8080",
			RequestPathURL:     "/v1/completions",
		}
	}

	pools := map[string]pipeline.WorkerPoolConfig{
		"test-pool": {
			ID:      "test-pool",
			Workers: 1,
		},
	}

	policy := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{})
	dispatch := policy.MergeRequestChannels(channels, pools)
	mergedChan := dispatch.Channels["test-pool"]

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
						Payload:  testPayload(map[string]any{"model": "test"}),
					},
				)
				channels[idx].Channel <- ir
			}
			close(channels[idx].Channel)
		}(chIdx)
	}

	// Consume the expected messages from the merged channel. Dynamic fan-in
	// channels stay open after their sources close so queues can be added later.
	received := make(map[string]bool)
	const expected = numChannels * msgsPerChannel
	deadline := time.After(10 * time.Second)
	for len(received) < expected {
		select {
		case msg := <-mergedChan:
			if msg.InternalRequest == nil || msg.InternalRequest.PublicRequest == nil {
				t.Fatal("merged channel returned an invalid message")
			}
			received[msg.InternalRequest.PublicRequest.ReqID()] = true
		case <-deadline:
			t.Fatalf("Timed out waiting for merged messages: got %d of %d", len(received), expected)
		}
	}
	wg.Wait()

	assert.Equal(t, expected, len(received), "Expected %d unique messages, got %d", expected, len(received))

	// Verify every expected message was received.
	for chIdx := range numChannels {
		for m := range msgsPerChannel {
			id := msgID(chIdx, m)
			assert.True(t, received[id], "Missing message %s", id)
		}
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
