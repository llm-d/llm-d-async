package async

import (
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
)

func TestProcessAllChannels(t *testing.T) {
	msgsPerChannel := 5
	channels := []api.RequestChannel{
		{Channel: make(chan api.RequestMessage, msgsPerChannel), IGWBaseURl: "", InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan api.RequestMessage, msgsPerChannel), IGWBaseURl: "", InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan api.RequestMessage, msgsPerChannel), IGWBaseURl: "", InferenceObjective: "", RequestPathURL: ""},
	}
	policy := NewRandomRobinPolicy()

	// Send messages to each channel
	for i, ch := range channels {
		for range msgsPerChannel {
			ch.Channel <- api.RequestMessage{Id: string(rune('A' + i))}
		}
	}
	mergedChannel := policy.MergeRequestChannels(channels).Channel
	close(channels[0].Channel)
	close(channels[1].Channel)
	close(channels[2].Channel)

	counts := map[string]int{}
	totalMessages := msgsPerChannel * 3
	for range totalMessages {
		msg := <-mergedChannel
		counts[msg.Id]++

	}

	for i := range 3 {
		id := string(rune('A' + i))
		if counts[id] != msgsPerChannel {
			t.Errorf("Expected %d messages from channel %s, got %d", msgsPerChannel, id, counts[id])
		}
	}
}

func TestMergedChannelIsBuffered(t *testing.T) {
	numChannels := 3
	channels := make([]api.RequestChannel, numChannels)
	for i := range numChannels {
		channels[i] = api.RequestChannel{Channel: make(chan api.RequestMessage, 1)}
	}
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

	// Send one message per input channel.
	for i, ch := range channels {
		ch.Channel <- api.RequestMessage{Id: string(rune('A' + i))}
	}

	// The merge goroutine should be able to forward all messages into the
	// buffered merged channel without a consumer draining it. With an
	// unbuffered channel this would deadlock because the goroutine blocks
	// on the first send.
	deadline := time.After(2 * time.Second)
	received := 0
	for received < numChannels {
		select {
		case <-merged.Channel:
			received++
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d messages — merged channel may be unbuffered", received, numChannels)
		}
	}
}
