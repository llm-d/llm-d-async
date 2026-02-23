package async

import (
	"testing"

	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
)

func TestProcessAllChannels(t *testing.T) {
	msgsPerChannel := 5
	channels := []api.RequestChannel{
		{Channel: make(chan api.RequestMessage, msgsPerChannel), InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan api.RequestMessage, msgsPerChannel), InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan api.RequestMessage, msgsPerChannel), InferenceObjective: "", RequestPathURL: ""},
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
