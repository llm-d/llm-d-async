package async

import (
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
)

func irID(id string) *api.InternalRequest {
	return api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
	})
}

func TestProcessAllChannels(t *testing.T) {
	msgsPerChannel := 5
	channels := []api.RequestChannel{
		{Channel: make(chan *api.InternalRequest, msgsPerChannel), IGWBaseURl: "", InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan *api.InternalRequest, msgsPerChannel), IGWBaseURl: "", InferenceObjective: "", RequestPathURL: ""},
		{Channel: make(chan *api.InternalRequest, msgsPerChannel), IGWBaseURl: "", InferenceObjective: "", RequestPathURL: ""},
	}
	policy := NewRandomRobinPolicy()

	// Send messages to each channel
	for i, ch := range channels {
		for range msgsPerChannel {
			ch.Channel <- irID(string(rune('A' + i)))
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
	merged := policy.MergeRequestChannels(nil)

	select {
	case _, ok := <-merged.Channel:
		if ok {
			t.Fatal("expected closed channel, but received a message")
		}
	case <-time.After(time.Second):
		t.Fatal("merged channel was not closed")
	}
}

func TestMetaAlignmentAfterChannelClosure(t *testing.T) {
	// Three channels, each with distinct metadata.
	channels := []api.RequestChannel{
		{Channel: make(chan *api.InternalRequest, 1), IGWBaseURl: "http://a", InferenceObjective: "obj-a", RequestPathURL: "/a"},
		{Channel: make(chan *api.InternalRequest, 1), IGWBaseURl: "http://b", InferenceObjective: "obj-b", RequestPathURL: "/b"},
		{Channel: make(chan *api.InternalRequest, 1), IGWBaseURl: "http://c", InferenceObjective: "obj-c", RequestPathURL: "/c"},
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
		case channels[2].Channel <- irID("probe-c"):
		}

		select {
		case <-realignDeadline:
			t.Fatal("timed out waiting for channel metadata realignment")
		case msg := <-merged.Channel:
			if msg.PublicRequest == nil {
				t.Fatal("nil request")
			}
			if msg.PublicRequest.ReqID() != "probe-c" {
				t.Fatalf("unexpected message id while waiting for realignment: %s", msg.PublicRequest.ReqID())
			}
			realigned = msg.RequestURL == "http://c/c" &&
				msg.HttpHeaders["x-gateway-inference-objective"] == "obj-c"
		}
	}

	// Send one message on each remaining channel.
	channels[0].Channel <- irID("from-a")
	channels[2].Channel <- irID("from-c")

	deadline := time.After(2 * time.Second)
	for range 2 {
		select {
		case msg := <-merged.Channel:
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
				if msg.RequestURL != "http://c/c" {
					t.Errorf("expected RequestURL http://c/c, got %s", msg.RequestURL)
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

func TestMergedChannelIsBuffered(t *testing.T) {
	numChannels := 3
	channels := make([]api.RequestChannel, numChannels)
	for i := range numChannels {
		channels[i] = api.RequestChannel{Channel: make(chan *api.InternalRequest, 1)}
	}
	policy := NewRandomRobinPolicy()
	merged := policy.MergeRequestChannels(channels)

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
		case <-merged.Channel:
			received++
		case <-deadline:
			t.Fatalf("timed out: only received %d/%d messages — merged channel may be unbuffered", received, numChannels)
		}
	}
}
