package tierpriority

import (
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func testMessage(tier, class string) *pipeline.EmbelishedRequestMessage {
	return &pipeline.EmbelishedRequestMessage{
		Labels: pipeline.Labels{
			DefaultTierLabel:  tier,
			DefaultClassLabel: class,
		},
	}
}

func sendMessage(ch chan *pipeline.EmbelishedRequestMessage, msg *pipeline.EmbelishedRequestMessage) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		ch <- msg
		close(done)
	}()
	return done
}

func requireSent(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send did not complete")
	}
}

func requireBlocked(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("send completed; expected per-subscription RMP bound to apply backpressure")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestReaderBoundsQueuedMessagesPerSubscription(t *testing.T) {
	p := New(Config{PerSubscriptionBuffer: 2})
	input := pipeline.RequestChannel{Channel: make(chan *pipeline.EmbelishedRequestMessage)}
	out := make(chan pipeline.EmbelishedRequestMessage)
	ps := newPoolState(p, "pool", []pipeline.RequestChannel{input}, out)
	go ps.reader(0, input)

	requireSent(t, sendMessage(input.Channel, testMessage("async", "overflow")))
	requireSent(t, sendMessage(input.Channel, testMessage("async", "overflow")))
	blocked := sendMessage(input.Channel, testMessage("async", "overflow"))
	requireBlocked(t, blocked)

	go ps.dispatcher()
	<-out
	requireSent(t, blocked)
}

func TestPerSubscriptionBoundDoesNotBlockOtherSubscriptions(t *testing.T) {
	p := New(Config{PerSubscriptionBuffer: 2})
	overflowInput := pipeline.RequestChannel{Channel: make(chan *pipeline.EmbelishedRequestMessage)}
	reservedInput := pipeline.RequestChannel{Channel: make(chan *pipeline.EmbelishedRequestMessage)}
	out := make(chan pipeline.EmbelishedRequestMessage)
	ps := newPoolState(p, "pool", []pipeline.RequestChannel{overflowInput, reservedInput}, out)
	go ps.reader(0, overflowInput)
	go ps.reader(1, reservedInput)

	requireSent(t, sendMessage(overflowInput.Channel, testMessage("async", "overflow")))
	requireSent(t, sendMessage(overflowInput.Channel, testMessage("async", "overflow")))
	blockedOverflow := sendMessage(overflowInput.Channel, testMessage("async", "overflow"))
	requireBlocked(t, blockedOverflow)

	reserved := sendMessage(reservedInput.Channel, testMessage("async", "reserved"))
	requireSent(t, reserved)

	go ps.dispatcher()
	<-out
	requireSent(t, blockedOverflow)
}

func TestParseConfigPerSubscriptionBuffer(t *testing.T) {
	cfg, err := parseConfigFromDeps(map[string]string{ConfigKeyPerSubBuffer: "7"})
	if err != nil {
		t.Fatalf("parseConfigFromDeps: %v", err)
	}
	if got := cfg.PerSubscriptionBuffer; got != 7 {
		t.Fatalf("PerSubscriptionBuffer = %d, want 7", got)
	}
}

func TestParseConfigPerSubscriptionBufferRejectsNonPositive(t *testing.T) {
	if _, err := parseConfigFromDeps(map[string]string{ConfigKeyPerSubBuffer: "0"}); err == nil {
		t.Fatal("expected error for non-positive per_subscription_buffer")
	}
}
