package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const (
	sameIDTimeout    = 5 * time.Second
	sameIDSubscriber = "same-id-sub"
	sameIDPool       = "same-id-pool"
)

// ackOutcome is how a receive callback finished.
type ackOutcome string

const (
	outcomeAck  ackOutcome = "ack"
	outcomeNack ackOutcome = "nack"
)

// deliveryResult is what a receive callback reports to the test goroutine. err
// is set when the Ack check could not read its metric; the test goroutine
// reports it, because t must not be used from the callback goroutine.
type deliveryResult struct {
	outcome ackOutcome
	err     error
}

// queueTagGate admits every request and tags it with its request ID as the
// queue ID, so the per-request message-latency series shows which deliveries
// were Acked: processMessages records that latency only on the Ack path.
type queueTagGate struct{}

var _ pipeline.Gate = (*queueTagGate)(nil)

func (queueTagGate) Budget(context.Context) float64 { return 1.0 }
func (queueTagGate) Apply(_ context.Context, ir *api.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	ir.QueueID = ir.PublicRequest.ReqID()
	return pipeline.Continue(), nil
}

// ackedLatencySamples returns how many Ack-path latency samples were recorded
// for requestID.
func ackedLatencySamples(requestID string) (uint64, error) {
	var m dto.Metric
	if err := metrics.MessageLatencyTime.WithLabelValues(requestID, sameIDSubscriber, sameIDPool).(prometheus.Metric).Write(&m); err != nil {
		return 0, fmt.Errorf("read latency metric: %w", err)
	}
	return m.GetHistogram().GetSampleCount(), nil
}

// sameIDHarness runs processMessages against a captured receive callback so a
// test can deliver several messages that share one Pub/Sub message ID.
type sameIDHarness struct {
	callback func(context.Context, *pubsub.Message)
	ctx      context.Context
	ch       chan *api.InternalRequest
}

func newSameIDHarness(t *testing.T, flow *PubSubMQFlow) *sameIDHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	h := &sameIDHarness{ctx: ctx, ch: make(chan *api.InternalRequest, 4)}
	captured := make(chan func(context.Context, *pubsub.Message), 1)
	go func() {
		_ = flow.processMessages(ctx, func(_ context.Context, f func(context.Context, *pubsub.Message)) error {
			captured <- f
			<-ctx.Done()
			return nil
		}, sameIDSubscriber, sameIDPool, h.ch, queueTagGate{}, nil, "")
	}()
	select {
	case h.callback = <-captured:
	case <-time.After(sameIDTimeout):
		t.Fatal("processMessages did not register its receive callback")
	}
	return h
}

// deliver invokes the receive callback for one delivery of message id. The
// returned channel yields the delivery's outcome once the callback returns.
// Ack and Nack are no-ops on a hand-built pubsub.Message, so an Ack is
// recognised by the latency sample processMessages records just before it.
func (h *sameIDHarness) deliver(t *testing.T, id, requestID, resultQueue string) <-chan deliveryResult {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"id":                requestID,
		"payload":           map[string]any{"prompt": "hi"},
		"result_queue_name": resultQueue,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	results := make(chan deliveryResult, 1)
	go func() {
		h.callback(h.ctx, &pubsub.Message{ID: id, Data: data})
		n, err := ackedLatencySamples(requestID)
		switch {
		case err != nil:
			results <- deliveryResult{err: err}
		case n > 0:
			results <- deliveryResult{outcome: outcomeAck}
		default:
			results <- deliveryResult{outcome: outcomeNack}
		}
	}()
	return results
}

func (h *sameIDHarness) nextRequest(t *testing.T) *api.InternalRequest {
	t.Helper()
	select {
	case ir := <-h.ch:
		return ir
	case <-time.After(sameIDTimeout):
		t.Fatal("timed out waiting for a request to reach the dispatch channel")
		return nil
	}
}

func awaitOutcome(t *testing.T, name string, results <-chan deliveryResult) ackOutcome {
	t.Helper()
	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("%s: %v", name, got.err)
		}
		return got.outcome
	case <-time.After(sameIDTimeout):
		t.Fatalf("%s: receive callback is still blocked: its result was routed to another delivery", name)
		return ""
	}
}

// startResultWorker runs resultWorker against the in-memory Pub/Sub fake with
// an existing "good-results" topic. Results routed to any other topic fail to
// publish.
func startResultWorker(t *testing.T, flow *PubSubMQFlow) {
	t.Helper()
	_, client, _ := newFakePubSubServer(t)
	topic := "projects/" + testProject + "/topics/good-results"
	if _, err := client.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{Name: topic}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		resultWorker(ctx, client, "", flow.resultChannel, &flow.deliveries)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
}

func publishResult(flow *PubSubMQFlow, ir *api.InternalRequest) {
	flow.resultChannel <- api.NewHTTPResult(ir.PublicRequest, ir.InternalRouting, 200, []byte(`{}`))
}

// Pub/Sub delivers at least once, so a message can be redelivered with the same
// ID while its first delivery is still running. Each delivery must be Acked or
// Nacked according to its own publish result.
func TestProcessMessages_SameIDDeliveriesFollowOwnResult(t *testing.T) {
	flow := &PubSubMQFlow{resultChannel: make(chan api.ResultMessage)}
	h := newSameIDHarness(t, flow)
	startResultWorker(t, flow)

	// Same message ID. "ok" publishes to an existing topic, "bad" to a topic
	// that does not exist, so one result succeeds and the other fails.
	okDone := h.deliver(t, "dup-id", "req-ok", "good-results")
	first := h.nextRequest(t)
	badDone := h.deliver(t, "dup-id", "req-bad", "missing-results")
	second := h.nextRequest(t)

	// The correlation ID stays the Pub/Sub message ID on every delivery.
	for _, ir := range []*api.InternalRequest{first, second} {
		if got := ir.TransportCorrelationID; got != "dup-id" {
			t.Errorf("TransportCorrelationID = %q, want the Pub/Sub message ID %q", got, "dup-id")
		}
	}

	// Publish the failing delivery's result first, then the successful one.
	publishResult(flow, second)
	publishResult(flow, first)

	if got := awaitOutcome(t, "delivery with a published result", okDone); got != outcomeAck {
		t.Errorf("delivery with a published result finished with %q, want %q", got, outcomeAck)
	}
	if got := awaitOutcome(t, "delivery whose publish failed", badDone); got != outcomeNack {
		t.Errorf("delivery whose publish failed finished with %q, want %q", got, outcomeNack)
	}
}

// The first delivery finishing must not take the second delivery's rendezvous
// with it: the second still receives its own result.
func TestProcessMessages_FirstDeliveryFinishingKeepsSecondReachable(t *testing.T) {
	flow := &PubSubMQFlow{resultChannel: make(chan api.ResultMessage)}
	h := newSameIDHarness(t, flow)
	startResultWorker(t, flow)

	firstDone := h.deliver(t, "dup-id", "req-first", "good-results")
	first := h.nextRequest(t)
	secondDone := h.deliver(t, "dup-id", "req-second", "missing-results")
	second := h.nextRequest(t)

	publishResult(flow, first)
	if got := awaitOutcome(t, "first delivery", firstDone); got != outcomeAck {
		t.Errorf("first delivery finished with %q, want %q", got, outcomeAck)
	}

	publishResult(flow, second)
	if got := awaitOutcome(t, "second delivery", secondDone); got != outcomeNack {
		t.Errorf("second delivery finished with %q, want %q", got, outcomeNack)
	}
}

// A retry of one delivery must Nack that delivery and leave a same-ID
// delivery that is still running untouched.
func TestAddMsgToRetryQueue_NacksOnlyRetriedDelivery(t *testing.T) {
	flow := &PubSubMQFlow{resultChannel: make(chan api.ResultMessage)}
	h := newSameIDHarness(t, flow)
	startResultWorker(t, flow)

	retried := h.deliver(t, "dup-id", "req-retried", "good-results")
	retriedIR := h.nextRequest(t)
	running := h.deliver(t, "dup-id", "req-running", "good-results")
	runningIR := h.nextRequest(t)

	retryCh := make(chan pipeline.RetryMessage)
	go addMsgToRetryQueue(h.ctx, retryCh, &flow.deliveries)
	retryCh <- pipeline.RetryMessage{EmbelishedRequestMessage: pipeline.EmbelishedRequestMessage{InternalRequest: retriedIR}}

	if got := awaitOutcome(t, "retried delivery", retried); got != outcomeNack {
		t.Errorf("retried delivery finished with %q, want %q", got, outcomeNack)
	}
	select {
	case got := <-running:
		t.Fatalf("delivery that was not retried finished early: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}

	// Its own result then completes it.
	publishResult(flow, runningIR)
	if got := awaitOutcome(t, "running delivery", running); got != outcomeAck {
		t.Errorf("running delivery finished with %q, want %q", got, outcomeAck)
	}
}
