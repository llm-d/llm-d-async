package producergcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/llm-d/llm-d-async/api"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const testProject = "test-project"

func newFake(t *testing.T) (*pubsub.Client, *pstest.Server) {
	t.Helper()
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })

	client, err := pubsub.NewClient(context.Background(), testProject,
		option.WithEndpoint(srv.Addr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatalf("create fake pubsub client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, srv
}

func newFakeClient(t *testing.T) *pubsub.Client {
	t.Helper()
	client, _ := newFake(t)
	return client
}

func testConfig() Config {
	return Config{
		ProjectID:             testProject,
		RequestTopicID:        "requests",
		RequestSubscriptionID: "request-sub",
		ResultTopicID:         "results",
		ResultRoute:           "producer-a",
	}
}

func newTestProducer(t *testing.T, client *pubsub.Client, cfg Config, opts ...Option) *Producer {
	t.Helper()
	opts = append([]Option{WithPubSubClient(client)}, opts...)
	p, err := NewProducer(context.Background(), cfg, opts...)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func receiveOne(t *testing.T, client *pubsub.Client, subID string, timeout time.Duration) *pubsub.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	sub := client.Subscriber(subID)
	sub.ReceiveSettings.MaxOutstandingMessages = 1
	sub.ReceiveSettings.NumGoroutines = 1
	var got *pubsub.Message
	err := sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		cp := *msg
		cp.Attributes = cloneAttrs(msg.Attributes)
		got = &cp
		msg.Ack()
		cancel()
	})
	if got == nil {
		t.Fatalf("did not receive message on %s: %v", subID, err)
	}
	return got
}

func cloneAttrs(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func publishDeadLetter(t *testing.T, client *pubsub.Client, topicID, id, route string) {
	t.Helper()
	data, err := json.Marshal(api.RequestMessage{
		ID:      id,
		Payload: json.RawMessage(`{"prompt":"hi"}`),
	})
	if err != nil {
		t.Fatalf("marshal dead letter: %v", err)
	}
	attrs := map[string]string{}
	if route != "" {
		attrs[api.ResultRouteAttribute] = route
	}
	publisher := client.Publisher(topicID)
	t.Cleanup(func() { publisher.Stop() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := publisher.Publish(ctx, &pubsub.Message{Data: data, Attributes: attrs}).Get(ctx); err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}
}

func publishResult(t *testing.T, client *pubsub.Client, topicID, id, route string) {
	t.Helper()
	data, err := json.Marshal(api.ResultMessage{ID: id, Payload: `{"ok":true}`})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	attrs := map[string]string{}
	if route != "" {
		attrs[api.ResultRouteAttribute] = route
	}
	publisher := client.Publisher(topicID)
	t.Cleanup(func() { publisher.Stop() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := publisher.Publish(ctx, &pubsub.Message{Data: data, Attributes: attrs}).Get(ctx); err != nil {
		t.Fatalf("publish result: %v", err)
	}
}

func validRequest(id string) *api.RequestMessage {
	return &api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(time.Hour).Unix(),
		Payload:  json.RawMessage(`{"prompt":"hi"}`),
		Metadata: map[string]string{"userid": "alice"},
	}
}

func TestNewProducerValidatesConfig(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)

	tests := []struct {
		name    string
		mut     func(*Config)
		wantSub string
	}{
		{name: "missing project", mut: func(c *Config) { c.ProjectID = "" }, wantSub: "ProjectID"},
		{name: "missing request topic", mut: func(c *Config) { c.RequestTopicID = "" }, wantSub: "RequestTopicID"},
		{name: "missing request sub", mut: func(c *Config) { c.RequestSubscriptionID = "" }, wantSub: "RequestSubscriptionID"},
		{name: "missing result topic", mut: func(c *Config) { c.ResultTopicID = "" }, wantSub: "ResultTopicID"},
		{name: "missing result route", mut: func(c *Config) { c.ResultRoute = "" }, wantSub: "ResultRoute"},
		{name: "route starts with goog", mut: func(c *Config) { c.ResultRoute = "goog-route" }, wantSub: "goog"},
		{name: "route too short", mut: func(c *Config) { c.ResultRoute = "ab" }, wantSub: "between"},
		{name: "route is not a resource id", mut: func(c *Config) {
			c.ResultRoute = "tenant:42"
			c.ResultSubscriptionID = "tenant-42-results"
		}, wantSub: "ResultRoute"},
		{name: "route filter too long", mut: func(c *Config) {
			c.ResultRoute = strings.Repeat("a", 229)
			c.ResultSubscriptionID = "tenant-results"
		}, wantSub: "256"},
		{name: "delivery attempts too low", mut: func(c *Config) { c.MaxDeliveryAttempts = 4 }, wantSub: "MaxDeliveryAttempts"},
		{name: "delivery attempts too high", mut: func(c *Config) { c.MaxDeliveryAttempts = 101 }, wantSub: "MaxDeliveryAttempts"},
		{name: "client project mismatch", mut: func(c *Config) { c.ProjectID = "other-project" }, wantSub: "does not match"},
		{name: "request topic is the dlq", mut: func(c *Config) { c.DeadLetterTopicID = c.RequestTopicID }, wantSub: "must differ"},
		{name: "request topic is the result topic", mut: func(c *Config) { c.ResultTopicID = c.RequestTopicID }, wantSub: "must differ"},
		{name: "dlq topic is the result topic", mut: func(c *Config) { c.DeadLetterTopicID = c.ResultTopicID }, wantSub: "must differ"},
		{name: "request and result subs collide", mut: func(c *Config) { c.ResultSubscriptionID = c.RequestSubscriptionID }, wantSub: "must differ"},
		{name: "dlq sub is the request sub", mut: func(c *Config) { c.DeadLetterSubscriptionID = c.RequestSubscriptionID }, wantSub: "must differ"},
		{name: "default dlq sub too long", mut: func(c *Config) {
			c.RequestTopicID = strings.Repeat("a", 200)
			c.ResultRoute = strings.Repeat("b", 200)
			c.ResultSubscriptionID = "results-sub"
		}, wantSub: "DeadLetterSubscriptionID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.mut(&cfg)
			_, err := NewProducer(context.Background(), cfg, WithPubSubClient(client))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}

func TestNewProducerCreatesResources(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())

	ctx := context.Background()
	reqSub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(testProject, "request-sub"),
	})
	if err != nil {
		t.Fatalf("get request subscription: %v", err)
	}
	if reqSub.GetTopic() != topicResource(testProject, "requests") {
		t.Errorf("request sub topic = %q", reqSub.GetTopic())
	}
	if !reqSub.GetEnableExactlyOnceDelivery() {
		t.Error("request subscription exactly-once not set")
	}
	if reqSub.GetRetryPolicy() == nil {
		t.Error("request subscription retry policy not set")
	}
	if reqSub.GetDeadLetterPolicy() == nil || reqSub.GetDeadLetterPolicy().GetDeadLetterTopic() != topicResource(testProject, "requests-dlq") {
		t.Errorf("request subscription DLQ = %+v", reqSub.GetDeadLetterPolicy())
	}
	if got := reqSub.GetDeadLetterPolicy().GetMaxDeliveryAttempts(); got != defaultDeliveryAttempts {
		t.Errorf("max delivery attempts = %d, want %d", got, defaultDeliveryAttempts)
	}
	if reqSub.GetAckDeadlineSeconds() != requestAckDeadlineSeconds {
		t.Errorf("request ack deadline = %d, want %d", reqSub.GetAckDeadlineSeconds(), requestAckDeadlineSeconds)
	}
	if reqSub.GetExpirationPolicy() == nil || reqSub.GetExpirationPolicy().GetTtl() != nil {
		t.Errorf("request subscription should never expire, got %+v", reqSub.GetExpirationPolicy())
	}

	resSub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(testProject, p.resultSubscriptionID),
	})
	if err != nil {
		t.Fatalf("get result subscription: %v", err)
	}
	wantFilter := resultRouteFilter("producer-a")
	if resSub.GetFilter() != wantFilter {
		t.Errorf("result filter = %q, want %q", resSub.GetFilter(), wantFilter)
	}
	if resSub.GetExpirationPolicy() == nil || resSub.GetExpirationPolicy().GetTtl() != nil {
		t.Errorf("result subscription should never expire, got %+v", resSub.GetExpirationPolicy())
	}

	dlqSub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(testProject, "requests-dlq-producer-a"),
	})
	if err != nil {
		t.Fatalf("get DLQ subscription: %v", err)
	}
	if dlqSub.GetFilter() != wantFilter {
		t.Errorf("dlq filter = %q, want %q", dlqSub.GetFilter(), wantFilter)
	}

	// Idempotent: a second New against the same resources succeeds.
	_ = newTestProducer(t, client, testConfig())
}

func TestNewProducerSetsMaxDeliveryAttempts(t *testing.T) {
	client := newFakeClient(t)
	cfg := testConfig()
	cfg.MaxDeliveryAttempts = 20
	_ = newTestProducer(t, client, cfg)

	ctx := context.Background()
	reqSub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(testProject, "request-sub"),
	})
	if err != nil {
		t.Fatalf("get request subscription: %v", err)
	}
	if got := reqSub.GetDeadLetterPolicy().GetMaxDeliveryAttempts(); got != 20 {
		t.Errorf("max delivery attempts = %d, want 20", got)
	}
}

func TestNewProducerRejectsTopicMismatch(t *testing.T) {
	client := newFakeClient(t)
	ctx := context.Background()
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicResource(testProject, "other")}); err != nil {
		t.Fatalf("create other topic: %v", err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subscriptionResource(testProject, "request-sub"),
		Topic: topicResource(testProject, "other"),
	}); err != nil {
		t.Fatalf("create mismatched sub: %v", err)
	}

	_, err := NewProducer(context.Background(), testConfig(), WithPubSubClient(client))
	if err == nil {
		t.Fatal("expected topic mismatch error")
	}
	if !strings.Contains(err.Error(), "attached to topic") {
		t.Errorf("error = %q, want topic mismatch", err)
	}
}

func TestNewProducerRejectsFilterMismatch(t *testing.T) {
	client := newFakeClient(t)
	ctx := context.Background()
	cfg := testConfig()
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicResource(testProject, cfg.ResultTopicID)}); err != nil {
		t.Fatalf("create result topic: %v", err)
	}
	if _, err := client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:   subscriptionResource(testProject, cfg.ResultRoute),
		Topic:  topicResource(testProject, cfg.ResultTopicID),
		Filter: `attributes.result_route = "other"`,
	}); err != nil {
		t.Fatalf("create mismatched result sub: %v", err)
	}

	_, err := NewProducer(context.Background(), cfg, WithPubSubClient(client))
	if err == nil {
		t.Fatal("expected filter mismatch error")
	}
	if !strings.Contains(err.Error(), "filter") {
		t.Errorf("error = %q, want filter mismatch", err)
	}
}

func TestSubmitRequestPublishesPlainMessage(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := validRequest("req-1")
	req.Metadata[api.ResultRouteAttribute] = "spoofed"
	if err := p.SubmitRequest(ctx, req); err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}

	msg := receiveOne(t, client, "request-sub", 5*time.Second)
	if len(msg.Attributes) != 1 || msg.Attributes[api.ResultRouteAttribute] != "producer-a" {
		t.Errorf("attributes = %v, want only result_route=producer-a", msg.Attributes)
	}

	var body map[string]any
	if err := json.Unmarshal(msg.Data, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if _, ok := body["request_kind"]; ok {
		t.Fatalf("published InternalRequest envelope: %s", msg.Data)
	}
	if body["id"] != "req-1" {
		t.Errorf("id = %v, want req-1", body["id"])
	}
	meta, _ := body["metadata"].(map[string]any)
	if meta["userid"] != "alice" {
		t.Errorf("body metadata = %v, want userid=alice", body["metadata"])
	}
}

func TestSubmitRequestRejectsExpiredDeadline(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())
	req := validRequest("req-expired")
	req.Deadline = time.Now().Add(-time.Minute).Unix()
	err := p.SubmitRequest(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Errorf("error = %v, want expired deadline", err)
	}
}

func TestGetResultFiltersByRoute(t *testing.T) {
	client := newFakeClient(t)
	cfgA := testConfig()
	cfgB := testConfig()
	cfgB.ResultRoute = "producer-b"
	a := newTestProducer(t, client, cfgA)
	b := newTestProducer(t, client, cfgB)

	publishResult(t, client, "results", "for-b", "producer-b")
	publishResult(t, client, "results", "for-a", "producer-a")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gotA, err := a.GetResult(ctx)
	if err != nil {
		t.Fatalf("producer-a GetResult: %v", err)
	}
	if gotA.ID != "for-a" {
		t.Errorf("producer-a got %q, want for-a", gotA.ID)
	}

	gotB, err := b.GetResult(ctx)
	if err != nil {
		t.Fatalf("producer-b GetResult: %v", err)
	}
	if gotB.ID != "for-b" {
		t.Errorf("producer-b got %q, want for-b", gotB.ID)
	}
}

func TestGetResultRespectsContext(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := p.GetResult(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context deadline", err)
	}
}

func TestSubmitRequestRejectsTypedNil(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())

	var req *api.RequestMessage
	var rreq *api.RedisRequest
	var preq *api.PubSubRequest
	for name, r := range map[string]api.Request{"RequestMessage": req, "RedisRequest": rreq, "PubSubRequest": preq} {
		if err := p.SubmitRequest(context.Background(), r); err == nil {
			t.Errorf("%s: expected error for typed-nil request, got nil", name)
		}
	}
}

// TestGetResultCancelledCallerDoesNotWaitBehindReceive checks that a second
// GetResult returns on its own deadline while another call is waiting.
func TestGetResultCancelledCallerDoesNotWaitBehindReceive(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())

	firstCtx, firstCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer firstCancel()
	firstDone := make(chan error, 1)
	go func() {
		_, err := p.GetResult(firstCtx)
		firstDone <- err
	}()

	// Let the first call enter Receive and hold the slot.
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := p.GetResult(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("GetResult blocked %v behind an in-flight receive; want prompt deadline", elapsed)
	}

	firstCancel()
	<-firstDone
}

func TestCancelRequests(t *testing.T) {
	t.Parallel()
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())
	if err := p.CancelRequests(context.Background(), nil); err != nil {
		t.Errorf("empty list: %v", err)
	}
	err := p.CancelRequests(context.Background(), []string{"req-1"})
	if !errors.Is(err, api.ErrNotSupported) {
		t.Errorf("error = %v, want ErrNotSupported", err)
	}
}

func TestWithoutCreateResourcesSkipsProvisioning(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig(), WithoutCreateResources())
	err := p.SubmitRequest(context.Background(), validRequest("req-1"))
	if err == nil {
		t.Fatal("expected publish to missing topic to fail")
	}
}

func TestSubmitAndGetResultRoundTrip(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.SubmitRequest(ctx, validRequest("round-trip")); err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	msg := receiveOne(t, client, "request-sub", 5*time.Second)
	route := msg.Attributes[api.ResultRouteAttribute]

	var wg sync.WaitGroup
	wg.Add(1)
	var result *api.ResultMessage
	var getErr error
	go func() {
		defer wg.Done()
		result, getErr = p.GetResult(ctx)
	}()
	publishResult(t, client, "results", "round-trip", route)
	wg.Wait()
	if getErr != nil {
		t.Fatalf("GetResult: %v", getErr)
	}
	if result.ID != "round-trip" {
		t.Errorf("result id = %q, want round-trip", result.ID)
	}
}

func TestGetResultDrainsSeveralResults(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())

	publishResult(t, client, "results", "one", "producer-a")
	publishResult(t, client, "results", "two", "producer-a")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := map[string]bool{}
	for range 2 {
		result, err := p.GetResult(ctx)
		if err != nil {
			t.Fatalf("GetResult: %v", err)
		}
		got[result.ID] = true
	}
	if !got["one"] || !got["two"] {
		t.Errorf("results = %v, want one and two", got)
	}
}

func TestResultRouteWithExplicitSubscriptionID(t *testing.T) {
	client := newFakeClient(t)
	cfg := testConfig()
	cfg.ResultRoute = "tenant-42"
	cfg.ResultSubscriptionID = "tenant-42-results"
	p := newTestProducer(t, client, cfg)

	ctx := context.Background()
	resSub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(testProject, "tenant-42-results"),
	})
	if err != nil {
		t.Fatalf("get result subscription: %v", err)
	}
	if resSub.GetFilter() != resultRouteFilter(p.resultRoute) {
		t.Errorf("filter = %q", resSub.GetFilter())
	}
}

func TestNewProducerRequiresContext(t *testing.T) {
	// A nil variable, not a nil literal, so staticcheck SA1012 stays quiet.
	// NewProducer still has to reject it.
	var ctx context.Context
	_, err := NewProducer(ctx, testConfig())
	if err == nil || !strings.Contains(err.Error(), "context") {
		t.Fatalf("error = %v, want context required", err)
	}
}

func TestProvisionContextUsesCallerDeadline(t *testing.T) {
	parent, stop := context.WithTimeout(context.Background(), time.Hour)
	defer stop()
	ctx, cancel := provisionContext(parent)
	defer cancel()
	got, ok := ctx.Deadline()
	want, _ := parent.Deadline()
	if !ok || !got.Equal(want) {
		t.Fatalf("deadline = %v ok=%v, want the caller deadline %v", got, ok, want)
	}

	ctx, cancel = provisionContext(context.Background())
	defer cancel()
	got, ok = ctx.Deadline()
	if !ok {
		t.Fatal("expected default deadline")
	}
	remaining := time.Until(got)
	if remaining > newProducerTimeout || remaining < newProducerTimeout-time.Second {
		t.Fatalf("default deadline remaining = %v, want about %v", remaining, newProducerTimeout)
	}
}

func TestCloseThenUse(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig())
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := p.SubmitRequest(context.Background(), validRequest("after-close")); !errors.Is(err, errProducerClosed) {
		t.Fatalf("SubmitRequest after Close = %v, want closed", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := p.GetResult(ctx); !errors.Is(err, errProducerClosed) {
		t.Fatalf("GetResult after Close = %v, want closed", err)
	}
}

func TestCloseDuringCallsDoesNotPanic(t *testing.T) {
	client, srv := newFake(t)
	p := newTestProducer(t, client, testConfig())
	publishResult(t, client, "results", "held", "producer-a")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := p.GetResult(ctx)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if got.ID != "held" {
		t.Fatalf("result id = %q, want held", got.ID)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = p.SubmitRequest(context.Background(), validRequest("req-close"))
	}()
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = p.GetResult(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for {
		acks, deliveries := messageStats(srv, "held")
		if acks == 1 && deliveries == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acks=%d deliveries=%d, want exactly one delivery and one ack", acks, deliveries)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func messageStats(srv *pstest.Server, id string) (acks, deliveries int) {
	for _, msg := range srv.Messages() {
		var body struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(msg.Data, &body); err != nil || body.ID != id {
			continue
		}
		return msg.Acks, msg.Deliveries
	}
	return 0, 0
}

func resultRouteStats(srv *pstest.Server, route string) (acked, delivered int) {
	for _, msg := range srv.Messages() {
		if msg.Attributes[api.ResultRouteAttribute] != route {
			continue
		}
		if msg.Deliveries > 0 {
			delivered++
		}
		if msg.Acks > 0 {
			acked++
		}
	}
	return acked, delivered
}

func TestGetResultAcksOnlyTheHandoff(t *testing.T) {
	client, srv := newFake(t)
	p := newTestProducer(t, client, testConfig())
	publishResult(t, client, "results", "one", "producer-a")
	publishResult(t, client, "results", "two", "producer-a")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := p.GetResult(ctx)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		acked, delivered := resultRouteStats(srv, "producer-a")
		if delivered >= 2 && acked == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered=%d acked=%d, want both pulled and only %q acked", delivered, acked, got.ID)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p2 := newTestProducer(t, client, testConfig())
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	other, err := p2.GetResult(ctx2)
	if err != nil {
		t.Fatalf("redelivered GetResult: %v", err)
	}
	if other.ID == got.ID {
		t.Fatalf("redelivered %q, which was already handed off", other.ID)
	}
}

func TestGetResultSurfacesDeadLetter(t *testing.T) {
	client, srv := newFake(t)
	a := newTestProducer(t, client, testConfig())
	cfgB := testConfig()
	cfgB.ResultRoute = "producer-b"
	_ = newTestProducer(t, client, cfgB)

	ctx := context.Background()
	for _, route := range []string{"producer-a", "producer-b"} {
		sub, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
			Subscription: subscriptionResource(testProject, "requests-dlq-"+route),
		})
		if err != nil {
			t.Fatalf("get DLQ subscription %s: %v", route, err)
		}
		if sub.GetFilter() != resultRouteFilter(route) {
			t.Errorf("dlq filter for %s = %q", route, sub.GetFilter())
		}
	}

	publishDeadLetter(t, client, a.deadLetterTopicID, "poisoned", a.resultRoute)
	getCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := a.GetResult(getCtx)
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if got.ID != "poisoned" || got.ErrorCode != api.ErrCodeDeadLettered {
		t.Fatalf("result = %+v, want dead-lettered poisoned", got)
	}
	if !strings.Contains(got.Payload, deadLetterMessage) {
		t.Fatalf("payload = %q, want %q", got.Payload, deadLetterMessage)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		acked, _ := resultRouteStats(srv, "producer-a")
		if acked == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead-letter acks = %d, want 1", acked)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReceiveErrorSurvivesRestart(t *testing.T) {
	client := newFakeClient(t)
	p := newTestProducer(t, client, testConfig(), WithoutCreateResources())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	for i := range errs {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.GetResult(ctx)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "without a message") {
			t.Fatalf("GetResult %d = %v, want the receive error", i, err)
		}
	}
}

func TestPoisonResultIsAcked(t *testing.T) {
	client, srv := newFake(t)
	p := newTestProducer(t, client, testConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	publisher := client.Publisher("results")
	t.Cleanup(func() { publisher.Stop() })
	if _, err := publisher.Publish(ctx, &pubsub.Message{
		Data:       []byte("not-json"),
		Attributes: map[string]string{api.ResultRouteAttribute: "producer-a"},
	}).Get(ctx); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	_, err := p.GetResult(ctx)
	if err == nil || !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("GetResult = %v, want unmarshal error", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		acked, _ := resultRouteStats(srv, "producer-a")
		if acked == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("poison acks = %d, want 1", acked)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
