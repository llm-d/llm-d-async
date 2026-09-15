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

func newFakeClient(t *testing.T) *pubsub.Client {
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
	p, err := NewProducer(cfg, opts...)
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
		Payload:  map[string]any{"prompt": "hi"},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.mut(&cfg)
			_, err := NewProducer(cfg, WithPubSubClient(client))
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
	if resSub.GetExpirationPolicy() == nil || resSub.GetExpirationPolicy().GetTtl() == nil {
		t.Fatal("result subscription missing expiration TTL")
	}
	if resSub.GetExpirationPolicy().GetTtl().AsDuration() != defaultResultSubscriptionExpiry {
		t.Errorf("result expiration = %v, want %v", resSub.GetExpirationPolicy().GetTtl().AsDuration(), defaultResultSubscriptionExpiry)
	}

	if _, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: subscriptionResource(testProject, "requests-dlq-sub"),
	}); err != nil {
		t.Fatalf("get DLQ subscription: %v", err)
	}

	// Idempotent: a second New against the same resources succeeds.
	_ = newTestProducer(t, client, testConfig())
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

	_, err := NewProducer(testConfig(), WithPubSubClient(client))
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

	_, err := NewProducer(cfg, WithPubSubClient(client))
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
	if msg.Attributes[api.ResultRouteAttribute] != "producer-a" {
		t.Errorf("attributes = %v, want result_route=producer-a", msg.Attributes)
	}
	if msg.Attributes["userid"] != "alice" {
		t.Errorf("caller metadata not copied: %v", msg.Attributes)
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
