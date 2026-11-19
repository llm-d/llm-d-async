package pubsub

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/llm-d/llm-d-async/api"
)

func TestLoadConfig_ResultTopicRequirement(t *testing.T) {
	tests := []struct {
		name                 string
		cfg                  string
		wantErr              string
		wantFirstTopicResult string
	}{
		{
			name: "flow default only (backward compatible)",
			cfg:  `{"project_id":"p","result_topic_id":"r","topics":[{"subscriber_id":"s","igw_base_url":"http://gw"}]}`,
		},
		{
			name:                 "every topic sets its own result topic",
			cfg:                  `{"project_id":"p","topics":[{"subscriber_id":"s1","igw_base_url":"http://gw","result_topic_id":"r1"},{"subscriber_id":"s2","igw_base_url":"http://gw","result_topic_id":"r2"}]}`,
			wantFirstTopicResult: "r1",
		},
		{
			name:                 "mixed topics with flow default",
			cfg:                  `{"project_id":"p","result_topic_id":"r","topics":[{"subscriber_id":"s1","igw_base_url":"http://gw","result_topic_id":"r1"},{"subscriber_id":"s2","igw_base_url":"http://gw"}]}`,
			wantFirstTopicResult: "r1",
		},
		{
			name:    "a topic without result topic and no flow default",
			cfg:     `{"project_id":"p","topics":[{"subscriber_id":"s1","igw_base_url":"http://gw","result_topic_id":"r1"},{"subscriber_id":"s2","igw_base_url":"http://gw"}]}`,
			wantErr: "result_topic_id is required",
		},
		{
			name:    "no topics and no flow default",
			cfg:     `{"project_id":"p","topics":[]}`,
			wantErr: "at least one topic must be configured",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig([]byte(tt.cfg))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("LoadConfig() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() unexpected error: %v", err)
			}
			if len(cfg.Topics) > 0 && cfg.Topics[0].ResultTopicID != tt.wantFirstTopicResult {
				t.Errorf("Topics[0].ResultTopicID = %q, want %q", cfg.Topics[0].ResultTopicID, tt.wantFirstTopicResult)
			}
		})
	}
}

// TestProcessMessages_ResultRouting verifies the destination stamped at pull
// time follows the sorted-set precedence: per-topic config > per-message
// result_queue_name > flow default (left empty for resultWorker to fill).
func TestProcessMessages_ResultRouting(t *testing.T) {
	tests := []struct {
		name          string
		topicResult   string
		messageResult string
		want          string
	}{
		{name: "neither set leaves flow default", want: ""},
		{name: "per-message only", messageResult: "msg-results", want: "msg-results"},
		{name: "per-topic only", topicResult: "topic-results", want: "topic-results"},
		{name: "per-topic wins over per-message", topicResult: "topic-results", messageResult: "msg-results", want: "topic-results"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flow := &PubSubMQFlow{resultChannel: make(chan api.ResultMessage, 1)}
			ch := make(chan *api.InternalRequest, 1)

			data := map[string]any{"id": "req-1", "payload": map[string]any{"prompt": "hi"}}
			if tt.messageResult != "" {
				data["result_queue_name"] = tt.messageResult
			}
			msgData, _ := json.Marshal(data)
			receive := func(ctx context.Context, f func(context.Context, *pubsub.Message)) error {
				f(ctx, &pubsub.Message{ID: "msg-" + tt.name, Data: msgData})
				return nil
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			var got *api.InternalRequest
			done := make(chan struct{})
			go func() {
				defer close(done)
				select {
				case ir := <-ch:
					got = ir
					if val, ok := resultChannels.Load(ir.TransportCorrelationID); ok {
						val.(chan bool) <- true
					}
				case <-ctx.Done():
				}
			}()

			func() {
				// Ack/Nack panic on a hand-built pubsub.Message; the routing
				// under test is stamped before that point.
				defer func() { _ = recover() }()
				_ = flow.processMessages(ctx, receive, "test-sub", "test-pool", ch, &mockAttributeGate{allowed: true}, nil, tt.topicResult)
			}()
			<-done

			if got == nil {
				t.Fatal("expected a request to be received, got nil")
			}
			if got.ResultQueueName != tt.want {
				t.Errorf("ResultQueueName = %q, want %q", got.ResultQueueName, tt.want)
			}
			if got.PublicRequest.ReqID() != "req-1" {
				t.Errorf("ReqID = %q, want req-1", got.PublicRequest.ReqID())
			}
		})
	}
}

// TestResultWorker_RoutesPerDestination drives resultWorker against the
// in-memory Pub/Sub fake and checks each result lands on its resolved topic.
func TestResultWorker_RoutesPerDestination(t *testing.T) {
	srv, client, _ := newFakePubSubServer(t)
	ctx := context.Background()
	for _, topic := range []string{"default-results", "team-a-results", "team-b-results"} {
		if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: "projects/" + testProject + "/topics/" + topic}); err != nil {
			t.Fatalf("failed to create topic %q: %v", topic, err)
		}
	}

	results := []api.ResultMessage{
		{ID: "r-default", Routing: api.InternalRouting{TransportCorrelationID: "c-default"}},
		{ID: "r-a1", Routing: api.InternalRouting{TransportCorrelationID: "c-a1", ResultQueueName: "team-a-results"}},
		{ID: "r-b", Routing: api.InternalRouting{TransportCorrelationID: "c-b", ResultQueueName: "team-b-results"}},
		{ID: "r-a2", Routing: api.InternalRouting{TransportCorrelationID: "c-a2", ResultQueueName: "team-a-results"}},
	}
	signals := make(map[string]chan bool, len(results))
	for _, r := range results {
		sig := make(chan bool, 1)
		signals[r.Routing.TransportCorrelationID] = sig
		resultChannels.Store(r.Routing.TransportCorrelationID, sig)
		t.Cleanup(func() { resultChannels.Delete(r.Routing.TransportCorrelationID) })
	}

	workerCtx, cancel := context.WithCancel(ctx)
	resultCh := make(chan api.ResultMessage)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		resultWorker(workerCtx, client, "default-results", resultCh)
	}()

	for _, r := range results {
		resultCh <- r
		select {
		case ok := <-signals[r.Routing.TransportCorrelationID]:
			if !ok {
				t.Fatalf("result %q signalled failure, want success", r.ID)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for result %q", r.ID)
		}
	}
	// Stopping the worker stops its publishers, flushing pending publishes.
	cancel()
	<-stopped

	gotTopic := make(map[string]string)
	for _, m := range srv.Messages() {
		var rm api.ResultMessage
		if err := json.Unmarshal(m.Data, &rm); err != nil {
			t.Fatalf("failed to decode published result: %v", err)
		}
		gotTopic[rm.ID] = m.Topic[strings.LastIndex(m.Topic, "/")+1:]
	}
	want := map[string]string{
		"r-default": "default-results",
		"r-a1":      "team-a-results",
		"r-b":       "team-b-results",
		"r-a2":      "team-a-results",
	}
	for id, topic := range want {
		if gotTopic[id] != topic {
			t.Errorf("result %q published to %q, want %q", id, gotTopic[id], topic)
		}
	}
	if len(gotTopic) != len(want) {
		t.Errorf("published %d results, want %d: %v", len(gotTopic), len(want), gotTopic)
	}
}

// TestResultWorker_NoDestinationNacks covers a result with no resolvable
// topic: it is not published and the receive callback is told to Nack.
func TestResultWorker_NoDestinationNacks(t *testing.T) {
	_, client, _ := newFakePubSubServer(t)

	sig := make(chan bool, 1)
	resultChannels.Store("c-none", sig)
	t.Cleanup(func() { resultChannels.Delete("c-none") })

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan api.ResultMessage)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		resultWorker(ctx, client, "", resultCh)
	}()
	defer func() {
		cancel()
		<-stopped
	}()

	resultCh <- api.ResultMessage{ID: "r-none", Routing: api.InternalRouting{TransportCorrelationID: "c-none"}}
	select {
	case ok := <-sig:
		if ok {
			t.Fatal("expected failure signal for result without a destination")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result signal")
	}
}

// TestResultWorker_PublishFailureNacks covers a result whose topic cannot be
// published to (here: it does not exist). The publish error must be reported
// as a failure so the request is Nacked and redelivered, not Acked and lost.
func TestResultWorker_PublishFailureNacks(t *testing.T) {
	srv, client, _ := newFakePubSubServer(t)

	sig := make(chan bool, 1)
	resultChannels.Store("c-missing", sig)
	t.Cleanup(func() { resultChannels.Delete("c-missing") })

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan api.ResultMessage)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		resultWorker(ctx, client, "", resultCh)
	}()
	defer func() {
		cancel()
		<-stopped
	}()

	resultCh <- api.ResultMessage{ID: "r-missing", Routing: api.InternalRouting{TransportCorrelationID: "c-missing", ResultQueueName: "no-such-topic"}}
	select {
	case ok := <-sig:
		if ok {
			t.Fatal("expected failure signal for a result published to a missing topic")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result signal")
	}
	if n := len(srv.Messages()); n != 0 {
		t.Errorf("published %d messages, want 0", n)
	}
}
