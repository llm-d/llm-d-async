package pubsub

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const PUBSUB_ID = "pubsub-id"

var pubSubClient *pubsub.Client

var (
	projectID = flag.String("pubsub.project-id", "", "GCP project ID for PubSub")

	requestPathURL      = flag.String("pubsub.request-path-url", "/v1/completions", "inference request path url. Mutally exclusive with pubsub.topics-config-file flag.")
	inferenceObjective  = flag.String("pubsub.inference-objective", "", "inference objective to use in requests. Mutally exclusive with pubsub.topics-config-file flag.")
	requestSubscriberID = flag.String("pubsub.request-subscriber-id", "", "GCP PubSub request topic subscriber ID. Mutally exclusive with pubsub.topics-config-file flag.")
	resultTopicID       = flag.String("pubsub.result-topic-id", "", "GCP PubSub topic ID for results")
	topicsConfigFile    = flag.String("pubsub.topics-config-file", "", "Topics Configuration file. Mutally exclusive with pubsub.request-subscriber-id, pubsub.request-path-url and pubsub.inference-objective flags. See documentation about syntax")

	resultChannels sync.Map
)

type TopicConfig struct {
	SubscriberID       string `json:"subscriber_id"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
}
type PubSubMQFlow struct {
	resultTopicID   string
	requestChannels []RequestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
}
type RequestChannelData struct {
	requestChannel api.RequestChannel
	subscriberID   string
}

func NewGCPPubSubMQFlow() *PubSubMQFlow {

	ctx := context.Background()
	var err error
	pubSubClient, err = pubsub.NewClient(ctx, *projectID)
	if err != nil {
		// TODO:
		panic(err)
	}
	var configs []TopicConfig
	if *topicsConfigFile != "" {
		data, err := os.ReadFile(*topicsConfigFile)
		if err != nil {
			panic(fmt.Sprintf("failed to read topics config file: %v", err))
		}

		if err := json.Unmarshal(data, &configs); err != nil {
			panic(fmt.Sprintf("failed to unmarshal topics config: %v", err))
		}
	} else {
		configs = []TopicConfig{{SubscriberID: *requestSubscriberID, InferenceObjective: *inferenceObjective, RequestPathURL: *requestPathURL}}
	}

	var channels []RequestChannelData
	for _, cfg := range configs {
		ch := make(chan api.RequestMessage)

		channels = append(channels, RequestChannelData{
			requestChannel: api.RequestChannel{
				Channel:            ch,
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
			},
			subscriberID: cfg.SubscriberID,
		})
	}

	return &PubSubMQFlow{
		resultTopicID:   *resultTopicID,
		requestChannels: channels,
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
	}
}

func (r *PubSubMQFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *PubSubMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *PubSubMQFlow) Characteristics() api.Characteristics {
	return api.Characteristics{
		HasExternalBackoff: true,
	}
}

func (r *PubSubMQFlow) RequestChannels() []api.RequestChannel {

	var channels []api.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
	}
	return channels
}

func (r *PubSubMQFlow) Start(ctx context.Context) {
	for _, channelData := range r.requestChannels {
		go requestWorker(ctx, pubSubClient, channelData.subscriberID, channelData.requestChannel.Channel)
	}
	publisher := pubSubClient.Publisher(r.resultTopicID)
	go resultWorker(ctx, publisher, r.resultChannel)

	go addMsgToRetryQueue(ctx, r.retryChannel)
}

func resultWorker(ctx context.Context, publisher *pubsub.Publisher, resultChannel chan api.ResultMessage) {

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-resultChannel:
			bytes, err := json.Marshal(msg)
			var msgBytes []byte
			if err != nil {
				msgBytes = []byte(fmt.Sprintf(`{"id" : "%s", "error":  "Failed to marshal result to string"}`, msg.Id))
			} else {
				msgBytes = bytes
			}
			publishPubSub(ctx, publisher, msgBytes, map[string]string{})
			pubsubID := msg.Metadata[PUBSUB_ID]
			value, _ := resultChannels.Load(pubsubID)
			resultChannel := value.(chan bool)
			resultChannel <- true

		}
	}
}

func publishPubSub(ctx context.Context, publisher *pubsub.Publisher, msg []byte, attrs map[string]string) {
	// TODO: check how to validate that message are actually being published
	publisher.Publish(ctx, &pubsub.Message{
		Data:       msg,
		Attributes: attrs,
	})

}

func addMsgToRetryQueue(ctx context.Context, retryChannel chan api.RetryMessage) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-retryChannel:
			pubsubID := msg.RequestMessage.Metadata[PUBSUB_ID]
			value, _ := resultChannels.Load(pubsubID)
			resultChannel := value.(chan bool)
			logger.V(logutil.DEBUG).Info("Retrying message", "pubsubID", pubsubID)
			resultChannel <- false

		}
	}

}

func requestWorker(ctx context.Context, pubSubClient *pubsub.Client, subscriberID string, ch chan api.RequestMessage) {
	logger := log.FromContext(ctx)

	sub := pubSubClient.Subscriber(subscriberID)

	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		deliveryAttempt := msg.DeliveryAttempt

		var msgObj api.RequestMessage
		err := json.Unmarshal(msg.Data, &msgObj)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message from request queue")
			msg.Ack()
			return
		}

		resultsChannel := make(chan bool, 1)
		resultChannels.Store(msg.ID, resultsChannel)
		defer resultChannels.Delete(msgObj.Id)

		if msgObj.Metadata == nil {
			msgObj.Metadata = make(map[string]string)
		}
		msgObj.Metadata[PUBSUB_ID] = msg.ID
		if deliveryAttempt != nil {
			msgObj.RetryCount = *deliveryAttempt - 1
		}

		ch <- msgObj

		result := <-resultsChannel
		if !result {
			msg.Nack()
		} else {
			msg.Ack()
		}
	})
	// TODO
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Fail to receive messages from request subscription")
	}

}
