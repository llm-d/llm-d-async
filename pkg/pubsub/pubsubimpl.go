package pubsub

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const PUBSUB_ID = "pubsub-id"

var pubSubClient *pubsub.Client

var (
	igwBaseURL          = flag.String("pubsub.igw-base-url", "", "Base URL for IGW. Mutually exclusive with pubsub.topics-config-file flag.")
	projectID           = flag.String("pubsub.project-id", "", "GCP project ID for PubSub")
	requestPathURL      = flag.String("pubsub.request-path-url", "/v1/completions", "inference request path url. Mutually exclusive with pubsub.topics-config-file flag.")
	inferenceObjective  = flag.String("pubsub.inference-objective", "", "inference objective to use in requests. Mutually exclusive with pubsub.topics-config-file flag.")
	requestSubscriberID = flag.String("pubsub.request-subscriber-id", "", "GCP PubSub request topic subscriber ID. Mutually exclusive with pubsub.topics-config-file flag.")
	resultTopicID       = flag.String("pubsub.result-topic-id", "", "GCP PubSub topic ID for results")
	topicsConfigFile    = flag.String("pubsub.topics-config-file", "", "Topics Configuration file. Mutually exclusive with pubsub.igw-base-url, pubsub.request-subscriber-id, pubsub.request-path-url and pubsub.inference-objective flags. See documentation about syntax")
	batchSize           = flag.Int("pubsub.batch-size", 10, "Number of inflight messages")

	resultChannels sync.Map
)

type TopicConfig struct {
	SubscriberID       string            `json:"subscriber_id"`
	InferenceObjective string            `json:"inference_objective"`
	RequestPathURL     string            `json:"request_path_url"`
	IGWBaseURl         string            `json:"igw_base_url"`
	GateType           string            `json:"gate_type"`
	GateParams         map[string]string `json:"gate_params,omitempty"`
}

var _ api.Flow = (*PubSubMQFlow)(nil)

type PubSubMQFlow struct {
	resultTopicID   string
	requestChannels []RequestChannelData
	retryChannel    chan api.RetryMessage
	resultChannel   chan api.ResultMessage
	gate            api.DispatchGate
	gateFactory     api.GateFactory
}
type RequestChannelData struct {
	requestChannel api.RequestChannel
	subscriberID   string
	gate           api.DispatchGate
}

// PubSubOption is a functional option for configuring PubSubMQFlow
type PubSubOption func(*PubSubMQFlow)

// WithGateFactory sets a GateFactory for per-topic gate instantiation.
// When set, gates are created per topic from config, overriding any global gate.
func WithGateFactory(factory api.GateFactory) PubSubOption {
	return func(p *PubSubMQFlow) {
		p.gateFactory = factory
	}
}

func NewGCPPubSubMQFlow(opts ...PubSubOption) *PubSubMQFlow {

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
		configs = []TopicConfig{{SubscriberID: *requestSubscriberID, IGWBaseURl: *igwBaseURL, InferenceObjective: *inferenceObjective, RequestPathURL: *requestPathURL}}
	}
	p := &PubSubMQFlow{
		resultTopicID:   *resultTopicID,
		requestChannels: make([]RequestChannelData, 0, len(configs)),
		retryChannel:    make(chan api.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
	}

	// Apply functional options
	for _, opt := range opts {
		opt(p)
	}

	// Create per-topic channels with gates
	for _, cfg := range configs {
		// Determine gate for this topic
		var gate api.DispatchGate
		if p.gateFactory != nil && cfg.GateType != "" {
			// Use factory to create per-topic gate
			var err error
			gate, err = p.gateFactory.CreateGate(cfg.GateType, cfg.GateParams)
			if err != nil {
				panic(fmt.Sprintf("failed to create gate for topic subscriber %q (gate_type=%q): %v", cfg.SubscriberID, cfg.GateType, err))
			}
		} else if p.gate != nil {
			// Fall back to global gate if provided
			gate = p.gate
		} else {
			// Default to always-open gate
			gate = api.ConstOpenGate()
		}

		ch := make(chan api.RequestMessage)
		p.requestChannels = append(p.requestChannels, RequestChannelData{
			requestChannel: api.RequestChannel{
				Channel:            ch,
				IGWBaseURl:         util.NormalizeBaseURL(cfg.IGWBaseURl),
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     util.NormalizeURLPath(cfg.RequestPathURL),
				Gate:               gate,
			},
			subscriberID: cfg.SubscriberID,
			gate:         gate,
		})
	}

	// Set default gate if not already set
	if p.gate == nil {
		p.gate = api.ConstOpenGate()
	}

	return p
}

func (r *PubSubMQFlow) RetryChannel() chan api.RetryMessage {
	return r.retryChannel
}

func (r *PubSubMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *PubSubMQFlow) Characteristics() api.Characteristics {
	return api.Characteristics{
		HasExternalBackoff:     true,
		SupportsMessageLatency: true,
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
		go r.requestWorker(ctx, pubSubClient, channelData.subscriberID, channelData.requestChannel.Channel, channelData.gate)
	}
	publisher := pubSubClient.Publisher(r.resultTopicID)
	go resultWorker(ctx, publisher, r.resultChannel)

	go addMsgToRetryQueue(ctx, r.retryChannel)
}

func resultWorker(ctx context.Context, publisher *pubsub.Publisher, resultChannel chan api.ResultMessage) {
	logger := log.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-resultChannel:
			bytes, err := json.Marshal(msg)
			var msgBytes []byte
			if err != nil {
				fallback := map[string]string{"id": msg.Id, "error": "Failed to marshal result to string"}
				msgBytes, _ = json.Marshal(fallback)
			} else {
				msgBytes = bytes
			}
			publishPubSub(ctx, publisher, msgBytes, map[string]string{})
			pubsubID := msg.Metadata[PUBSUB_ID]
			value, ok := resultChannels.Load(pubsubID)
			if !ok {
				logger.V(logutil.DEFAULT).Error(nil, "Result channel not found for message", "pubsubID", pubsubID)
				continue
			}
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
			value, ok := resultChannels.Load(pubsubID)
			if !ok {
				logger.V(logutil.DEFAULT).Error(nil, "Result channel not found for retry message", "pubsubID", pubsubID)
				continue
			}
			resultChannel := value.(chan bool)
			logger.V(logutil.DEBUG).Info("Retrying message", "pubsubID", pubsubID)
			resultChannel <- false

		}
	}

}

func (r *PubSubMQFlow) requestWorker(ctx context.Context, pubSubClient *pubsub.Client, subscriberID string, ch chan api.RequestMessage, gate api.DispatchGate) {
	logger := log.FromContext(ctx)

	sub := pubSubClient.Subscriber(subscriberID)

	for {
		receiveCtx, cancel := context.WithCancel(ctx)
		budget := gate.Budget(ctx)
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if gate.Budget(ctx) != budget {
						cancel() // Trigger restart with different limit
						return
					}
				case <-receiveCtx.Done():
					return
				}
			}
		}()

		currBatchSize := int(math.Floor(float64(*batchSize) * budget))
		logger.V(logutil.DEFAULT).Info("PubSub MaxOutstandingMessages", "value", currBatchSize)
		sub.ReceiveSettings.MaxOutstandingMessages = currBatchSize
		sub.ReceiveSettings.NumGoroutines = 1
		if currBatchSize <= 0 {
			<-receiveCtx.Done()
			cancel()
			continue
		}
		err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {

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
			defer resultChannels.Delete(msg.ID)

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
				metrics.MessageLatencyTime.Observe(float64(time.Since(msg.PublishTime).Milliseconds()))
				msg.Ack()
			}
		})
		cancel()
		// TODO
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Fail to receive messages from request subscription")
		}
	}

}
