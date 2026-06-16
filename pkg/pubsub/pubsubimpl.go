package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

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

const quotaExceededNackDelay = 10 * time.Second

type TopicConfig struct {
	SubscriberID       string            `json:"subscriber_id"`
	WorkerPoolID       string            `json:"worker_pool_id"`
	InferenceObjective string            `json:"inference_objective"`
	RequestPathURL     string            `json:"request_path_url"`
	IGWBaseURL         string            `json:"igw_base_url"`
	GateType           string            `json:"gate_type"`
	GateParams         map[string]string `json:"gate_params,omitempty"`
}

var _ pipeline.Flow = (*PubSubMQFlow)(nil)

type PubSubMQFlow struct {
	resultTopicID   string
	requestChannels []RequestChannelData
	retryChannel    chan pipeline.RetryMessage
	resultChannel   chan api.ResultMessage
	gate            pipeline.DispatchGate
	gateFactory     pipeline.GateFactory
	workerPools     []pipeline.WorkerPoolConfig
	consumeCancel   context.CancelFunc
	consumeWg       sync.WaitGroup
	drainCancel     context.CancelFunc
	drainWg         sync.WaitGroup
	metricClient    *monitoring.MetricClient
	projectID       string
}
type RequestChannelData struct {
	requestChannel pipeline.RequestChannel
	subscriberID   string
	gate           pipeline.DispatchGate
}

// PubSubOption is a functional option for configuring PubSubMQFlow
type PubSubOption func(*PubSubMQFlow)

// WithGateFactory sets a GateFactory for per-topic gate instantiation.
// When set, gates are created per topic from config, overriding any global gate.
func WithGateFactory(factory pipeline.GateFactory) PubSubOption {
	return func(p *PubSubMQFlow) {
		p.gateFactory = factory
	}
}

// WithWorkerPools sets the pool configurations to resolve named pools.
func WithWorkerPools(workerPools []pipeline.WorkerPoolConfig) PubSubOption {
	return func(p *PubSubMQFlow) {
		p.workerPools = workerPools
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
		configs = []TopicConfig{{
			SubscriberID:       *requestSubscriberID,
			WorkerPoolID:       "default",
			InferenceObjective: *inferenceObjective,
			IGWBaseURL:         *igwBaseURL,
			RequestPathURL:     *requestPathURL,
		}}
	}
	p := &PubSubMQFlow{
		resultTopicID:   *resultTopicID,
		requestChannels: make([]RequestChannelData, 0, len(configs)),
		retryChannel:    make(chan pipeline.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
		projectID:       *projectID,
	}

	// Cloud Monitoring client for broker-backlog metrics. Best-effort: if it
	// can't be created (e.g. missing credentials), backlog reporting is
	// skipped but the flow still operates normally.
	if metricClient, mErr := monitoring.NewMetricClient(ctx); mErr != nil {
		log.FromContext(ctx).V(logutil.DEFAULT).Error(mErr, "Failed to create Cloud Monitoring client; broker backlog metrics disabled")
	} else {
		p.metricClient = metricClient
	}

	// Apply functional options
	for _, opt := range opts {
		opt(p)
	}

	// Create per-topic channels with gates
	for _, cfg := range configs {
		workerPoolID := cfg.WorkerPoolID
		if workerPoolID == "" {
			workerPoolID = "default"
		}

		found := false
		for _, pool := range p.workerPools {
			if pool.ID == workerPoolID {
				found = true
				break
			}
		}
		if !found {
			panic(fmt.Sprintf("worker pool %q specified in topic config not found in pool configuration", workerPoolID))
		}

		if cfg.IGWBaseURL == "" {
			panic(fmt.Sprintf("topic config for subscriber %q: igw_base_url must be specified", cfg.SubscriberID))
		}

		reqPath := cfg.RequestPathURL
		if reqPath == "" {
			reqPath = "/v1/completions"
		}

		// Determine gate for this topic
		var gate pipeline.DispatchGate
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
			gate = pipeline.ConstOpenGate()
		}

		ch := make(chan *api.InternalRequest)
		p.requestChannels = append(p.requestChannels, RequestChannelData{
			requestChannel: pipeline.RequestChannel{
				Channel:            ch,
				IGWBaseURL:         cfg.IGWBaseURL,
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     reqPath,
				Gate:               gate,
				WorkerPoolID:       workerPoolID,
			},
			subscriberID: cfg.SubscriberID,
			gate:         gate,
		})
	}

	// Set default gate if not already set
	if p.gate == nil {
		p.gate = pipeline.ConstOpenGate()
	}

	return p
}

func (r *PubSubMQFlow) RetryChannel() chan pipeline.RetryMessage {
	return r.retryChannel
}

func (r *PubSubMQFlow) ResultChannel() chan api.ResultMessage {
	return r.resultChannel
}

func (r *PubSubMQFlow) Characteristics() pipeline.Characteristics {
	return pipeline.Characteristics{
		HasExternalBackoff:     true,
		SupportsMessageLatency: true,
	}
}

func (r *PubSubMQFlow) RequestChannels() []pipeline.RequestChannel {

	var channels []pipeline.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
	}
	return channels
}

func (r *PubSubMQFlow) Start(ctx context.Context) {
	logger := log.FromContext(ctx)
	consumeCtx, consumeCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.consumeCancel = consumeCancel

	drainCtx, drainCancel := context.WithCancel(log.IntoContext(context.Background(), logger))
	r.drainCancel = drainCancel

	for _, channelData := range r.requestChannels {
		r.consumeWg.Add(1)
		go func(cd RequestChannelData) {
			defer r.consumeWg.Done()
			r.requestWorker(consumeCtx, pubSubClient, cd.subscriberID, cd.requestChannel.WorkerPoolID, cd.requestChannel.Channel, cd.gate)
		}(channelData)
	}
	publisher := pubSubClient.Publisher(r.resultTopicID)
	r.drainWg.Add(2)
	go func() { defer r.drainWg.Done(); resultWorker(drainCtx, publisher, r.resultChannel) }()
	go func() { defer r.drainWg.Done(); addMsgToRetryQueue(drainCtx, r.retryChannel) }()
}

func (r *PubSubMQFlow) StopConsuming() {
	if r.consumeCancel != nil {
		r.consumeCancel()
	}
	r.consumeWg.Wait()
}

func (r *PubSubMQFlow) Shutdown() {
	if r.drainCancel != nil {
		r.drainCancel()
	}
	r.drainWg.Wait()
	if r.metricClient != nil {
		_ = r.metricClient.Close()
	}
}

// QueueBacklog reports the number of undelivered messages per subscription,
// sourced from the Cloud Monitoring metric
// pubsub.googleapis.com/subscription/num_undelivered_messages. The value is
// approximate and lags real time by the metric's sampling interval.
func (r *PubSubMQFlow) QueueBacklog(ctx context.Context) ([]pipeline.QueueBacklogStat, error) {
	if r.metricClient == nil {
		// Backlog reporting was disabled at startup (already logged when the
		// client failed to initialize); no-op rather than erroring every poll.
		return nil, nil
	}
	now := time.Now()
	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-5 * time.Minute)),
		EndTime:   timestamppb.New(now),
	}

	stats := make([]pipeline.QueueBacklogStat, 0, len(r.requestChannels))
	var firstErr error
	for _, cd := range r.requestChannels {
		subID := cd.subscriberID
		req := &monitoringpb.ListTimeSeriesRequest{
			Name: "projects/" + r.projectID,
			Filter: fmt.Sprintf(
				`metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.labels.subscription_id="%s"`,
				subID),
			Interval: interval,
			View:     monitoringpb.ListTimeSeriesRequest_FULL,
		}
		it := r.metricClient.ListTimeSeries(ctx, req)
		ts, err := it.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				// No sample in the window: for a configured subscription this
				// normally means it has drained. Report 0 so the gauge does not
				// retain a stale (high) value after the queue empties.
				stats = append(stats, pipeline.QueueBacklogStat{
					QueueName: subID,
					PoolName:  cd.requestChannel.WorkerPoolID,
				})
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("list time series for subscription %q: %w", subID, err)
			}
			// Report 0 rather than skipping so the gauge does not retain a
			// stale value for this subscription after a failed poll.
			stats = append(stats, pipeline.QueueBacklogStat{
				QueueName: subID,
				PoolName:  cd.requestChannel.WorkerPoolID,
			})
			continue
		}
		points := ts.GetPoints()
		if len(points) == 0 {
			stats = append(stats, pipeline.QueueBacklogStat{
				QueueName: subID,
				PoolName:  cd.requestChannel.WorkerPoolID,
			})
			continue
		}
		// Points are returned newest-first; the first is the latest sample.
		stats = append(stats, pipeline.QueueBacklogStat{
			QueueName: subID,
			PoolName:  cd.requestChannel.WorkerPoolID,
			Depth:     points[0].GetValue().GetInt64Value(),
		})
	}
	return stats, firstErr
}

var _ pipeline.BacklogReporter = (*PubSubMQFlow)(nil)

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
				fallback := map[string]string{"id": msg.ID, "error": "Failed to marshal result to string"}
				msgBytes, _ = json.Marshal(fallback)
			} else {
				msgBytes = bytes
			}
			publishPubSub(ctx, publisher, msgBytes, map[string]string{})
			value, ok := resultChannels.Load(msg.Routing.TransportCorrelationID)
			if !ok {
				logger.V(logutil.DEFAULT).Error(nil, "Result channel not found for message", "pubsubID", msg.Routing.TransportCorrelationID)
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

func addMsgToRetryQueue(ctx context.Context, retryChannel chan pipeline.RetryMessage) {
	logger := log.FromContext(ctx)

	handleRetry := func(msg pipeline.RetryMessage) {
		if msg.InternalRequest == nil {
			return
		}
		value, ok := resultChannels.Load(msg.TransportCorrelationID)
		if !ok {
			logger.V(logutil.DEFAULT).Error(nil, "Result channel not found for retry message", "pubsubID", msg.TransportCorrelationID)
			return
		}
		resultChannel := value.(chan bool)
		logger.V(logutil.DEBUG).Info("Retrying message", "pubsubID", msg.TransportCorrelationID)
		resultChannel <- false
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-retryChannel:
					handleRetry(msg)
				default:
					return
				}
			}

		case msg := <-retryChannel:
			handleRetry(msg)
		}
	}
}

func (r *PubSubMQFlow) requestWorker(ctx context.Context, pubSubClient *pubsub.Client, subscriberID, poolID string, ch chan *api.InternalRequest, gate pipeline.DispatchGate) {
	logger := log.FromContext(ctx)

	sub := pubSubClient.Subscriber(subscriberID)

	for ctx.Err() == nil {
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

		err := r.processMessages(receiveCtx, sub.Receive, subscriberID, poolID, ch, gate)

		cancel()
		// TODO
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Fail to receive messages from request subscription")
		}
	}

}

type receiveFunc func(context.Context, func(context.Context, *pubsub.Message)) error

func (r *PubSubMQFlow) processMessages(ctx context.Context, receive receiveFunc, subscriberID string, poolID string, ch chan *api.InternalRequest, gate pipeline.DispatchGate) error {
	logger := log.FromContext(ctx)
	return receive(ctx, func(ctx context.Context, msg *pubsub.Message) {

		var body api.RequestMessage
		err := json.Unmarshal(msg.Data, &body)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message from request queue")
			msg.Ack()
			return
		}

		// Carry the subscription as the request queue label so all per-queue
		// metrics (throughput, depth, inflight, latency) align with the
		// async_broker_backlog gauge, which is keyed by subscription ID.
		irout := api.InternalRouting{TransportCorrelationID: msg.ID, RequestQueueName: subscriberID}
		if msg.DeliveryAttempt != nil {
			irout.RetryCount = *msg.DeliveryAttempt - 1
		}
		ir := api.NewInternalRequest(irout, &body)

		// Per-attribute gating
		var release func()
		if attrGate, ok := gate.(pipeline.AttributeGate); ok {
			res, err := attrGate.Acquire(ctx, msg.Attributes)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to acquire attribute quota")
				msg.Nack()
				return
			}
			if !res.Allowed {
				logger.V(logutil.DEBUG).Info("Quota exceeded, delaying Nack", "msgID", msg.ID, "delay", quotaExceededNackDelay)
				go func() {
					select {
					case <-time.After(quotaExceededNackDelay):
						msg.Nack()
					case <-ctx.Done():
						msg.Nack()
					}
				}()
				return
			}
			release = res.Release
			ir.Classification = res.Classification
		} else {
			release = func() {}
		}
		defer release()

		resultsChannel := make(chan bool, 1)
		resultChannels.Store(msg.ID, resultsChannel)
		defer resultChannels.Delete(msg.ID)

		ch <- ir

		result := <-resultsChannel
		if !result {
			msg.Nack()
		} else {
			metrics.RecordMessageLatency(float64(time.Since(msg.PublishTime).Milliseconds()), ir.QueueID, ir.RequestQueueName, poolID)
			msg.Ack()
		}
	})
}
