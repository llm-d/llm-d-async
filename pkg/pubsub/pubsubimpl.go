package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

var pubSubClient *pubsub.Client

var resultChannels sync.Map

const quotaExceededNackDelay = 10 * time.Second

type TopicConfig struct {
	SubscriberID       string `json:"subscriber_id"`
	WorkerPoolID       string `json:"worker_pool_id"`
	InferenceObjective string `json:"inference_objective"`
	RequestPathURL     string `json:"request_path_url"`
	IGWBaseURL         string `json:"igw_base_url"`
	pipeline.GateConfig
	Labels map[string]string `json:"labels,omitempty"`
}

var _ pipeline.Flow = (*PubSubMQFlow)(nil)

type PubSubMQFlow struct {
	resultTopicID   string
	requestChannels []RequestChannelData
	retryChannel    chan pipeline.RetryMessage
	resultChannel   chan api.ResultMessage
	batchSize       int
	gate            pipeline.Gate
	gateFactory     pipeline.GateFactory
	workerPools     []pipeline.WorkerPoolConfig
	consumeCancel   context.CancelFunc
	consumeWg       sync.WaitGroup
	drainCancel     context.CancelFunc
	drainWg         sync.WaitGroup
	metricClient    *monitoring.MetricClient
	projectID       string
	client          *pubsub.Client
	// consumeHealth caches per-subscription connectivity observed by the
	// consume loop (requestWorker), keyed by subscriber ID. It is populated once
	// at construction; the map itself is never mutated afterwards, so concurrent
	// reads in HealthCheck race-free against per-entry updates guarded by
	// subHealth's own mutex.
	consumeHealth map[string]*subHealth
}

// subHealth records the most recent broker interaction observed by the consume
// loop for a single subscription. A successful Receive callback is authoritative
// proof of connectivity; a Receive error (broker unreachable, subscription gone)
// marks the subscription unhealthy until the next success.
type subHealth struct {
	mu        sync.Mutex
	lastOK    time.Time
	lastErr   error
	lastErrAt time.Time
}

type RequestChannelData struct {
	requestChannel pipeline.RequestChannel
	subscriberID   string
	gate           pipeline.Gate
	labels         map[string]string
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

func NewGCPPubSubMQFlow(pubsubOpts Options, fns ...PubSubOption) (*PubSubMQFlow, error) {

	ctx := context.Background()
	var err error
	pubSubClient, err = pubsub.NewClient(ctx, pubsubOpts.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create PubSub client: %w", err)
	}
	var configs []TopicConfig
	if pubsubOpts.TopicsConfigFile != "" {
		data, err := os.ReadFile(pubsubOpts.TopicsConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read topics config file: %w", err)
		}

		if err := json.Unmarshal(data, &configs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal topics config: %w", err)
		}
	} else {
		configs = []TopicConfig{{
			SubscriberID:       pubsubOpts.RequestSubscriberID,
			WorkerPoolID:       "default",
			InferenceObjective: pubsubOpts.InferenceObjective,
			IGWBaseURL:         pubsubOpts.IGWBaseURL,
			RequestPathURL:     pubsubOpts.RequestPathURL,
		}}
	}
	p := &PubSubMQFlow{
		resultTopicID:   pubsubOpts.ResultTopicID,
		requestChannels: make([]RequestChannelData, 0, len(configs)),
		retryChannel:    make(chan pipeline.RetryMessage),
		resultChannel:   make(chan api.ResultMessage),
		batchSize:       pubsubOpts.BatchSize,
		projectID:       pubsubOpts.ProjectID,
		client:          pubSubClient,
		consumeHealth:   make(map[string]*subHealth, len(configs)),
	}

	if metricClient, mErr := monitoring.NewMetricClient(ctx); mErr != nil {
		log.FromContext(ctx).V(logutil.DEFAULT).Error(mErr, "Failed to create Cloud Monitoring client; broker backlog metrics disabled")
	} else {
		p.metricClient = metricClient
	}

	for _, fn := range fns {
		fn(p)
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
			return nil, fmt.Errorf("worker pool %q specified in topic config not found in pool configuration", workerPoolID)
		}

		if cfg.IGWBaseURL == "" {
			return nil, fmt.Errorf("topic config for subscriber %q: igw_base_url must be specified", cfg.SubscriberID)
		}

		reqPath := cfg.RequestPathURL
		if reqPath == "" {
			reqPath = "/v1/completions"
		}

		// Determine gate for this topic
		var gate pipeline.Gate
		if p.gateFactory != nil && cfg.GateType != "" {
			// Use factory to create per-topic gate
			gate, err = p.gateFactory.CreateGate(cfg.GateConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create gate for topic subscriber %q (gate_type=%q): %w", cfg.SubscriberID, cfg.GateType, err)
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
			labels:       cfg.Labels,
		})
		p.consumeHealth[cfg.SubscriberID] = &subHealth{}
	}

	// Set default gate if not already set
	if p.gate == nil {
		p.gate = pipeline.ConstOpenGate()
	}

	return p, nil
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

var _ pipeline.HealthChecker = (*PubSubMQFlow)(nil)

// consumeHealthTTL bounds how long a successful Receive is trusted as proof of
// connectivity before HealthCheck falls back to an active subscription probe.
const consumeHealthTTL = 30 * time.Second

// HealthCheck backs the /readyz probe by confirming each configured request
// subscription is reachable. It prefers the passive signal from the consume loop
// (requestWorker): a recent successful Receive proves the broker round-tripped
// without any extra API call or IAM permission, and a recent Receive error marks
// the pod not-ready. When the consume loop has no fresh signal (a quiet queue, or
// before Start), it falls back to an active GetSubscription probe. An unreachable
// Pub/Sub backend surfaces as a gRPC Unavailable error and marks the pod
// not-ready, fixing the original bug where such pods reported ready.
func (r *PubSubMQFlow) HealthCheck(ctx context.Context) error {
	for _, cd := range r.requestChannels {
		if h := r.consumeHealth[cd.subscriberID]; h != nil {
			h.mu.Lock()
			lastOK, lastErr, lastErrAt := h.lastOK, h.lastErr, h.lastErrAt
			h.mu.Unlock()

			// A recent consume error is authoritative proof of trouble. A stale one
			// is not: the consume loop only refreshes lastOK when a message is
			// actually delivered, so on a quiet subscription an old error would
			// otherwise pin the pod not-ready forever even after the broker
			// recovers. Once the error ages past the TTL, fall through to the
			// active probe below, which re-confirms reachability and lets an idle
			// subscription recover readiness.
			if lastErr != nil && lastErrAt.After(lastOK) && time.Since(lastErrAt) < consumeHealthTTL {
				return fmt.Errorf("pubsub subscription %q consume loop unhealthy: %w", cd.subscriberID, lastErr)
			}
			// A recent success proves connectivity; skip the admin call.
			if !lastOK.IsZero() && time.Since(lastOK) < consumeHealthTTL {
				continue
			}
		}
		// No fresh consume signal: actively probe the subscription.
		if err := r.probeSubscription(ctx, cd.subscriberID); err != nil {
			return err
		}
	}
	return nil
}

// probeSubscription actively confirms a subscription is reachable via the admin
// API. A PermissionDenied response still proves the broker is reachable: the
// consume-only role (roles/pubsub.subscriber) lacks pubsub.subscriptions.get, so
// we can confirm connectivity but not introspect the subscription. Treat it as
// healthy rather than failing readiness for a correctly-configured consumer.
func (r *PubSubMQFlow) probeSubscription(ctx context.Context, subscriberID string) error {
	// Subscriber() normalizes both short IDs and full resource paths; reuse its
	// result as the fully-qualified name for the admin lookup.
	name := r.client.Subscriber(subscriberID).String()
	_, err := r.client.SubscriptionAdminClient.GetSubscription(ctx,
		&pubsubpb.GetSubscriptionRequest{Subscription: name})
	if err == nil || status.Code(err) == codes.PermissionDenied {
		return nil
	}
	return fmt.Errorf("pubsub subscription %q health check failed: %w", subscriberID, err)
}

// recordConsumeOK marks a subscription healthy after a successful broker
// round-trip observed by the consume loop.
func (r *PubSubMQFlow) recordConsumeOK(subscriberID string) {
	if h := r.consumeHealth[subscriberID]; h != nil {
		h.mu.Lock()
		h.lastOK = time.Now()
		h.lastErr = nil
		h.mu.Unlock()
	}
}

// recordConsumeErr records a consume-loop Receive failure so HealthCheck can
// surface it. Intentional shutdown/restart cancellations are filtered by the
// caller and must not be passed here.
func (r *PubSubMQFlow) recordConsumeErr(subscriberID string, err error) {
	if h := r.consumeHealth[subscriberID]; h != nil {
		h.mu.Lock()
		h.lastErr = err
		h.lastErrAt = time.Now()
		h.mu.Unlock()
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
			r.requestWorker(consumeCtx, pubSubClient, cd.subscriberID, cd.requestChannel.WorkerPoolID, cd.requestChannel.Channel, cd.gate, cd.labels)
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

func (r *PubSubMQFlow) requestWorker(ctx context.Context, pubSubClient *pubsub.Client, subscriberID, poolID string, ch chan *api.InternalRequest, gate pipeline.Gate, labels map[string]string) {
	logger := log.FromContext(ctx)

	sub := pubSubClient.Subscriber(subscriberID)

	for ctx.Err() == nil {
		receiveCtx, cancel := context.WithCancel(ctx)
		budget := gate.Budget(ctx)
		metrics.SetDispatchBudget(budget, "", subscriberID, poolID)
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

		currBatchSize := int(math.Floor(float64(r.batchSize) * budget))
		logger.V(logutil.DEFAULT).Info("PubSub MaxOutstandingMessages", "value", currBatchSize)
		sub.ReceiveSettings.MaxOutstandingMessages = currBatchSize
		sub.ReceiveSettings.NumGoroutines = 1
		if currBatchSize <= 0 {
			<-receiveCtx.Done()
			cancel()
			continue
		}

		err := r.processMessages(receiveCtx, sub.Receive, subscriberID, poolID, ch, gate, labels)

		cancel()
		// TODO
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Fail to receive messages from request subscription")
			// Record only genuine receive failures for readiness. A restart
			// triggered by a budget change or shutdown cancels receiveCtx and
			// surfaces as context.Canceled, which is not a connectivity problem.
			if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				r.recordConsumeErr(subscriberID, err)
			}
		}
	}

}

type receiveFunc func(context.Context, func(context.Context, *pubsub.Message)) error

func (r *PubSubMQFlow) processMessages(ctx context.Context, receive receiveFunc, subscriberID string, poolID string, ch chan *api.InternalRequest, gate pipeline.Gate, labels map[string]string) error {
	logger := log.FromContext(ctx)
	return receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// A delivered message is authoritative proof the broker round-tripped;
		// refresh the passive health signal read by HealthCheck.
		r.recordConsumeOK(subscriberID)

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
		if len(labels) > 0 {
			irout.Labels = make(map[string]string, len(labels))
			for k, v := range labels {
				irout.Labels[k] = v
			}
		}
		if body.Metadata == nil {
			body.Metadata = make(map[string]string)
		}
		for k, v := range msg.Attributes {
			body.Metadata[k] = v
		}

		ir := api.NewInternalRequest(irout, &body)
		var releases []pipeline.GateReleaseFunc
		defer func() {
			pipeline.ReleaseGateReleases(releases)
		}()

		resultsChannel := make(chan bool, 1)
		resultChannels.Store(msg.ID, resultsChannel)
		defer resultChannels.Delete(msg.ID)

		// Apply gate
		verdict, err := gate.Apply(ctx, ir, &releases)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Gating failed")
			metrics.RecordGateDecision(metrics.ReasonError, "", subscriberID, poolID)
			msg.Nack()
			return
		}

		if verdict.Action == pipeline.ActionRefuse {
			reason := metrics.ReasonGateClosed
			if ir.GetClassification() == api.ClassificationOverflow {
				reason = metrics.ReasonQuotaExhausted
			}
			metrics.RecordGateDecision(reason, "", subscriberID, poolID)
			logger.V(logutil.DEBUG).Info("Quota exceeded or capacity full, delaying Nack", "msgID", msg.ID, "delay", quotaExceededNackDelay)
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

		if verdict.Action == pipeline.ActionDrop {
			metrics.RecordGateDecision(metrics.ReasonDropped, "", subscriberID, poolID)
			var resultMsg api.ResultMessage
			if verdict.Result != nil {
				resultMsg = *verdict.Result
				resultMsg.Routing = ir.InternalRouting
			} else {
				resultMsg = api.NewGateDroppedResult(&body, ir.InternalRouting)
			}
			r.resultChannel <- resultMsg

			// Wait for the result worker to finish publishing and signal true
			result := <-resultsChannel
			if !result {
				msg.Nack()
			} else {
				msg.Ack()
			}
			return
		}

		// Stamp ingestion time as the message enters the in-process buffer so the
		// worker can record queue residence time when it pulls the message.
		ir.IngestionTime = time.Now()

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
