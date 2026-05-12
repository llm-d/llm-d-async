package pubsub

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/llm-d-incubation/llm-d-async/pkg/metrics"
	"github.com/llm-d-incubation/llm-d-async/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const PUBSUB_ID = "pubsub-id"

const (
	processorClusterAttr = "processor_cluster"
	processorRegionAttr  = "processor_region"
	processorPodAttr     = "processor_pod"
)

const (
	processorClusterEnv = "ASYNC_PROCESSOR_CLUSTER"
	processorRegionEnv  = "ASYNC_PROCESSOR_REGION"
	processorPodEnv     = "POD_NAME"
)

var pubSubClient *pubsub.Client

type ackAction struct {
	ack       bool
	nackDelay time.Duration
}

const (
	progressLogInterval  = time.Minute
	retryReasonRateLimit = "rate_limit_429"
)

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
	IGWBaseURL         string            `json:"igw_base_url"`
	HTTPHeaders        map[string]string `json:"http_headers,omitempty"`
	ModelNameOverride  string            `json:"model_name_override,omitempty"`
	GateType           string            `json:"gate_type"`
	GateParams         map[string]string `json:"gate_params,omitempty"`
	// Labels is the subscription's static label set. Merged at startup
	// onto the channel's effective label set (with the pool's labels +
	// auto-injected pool ID as lower-precedence layers); flows onto
	// every message from this subscription as msg.Labels. Producer-
	// controlled per-message correlation data round-trips through the
	// existing body.Metadata / result.Metadata path — Labels is for
	// operator-pinned classification, not transport attributes.
	Labels map[string]string `json:"labels,omitempty"`
	// Gates is the chain of subscription gates evaluated in the Flow
	// callback at pull time, before the message is forwarded to the
	// merge policy. Gates here may mutate labels (e.g. a reservation
	// classifier stamping `class=reserved` or `class=overflow`) and
	// the merge policy will see the mutations when it buckets the
	// message. Must be fast — slow gates here block the receive
	// callback and stall prefetch. Stateful gates whose releases need
	// to survive past dispatch are not currently supported as
	// subscription gates; put those on the pool's Gates instead, where
	// they are evaluated downstream of the merge policy with full
	// release plumbing.
	Gates []pipeline.GateConfig `json:"gates,omitempty"`
	// Pool identifies which inference pool (declared in the FlowConfig's
	// Pools list) this subscription routes to. If empty in the new config
	// format, the Flow synthesizes a singleton pool from the legacy
	// per-subscription fields (IGWBaseURL, RequestPathURL, HTTPHeaders,
	// ModelNameOverride, GateType, GateParams) so existing deployments
	// keep working unmodified.
	Pool string `json:"pool,omitempty"`
}

// FlowConfig is the new file format: an object with named pools and a
// list of subscriptions referencing pools by ID. Backwards-compatible:
// a topics config file containing a raw JSON array of TopicConfig is
// still accepted; each entry is treated as a one-pool subscription
// pair with the pool synthesized from the topic's legacy fields.
type FlowConfig struct {
	Pools         []pipeline.Pool `json:"pools,omitempty"`
	Subscriptions []TopicConfig   `json:"subscriptions,omitempty"`
}

var _ pipeline.Flow = (*PubSubMQFlow)(nil)

type PubSubMQFlow struct {
	resultTopicID   string
	requestChannels []RequestChannelData
	pools           []pipeline.Pool
	retryChannel    chan pipeline.RetryMessage
	resultChannel   chan api.ResultMessage
	gateFactory     pipeline.GateFactory
}

type progressStats struct {
	subscriberID string
	pulled       atomic.Int64
	dispatched   atomic.Int64
	succeeded    atomic.Int64
	failed       atomic.Int64
	retrying     atomic.Int64
	rateLimited  atomic.Int64
	acked        atomic.Int64
	delayedNacks atomic.Int64
	gateDenied   atomic.Int64
	inFlight     atomic.Int64

	// dispatchInFlight tracks how many requests currently sit between a
	// successful pool-gate admission and the matching Release — i.e.
	// the gate-level in-flight count. Useful for verifying that a
	// concurrency-cap gate (local-max-concurrency) is actually biting:
	// dispatchInFlightPeak should saturate at max_concurrency.
	dispatchInFlight     atomic.Int64
	dispatchInFlightPeak atomic.Int64
}

type countingGate struct {
	// gates is the pool gate chain for the pool this subscription
	// routes to. countingGate is a stats-instrumentation wrapper that
	// runs the chain via pipeline.ApplyChain and tracks per-subscription
	// dispatch in-flight and gate-denied counters. Multiple
	// subscriptions in the same pool share an underlying chain via
	// reference equality of the gates slice.
	gates []pipeline.Gate
	stats *progressStats
}

type resultTracker struct {
	resultCh chan ackAction
	stats    *progressStats
}

// Apply implements pipeline.Gate by running the pool gate chain via
// pipeline.ApplyChain and instrumenting per-subscription stats around
// it. Continue increments dispatchInFlight (with peak tracking) and
// attaches a release that decrements on cleanup. Refuse increments
// gateDenied. Drop is propagated as-is.
func (g *countingGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	v, err := pipeline.ApplyChain(ctx, msg, g.gates)
	if err != nil {
		return v, err
	}
	if !v.Terminate {
		current := g.stats.dispatchInFlight.Add(1)
		updatePeak(&g.stats.dispatchInFlightPeak, current)
		msg.AttachRelease(func() { g.stats.dispatchInFlight.Add(-1) })
		return v, nil
	}
	if v.Redeliver {
		g.stats.gateDenied.Add(1)
	}
	return v, nil
}

// updatePeak is a CAS-loop helper for tracking the maximum value seen
// by an atomic.Int64 over time.
func updatePeak(peak *atomic.Int64, current int64) {
	for {
		p := peak.Load()
		if current <= p {
			return
		}
		if peak.CompareAndSwap(p, current) {
			return
		}
	}
}

func (s *progressStats) logProgress(ctx context.Context) {
	logger := log.FromContext(ctx)
	ticker := time.NewTicker(progressLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentDispatch := s.dispatchInFlight.Load()
			dispatchPeak := s.dispatchInFlightPeak.Swap(currentDispatch)
			logger.V(logutil.DEFAULT).Info("Async processor progress",
				"subscriberID", s.subscriberID,
				"pulled", s.pulled.Swap(0),
				"dispatched", s.dispatched.Swap(0),
				"succeeded", s.succeeded.Swap(0),
				"failed", s.failed.Swap(0),
				"retrying", s.retrying.Swap(0),
				"rateLimited429", s.rateLimited.Swap(0),
				"acked", s.acked.Swap(0),
				"delayedNacks", s.delayedNacks.Swap(0),
				"gateDenied", s.gateDenied.Swap(0),
				"inFlight", s.inFlight.Load(),
				"dispatchInFlight", currentDispatch,
				"dispatchInFlightPeak", dispatchPeak)
		}
	}
}

type RequestChannelData struct {
	requestChannel pipeline.RequestChannel
	subscriberID   string
	gate           pipeline.Gate
	// subscriptionGates is the chain run in the receive callback, after
	// the message is unmarshaled and labels are stamped, before
	// forwarding to the merge policy. Sourced from TopicConfig.Gates.
	// Gates here may mutate labels (the merge policy sees the mutations
	// when bucketing) but must be fast — slow gates block the receive
	// callback and stall prefetch.
	subscriptionGates []pipeline.Gate
	stats             *progressStats
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

func NewGCPPubSubMQFlow(opts ...PubSubOption) *PubSubMQFlow {

	ctx := context.Background()
	var err error
	pubSubClient, err = pubsub.NewClient(ctx, *projectID)
	if err != nil {
		// TODO:
		panic(err)
	}
	configs, pools := loadTopicsConfig()
	p := &PubSubMQFlow{
		resultTopicID:   *resultTopicID,
		requestChannels: make([]RequestChannelData, 0, len(configs)),
		retryChannel:    make(chan pipeline.RetryMessage),
		resultChannel:   make(chan api.ResultMessage, 1000),
	}

	// Apply functional options
	for _, opt := range opts {
		opt(p)
	}

	// Materialize the resolved Pool list (deduped by ID) so the Flow
	// can expose it via Pools(). Iterates configs to preserve the
	// declaration order of subscriptions (good for deterministic
	// startup logs); each Pool is added once on first sight.
	seen := map[string]bool{}
	for _, cfg := range configs {
		poolID := cfg.Pool
		if poolID == "" {
			poolID = cfg.SubscriberID
		}
		if seen[poolID] {
			continue
		}
		seen[poolID] = true
		if pool, ok := pools[poolID]; ok {
			p.pools = append(p.pools, pool)
		}
	}

	// Construct one pool gate chain per pool. Sharing the chain across all
	// subscriptions in a pool is the whole point of pool-scoped
	// admission control: a LocalMaxConcurrencyGate with cap=N caps
	// in-flight to N for the pool as a whole, not N per subscription.
	// Per-pool stats track gate-level in-flight; per-subscription stats
	// (defined later in the construction loop) continue to track
	// transport-level metrics (pulled, dispatched, etc.).
	// Each pool gets one chain of pool gates, shared across all
	// subscriptions routing to it. The worker walks the chain via
	// pipeline.ApplyChain at dispatch time. countingGate wraps the
	// chain output for per-sub stats accounting (still applied per
	// subscription below).
	gatesByPoolID := map[string][]pipeline.Gate{}
	for _, pool := range p.pools {
		gates, err := buildPoolGateChain(p.gateFactory, pool)
		if err != nil {
			panic(fmt.Sprintf("failed to build pool gates for %q: %v", pool.ID, err))
		}
		gatesByPoolID[pool.ID] = gates
	}

	// Create per-topic channels with gates
	for _, cfg := range configs {
		headers, err := util.ExpandEnvMapValues(cfg.HTTPHeaders)
		if err != nil {
			panic(fmt.Sprintf("failed to expand http_headers for topic subscriber %q: %v", cfg.SubscriberID, err))
		}

		logger := log.FromContext(ctx)

		stats := &progressStats{subscriberID: cfg.SubscriberID}

		// Resolve the subscription's pool. Pool fields (gateway URL,
		// request path, headers, model-name override, pool gate chain)
		// take precedence over the subscription's legacy per-topic
		// fields when both are present; the synthesizer covers legacy
		// configs by copying those legacy fields onto a singleton
		// pool, so the result is uniform downstream.
		poolID := cfg.Pool
		if poolID == "" {
			poolID = cfg.SubscriberID
		}
		pool, ok := pools[poolID]
		if !ok {
			panic(fmt.Sprintf("subscription %q references unknown pool %q", cfg.SubscriberID, poolID))
		}

		// Use the per-pool gate chain (already built before this loop).
		// All subscriptions in this pool share the same underlying chain
		// (and hence the same LocalMaxConcurrencyGate semaphores, etc.),
		// which is the whole point of pool-scoped admission control.
		// The countingGate wrapper is still per-subscription so per-sub
		// stats reflect each subscription's contribution.
		var gate pipeline.Gate = &countingGate{gates: gatesByPoolID[pool.ID], stats: stats}

		// HTTP dispatch fields come from the pool when set; fall back
		// to the legacy subscription fields when the pool doesn't
		// override (handles the synthesized-pool case naturally too).
		gwURL := pool.GatewayURL
		if gwURL == "" {
			gwURL = cfg.IGWBaseURL
		}
		reqPath := pool.RequestPath
		if reqPath == "" {
			reqPath = cfg.RequestPathURL
		}
		poolHeaders := pool.HTTPHeaders
		if poolHeaders == nil {
			poolHeaders = headers
		} else {
			expanded, err := util.ExpandEnvMapValues(poolHeaders)
			if err != nil {
				panic(fmt.Sprintf("failed to expand http_headers for pool %q: %v", pool.ID, err))
			}
			poolHeaders = expanded
		}
		modelOverride := pool.ModelNameOverride
		if modelOverride == "" {
			modelOverride = cfg.ModelNameOverride
		}

		normalizedRequestPath := util.NormalizeURLPath(reqPath)
		normalizedBaseURL := util.NormalizeBaseURL(gwURL)
		logger.V(logutil.DEFAULT).Info("Configured PubSub queue",
			"subscriberID", cfg.SubscriberID,
			"poolID", pool.ID,
			"igwBaseURL", normalizedBaseURL,
			"requestPathURL", normalizedRequestPath,
			"modelNameOverride", modelOverride,
			"poolGates", poolGateTypesOf(pool),
			"subscriptionGates", subscriptionGateTypesOf(cfg.Gates))

		// Build the effective static label set for this subscription.
		// Precedence (lowest to highest): auto-injected pool ID,
		// pool's declared labels, subscription's declared labels.
		// Higher layers override lower on key collision. Operators
		// can manually set a "pool" key in subscription/pool labels
		// to override the auto-injection (rarely useful — flagged
		// for the few cases where the pool label needs a value
		// distinct from the Pool.ID).
		channelLabels := pipeline.Labels{"pool": pool.ID}
		for k, v := range pool.Labels {
			channelLabels[k] = v
		}
		for k, v := range cfg.Labels {
			channelLabels[k] = v
		}

		subscriptionGates, err := buildSubscriptionGateChain(p.gateFactory, cfg.SubscriberID, cfg.Gates)
		if err != nil {
			panic(fmt.Sprintf("failed to build subscription gates for %q: %v", cfg.SubscriberID, err))
		}

		ch := make(chan *pipeline.EmbelishedRequestMessage)
		p.requestChannels = append(p.requestChannels, RequestChannelData{
			requestChannel: pipeline.RequestChannel{
				Channel:            ch,
				IGWBaseURL:         normalizedBaseURL,
				InferenceObjective: cfg.InferenceObjective,
				RequestPathURL:     normalizedRequestPath,
				HTTPHeaders:        poolHeaders,
				ModelNameOverride:  modelOverride,
				Gate:               gate,
				Labels:             channelLabels,
				PoolID:             pool.ID,
			},
			subscriberID:      cfg.SubscriberID,
			gate:              gate,
			subscriptionGates: subscriptionGates,
			stats:             stats,
		})
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

func (r *PubSubMQFlow) Pools() []pipeline.Pool {
	out := make([]pipeline.Pool, len(r.pools))
	copy(out, r.pools)
	return out
}

func (r *PubSubMQFlow) RequestChannels() []pipeline.RequestChannel {

	var channels []pipeline.RequestChannel
	for _, channelData := range r.requestChannels {
		channels = append(channels, channelData.requestChannel)
	}
	return channels
}

func (r *PubSubMQFlow) Start(ctx context.Context) {
	for _, channelData := range r.requestChannels {
		go channelData.stats.logProgress(ctx)
		go r.requestWorker(ctx, pubSubClient, channelData.subscriberID, channelData.requestChannel, channelData.stats, channelData.subscriptionGates)
	}
	publisher := pubSubClient.Publisher(r.resultTopicID)
	for i := 0; i < 4; i++ {
		go resultWorker(ctx, publisher, r.resultChannel)
	}

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
				fallback := map[string]string{"id": msg.ID, "error": "Failed to marshal result to string"}
				msgBytes, _ = json.Marshal(fallback)
			} else {
				msgBytes = bytes
			}
			// Result attributes come from the producer-stamped metadata
			// round-trip plus the per-pod processor identity
			// (cluster/region/pod env). The Labels namespace is
			// operator-controlled and intentionally does not bleed into
			// result attrs — consumer filtering relies on the metadata
			// round-trip path.
			attrs := resultAttributes(msg.Metadata)
			publishPubSub(ctx, publisher, msgBytes, attrs)
			pubsubID := msg.Routing.TransportCorrelationID
			tracker, ok := loadResultTracker(pubsubID)
			if !ok {
				logger.V(logutil.DEFAULT).Error(nil, "Result channel not found for message", "pubsubID", pubsubID)
				continue
			}
			if resultHasError(msg.Payload) {
				if tracker.stats != nil {
					tracker.stats.failed.Add(1)
				}
			} else {
				if tracker.stats != nil {
					tracker.stats.succeeded.Add(1)
				}
			}
			tracker.resultCh <- ackAction{ack: true}

		}
	}
}

func resultAttributes(metadata map[string]string) map[string]string {
	attrs := make(map[string]string)
	for k, v := range metadata {
		if k != PUBSUB_ID {
			attrs[k] = v
		}
	}
	for k, v := range processorAttributes() {
		attrs[k] = v
	}
	return attrs
}

func processorAttributes() map[string]string {
	attrs := make(map[string]string)
	if cluster := os.Getenv(processorClusterEnv); cluster != "" {
		attrs[processorClusterAttr] = cluster
	}
	if region := os.Getenv(processorRegionEnv); region != "" {
		attrs[processorRegionAttr] = region
	}
	if pod := os.Getenv(processorPodEnv); pod != "" {
		attrs[processorPodAttr] = pod
	}
	return attrs
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

	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-retryChannel:
			if msg.InternalRequest == nil {
				continue
			}
			pubsubID := msg.InternalRouting.TransportCorrelationID
			tracker, ok := loadResultTracker(pubsubID)
			if !ok {
				logger.V(logutil.DEFAULT).Error(nil, "Result channel not found for retry message", "pubsubID", pubsubID)
				continue
			}
			nackDelay := time.Duration(msg.BackoffDurationSeconds * float64(time.Second))
			if tracker.stats != nil {
				tracker.stats.retrying.Add(1)
				if msg.RetryReason == retryReasonRateLimit {
					tracker.stats.rateLimited.Add(1)
				}
			}
			tracker.resultCh <- ackAction{ack: false, nackDelay: nackDelay}

		}
	}

}

func (r *PubSubMQFlow) requestWorker(ctx context.Context, pubSubClient *pubsub.Client, subscriberID string, requestChannel pipeline.RequestChannel, stats *progressStats, subscriptionGates []pipeline.Gate) {
	ch := requestChannel.Channel
	channelLabels := requestChannel.Labels
	logger := log.FromContext(ctx)

	sub := pubSubClient.Subscriber(subscriberID)

	// Prefetch settings: the callback returns immediately (non-blocking),
	// so MaxOutstandingMessages controls the prefetch buffer depth, not
	// concurrency. Set high so the stream continuously pulls while
	// workers process the current batch.
	//
	// NumGoroutines MUST be 1. The Go Pub/Sub client has a known bug
	// where NumGoroutines > 1 causes flow control issues:
	// https://github.com/googleapis/google-cloud-go/wiki/Fine-Tuning-PubSub-Receive-Performance
	//
	// MaxOutstandingBytes = -1 removes the bytes-based limit (default
	// 1GB) so only message count gates prefetch.
	prefetchDepth := *batchSize * 5
	if prefetchDepth < 1000 {
		prefetchDepth = 1000
	}
	sub.ReceiveSettings.MaxOutstandingMessages = prefetchDepth
	sub.ReceiveSettings.MaxOutstandingBytes = -1
	sub.ReceiveSettings.NumGoroutines = 1
	sub.ReceiveSettings.MaxExtension = 10 * time.Minute
	logger.V(logutil.DEFAULT).Info("PubSub prefetch configured",
		"subscriberID", subscriberID,
		"MaxOutstandingMessages", prefetchDepth,
		"NumGoroutines", 1,
		"MaxExtension", "10m")

	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {

		deliveryAttempt := msg.DeliveryAttempt

		var body api.RequestMessage
		err := json.Unmarshal(msg.Data, &body)
		if err != nil {
			logger.V(logutil.DEFAULT).Error(err, "Failed to unmarshal message from request queue")
			msg.Ack()
			return
		}
		stats.pulled.Add(1)

		if body.Metadata == nil {
			body.Metadata = make(map[string]string)
		}
		resultsChannel := make(chan ackAction, 1)
		resultChannels.Store(msg.ID, resultTracker{
			resultCh: resultsChannel,
			stats:    stats,
		})
		stats.inFlight.Add(1)

		for k, v := range msg.Attributes {
			if k == PUBSUB_ID {
				continue
			}
			body.Metadata[k] = v
		}

		irout := api.InternalRouting{TransportCorrelationID: msg.ID}
		if deliveryAttempt != nil {
			irout.RetryCount = *deliveryAttempt - 1
		}
		// channelLabels is the operator-defined static label set (pool ID
		// + pool.Labels + subscription.Labels), already merged at
		// startup. Producers cannot influence Labels — per-message
		// transport attributes round-trip via body.Metadata /
		// result.Metadata instead, not via the Labels namespace.
		irout.Labels = channelLabels.Clone()
		ir := api.NewInternalRequest(irout, &body)

		// Build the full EmbelishedRequestMessage now so subscription-gate
		// releases ride with the message all the way to the worker. With
		// the channel type carrying *EmbelishedRequestMessage, we build
		// once here and send directly.
		emb := buildEmbelishedFromChannel(ir, requestChannel)

		// Subscription gates: run the chain on emb. Labels are aliased
		// from irout.Labels so gate mutations are visible in the routing.
		// Must be fast — slow gates block the receive callback and stall
		// prefetch. On Continue, releases stay attached to emb and fire
		// at worker terminal via defer msg.FireReleases().
		//
		// Pool gates are NOT applied here — they run in the per-pool
		// worker pool downstream of the merge policy, via emb.Gate.
		// Running them here would stall prefetch on saturated pools,
		// which is precisely what this design avoids.
		if len(subscriptionGates) > 0 {
			v, err := pipeline.ApplyChain(ctx, emb, subscriptionGates)
			if err != nil {
				logger.V(logutil.DEFAULT).Error(err, "subscription gate chain error; treating as Refuse", "subscriberID", subscriberID)
				emb.FireReleases()
				resultsChannel <- ackAction{ack: false, nackDelay: 30 * time.Second}
				return
			}
			if v.Terminate {
				stats.gateDenied.Add(1)
				emb.FireReleases()
				if v.Result != nil {
					select {
					case r.resultChannel <- *v.Result:
					case <-ctx.Done():
					}
				}
				if v.Redeliver {
					resultsChannel <- ackAction{ack: false, nackDelay: 30 * time.Second}
				} else {
					resultsChannel <- ackAction{ack: true}
				}
				return
			}
		}

		ch <- emb
		stats.dispatched.Add(1)

		// Return from the callback immediately so sub.Receive can
		// pull more messages. The library keeps extending the ack
		// deadline automatically until we call Ack/Nack. Handle
		// the ack lifecycle in a background goroutine.
		go func() {
			defer func() {
				resultChannels.Delete(msg.ID)
				stats.inFlight.Add(-1)
			}()
			var result ackAction
			select {
			case result = <-resultsChannel:
			case <-ctx.Done():
				// Worker pool exited before this message reached a
				// terminal state. Nack so Pub/Sub redelivers it
				// rather than waiting for the ack deadline to expire.
				msg.Nack()
				return
			}
			if result.ack {
				metrics.MessageLatencyTime.Observe(float64(time.Since(msg.PublishTime).Milliseconds()))
				msg.Ack()
				stats.acked.Add(1)
				return
			}

			if result.nackDelay > 0 {
				timer := time.NewTimer(result.nackDelay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if !result.ack {
				msg.Nack()
				stats.delayedNacks.Add(1)
			}
		}()
	})
	if err != nil {
		logger.V(logutil.DEFAULT).Error(err, "Fail to receive messages from request subscription")
	}
}

func loadResultTracker(pubsubID string) (resultTracker, bool) {
	value, ok := resultChannels.Load(pubsubID)
	if !ok {
		return resultTracker{}, false
	}
	tracker, ok := value.(resultTracker)
	if !ok {
		return resultTracker{}, false
	}
	return tracker, true
}

func resultHasError(payload string) bool {
	var result map[string]any
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		return false
	}
	_, ok := result["error"]
	return ok
}

// loadTopicsConfig reads the topics-config file and returns the
// subscription list plus a map of pool ID -> Pool. Both formats are
// supported:
//
//  1. Legacy: JSON array of TopicConfig. Each subscription is wrapped
//     in a synthesized singleton pool named after its subscriber_id
//     that inherits the subscription's legacy HTTP/gate fields.
//  2. New:    JSON object { "pools": [...], "subscriptions": [...] }.
//     Pools are declared explicitly; subscriptions reference them by
//     ID. Subscriptions that do not declare a Pool get a synthesized
//     one (same as legacy mode).
//
// When no config file is provided the flag-based legacy single-topic
// fallback is used.
func loadTopicsConfig() ([]TopicConfig, map[string]pipeline.Pool) {
	if *topicsConfigFile == "" {
		cfg := TopicConfig{
			SubscriberID:       *requestSubscriberID,
			IGWBaseURL:         *igwBaseURL,
			InferenceObjective: *inferenceObjective,
			RequestPathURL:     *requestPathURL,
		}
		return []TopicConfig{cfg}, synthesizePoolsForLegacy([]TopicConfig{cfg})
	}
	data, err := os.ReadFile(*topicsConfigFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read topics config file: %v", err))
	}
	trimmed := bytesTrimLeftSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		// Legacy array-of-TopicConfig format.
		var subs []TopicConfig
		if err := json.Unmarshal(data, &subs); err != nil {
			panic(fmt.Sprintf("failed to unmarshal topics config (legacy array): %v", err))
		}
		return subs, synthesizePoolsForLegacy(subs)
	}
	// New object format with explicit pools.
	var fc FlowConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		panic(fmt.Sprintf("failed to unmarshal topics config (new object): %v", err))
	}
	pools := map[string]pipeline.Pool{}
	for _, p := range fc.Pools {
		if p.ID == "" {
			panic("pool in flow config has empty id")
		}
		if _, exists := pools[p.ID]; exists {
			panic(fmt.Sprintf("duplicate pool id %q in flow config", p.ID))
		}
		pools[p.ID] = p
	}
	// Subscriptions that don't reference a declared pool get a
	// synthesized singleton pool. This handles half-migrations
	// gracefully.
	for _, s := range fc.Subscriptions {
		if s.Pool == "" {
			pools[s.SubscriberID] = synthesizePool(s)
		} else if _, ok := pools[s.Pool]; !ok {
			panic(fmt.Sprintf("subscription %q references unknown pool %q", s.SubscriberID, s.Pool))
		}
	}
	return fc.Subscriptions, pools
}

// synthesizePoolsForLegacy creates one synthesized pool per subscription
// for the legacy config format, where pool-level config lived alongside
// subscription-level config on each TopicConfig.
func synthesizePoolsForLegacy(subs []TopicConfig) map[string]pipeline.Pool {
	pools := make(map[string]pipeline.Pool, len(subs))
	for _, s := range subs {
		pools[s.SubscriberID] = synthesizePool(s)
	}
	return pools
}

// synthesizePool builds a Pool from a TopicConfig's legacy fields,
// named after the subscription. The pool's Gates list contains the
// subscription's legacy gate as its sole entry (the legacy
// per-subscription gate is morally a pool gate by the new semantics,
// since it ran at the worker site, so it slots into the pool's Gates
// chain naturally).
func synthesizePool(s TopicConfig) pipeline.Pool {
	pool := pipeline.Pool{
		ID:                s.SubscriberID,
		GatewayURL:        s.IGWBaseURL,
		RequestPath:       s.RequestPathURL,
		HTTPHeaders:       s.HTTPHeaders,
		ModelNameOverride: s.ModelNameOverride,
	}
	if s.GateType != "" {
		pool.Gates = []pipeline.GateConfig{{
			Type:   s.GateType,
			Params: s.GateParams,
		}}
	}
	return pool
}

// bytesTrimLeftSpace returns b with leading ASCII whitespace removed.
// Tiny helper to peek at the first non-whitespace byte without pulling
// in the unicode/strings packages for one use site.
func bytesTrimLeftSpace(b []byte) []byte {
	for i, c := range b {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			return b[i:]
		}
	}
	return nil
}

// poolGateTypesOf returns a comma-separated list of the pool's gate
// types for logging, or "" if the pool has no gates configured.
func poolGateTypesOf(p pipeline.Pool) string {
	return gateTypesOf(p.Gates)
}

// subscriptionGateTypesOf returns a comma-separated list of subscription gate
// types for logging, or "" if the subscription has no gates configured.
func subscriptionGateTypesOf(gs []pipeline.GateConfig) string {
	return gateTypesOf(gs)
}

func gateTypesOf(gs []pipeline.GateConfig) string {
	if len(gs) == 0 {
		return ""
	}
	out := ""
	for i, g := range gs {
		if i > 0 {
			out += ","
		}
		out += g.Type
	}
	return out
}

// buildEmbelishedFromChannel creates a fully-populated EmbelishedRequestMessage
// from the pulled InternalRequest and the subscription's RequestChannel metadata.
// All HTTP dispatch fields are set here so the message is ready for the merge
// policy and worker without any re-wrapping step. Labels are aliased from
// ir.Labels (which the callback seeds from channelLabels), so subscription-gate
// label mutations are reflected in the routing without copying.
func buildEmbelishedFromChannel(ir *api.InternalRequest, ch pipeline.RequestChannel) *pipeline.EmbelishedRequestMessage {
	requestURL := ch.IGWBaseURL + ch.RequestPathURL
	if ir.PublicRequest != nil {
		if ep := ir.PublicRequest.ReqEndpoint(); ep != "" {
			requestURL = ch.IGWBaseURL + ep
		}
	}
	headers := map[string]string{"Content-Type": "application/json"}
	if ch.InferenceObjective != "" {
		headers["x-gateway-inference-objective"] = ch.InferenceObjective
	}
	for k, v := range ch.HTTPHeaders {
		headers[k] = v
	}
	return &pipeline.EmbelishedRequestMessage{
		InternalRequest:   ir,
		HttpHeaders:       headers,
		RequestURL:        requestURL,
		Gate:              ch.Gate,
		ModelNameOverride: ch.ModelNameOverride,
		Labels:            pipeline.Labels(ir.Labels),
		PoolID:            ch.PoolID,
	}
}

// buildPoolGateChain constructs the chain of pool gates for a single
// pool. Each entry in pool.Gates becomes one pipeline.Gate built via
// the factory; the worker walks the slice via pipeline.ApplyChain.
// Returns an empty slice (which ApplyChain treats as always-Continue)
// when the pool has no gates configured.
func buildPoolGateChain(factory pipeline.GateFactory, pool pipeline.Pool) ([]pipeline.Gate, error) {
	if len(pool.Gates) == 0 {
		return nil, nil
	}
	if factory == nil {
		return nil, fmt.Errorf("pool %q declares %d gate(s) but no GateFactory was wired into the Flow", pool.ID, len(pool.Gates))
	}
	out := make([]pipeline.Gate, 0, len(pool.Gates))
	for i, cfg := range pool.Gates {
		g, err := factory.CreateGate(cfg.Type, cfg.Params)
		if err != nil {
			return nil, fmt.Errorf("pool %q gate[%d] (type=%q): %w", pool.ID, i, cfg.Type, err)
		}
		out = append(out, g)
	}
	return out, nil
}

// buildSubscriptionGateChain constructs the chain of subscription gates
// for a subscription. Mirrors buildPoolGateChain but reads from
// TopicConfig.Gates.
func buildSubscriptionGateChain(factory pipeline.GateFactory, subID string, gateCfgs []pipeline.GateConfig) ([]pipeline.Gate, error) {
	if len(gateCfgs) == 0 {
		return nil, nil
	}
	if factory == nil {
		return nil, fmt.Errorf("subscription %q declares %d gate(s) but no GateFactory was wired into the Flow", subID, len(gateCfgs))
	}
	out := make([]pipeline.Gate, 0, len(gateCfgs))
	for i, cfg := range gateCfgs {
		g, err := factory.CreateGate(cfg.Type, cfg.Params)
		if err != nil {
			return nil, fmt.Errorf("subscription %q gate[%d] (type=%q): %w", subID, i, cfg.Type, err)
		}
		out = append(out, g)
	}
	return out, nil
}
