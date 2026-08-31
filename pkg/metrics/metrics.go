// Package metrics provides metrics registration for the async processor.
package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	controllerruntime "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// SchedulerSubsystem is the metric prefix of the package.
	SchedulerSubsystem = "llm_d_async"

	LabelQueueID   = "queue_id"
	LabelQueueName = "queue_name"
	LabelPoolName  = "pool_name"
	LabelReason    = "reason"
	LabelDirection = "direction"

	// LabelInferencePool names the InferencePool a gate queries. It is distinct
	// from pool_name, which always names the async worker pool that owns the
	// series — several worker pools may gate on one InferencePool, and one
	// worker pool may serve several.
	LabelInferencePool = "inference_pool"
)

var queueLabels = []string{LabelQueueID, LabelQueueName, LabelPoolName}

// gateLabels is queueLabels plus the queried InferencePool. Gate gauges carry
// the full queue triple so they join with the queue's other series; a
// pool-level gate leaves queue_id and queue_name empty.
var gateLabels = []string{LabelQueueID, LabelQueueName, LabelPoolName, LabelInferencePool}

var (
	Retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_request_retries_total",
		Help: "Total number of async request retries.",
	}, queueLabels)
	AsyncReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_request_total",
		Help: "Total number of async requests.",
	}, queueLabels)
	DispatchedReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_dispatched_requests_total",
		Help: "Total number of downstream inference dispatch attempts, including retries. Apply rate() to this counter to observe the batch admission rate.",
	}, queueLabels)
	ExceededDeadlineReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_exceeded_deadline_requests_total",
		Help: "Total number of async requests that exceeded their deadline.",
	}, queueLabels)
	FailedReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_failed_requests_total",
		Help: "Total number of async requests that failed.",
	}, queueLabels)
	SuccessfulReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_successful_requests_total",
		Help: "Total number of async requests that succeeded.",
	}, queueLabels)
	SheddedRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_shedded_requests_total",
		Help: "Total number of async requests that were shedded.",
	}, queueLabels)
	Tokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_tokens_total",
		Help: "Total number of tokens processed by successfully-dispatched requests, parsed best-effort from the OpenAI usage object in 2xx response bodies (direction=input maps prompt_tokens, direction=output maps completion_tokens). No-op when usage is absent or the body is not parseable (e.g. streaming responses); non-OpenAI gateways undercount by design.",
	}, []string{LabelQueueID, LabelQueueName, LabelPoolName, LabelDirection})
	MessageLatencyTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: SchedulerSubsystem, Name: "async_message_latency_time_millis",
		Help:    "Time from message publish to message being successfully processed.",
		Buckets: []float64{100, 1000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000},
	}, queueLabels)
	InferenceLatencyTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: SchedulerSubsystem, Name: "async_inference_latency_time_millis",
		Help:    "Time spent calling the inference gateway (IGW), separating model time from queue time.",
		Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000},
	}, queueLabels)
	QueueResidenceTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: SchedulerSubsystem, Name: "async_queue_residence_time_millis",
		Help: "Time a message spent buffered in-process from broker ingestion until a worker pulled it (the async delay introduced by the system).",
		// Residence time can range from sub-second up to a full day under
		// sustained backlog, so buckets span 500ms (smallest) to 24h, all in ms.
		Buckets: []float64{
			500,      // 500ms
			1000,     // 1s
			2000,     // 2s
			5000,     // 5s
			10000,    // 10s
			30000,    // 30s
			60000,    // 1m
			120000,   // 2m
			300000,   // 5m
			600000,   // 10m
			1800000,  // 30m
			3600000,  // 1h
			7200000,  // 2h
			21600000, // 6h
			43200000, // 12h
			86400000, // 24h
		},
	}, queueLabels)
	QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_queue_depth",
		Help: "Number of requests received from the broker and buffered in-process awaiting an available worker.",
	}, queueLabels)
	InflightRequests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_inflight_requests",
		Help: "Number of requests currently being processed by workers (dispatched to inference, awaiting a response).",
	}, queueLabels)
	BrokerBacklog = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_broker_backlog",
		Help: "Number of undelivered/pending messages held by the broker queue.",
	}, queueLabels)
	BrokerBacklogSourceAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_broker_backlog_source_available",
		Help: "1 when the most recent broker-backlog read for the queue succeeded; 0 when the source was unavailable or returned an error.",
	}, queueLabels)
	DispatchBudget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_dispatch_budget",
		Help: "Current dispatch budget [0.0-1.0] returned by the queue's gate; the fraction of system capacity available for new requests (0.0 = gate fully closed).",
	}, queueLabels)
	PoolWorkerLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_pool_worker_limit",
		Help: "Configured number of concurrent workers (the concurrency limit) for a pool. Compare against async_inflight_requests for worker utilization.",
	}, []string{LabelPoolName})
	QueueConfigReloads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_queue_config_reloads_total",
		Help: "Count of queue-config hot reload attempts, by result (success or error). Errors leave the last good configuration in effect.",
	}, []string{"result"})
	QueueConfigLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_queue_config_last_success_timestamp_seconds",
		Help: "Unix timestamp of the last successfully applied queue-config hot reload.",
	})
	DrainLimitRPS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_drain_limit_rps",
		Help: "Leased maximum downstream admission rate observed by the most recent gate evaluation for a worker pool. Zero with observed lease_valid=1 means an explicit pause.",
	}, []string{LabelPoolName})
	DrainLimitLeaseValid = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_drain_limit_lease_valid",
		Help: "1 when the most recent gate evaluation observed a syntactically valid, unexpired external drain-limit lease; 0 when that evaluation found it missing, malformed, mismatched, expired, or unreadable. This observation does not self-expire while the gate is idle; combine it with valid_until_seconds > time().",
	}, []string{LabelPoolName})
	DrainLimitValidUntil = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_drain_limit_valid_until_seconds",
		Help: "Unix timestamp in seconds at which the valid external drain-limit lease observed by the most recent gate evaluation expires. Zero when that evaluation observed no valid lease.",
	}, []string{LabelPoolName})
	GateDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_gate_decisions_total",
		Help: "Count of gate decisions that prevented dispatch, by reason (gate_closed, quota_exhausted, dropped, error). quota_exhausted/dropped/error count individual messages refused after they were dequeued; gate_closed counts those plus each dequeue round in which the gate's budget shrank the batch to zero, which is how budget-based gates (prometheus-budget/-saturation/-query) shed work before any message is dequeued.",
	}, []string{LabelQueueID, LabelQueueName, LabelPoolName, LabelReason})
	GateMetricValue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_gate_metric_value",
		Help: "Raw metric value last read by a metric-based dispatch gate (prometheus-saturation/-budget/-query), i.e. the value compared against async_gate_metric_threshold to decide the gate. The gate closes when value <= threshold. For the saturation gate the value is 1 - saturation. Labeled by the queue or worker pool that owns the gate; inference_pool names the InferencePool the gate queries.",
	}, gateLabels)
	GateMetricThreshold = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_gate_metric_threshold",
		Help: "Threshold a metric-based dispatch gate compares async_gate_metric_value against; the gate closes when value <= this threshold.",
	}, gateLabels)
	GateMetricSourceAvailable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_gate_metric_source_available",
		Help: "1 when a metric-based dispatch gate's last evaluation got a usable reading from its metric source, 0 when it fell back to the configured 'fallback' budget (query error, no samples, or NaN/Inf). Distinguishes a fallback budget from a real reading of the same number: async_dispatch_budget 0 with this at 1 means a saturated pool, at 0 means unreadable metrics. async_gate_metric_value is stale whenever this is 0.",
	}, gateLabels)
)

// Gate decision reason label values for async_gate_decisions_total.
const (
	ReasonGateClosed     = "gate_closed"
	ReasonQuotaExhausted = "quota_exhausted"
	ReasonDropped        = "dropped"
	ReasonError          = "error"
)

// DeadlineProximity is a per-poll snapshot histogram of the time remaining
// until deadline (ms) for items still queued in the broker. Bucket counts are
// exact per-poll broker queries (ZCOUNT), not samples; le="0" holds items
// already past their deadline but not yet dequeued. Unlike a classic histogram
// it is not monotonic: every poll replaces the snapshot, so rate() is
// meaningless — use histogram_quantile per scrape. The _sum is estimated from
// bucket midpoints (expired items count as 0; the overflow bucket at the last
// boundary). Only populated for brokers that expose per-item deadlines (Redis
// sorted sets).
var DeadlineProximity = newDeadlineProximityCollector()

// deadlineProximityBuckets are the le bucket boundaries, most urgent first.
// The 0 boundary separates expired items (deadline <= now) from the rest; the
// remaining boundaries match the queue residence time buckets.
var deadlineProximityBuckets = []time.Duration{
	0,
	time.Second,
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	time.Minute,
	2 * time.Minute,
	5 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
	time.Hour,
	2 * time.Hour,
	6 * time.Hour,
	24 * time.Hour,
}

// DeadlineProximityBuckets returns the le bucket boundaries, most urgent
// first, in the order cumulative counts are fed to SetDeadlineProximity.
// Callers must not mutate the returned slice.
func DeadlineProximityBuckets() []time.Duration {
	return append([]time.Duration(nil), deadlineProximityBuckets...)
}

var deadlineProximityBucketLabels = buildDeadlineProximityBucketLabels()

// DeadlineProximityBucketLabels returns the le bucket labels in the same order
// as DeadlineProximityBuckets. Callers must not mutate the returned slice.
func DeadlineProximityBucketLabels() []string {
	return append([]string(nil), deadlineProximityBucketLabels...)
}

func buildDeadlineProximityBucketLabels() []string {
	labels := make([]string, 0, len(deadlineProximityBuckets))
	for _, b := range deadlineProximityBuckets {
		labels = append(labels, strconv.FormatInt(int64(b.Milliseconds()), 10))
	}
	return labels
}

// deadlineProximitySeries is one label set's cumulative bucket counts, aligned
// with deadlineProximityBuckets.
type deadlineProximitySeries struct {
	labelValues []string
	cumulative  []int64
}

// deadlineProximityCollector emits async_deadline_proximity_millis as a custom
// collector because the backlog poller feeds exact pre-computed bucket counts,
// which a HistogramVec cannot accept (it only observes individual values).
var _ prometheus.Collector = (*deadlineProximityCollector)(nil)

type deadlineProximityCollector struct {
	desc   *prometheus.Desc
	mu     sync.Mutex
	series map[string]*deadlineProximitySeries
}

func newDeadlineProximityCollector() *deadlineProximityCollector {
	return &deadlineProximityCollector{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(SchedulerSubsystem, "", "async_deadline_proximity_millis"),
			"Time remaining until deadline (ms) for items still queued in the broker, as a per-poll snapshot histogram of exact bucket counts (le=\"0\" holds items past their deadline but still queued). Counts are exact broker queries, not samples; the _sum is estimated from bucket midpoints. Only populated for brokers that expose per-item deadlines (Redis sorted sets). Not monotonic: each poll replaces the snapshot, so rate() is meaningless; use histogram_quantile per scrape.",
			queueLabels, nil),
		series: make(map[string]*deadlineProximitySeries),
	}
}

func (c *deadlineProximityCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *deadlineProximityCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.series {
		ch <- prometheus.MustNewConstHistogram(c.desc, s.total(), s.estimatedSum(), s.bucketCounts(), s.labelValues...)
	}
}

// bucketCounts returns every le bucket with its cumulative count, including
// zero-count buckets, so empty queues still expose the full bucket shape.
func (s *deadlineProximitySeries) bucketCounts() map[float64]uint64 {
	buckets := make(map[float64]uint64, len(s.cumulative))
	for i, b := range deadlineProximityBuckets {
		// #nosec G115 -- Set clamps cumulative counts to >= 0, so the conversion cannot wrap.
		buckets[float64(b.Milliseconds())] = uint64(s.cumulative[i])
	}
	return buckets
}

// estimatedSum approximates the histogram _sum from bucket midpoints: the
// expired bucket (le=0) counts as 0, interior buckets at the midpoint of their
// range. Prometheus histograms only accept Observe(), so a pre-aggregated
// histogram has no real sum; the estimate only affects averages, never
// histogram_quantile (which reads bucket counts).
func (s *deadlineProximitySeries) estimatedSum() float64 {
	var sum float64
	var prevBound, prevCount float64
	for i, b := range deadlineProximityBuckets {
		bound := float64(b.Milliseconds())
		count := float64(s.cumulative[i]) - prevCount
		if count > 0 {
			sum += count * (prevBound + bound) / 2
		}
		prevBound, prevCount = bound, float64(s.cumulative[i])
	}
	return sum
}

func (s *deadlineProximitySeries) total() uint64 {
	if len(s.cumulative) == 0 {
		return 0
	}
	// #nosec G115 -- Set clamps cumulative counts to >= 0, so the conversion cannot wrap.
	return uint64(s.cumulative[len(s.cumulative)-1])
}

// Set replaces the snapshot for a queue with exact cumulative bucket counts,
// aligned with DeadlineProximityBuckets. A length mismatch is an internal
// invariant violation and is ignored.
func (c *deadlineProximityCollector) Set(queueID, queueName, poolName string, cumulative []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(cumulative) != len(deadlineProximityBuckets) {
		return
	}
	cs := make([]int64, len(cumulative))
	for i, v := range cumulative {
		if v > 0 {
			cs[i] = v
		}
	}
	c.series[queueID+"\x00"+queueName+"\x00"+poolName] = &deadlineProximitySeries{
		labelValues: []string{queueID, queueName, poolName},
		cumulative:  cs,
	}
}

func (c *deadlineProximityCollector) Delete(queueID, queueName, poolName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.series, queueID+"\x00"+queueName+"\x00"+poolName)
}

// Reset clears every snapshot series.
func (c *deadlineProximityCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.series = make(map[string]*deadlineProximitySeries)
}

func RecordRetry(queueID, queueName, poolName string) {
	Retries.WithLabelValues(queueID, queueName, poolName).Inc()
}

func RecordAsyncReq(queueID, queueName, poolName string) {
	AsyncReqs.WithLabelValues(queueID, queueName, poolName).Inc()
}

// RecordDispatchedReq records an attempt that is about to consume downstream
// inference capacity. Unlike RecordAsyncReq, retries are intentionally counted.
func RecordDispatchedReq(queueID, queueName, poolName string) {
	DispatchedReqs.WithLabelValues(queueID, queueName, poolName).Inc()
}

func RecordExceededDeadlineReq(queueID, queueName, poolName string) {
	ExceededDeadlineReqs.WithLabelValues(queueID, queueName, poolName).Inc()
}

func RecordFailedReq(queueID, queueName, poolName string) {
	FailedReqs.WithLabelValues(queueID, queueName, poolName).Inc()
}

func RecordSuccessfulReq(queueID, queueName, poolName string) {
	SuccessfulReqs.WithLabelValues(queueID, queueName, poolName).Inc()
}

func RecordSheddedReq(queueID, queueName, poolName string) {
	SheddedRequests.WithLabelValues(queueID, queueName, poolName).Inc()
}

// RecordTokens increments the token counters for both directions.
func RecordTokens(inputTokens, outputTokens int64, queueID, queueName, poolName string) {
	Tokens.WithLabelValues(queueID, queueName, poolName, "input").Add(float64(inputTokens))
	Tokens.WithLabelValues(queueID, queueName, poolName, "output").Add(float64(outputTokens))
}

func RecordMessageLatency(millis float64, queueID, queueName, poolName string) {
	MessageLatencyTime.WithLabelValues(queueID, queueName, poolName).Observe(millis)
}

// RecordInferenceLatency observes the time spent calling the inference gateway.
func RecordInferenceLatency(millis float64, queueID, queueName, poolName string) {
	InferenceLatencyTime.WithLabelValues(queueID, queueName, poolName).Observe(millis)
}

// RecordQueueResidenceTime observes the time a message spent buffered in-process
// from broker ingestion until a worker pulled it.
func RecordQueueResidenceTime(millis float64, queueID, queueName, poolName string) {
	QueueResidenceTime.WithLabelValues(queueID, queueName, poolName).Observe(millis)
}

// SetDeadlineProximity replaces a queue's deadline-proximity snapshot
// histogram with the exact cumulative bucket counts read from the broker
// (order aligned with DeadlineProximityBuckets).
func SetDeadlineProximity(queueID, queueName, poolName string, cumulative []int64) {
	DeadlineProximity.Set(queueID, queueName, poolName, cumulative)
}

// IncQueueDepth increments the count of in-process buffered requests.
func IncQueueDepth(queueID, queueName, poolName string) {
	QueueDepth.WithLabelValues(queueID, queueName, poolName).Inc()
}

// DecQueueDepth decrements the count of in-process buffered requests.
func DecQueueDepth(queueID, queueName, poolName string) {
	QueueDepth.WithLabelValues(queueID, queueName, poolName).Dec()
}

// IncInflight increments the count of requests actively processed by workers.
func IncInflight(queueID, queueName, poolName string) {
	InflightRequests.WithLabelValues(queueID, queueName, poolName).Inc()
}

// DecInflight decrements the count of requests actively processed by workers.
func DecInflight(queueID, queueName, poolName string) {
	InflightRequests.WithLabelValues(queueID, queueName, poolName).Dec()
}

// RecordQueueConfigReload counts one queue-config hot reload attempt and,
// on success, marks the last-applied timestamp so operators can compare
// desired and actual configuration generations.
func RecordQueueConfigReload(success bool) {
	if success {
		QueueConfigReloads.WithLabelValues("success").Inc()
		QueueConfigLastSuccess.SetToCurrentTime()
		return
	}
	QueueConfigReloads.WithLabelValues("error").Inc()
}

// SetBrokerBacklog sets the broker-side backlog for a queue.
func SetBrokerBacklog(queueID, queueName, poolName string, n float64) {
	BrokerBacklog.WithLabelValues(queueID, queueName, poolName).Set(n)
}

// SetBrokerBacklogSourceAvailable records whether the most recent broker-side
// backlog read produced a trustworthy value for a queue.
func SetBrokerBacklogSourceAvailable(queueID, queueName, poolName string, available bool) {
	v := 0.0
	if available {
		v = 1.0
	}
	BrokerBacklogSourceAvailable.WithLabelValues(queueID, queueName, poolName).Set(v)
}

// SetDispatchBudget sets the current dispatch budget [0.0-1.0] for a queue's gate.
func SetDispatchBudget(budget float64, queueID, queueName, poolName string) {
	DispatchBudget.WithLabelValues(queueID, queueName, poolName).Set(budget)
}

// RemoveQueueSnapshots deletes point-in-time series for a queue that is no
// longer configured. Historical counters and latency histograms are retained.
func RemoveQueueSnapshots(queueID, queueName, poolName string) {
	BrokerBacklog.DeleteLabelValues(queueID, queueName, poolName)
	BrokerBacklogSourceAvailable.DeleteLabelValues(queueID, queueName, poolName)
	DispatchBudget.DeleteLabelValues(queueID, queueName, poolName)
	DeadlineProximity.Delete(queueID, queueName, poolName)
}

// SetPoolWorkerLimit sets the configured worker concurrency limit for a pool.
func SetPoolWorkerLimit(poolName string, n float64) {
	PoolWorkerLimit.WithLabelValues(poolName).Set(n)
}

// SetDrainLimit records the external controller lease observed by the most
// recent gate evaluation. Callers determine live validity by comparing the
// exported valid-until timestamp with the current time.
func SetDrainLimit(poolName string, maxAdmissionRPS float64, validUntilUnixMillis int64, leaseValid bool) {
	if !leaseValid {
		maxAdmissionRPS = 0
		validUntilUnixMillis = 0
	}
	valid := 0.0
	if leaseValid {
		valid = 1.0
	}
	DrainLimitRPS.WithLabelValues(poolName).Set(maxAdmissionRPS)
	DrainLimitLeaseValid.WithLabelValues(poolName).Set(valid)
	DrainLimitValidUntil.WithLabelValues(poolName).Set(float64(validUntilUnixMillis) / 1000.0)
}

// RecordGateDecision increments the count of gate decisions that prevented a
// message from being dispatched, labeled by reason.
func RecordGateDecision(reason, queueID, queueName, poolName string) {
	GateDecisions.WithLabelValues(queueID, queueName, poolName, reason).Inc()
}

// InitGateDecisions pre-creates a queue's async_gate_decisions_total series with
// every reason at 0. A CounterVec label set that has never been incremented is
// absent from /metrics entirely, so querying a reason that has not fired yet
// yields an empty vector rather than 0 — indistinguishable from a queue that was
// never configured or a scrape that never landed.
func InitGateDecisions(queueID, queueName, poolName string) {
	for _, reason := range []string{ReasonGateClosed, ReasonQuotaExhausted, ReasonDropped, ReasonError} {
		GateDecisions.WithLabelValues(queueID, queueName, poolName, reason)
	}
}

// SetGateMetricValue records the raw metric value a metric-based dispatch gate
// last read and the threshold it is compared against. Helps answer "why is the
// gate closed?" (value <= threshold). queueID/queueName/poolName identify the
// gate's owner and match the labels on that queue's other series; inferencePool
// is the InferencePool the gate queries, and is empty when the gate does not
// name one.
func SetGateMetricValue(value, threshold float64, queueID, queueName, poolName, inferencePool string) {
	GateMetricValue.WithLabelValues(queueID, queueName, poolName, inferencePool).Set(value)
	GateMetricThreshold.WithLabelValues(queueID, queueName, poolName, inferencePool).Set(threshold)
}

// SetGateMetricSourceAvailable records whether a metric-based dispatch gate's last
// evaluation obtained a usable reading. Helps answer "is this budget a real
// measurement or the configured fallback?". Carries the same labels as
// SetGateMetricValue so the three series join.
func SetGateMetricSourceAvailable(available bool, queueID, queueName, poolName, inferencePool string) {
	v := 0.0
	if available {
		v = 1.0
	}
	GateMetricSourceAvailable.WithLabelValues(queueID, queueName, poolName, inferencePool).Set(v)
}

// GetCollectors returns all custom collectors for the async processor.
func GetAsyncProcessorCollectors(supportsMessageLatency bool) []prometheus.Collector {
	collectors := []prometheus.Collector{
		Retries, AsyncReqs, DispatchedReqs, ExceededDeadlineReqs, FailedReqs, SuccessfulReqs, SheddedRequests, Tokens,
		QueueDepth, InflightRequests, BrokerBacklog, BrokerBacklogSourceAvailable, InferenceLatencyTime, QueueResidenceTime,
		DeadlineProximity,
		DispatchBudget, PoolWorkerLimit, QueueConfigReloads, QueueConfigLastSuccess,
		DrainLimitRPS, DrainLimitLeaseValid, DrainLimitValidUntil, GateDecisions,
		GateMetricValue, GateMetricThreshold, GateMetricSourceAvailable,
	}
	if supportsMessageLatency {
		collectors = append(collectors, MessageLatencyTime)
	}
	return collectors
}

var registerMetrics sync.Once

// Register all metrics.
func Register(customCollectors ...prometheus.Collector) {
	registerMetrics.Do(func() {
		for _, collector := range customCollectors {
			controllerruntime.Registry.MustRegister(collector)
		}
	})
}
