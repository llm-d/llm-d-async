// Package metrics provides metrics registration for the async processor.
package metrics

import (
	"sync"

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
)

var queueLabels = []string{LabelQueueID, LabelQueueName, LabelPoolName}

var (
	Retries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_request_retries_total",
		Help: "Total number of async request retries.",
	}, queueLabels)
	AsyncReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_request_total",
		Help: "Total number of async requests.",
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
	DispatchBudget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_dispatch_budget",
		Help: "Current dispatch budget [0.0-1.0] returned by the queue's gate; the fraction of system capacity available for new requests (0.0 = gate fully closed).",
	}, queueLabels)
	PoolWorkerLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: SchedulerSubsystem, Name: "async_pool_worker_limit",
		Help: "Configured number of concurrent workers (the concurrency limit) for a pool. Compare against async_inflight_requests for worker utilization.",
	}, []string{LabelPoolName})
	GateDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: SchedulerSubsystem, Name: "async_gate_decisions_total",
		Help: "Count of gate decisions that prevented a message from being dispatched, by reason (gate_closed, quota_exhausted, dropped, error).",
	}, []string{LabelQueueID, LabelQueueName, LabelPoolName, LabelReason})
)

// Gate decision reason label values for async_gate_decisions_total.
const (
	ReasonGateClosed     = "gate_closed"
	ReasonQuotaExhausted = "quota_exhausted"
	ReasonDropped        = "dropped"
	ReasonError          = "error"
)

func RecordRetry(queueID, queueName, poolName string) {
	Retries.WithLabelValues(queueID, queueName, poolName).Inc()
}

func RecordAsyncReq(queueID, queueName, poolName string) {
	AsyncReqs.WithLabelValues(queueID, queueName, poolName).Inc()
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

// SetBrokerBacklog sets the broker-side backlog for a queue.
func SetBrokerBacklog(queueID, queueName, poolName string, n float64) {
	BrokerBacklog.WithLabelValues(queueID, queueName, poolName).Set(n)
}

// SetDispatchBudget sets the current dispatch budget [0.0-1.0] for a queue's gate.
func SetDispatchBudget(budget float64, queueID, queueName, poolName string) {
	DispatchBudget.WithLabelValues(queueID, queueName, poolName).Set(budget)
}

// SetPoolWorkerLimit sets the configured worker concurrency limit for a pool.
func SetPoolWorkerLimit(poolName string, n float64) {
	PoolWorkerLimit.WithLabelValues(poolName).Set(n)
}

// RecordGateDecision increments the count of gate decisions that prevented a
// message from being dispatched, labeled by reason.
func RecordGateDecision(reason, queueID, queueName, poolName string) {
	GateDecisions.WithLabelValues(queueID, queueName, poolName, reason).Inc()
}

// GetCollectors returns all custom collectors for the async processor.
func GetAsyncProcessorCollectors(supportsMessageLatency bool) []prometheus.Collector {
	collectors := []prometheus.Collector{
		Retries, AsyncReqs, ExceededDeadlineReqs, FailedReqs, SuccessfulReqs, SheddedRequests,
		QueueDepth, InflightRequests, BrokerBacklog, InferenceLatencyTime, QueueResidenceTime,
		DispatchBudget, PoolWorkerLimit, GateDecisions,
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
