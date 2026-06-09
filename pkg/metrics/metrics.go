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
)

var queueLabels = []string{LabelQueueID, LabelQueueName}

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
)

func RecordRetry(queueID, queueName string) {
	Retries.WithLabelValues(queueID, queueName).Inc()
}

func RecordAsyncReq(queueID, queueName string) {
	AsyncReqs.WithLabelValues(queueID, queueName).Inc()
}

func RecordExceededDeadlineReq(queueID, queueName string) {
	ExceededDeadlineReqs.WithLabelValues(queueID, queueName).Inc()
}

func RecordFailedReq(queueID, queueName string) {
	FailedReqs.WithLabelValues(queueID, queueName).Inc()
}

func RecordSuccessfulReq(queueID, queueName string) {
	SuccessfulReqs.WithLabelValues(queueID, queueName).Inc()
}

func RecordSheddedReq(queueID, queueName string) {
	SheddedRequests.WithLabelValues(queueID, queueName).Inc()
}

func RecordMessageLatency(millis float64, queueID, queueName string) {
	MessageLatencyTime.WithLabelValues(queueID, queueName).Observe(millis)
}

// IncQueueDepth increments the count of in-process buffered requests.
func IncQueueDepth(queueID, queueName string) {
	QueueDepth.WithLabelValues(queueID, queueName).Inc()
}

// DecQueueDepth decrements the count of in-process buffered requests.
func DecQueueDepth(queueID, queueName string) {
	QueueDepth.WithLabelValues(queueID, queueName).Dec()
}

// IncInflight increments the count of requests actively processed by workers.
func IncInflight(queueID, queueName string) {
	InflightRequests.WithLabelValues(queueID, queueName).Inc()
}

// DecInflight decrements the count of requests actively processed by workers.
func DecInflight(queueID, queueName string) {
	InflightRequests.WithLabelValues(queueID, queueName).Dec()
}

// SetBrokerBacklog sets the broker-side backlog for a queue.
func SetBrokerBacklog(queueID, queueName string, n float64) {
	BrokerBacklog.WithLabelValues(queueID, queueName).Set(n)
}

// GetCollectors returns all custom collectors for the async processor.
func GetAsyncProcessorCollectors(supportsMessageLatency bool) []prometheus.Collector {
	collectors := []prometheus.Collector{
		Retries, AsyncReqs, ExceededDeadlineReqs, FailedReqs, SuccessfulReqs, SheddedRequests,
		QueueDepth, InflightRequests, BrokerBacklog,
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
