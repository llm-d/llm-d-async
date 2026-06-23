package redis

import (
	"os"

	"github.com/spf13/pflag"
)

// ConnectionOptions holds the Redis connection configuration.
type ConnectionOptions struct {
	URL string
}

func NewConnectionOptions() *ConnectionOptions {
	return &ConnectionOptions{
		URL: os.Getenv("REDIS_URL"),
	}
}

func (o *ConnectionOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.URL, "redis.url", o.URL, "Redis URL (e.g. redis://user:pass@host:port/db or rediss://... for TLS)")
}

// PubSubFlowOptions holds CLI flags for the Redis pub/sub flow.
type PubSubFlowOptions struct {
	IGWBaseURL         string
	RequestPathURL     string
	InferenceObjective string
	RequestQueueName   string
	RetryQueueName     string
	ResultQueueName    string
	QueuesConfig       string
	QueuesConfigFile   string
}

func NewPubSubFlowOptions() *PubSubFlowOptions {
	return &PubSubFlowOptions{
		RequestPathURL:   "/v1/completions",
		RequestQueueName: "request-queue",
		RetryQueueName:   "retry-sortedset",
		ResultQueueName:  "result-queue",
	}
}

func (o *PubSubFlowOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.IGWBaseURL, "redis.igw-base-url", o.IGWBaseURL, "Base URL for IGW. Mutually exclusive with redis.queues-config-file flag.")
	fs.StringVar(&o.RequestPathURL, "redis.request-path-url", o.RequestPathURL, "request path url. Mutually exclusive with redis.queues-config-file flag.")
	fs.StringVar(&o.InferenceObjective, "redis.inference-objective", o.InferenceObjective, "inference objective to use in requests. Mutually exclusive with redis.queues-config-file flag.")
	fs.StringVar(&o.RequestQueueName, "redis.request-queue-name", o.RequestQueueName, "name of the Redis channel for request messages. Mutually exclusive with redis.queues-config-file flag.")
	fs.StringVar(&o.RetryQueueName, "redis.retry-queue-name", o.RetryQueueName, "name of the Redis sorted set for retry messages")
	fs.StringVar(&o.ResultQueueName, "redis.result-queue-name", o.ResultQueueName, "name of the Redis channel for result messages")
	fs.StringVar(&o.QueuesConfig, "redis.queues-config", o.QueuesConfig, "Inline JSON queues configuration. Takes precedence over redis.queues-config-file and single-queue flags.")
	fs.StringVar(&o.QueuesConfigFile, "redis.queues-config-file", o.QueuesConfigFile, "Queues Configuration file. Mutually exclusive with redis.igw-base-url, redis.request-queue-name, redis.request-path-url and redis.inference-objective flags. See documentation about syntax")
}

// HasQueueConfig reports whether any multi-queue configuration is set.
func (o *PubSubFlowOptions) HasQueueConfig() bool {
	return o.QueuesConfig != "" || o.QueuesConfigFile != ""
}

// SortedSetFlowOptions holds CLI flags for the Redis sorted-set flow.
type SortedSetFlowOptions struct {
	IGWBaseURL         string
	RequestPathURL     string
	InferenceObjective string
	RequestQueueName   string
	ResultQueueName    string
	QueuesConfig       string
	QueuesConfigFile   string
	PollIntervalMs     int
	BatchSize          int
	GateType           string
	GateParamsJSON     string
}

func NewSortedSetFlowOptions() *SortedSetFlowOptions {
	return &SortedSetFlowOptions{
		RequestPathURL:   "/v1/completions",
		RequestQueueName: "request-sortedset",
		ResultQueueName:  "result-list",
		PollIntervalMs:   1000,
		BatchSize:        10,
		GateParamsJSON:   "{}",
	}
}

func (o *SortedSetFlowOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.IGWBaseURL, "redis.ss.igw-base-url", o.IGWBaseURL, "IGW base URL")
	fs.StringVar(&o.RequestPathURL, "redis.ss.request-path-url", o.RequestPathURL, "Request path URL")
	fs.StringVar(&o.InferenceObjective, "redis.ss.inference-objective", o.InferenceObjective, "Inference objective header")
	fs.StringVar(&o.RequestQueueName, "redis.ss.request-queue-name", o.RequestQueueName, "Request sorted set name")
	fs.StringVar(&o.ResultQueueName, "redis.ss.result-queue-name", o.ResultQueueName, "Result list name")
	fs.StringVar(&o.QueuesConfig, "redis.ss.queues-config", o.QueuesConfig, "Inline JSON queues configuration")
	fs.StringVar(&o.QueuesConfigFile, "redis.ss.queues-config-file", o.QueuesConfigFile, "Multiple queues config file")
	fs.IntVar(&o.PollIntervalMs, "redis.ss.poll-interval-ms", o.PollIntervalMs, "Poll interval in milliseconds")
	fs.IntVar(&o.BatchSize, "redis.ss.batch-size", o.BatchSize, "Number of messages to process per poll")
	fs.StringVar(&o.GateType, "redis.ss.gate-type", o.GateType, "Gate type for single-queue mode (e.g. redis, prometheus-saturation, prometheus-budget)")
	fs.StringVar(&o.GateParamsJSON, "redis.ss.gate-params", o.GateParamsJSON, "JSON-encoded gate params map for single-queue mode")
}

// HasQueueConfig reports whether any multi-queue configuration is set.
func (o *SortedSetFlowOptions) HasQueueConfig() bool {
	return o.QueuesConfig != "" || o.QueuesConfigFile != ""
}
