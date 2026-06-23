package server

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/llm-d-incubation/llm-d-async/internal/logging"
	"github.com/llm-d-incubation/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d-incubation/llm-d-async/pkg/pubsub"
	"github.com/llm-d-incubation/llm-d-async/pkg/redis"
	"github.com/spf13/pflag"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

type ServerConfig struct {
	HealthPort          int
	MetricsPort         int
	MetricsEndpointAuth bool
}

type TLSConfig struct {
	CACert             string
	Cert               string
	Key                string
	InsecureSkipVerify bool
}

type WorkerConfig struct {
	Concurrency    int
	RequestTimeout time.Duration
	DrainTimeout   time.Duration
	PoolConfigFile string
}

type QueueConfig struct {
	Impl                string
	MergePolicy         string
	BacklogPollInterval time.Duration
}

type ObservabilityConfig struct {
	Verbosity    int
	RedisTracing bool
}

type PrometheusConfig struct {
	URL      string
	CacheTTL time.Duration
}

type Config struct {
	Server              ServerConfig
	TLS                 TLSConfig
	Worker              WorkerConfig
	Queue               QueueConfig
	Observability       ObservabilityConfig
	Prometheus          PrometheusConfig
	TransformConfigFile string
}

type Options struct {
	Config

	Redis           redis.PubSubFlowOptions
	RedisSortedSet  redis.SortedSetFlowOptions
	RedisConnection redis.ConnectionOptions
	PubSub          pubsub.Options

	loggingOptions zap.Options
}

func NewOptions() *Options {
	return &Options{
		Config: Config{
			Server: ServerConfig{
				HealthPort:          8081,
				MetricsPort:         9090,
				MetricsEndpointAuth: true,
			},
			Worker: WorkerConfig{
				Concurrency:    8,
				RequestTimeout: 5 * time.Minute,
				DrainTimeout:   2 * time.Minute,
			},
			Queue: QueueConfig{
				Impl:                "redis-pubsub",
				MergePolicy:         "random-robin",
				BacklogPollInterval: 15 * time.Second,
			},
			Observability: ObservabilityConfig{
				Verbosity: logging.DEFAULT,
			},
			Prometheus: PrometheusConfig{
				CacheTTL: flowcontrol.DefaultCacheTTL,
			},
		},
		Redis:           *redis.NewPubSubFlowOptions(),
		RedisSortedSet:  *redis.NewSortedSetFlowOptions(),
		RedisConnection: *redis.NewConnectionOptions(),
		PubSub:          *pubsub.NewOptions(),
		loggingOptions:  zap.Options{Development: true},
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.IntVarP(&o.Observability.Verbosity, "v", "v", o.Observability.Verbosity, "number for the log level verbosity")

	fs.IntVar(&o.Server.HealthPort, "health-port", o.Server.HealthPort, "The health probe port")
	fs.IntVar(&o.Server.MetricsPort, "metrics-port", o.Server.MetricsPort, "The metrics port")
	fs.BoolVar(&o.Server.MetricsEndpointAuth, "metrics-endpoint-auth", o.Server.MetricsEndpointAuth, "Enables authentication and authorization of the metrics endpoint")

	fs.IntVar(&o.Worker.Concurrency, "concurrency", o.Worker.Concurrency, "number of concurrent workers")
	fs.DurationVar(&o.Worker.RequestTimeout, "request-timeout", o.Worker.RequestTimeout, "timeout for individual inference requests")
	fs.DurationVar(&o.Worker.DrainTimeout, "drain-timeout", o.Worker.DrainTimeout, "maximum time to wait for in-flight requests to complete after SIGTERM")
	fs.StringVar(&o.Worker.PoolConfigFile, "pool-config-file", o.Worker.PoolConfigFile, "Path to the pools configuration JSON file")

	fs.StringVar(&o.Queue.MergePolicy, "request-merge-policy", o.Queue.MergePolicy, "The request merge policy to use. Supported policies: random-robin")
	fs.StringVar(&o.Queue.Impl, "message-queue-impl", o.Queue.Impl, "The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, gcp-pubsub, gcp-pubsub-gated")
	fs.DurationVar(&o.Queue.BacklogPollInterval, "metrics-backlog-poll-interval", o.Queue.BacklogPollInterval, "interval to poll the broker for queue backlog metrics (0 disables); only applies to flows that support it (redis-sortedset, gcp-pubsub)")

	fs.BoolVar(&o.Observability.RedisTracing, "redis-tracing", o.Observability.RedisTracing, "Enable per-command Redis tracing spans (high volume, use for debugging only)")

	fs.StringVar(&o.TLS.CACert, "tls-ca-cert", o.TLS.CACert, "Path to CA certificate file (PEM) for verifying the inference gateway")
	fs.StringVar(&o.TLS.Cert, "tls-cert", o.TLS.Cert, "Path to client certificate file (PEM) for mTLS")
	fs.StringVar(&o.TLS.Key, "tls-key", o.TLS.Key, "Path to client key file (PEM) for mTLS")
	fs.BoolVar(&o.TLS.InsecureSkipVerify, "tls-insecure-skip-verify", o.TLS.InsecureSkipVerify, "Skip TLS certificate verification (dev/test only)")

	fs.StringVar(&o.TransformConfigFile, "transform-config-file", o.TransformConfigFile, "Path to the body-transform plugins configuration JSON file (object with a requestTransforms array; empty disables transforms)")

	fs.StringVar(&o.Prometheus.URL, "prometheus-url", o.Prometheus.URL, "Prometheus server URL for metric-based gates (e.g., http://localhost:9090)")
	fs.DurationVar(&o.Prometheus.CacheTTL, "prometheus-cache-ttl", o.Prometheus.CacheTTL, "TTL for cached Prometheus metrics (e.g., 5s, 0s to disable)")

	// Backend-specific flags
	o.RedisConnection.AddFlags(fs)
	o.Redis.AddFlags(fs)
	o.RedisSortedSet.AddFlags(fs)
	o.PubSub.AddFlags(fs)

	// Zap logging flags (bridged from standard flag)
	goFlagSet := flag.NewFlagSet("", flag.ContinueOnError)
	o.loggingOptions.BindFlags(goFlagSet)
	fs.AddGoFlagSet(goFlagSet)
}

// LoggingOptions returns the zap options for initializing the logger.
func (o *Options) LoggingOptions() *zap.Options {
	return &o.loggingOptions
}

// IsQueueConfigSet reports whether any backend has multi-queue/topic configuration set.
func (o *Options) IsQueueConfigSet() bool {
	return o.Redis.HasQueueConfig() || o.RedisSortedSet.HasQueueConfig() || o.PubSub.HasTopicConfig()
}

func (o *Options) Complete() error {
	hasQueueConfig := o.IsQueueConfigSet()
	hasPoolConfig := o.Worker.PoolConfigFile != ""

	if hasPoolConfig && !hasQueueConfig {
		return fmt.Errorf("pool-config-file can only be specified when queues/topics config file is also specified")
	}

	return nil
}

var (
	validQueueImpls    = []string{"redis-pubsub", "redis-sortedset", "gcp-pubsub", "gcp-pubsub-gated"}
	validMergePolicies = []string{"random-robin"}
)

func (o *Options) Validate() error {
	if !contains(validQueueImpls, o.Queue.Impl) {
		return fmt.Errorf("--message-queue-impl must be one of: %s", strings.Join(validQueueImpls, ", "))
	}
	if !contains(validMergePolicies, o.Queue.MergePolicy) {
		return fmt.Errorf("--request-merge-policy must be one of: %s", strings.Join(validMergePolicies, ", "))
	}
	if strings.HasPrefix(o.Queue.Impl, "redis") && o.RedisConnection.URL == "" {
		return fmt.Errorf("--redis.url (or REDIS_URL env var) is required when using %s", o.Queue.Impl)
	}
	if strings.HasPrefix(o.Queue.Impl, "gcp-pubsub") && o.PubSub.ProjectID == "" {
		return fmt.Errorf("--pubsub.project-id is required when using %s", o.Queue.Impl)
	}
	if (o.TLS.Cert != "") != (o.TLS.Key != "") {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}
	return nil
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
