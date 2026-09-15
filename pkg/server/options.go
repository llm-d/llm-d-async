package server

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/llm-d/llm-d-async/internal/logging"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	"github.com/llm-d/llm-d-async/pkg/pubsub"
	"github.com/llm-d/llm-d-async/pkg/redis"
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

// TransportOptions groups the transport selection and configuration flags.
type TransportOptions struct {
	Type                  string
	Config                string
	ConfigFile            string
	ConfigWatchInterval   time.Duration
	MergePolicyConfigFile string
	BacklogPollInterval   time.Duration
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
	Transport           TransportOptions
	Observability       ObservabilityConfig
	Prometheus          PrometheusConfig
	TransformConfigFile string

	// MessageQueueImpl backs the deprecated --message-queue-impl flag. Prefer
	// Transport.Type (--transport). Retained for backwards compatibility.
	MessageQueueImpl string
}

type Options struct {
	Config

	// Deprecated per-backend flag options, retained for backwards compatibility.
	// They are translated into the transport config by the compat shim.
	Redis           redis.PubSubFlowOptions
	RedisSortedSet  redis.SortedSetFlowOptions
	RedisConnection redis.ConnectionOptions
	PubSub          pubsub.Options

	loggingOptions zap.Options

	// legacyMergePolicyConfigFile backs the deprecated --request-merge-policy-config
	// flag. Prefer Transport.MergePolicyConfigFile (--request-merge-policy-config-file).
	legacyMergePolicyConfigFile string

	// flagSet is captured in AddFlags so Validate/Complete and the compat shim
	// can inspect which flags were explicitly set (fs.Changed).
	flagSet *pflag.FlagSet
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
				Concurrency:    64,
				RequestTimeout: 5 * time.Minute,
				DrainTimeout:   2 * time.Minute,
			},
			Transport: TransportOptions{
				Type:                "redis-pubsub",
				BacklogPollInterval: 15 * time.Second,
			},
			Observability: ObservabilityConfig{
				Verbosity: logging.DEFAULT,
			},
			Prometheus: PrometheusConfig{
				CacheTTL: flowcontrol.DefaultCacheTTL,
			},
			MessageQueueImpl: "redis-pubsub",
		},
		Redis:           *redis.NewPubSubFlowOptions(),
		RedisSortedSet:  *redis.NewSortedSetFlowOptions(),
		RedisConnection: *redis.NewConnectionOptions(),
		PubSub:          *pubsub.NewOptions(),
		loggingOptions:  zap.Options{Development: true},
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.flagSet = fs

	fs.IntVarP(&o.Observability.Verbosity, "v", "v", o.Observability.Verbosity, "number for the log level verbosity")

	fs.IntVar(&o.Server.HealthPort, "health-port", o.Server.HealthPort, "The health probe port")
	fs.IntVar(&o.Server.MetricsPort, "metrics-port", o.Server.MetricsPort, "The metrics port")
	fs.BoolVar(&o.Server.MetricsEndpointAuth, "metrics-endpoint-auth", o.Server.MetricsEndpointAuth, "Enables authentication and authorization of the metrics endpoint")

	fs.IntVar(&o.Worker.Concurrency, "concurrency", o.Worker.Concurrency, "number of concurrent workers")
	fs.DurationVar(&o.Worker.RequestTimeout, "request-timeout", o.Worker.RequestTimeout, "timeout for individual inference requests")
	fs.DurationVar(&o.Worker.DrainTimeout, "drain-timeout", o.Worker.DrainTimeout, "maximum time to wait for in-flight requests to complete after SIGTERM")
	fs.StringVar(&o.Worker.PoolConfigFile, "pool-config-file", o.Worker.PoolConfigFile, "Path to the pools configuration JSON file")

	fs.StringVar(&o.Transport.Type, "transport", o.Transport.Type, "The transport implementation to use. Supported: redis-pubsub, redis-sortedset, gcp-pubsub, sql")
	fs.StringVar(&o.Transport.Config, "transport-config", o.Transport.Config, "Inline JSON transport configuration. Mutually exclusive with --transport-config-file.")
	fs.StringVar(&o.Transport.ConfigFile, "transport-config-file", o.Transport.ConfigFile, "Path to transport configuration JSON file. Mutually exclusive with --transport-config.")
	fs.DurationVar(&o.Transport.ConfigWatchInterval, "transport-config-watch-interval", o.Transport.ConfigWatchInterval, "If positive, periodically reload the queues field of --transport-config-file; 0 disables hot reload (default)")
	fs.StringVar(&o.Transport.MergePolicyConfigFile, "request-merge-policy-config-file", o.Transport.MergePolicyConfigFile, "Path to the request merge policy configuration JSON file (empty defaults to random-robin)")
	// Deprecated: use --request-merge-policy-config-file. Retained for backwards compatibility.
	fs.StringVar(&o.legacyMergePolicyConfigFile, "request-merge-policy-config", o.legacyMergePolicyConfigFile, "Deprecated: use --request-merge-policy-config-file. Path to the request merge policy configuration JSON file")
	fs.DurationVar(&o.Transport.BacklogPollInterval, "metrics-backlog-poll-interval", o.Transport.BacklogPollInterval, "interval to poll the broker for queue backlog metrics (0 disables); only applies to flows that support it (redis-sortedset, gcp-pubsub, sql)")

	// Deprecated: use --transport / --transport-config instead. Retained for backwards compatibility.
	fs.StringVar(&o.MessageQueueImpl, "message-queue-impl", o.MessageQueueImpl, "Deprecated: use --transport. The message queue implementation to use. Supported implementations: redis-pubsub, redis-sortedset, gcp-pubsub, gcp-pubsub-gated")
	fs.BoolVar(&o.Observability.RedisTracing, "redis-tracing", o.Observability.RedisTracing, "Deprecated: set enable_tracing in --transport-config. Enable per-command Redis tracing spans (high volume, use for debugging only)")

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

// usingNewTransport reports whether the caller opted into the new transport
// flags (--transport / --transport-config / --transport-config-file). When true,
// the deprecated per-backend flags are ignored.
func (o *Options) usingNewTransport() bool {
	if o.flagSet == nil {
		return false
	}
	return o.flagSet.Changed("transport") ||
		o.flagSet.Changed("transport-config") ||
		o.flagSet.Changed("transport-config-file")
}

// mergePolicyConfigFile returns the effective merge-policy config file path,
// preferring the canonical --request-merge-policy-config-file over the
// deprecated --request-merge-policy-config alias when both are set.
func (o *Options) mergePolicyConfigFile() string {
	if o.flagSet != nil &&
		o.flagSet.Changed("request-merge-policy-config") &&
		!o.flagSet.Changed("request-merge-policy-config-file") {
		return o.legacyMergePolicyConfigFile
	}
	return o.Transport.MergePolicyConfigFile
}

func (o *Options) Complete() error {
	hasPoolConfig := o.Worker.PoolConfigFile != ""

	var hasTransportConfig bool
	if o.usingNewTransport() {
		hasTransportConfig = o.Transport.Config != "" || o.Transport.ConfigFile != ""
	} else {
		hasTransportConfig = o.IsQueueConfigSet()
	}

	if hasPoolConfig && !hasTransportConfig {
		return fmt.Errorf("pool-config-file can only be specified when a transport/queues config is also specified")
	}

	return nil
}

var (
	validTransports = []string{"redis-pubsub", "redis-sortedset", "gcp-pubsub", "sql"}
	validQueueImpls = []string{"redis-pubsub", "redis-sortedset", "gcp-pubsub", "gcp-pubsub-gated"}
)

func (o *Options) Validate() error {
	if o.Transport.ConfigWatchInterval < 0 {
		return fmt.Errorf("--transport-config-watch-interval must not be negative")
	}
	if o.Transport.ConfigWatchInterval > 0 {
		if !o.usingNewTransport() || o.Transport.Type != "redis-sortedset" {
			return fmt.Errorf("--transport-config-watch-interval requires --transport redis-sortedset")
		}
		if o.Transport.Config != "" || o.Transport.ConfigFile == "" {
			return fmt.Errorf("--transport-config-watch-interval requires --transport-config-file and cannot be used with --transport-config")
		}
	}
	if o.usingNewTransport() {
		if err := o.validateNewTransport(); err != nil {
			return err
		}
	} else if err := o.validateLegacyTransport(); err != nil {
		return err
	}
	if (o.TLS.Cert != "") != (o.TLS.Key != "") {
		return fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}
	return nil
}

func (o *Options) validateNewTransport() error {
	if !contains(validTransports, o.Transport.Type) {
		return fmt.Errorf("--transport must be one of: %s", strings.Join(validTransports, ", "))
	}
	hasInline := o.Transport.Config != ""
	hasFile := o.Transport.ConfigFile != ""
	if hasInline && hasFile {
		return fmt.Errorf("--transport-config and --transport-config-file are mutually exclusive")
	}
	if !hasInline && !hasFile {
		return fmt.Errorf("--transport-config or --transport-config-file is required")
	}
	return nil
}

func (o *Options) validateLegacyTransport() error {
	if !contains(validQueueImpls, o.MessageQueueImpl) {
		return fmt.Errorf("--message-queue-impl must be one of: %s", strings.Join(validQueueImpls, ", "))
	}
	if strings.HasPrefix(o.MessageQueueImpl, "redis") && o.RedisConnection.URL == "" {
		return fmt.Errorf("--redis.url (or REDIS_URL env var) is required when using %s", o.MessageQueueImpl)
	}
	if strings.HasPrefix(o.MessageQueueImpl, "gcp-pubsub") && o.PubSub.ProjectID == "" {
		return fmt.Errorf("--pubsub.project-id is required when using %s", o.MessageQueueImpl)
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
