package redis

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/pflag"
)

// PubSubConfig is the transport config for the Redis pub/sub flow.
// It is parsed from JSON provided via --transport-config or --transport-config-file.
type PubSubConfig struct {
	URL string `json:"url,omitempty"`
	// Mode, Addrs and MasterName select the Redis topology; see ConnectionConfig.
	Mode            string        `json:"mode,omitempty"`
	Addrs           []string      `json:"addrs,omitempty"`
	MasterName      string        `json:"master_name,omitempty"`
	RetryQueueName  string        `json:"retry_queue_name,omitempty"`
	ResultQueueName string        `json:"result_queue_name,omitempty"`
	EnableTracing   bool          `json:"enable_tracing,omitempty"`
	Queues          []QueueConfig `json:"queues"`
}

// LoadPubSubConfig parses, applies env overrides/defaults, and validates a PubSubConfig.
func LoadPubSubConfig(data []byte) (*PubSubConfig, error) {
	var cfg PubSubConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse redis-pubsub transport config: %w", err)
	}
	cfg.ApplyEnvOverrides()
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid redis-pubsub transport config: %w", err)
	}
	return &cfg, nil
}

// ApplyEnvOverrides seeds URL from REDIS_URL only when it is not already set in
// the config. An explicit url in --transport-config therefore wins over the
// environment; REDIS_URL is a fallback default, not an override.
func (c *PubSubConfig) ApplyEnvOverrides() {
	if c.URL == "" {
		c.URL = os.Getenv("REDIS_URL")
	}
}

func (c *PubSubConfig) ApplyDefaults() {
	if c.RetryQueueName == "" {
		c.RetryQueueName = "retry-sortedset"
	}
	if c.ResultQueueName == "" {
		c.ResultQueueName = "result-queue"
	}
	for i := range c.Queues {
		if c.Queues[i].RequestPathURL == "" {
			c.Queues[i].RequestPathURL = "/v1/completions"
		}
		if c.Queues[i].WorkerPoolID == "" {
			c.Queues[i].WorkerPoolID = "default"
		}
	}
}

func (c *PubSubConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("url is required (set url in the transport config or REDIS_URL)")
	}
	if err := validateConnection(c.connection()); err != nil {
		return err
	}
	if len(c.Queues) == 0 {
		return fmt.Errorf("at least one queue must be configured")
	}
	for _, q := range c.Queues {
		if q.QueueName == "" {
			return fmt.Errorf("queue_name is required for each queue")
		}
		if q.IGWBaseURL == "" {
			return fmt.Errorf("queue %q: igw_base_url must be specified", q.QueueName)
		}
	}
	return nil
}

func (c *PubSubConfig) connection() ConnectionConfig {
	return ConnectionConfig{URL: c.URL, Mode: c.Mode, Addrs: c.Addrs, MasterName: c.MasterName}
}

// SortedSetConfig is the transport config for the Redis sorted-set flow.
// It is parsed from JSON provided via --transport-config or --transport-config-file.
type SortedSetConfig struct {
	URL string `json:"url,omitempty"`
	// Mode, Addrs and MasterName select the Redis topology; see ConnectionConfig.
	Mode       string   `json:"mode,omitempty"`
	Addrs      []string `json:"addrs,omitempty"`
	MasterName string   `json:"master_name,omitempty"`
	// RetryQueueName is the sorted set holding backoff retries, scored by
	// retry-due time. Retries re-enter their request queue (with the original
	// deadline score) only once due, so backoff is actually enforced.
	RetryQueueName  string `json:"retry_queue_name,omitempty"`
	ResultQueueName string `json:"result_queue_name,omitempty"`
	PollIntervalMs  int    `json:"poll_interval_ms,omitempty"`
	BatchSize       int    `json:"batch_size,omitempty"`
	EnableTracing   bool   `json:"enable_tracing,omitempty"`
	// ClaimLeaseTTLSeconds bounds how long one consumer may hold a dequeued
	// request before survivors treat it as abandoned and redeliver it.
	// Should exceed the longest possible inference time for the queues served.
	ClaimLeaseTTLSeconds int64 `json:"claim_lease_ttl_seconds,omitempty"`
	// ClaimReclaimIntervalMs is how often expired claims are scanned for
	// redelivery.
	ClaimReclaimIntervalMs int64                  `json:"claim_reclaim_interval_ms,omitempty"`
	Queues                 []SortedSetQueueConfig `json:"queues"`
}

// LoadSortedSetConfig parses, applies env overrides/defaults, and validates a SortedSetConfig.
func LoadSortedSetConfig(data []byte) (*SortedSetConfig, error) {
	return loadSortedSetConfig(data, false)
}

// LoadSortedSetConfigAllowEmptyQueues is used by queue hot reload, where an
// operator may temporarily drain every queue. Startup still requires a queue.
func LoadSortedSetConfigAllowEmptyQueues(data []byte) (*SortedSetConfig, error) {
	return loadSortedSetConfig(data, true)
}

func loadSortedSetConfig(data []byte, allowEmptyQueues bool) (*SortedSetConfig, error) {
	var cfg SortedSetConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse redis-sortedset transport config: %w", err)
	}
	cfg.ApplyEnvOverrides()
	cfg.ApplyDefaults()
	if err := cfg.validate(allowEmptyQueues); err != nil {
		return nil, fmt.Errorf("invalid redis-sortedset transport config: %w", err)
	}
	return &cfg, nil
}

// ApplyEnvOverrides seeds URL from REDIS_URL only when it is not already set in
// the config. An explicit url in --transport-config therefore wins over the
// environment; REDIS_URL is a fallback default, not an override.
func (c *SortedSetConfig) ApplyEnvOverrides() {
	if c.URL == "" {
		c.URL = os.Getenv("REDIS_URL")
	}
}

func (c *SortedSetConfig) ApplyDefaults() {
	if c.RetryQueueName == "" {
		c.RetryQueueName = "retry-sortedset"
	}
	if c.ResultQueueName == "" {
		c.ResultQueueName = "result-list"
	}
	if c.PollIntervalMs == 0 {
		c.PollIntervalMs = 1000
	}
	if c.BatchSize == 0 {
		c.BatchSize = 10
	}
	// Durable-dequeue defaults: a 5-minute lease comfortably exceeds
	// typical inference latencies while keeping crash redelivery bounded;
	// claims are scanned every 15s.
	if c.ClaimLeaseTTLSeconds == 0 {
		c.ClaimLeaseTTLSeconds = 300
	}
	if c.ClaimReclaimIntervalMs == 0 {
		c.ClaimReclaimIntervalMs = 15000
	}
	for i := range c.Queues {
		if c.Queues[i].RequestPathURL == "" {
			c.Queues[i].RequestPathURL = "/v1/completions"
		}
		if c.Queues[i].WorkerPoolID == "" {
			c.Queues[i].WorkerPoolID = "default"
		}
		if c.Queues[i].ID == "" {
			c.Queues[i].ID = c.Queues[i].QueueName
		}
	}
}

func (c *SortedSetConfig) Validate() error {
	return c.validate(false)
}

func (c *SortedSetConfig) validate(allowEmptyQueues bool) error {
	if c.URL == "" {
		return fmt.Errorf("url is required (set url in the transport config or REDIS_URL)")
	}
	if err := validateConnection(c.connection()); err != nil {
		return err
	}
	if c.PollIntervalMs < 0 {
		return fmt.Errorf("poll_interval_ms must be non-negative")
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("batch_size must be non-negative")
	}
	if c.ClaimLeaseTTLSeconds < 0 {
		return fmt.Errorf("claim_lease_ttl_seconds must be non-negative")
	}
	if c.ClaimReclaimIntervalMs < 0 {
		return fmt.Errorf("claim_reclaim_interval_ms must be non-negative")
	}
	if !allowEmptyQueues && len(c.Queues) == 0 {
		return fmt.Errorf("at least one queue must be configured")
	}
	seenID := make(map[string]bool, len(c.Queues))
	seenQueue := make(map[string]bool, len(c.Queues))
	for _, q := range c.Queues {
		if q.QueueName == "" {
			return fmt.Errorf("queue_name is required for each queue")
		}
		if q.IGWBaseURL == "" {
			return fmt.Errorf("queue %q: igw_base_url must be specified", q.QueueName)
		}
		if seenID[q.ID] {
			return fmt.Errorf("duplicate queue id %q", q.ID)
		}
		seenID[q.ID] = true
		if seenQueue[q.QueueName] {
			return fmt.Errorf("duplicate queue_name %q", q.QueueName)
		}
		seenQueue[q.QueueName] = true
	}
	return nil
}

func (c *SortedSetConfig) connection() ConnectionConfig {
	return ConnectionConfig{URL: c.URL, Mode: c.Mode, Addrs: c.Addrs, MasterName: c.MasterName}
}

// --- Deprecated per-backend flag options (kept for backwards compatibility) ---
// These structs and their AddFlags register the legacy --redis.* / --redis.ss.*
// flags. They are translated into the transport configs above by the server's
// backwards-compat shim. Prefer --transport-config / --transport-config-file.

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
