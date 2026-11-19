package pubsub

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/pflag"
)

// Config is the transport config for the GCP PubSub flow.
// It is parsed from JSON provided via --transport-config or --transport-config-file.
type Config struct {
	ProjectID string `json:"project_id"`
	// ResultTopicID is the flow-level default result topic. It is used when
	// neither the topic entry nor the message names a destination, and may be
	// omitted only when every topic entry sets its own result_topic_id.
	ResultTopicID string        `json:"result_topic_id"`
	BatchSize     int           `json:"batch_size,omitempty"`
	Topics        []TopicConfig `json:"topics"`
}

// LoadConfig parses, applies defaults, and validates a GCP PubSub Config.
func LoadConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse gcp-pubsub transport config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid gcp-pubsub transport config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) ApplyDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = 10
	}
	for i := range c.Topics {
		if c.Topics[i].RequestPathURL == "" {
			c.Topics[i].RequestPathURL = "/v1/completions"
		}
		if c.Topics[i].WorkerPoolID == "" {
			c.Topics[i].WorkerPoolID = "default"
		}
	}
}

func (c *Config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("project_id is required for gcp-pubsub transport")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be a positive integer, got %d", c.BatchSize)
	}
	if len(c.Topics) == 0 {
		return fmt.Errorf("at least one topic must be configured")
	}
	if c.ResultTopicID == "" && !c.allTopicsHaveResultTopic() {
		return fmt.Errorf("result_topic_id is required for gcp-pubsub transport unless every topic sets its own result_topic_id")
	}
	for _, t := range c.Topics {
		if t.SubscriberID == "" {
			return fmt.Errorf("subscriber_id is required for each topic")
		}
		if t.IGWBaseURL == "" {
			return fmt.Errorf("topic subscriber %q: igw_base_url must be specified", t.SubscriberID)
		}
	}
	return nil
}

// allTopicsHaveResultTopic reports whether every topic entry overrides the
// result destination, which makes the flow-level default unreachable.
func (c *Config) allTopicsHaveResultTopic() bool {
	for _, t := range c.Topics {
		if t.ResultTopicID == "" {
			return false
		}
	}
	return true
}

// Options holds CLI flags for the GCP PubSub flow.
type Options struct {
	IGWBaseURL          string
	ProjectID           string
	RequestPathURL      string
	InferenceObjective  string
	RequestSubscriberID string
	ResultTopicID       string
	TopicsConfigFile    string
	BatchSize           int
}

func NewOptions() *Options {
	return &Options{
		RequestPathURL: "/v1/completions",
		BatchSize:      10,
	}
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.IGWBaseURL, "pubsub.igw-base-url", o.IGWBaseURL, "Base URL for IGW. Mutually exclusive with pubsub.topics-config-file flag.")
	fs.StringVar(&o.ProjectID, "pubsub.project-id", o.ProjectID, "GCP project ID for PubSub")
	fs.StringVar(&o.RequestPathURL, "pubsub.request-path-url", o.RequestPathURL, "inference request path url. Mutually exclusive with pubsub.topics-config-file flag.")
	fs.StringVar(&o.InferenceObjective, "pubsub.inference-objective", o.InferenceObjective, "inference objective to use in requests. Mutually exclusive with pubsub.topics-config-file flag.")
	fs.StringVar(&o.RequestSubscriberID, "pubsub.request-subscriber-id", o.RequestSubscriberID, "GCP PubSub request topic subscriber ID. Mutually exclusive with pubsub.topics-config-file flag.")
	fs.StringVar(&o.ResultTopicID, "pubsub.result-topic-id", o.ResultTopicID, "GCP PubSub topic ID for results")
	fs.StringVar(&o.TopicsConfigFile, "pubsub.topics-config-file", o.TopicsConfigFile, "Topics Configuration file. Mutually exclusive with pubsub.igw-base-url, pubsub.request-subscriber-id, pubsub.request-path-url and pubsub.inference-objective flags. See documentation about syntax")
	fs.IntVar(&o.BatchSize, "pubsub.batch-size", o.BatchSize, "Number of inflight messages")
}

// HasTopicConfig reports whether a multi-topic configuration file is set.
func (o *Options) HasTopicConfig() bool {
	return o.TopicsConfigFile != ""
}
