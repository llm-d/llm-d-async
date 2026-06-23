package pubsub

import (
	"github.com/spf13/pflag"
)

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
