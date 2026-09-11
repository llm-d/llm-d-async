package server

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/pubsub"
	"github.com/llm-d/llm-d-async/pkg/redis"
)

// deprecatedFlags maps a deprecated flag name to the suggested replacement.
// The new --transport / --transport-config[-file] surface supersedes all of
// these; they are kept working via the translation shim below.
func deprecatedFlags() map[string]string {
	return map[string]string{
		"message-queue-impl": "--transport",
		"redis-tracing":      "enable_tracing in --transport-config",

		// Redis connection (shared by redis-pubsub and redis-sortedset).
		"redis.url": "url in --transport-config",

		// redis-pubsub per-backend flags.
		"redis.igw-base-url":        "--transport-config (redis-pubsub)",
		"redis.request-path-url":    "--transport-config (redis-pubsub)",
		"redis.inference-objective": "--transport-config (redis-pubsub)",
		"redis.request-queue-name":  "--transport-config (redis-pubsub)",
		"redis.retry-queue-name":    "--transport-config (redis-pubsub)",
		"redis.result-queue-name":   "--transport-config (redis-pubsub)",
		"redis.queues-config":       "--transport-config (redis-pubsub)",
		"redis.queues-config-file":  "--transport-config-file (redis-pubsub)",

		// redis-sortedset per-backend flags.
		"redis.ss.igw-base-url":        "--transport-config (redis-sortedset)",
		"redis.ss.request-path-url":    "--transport-config (redis-sortedset)",
		"redis.ss.inference-objective": "--transport-config (redis-sortedset)",
		"redis.ss.request-queue-name":  "--transport-config (redis-sortedset)",
		"redis.ss.result-queue-name":   "--transport-config (redis-sortedset)",
		"redis.ss.queues-config":       "--transport-config (redis-sortedset)",
		"redis.ss.queues-config-file":  "--transport-config-file (redis-sortedset)",
		"redis.ss.poll-interval-ms":    "poll_interval_ms in --transport-config",
		"redis.ss.batch-size":          "batch_size in --transport-config",
		"redis.ss.gate-type":           "gate_type in --transport-config",
		"redis.ss.gate-params":         "gate_params in --transport-config",

		// gcp-pubsub per-backend flags.
		"pubsub.igw-base-url":          "--transport-config (gcp-pubsub)",
		"pubsub.project-id":            "project_id in --transport-config",
		"pubsub.request-path-url":      "--transport-config (gcp-pubsub)",
		"pubsub.inference-objective":   "--transport-config (gcp-pubsub)",
		"pubsub.request-subscriber-id": "--transport-config (gcp-pubsub)",
		"pubsub.result-topic-id":       "result_topic_id in --transport-config",
		"pubsub.topics-config-file":    "--transport-config-file (gcp-pubsub)",
		"pubsub.batch-size":            "batch_size in --transport-config",
	}
}

// warnDeprecatedFlags logs a deprecation warning for every deprecated flag the
// user explicitly set. When the new transport flags are in use, the legacy flags
// are ignored and the warning says so.
func (o *Options) warnDeprecatedFlags(logger logr.Logger) {
	if o.flagSet == nil {
		return
	}
	newPath := o.usingNewTransport()
	for name, replacement := range deprecatedFlags() {
		if !o.flagSet.Changed(name) {
			continue
		}
		if newPath {
			logger.Info("Deprecated flag ignored because --transport/--transport-config was provided",
				"flag", "--"+name, "use", replacement)
		} else {
			logger.Info("Deprecated flag used; it still works but will be removed in a future release. Prefer the transport config.",
				"flag", "--"+name, "use", replacement)
		}
	}

	// The merge-policy config flag was renamed for naming consistency with
	// --transport-config-file. It is independent of the transport selection, so
	// it is never "ignored" — it always works, just under the new name.
	if o.flagSet.Changed("request-merge-policy-config") {
		logger.Info("Deprecated flag used; it still works but will be removed in a future release.",
			"flag", "--request-merge-policy-config", "use", "--request-merge-policy-config-file")
	}
}

// warnDeprecatedTransport logs a deprecation warning when the effective
// transport is redis-pubsub. The transport is deprecated in this release: it
// keeps working through the next release as well and may be removed no earlier
// than the release after that (N+2 deprecation policy).
func (o *Options) warnDeprecatedTransport(logger logr.Logger) {
	if o.effectiveTransportType() != "redis-pubsub" {
		return
	}
	logger.Info("Deprecated transport in use; it still works but will be removed in a future release. Prefer redis-sortedset.",
		"transport", "redis-pubsub", "use", "redis-sortedset")
}

// effectiveTransportType returns the transport that will be used, honoring the
// new --transport flag first and falling back to the deprecated
// --message-queue-impl (normalizing the retired gcp-pubsub-gated alias).
func (o *Options) effectiveTransportType() string {
	if o.usingNewTransport() {
		return o.Transport.Type
	}
	return normalizeLegacyImpl(o.MessageQueueImpl)
}

// resolveTransport returns the effective transport type and its config bytes. It
// prefers the new --transport / --transport-config[-file] flags; when those are
// not set it translates the deprecated per-backend flags into an equivalent
// transport config.
func (o *Options) resolveTransport() (string, []byte, error) {
	if o.usingNewTransport() {
		data, err := loadTransportConfigBytes(o.Transport)
		if err != nil {
			return "", nil, err
		}
		return o.Transport.Type, data, nil
	}

	transportType := normalizeLegacyImpl(o.MessageQueueImpl)
	data, err := o.synthesizeTransportConfig(transportType)
	if err != nil {
		return "", nil, err
	}
	return transportType, data, nil
}

// legacyUngatedPubSub reports whether the flow was selected via the deprecated
// --message-queue-impl=gcp-pubsub alias (the plain, ungated one). On the legacy
// path that alias wired no gate factory, so a gate_type in a topics-config file
// was inert and every topic got an always-open gate. We preserve that by
// withholding the factory for it: the retired gcp-pubsub-gated alias and the new
// --transport surface both gate normally.
func (o *Options) legacyUngatedPubSub() bool {
	return !o.usingNewTransport() && o.MessageQueueImpl == "gcp-pubsub"
}

// normalizeLegacyImpl maps the retired gcp-pubsub-gated alias onto gcp-pubsub;
// gating is now expressed per-topic via gate_type in the transport config.
func normalizeLegacyImpl(impl string) string {
	if impl == "gcp-pubsub-gated" {
		return "gcp-pubsub"
	}
	return impl
}

// synthesizeTransportConfig builds a transport config JSON blob from the
// deprecated per-backend flags for the given transport type.
func (o *Options) synthesizeTransportConfig(transportType string) ([]byte, error) {
	switch transportType {
	case "redis-pubsub":
		return o.synthesizeRedisPubSubConfig()
	case "redis-sortedset":
		return o.synthesizeRedisSortedSetConfig()
	case "gcp-pubsub":
		return o.synthesizePubSubConfig()
	default:
		return nil, fmt.Errorf("unknown message queue implementation: %s", o.MessageQueueImpl)
	}
}

func (o *Options) synthesizeRedisPubSubConfig() ([]byte, error) {
	queues, err := legacyRedisPubSubQueues(o.Redis)
	if err != nil {
		return nil, err
	}
	cfg := redis.PubSubConfig{
		URL:             o.RedisConnection.URL,
		RetryQueueName:  o.Redis.RetryQueueName,
		ResultQueueName: o.Redis.ResultQueueName,
		EnableTracing:   o.Observability.RedisTracing,
		Queues:          queues,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal redis-pubsub config: %w", err)
	}
	return data, nil
}

func legacyRedisPubSubQueues(opts redis.PubSubFlowOptions) ([]redis.QueueConfig, error) {
	if opts.QueuesConfig != "" {
		var qs []redis.QueueConfig
		if err := json.Unmarshal([]byte(opts.QueuesConfig), &qs); err != nil {
			return nil, fmt.Errorf("failed to parse --redis.queues-config: %w", err)
		}
		return qs, nil
	}
	if opts.QueuesConfigFile != "" {
		data, err := os.ReadFile(opts.QueuesConfigFile) // #nosec G304 -- path from trusted CLI flag
		if err != nil {
			return nil, fmt.Errorf("failed to read --redis.queues-config-file: %w", err)
		}
		var qs []redis.QueueConfig
		if err := json.Unmarshal(data, &qs); err != nil {
			return nil, fmt.Errorf("failed to parse --redis.queues-config-file: %w", err)
		}
		return qs, nil
	}
	return []redis.QueueConfig{{
		QueueName:          opts.RequestQueueName,
		WorkerPoolID:       "default",
		InferenceObjective: opts.InferenceObjective,
		IGWBaseURL:         opts.IGWBaseURL,
		RequestPathURL:     opts.RequestPathURL,
	}}, nil
}

func (o *Options) synthesizeRedisSortedSetConfig() ([]byte, error) {
	queues, err := legacyRedisSortedSetQueues(o.RedisSortedSet)
	if err != nil {
		return nil, err
	}
	cfg := redis.SortedSetConfig{
		URL:             o.RedisConnection.URL,
		ResultQueueName: o.RedisSortedSet.ResultQueueName,
		PollIntervalMs:  o.RedisSortedSet.PollIntervalMs,
		BatchSize:       o.RedisSortedSet.BatchSize,
		EnableTracing:   o.Observability.RedisTracing,
		Queues:          queues,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal redis-sortedset config: %w", err)
	}
	return data, nil
}

func legacyRedisSortedSetQueues(opts redis.SortedSetFlowOptions) ([]redis.SortedSetQueueConfig, error) {
	if opts.QueuesConfig != "" {
		var qs []redis.SortedSetQueueConfig
		if err := json.Unmarshal([]byte(opts.QueuesConfig), &qs); err != nil {
			return nil, fmt.Errorf("failed to parse --redis.ss.queues-config: %w", err)
		}
		return qs, nil
	}
	if opts.QueuesConfigFile != "" {
		data, err := os.ReadFile(opts.QueuesConfigFile) // #nosec G304 -- path from trusted CLI flag
		if err != nil {
			return nil, fmt.Errorf("failed to read --redis.ss.queues-config-file: %w", err)
		}
		var qs []redis.SortedSetQueueConfig
		if err := json.Unmarshal(data, &qs); err != nil {
			return nil, fmt.Errorf("failed to parse --redis.ss.queues-config-file: %w", err)
		}
		return qs, nil
	}
	gateParams, err := parseGateParamsJSON(opts.GateParamsJSON)
	if err != nil {
		return nil, err
	}
	return []redis.SortedSetQueueConfig{{
		QueueName:          opts.RequestQueueName,
		WorkerPoolID:       "default",
		InferenceObjective: opts.InferenceObjective,
		IGWBaseURL:         opts.IGWBaseURL,
		RequestPathURL:     opts.RequestPathURL,
		GateConfig:         pipeline.GateConfig{GateType: opts.GateType, GateParams: gateParams},
	}}, nil
}

func (o *Options) synthesizePubSubConfig() ([]byte, error) {
	topics, err := legacyPubSubTopics(o.PubSub)
	if err != nil {
		return nil, err
	}
	cfg := pubsub.Config{
		ProjectID:     o.PubSub.ProjectID,
		ResultTopicID: o.PubSub.ResultTopicID,
		BatchSize:     o.PubSub.BatchSize,
		Topics:        topics,
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal gcp-pubsub config: %w", err)
	}
	return data, nil
}

func legacyPubSubTopics(opts pubsub.Options) ([]pubsub.TopicConfig, error) {
	if opts.TopicsConfigFile != "" {
		data, err := os.ReadFile(opts.TopicsConfigFile) // #nosec G304 -- path from trusted CLI flag
		if err != nil {
			return nil, fmt.Errorf("failed to read --pubsub.topics-config-file: %w", err)
		}
		var ts []pubsub.TopicConfig
		if err := json.Unmarshal(data, &ts); err != nil {
			return nil, fmt.Errorf("failed to parse --pubsub.topics-config-file: %w", err)
		}
		return ts, nil
	}
	return []pubsub.TopicConfig{{
		SubscriberID:       opts.RequestSubscriberID,
		WorkerPoolID:       "default",
		InferenceObjective: opts.InferenceObjective,
		IGWBaseURL:         opts.IGWBaseURL,
		RequestPathURL:     opts.RequestPathURL,
	}}, nil
}

// parseGateParamsJSON parses the deprecated --redis.ss.gate-params JSON string
// into a map for the synthesized gate config.
func parseGateParamsJSON(s string) (map[string]any, error) {
	m := map[string]any{}
	if s == "" || s == "{}" {
		return m, nil
	}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("failed to parse --redis.ss.gate-params: %w", err)
	}
	return m, nil
}
