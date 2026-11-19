package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/llm-d/llm-d-async/pkg/pubsub"
	"github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/spf13/pflag"
)

// newTestOptions builds an Options with a fresh flag set parsed from args, so
// tests exercise the same fs.Changed bookkeeping the real CLI relies on.
func newTestOptions(t *testing.T, args ...string) *Options {
	t.Helper()
	o := NewOptions()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	o.AddFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return o
}

func TestSynthesizeRedisPubSubConfig_SingleQueue(t *testing.T) {
	o := newTestOptions(t,
		"--message-queue-impl=redis-pubsub",
		"--redis.url=redis://localhost:6379",
		"--redis.igw-base-url=http://gw",
		"--redis.request-queue-name=my-queue",
		"--redis.retry-queue-name=my-retry",
		"--redis.result-queue-name=my-result",
		"--redis-tracing",
	)

	data, err := o.synthesizeRedisPubSubConfig()
	if err != nil {
		t.Fatalf("synthesizeRedisPubSubConfig: %v", err)
	}
	var cfg redis.PubSubConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("unmarshal synthesized config: %v", err)
	}

	if cfg.URL != "redis://localhost:6379" {
		t.Errorf("URL = %q, want redis://localhost:6379", cfg.URL)
	}
	if cfg.RetryQueueName != "my-retry" || cfg.ResultQueueName != "my-result" {
		t.Errorf("queue names = %q/%q, want my-retry/my-result", cfg.RetryQueueName, cfg.ResultQueueName)
	}
	if !cfg.EnableTracing {
		t.Error("EnableTracing = false, want true (from --redis-tracing)")
	}
	if len(cfg.Queues) != 1 || cfg.Queues[0].QueueName != "my-queue" || cfg.Queues[0].IGWBaseURL != "http://gw" {
		t.Errorf("queues = %+v, want single my-queue/http://gw", cfg.Queues)
	}

	// The synthesized blob must satisfy the real loader end to end.
	if _, err := redis.LoadPubSubConfig(data); err != nil {
		t.Errorf("LoadPubSubConfig rejected synthesized config: %v", err)
	}
}

func TestSynthesizeRedisSortedSetConfig_SingleQueueGate(t *testing.T) {
	o := newTestOptions(t,
		"--message-queue-impl=redis-sortedset",
		"--redis.url=redis://localhost:6379",
		"--redis.ss.igw-base-url=http://gw",
		"--redis.ss.request-queue-name=ss-queue",
		"--redis.ss.gate-type=prometheus-saturation",
		"--redis.ss.gate-params={\"threshold\":0.7}",
	)

	data, err := o.synthesizeRedisSortedSetConfig()
	if err != nil {
		t.Fatalf("synthesizeRedisSortedSetConfig: %v", err)
	}
	var cfg redis.SortedSetConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("unmarshal synthesized config: %v", err)
	}
	if len(cfg.Queues) != 1 {
		t.Fatalf("queues = %+v, want 1", cfg.Queues)
	}
	q := cfg.Queues[0]
	if q.QueueName != "ss-queue" || q.IGWBaseURL != "http://gw" {
		t.Errorf("queue = %+v, want ss-queue/http://gw", q)
	}
	if q.GateType != "prometheus-saturation" {
		t.Errorf("GateType = %q, want prometheus-saturation", q.GateType)
	}
	if q.GateParams["threshold"] != 0.7 {
		t.Errorf("GateParams[threshold] = %v, want 0.7", q.GateParams["threshold"])
	}

	// The synthesized blob must satisfy the real loader end to end (defaults
	// for claim lease/reclaim are applied by LoadSortedSetConfig).
	if _, err := redis.LoadSortedSetConfig(data); err != nil {
		t.Errorf("LoadSortedSetConfig rejected synthesized config: %v", err)
	}
}

func TestSynthesizePubSubConfig_SingleTopic(t *testing.T) {
	o := newTestOptions(t,
		"--message-queue-impl=gcp-pubsub",
		"--pubsub.project-id=proj",
		"--pubsub.request-subscriber-id=sub-1",
		"--pubsub.igw-base-url=http://gw",
		"--pubsub.result-topic-id=results",
	)

	data, err := o.synthesizePubSubConfig()
	if err != nil {
		t.Fatalf("synthesizePubSubConfig: %v", err)
	}
	var cfg pubsub.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("unmarshal synthesized config: %v", err)
	}
	if cfg.ProjectID != "proj" || cfg.ResultTopicID != "results" {
		t.Errorf("project/result = %q/%q, want proj/results", cfg.ProjectID, cfg.ResultTopicID)
	}
	if len(cfg.Topics) != 1 || cfg.Topics[0].SubscriberID != "sub-1" || cfg.Topics[0].IGWBaseURL != "http://gw" {
		t.Errorf("topics = %+v, want single sub-1/http://gw", cfg.Topics)
	}
	if _, err := pubsub.LoadConfig(data); err != nil {
		t.Errorf("LoadConfig rejected synthesized config: %v", err)
	}
}

func TestResolveTransport_PrefersNewFlags(t *testing.T) {
	inline := `{"url":"redis://new","queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`
	o := newTestOptions(t,
		"--transport=redis-pubsub",
		"--transport-config="+inline,
		// Legacy flags set too; they must be ignored.
		"--message-queue-impl=gcp-pubsub",
		"--redis.url=redis://legacy",
	)

	transportType, data, err := o.resolveTransport()
	if err != nil {
		t.Fatalf("resolveTransport: %v", err)
	}
	if transportType != "redis-pubsub" {
		t.Errorf("transportType = %q, want redis-pubsub (new flag wins)", transportType)
	}
	if string(data) != inline {
		t.Errorf("config bytes = %q, want the inline --transport-config", string(data))
	}
}

func TestValidateTransportConfigWatchCombinations(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "canonical file sorted set",
			args: []string{"--transport=redis-sortedset", "--transport-config-file=/tmp/transport.json", "--transport-config-watch-interval=1s"},
		},
		{
			name: "inline config rejected",
			args: []string{"--transport=redis-sortedset", `--transport-config={"url":"redis://x","queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`, "--transport-config-watch-interval=1s"},
			want: "--transport-config-file",
		},
		{
			name: "other transport rejected",
			args: []string{"--transport=redis-pubsub", "--transport-config-file=/tmp/transport.json", "--transport-config-watch-interval=1s"},
			want: "redis-sortedset",
		},
		{
			name: "legacy transport rejected",
			args: []string{"--message-queue-impl=redis-sortedset", "--transport-config-watch-interval=1s"},
			want: "redis-sortedset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newTestOptions(t, tt.args...)
			err := o.Validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("Validate() failed: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestResolveTransport_NormalizesGatedAlias(t *testing.T) {
	o := newTestOptions(t,
		"--message-queue-impl=gcp-pubsub-gated",
		"--pubsub.project-id=proj",
		"--pubsub.request-subscriber-id=sub-1",
		"--pubsub.igw-base-url=http://gw",
	)
	transportType, _, err := o.resolveTransport()
	if err != nil {
		t.Fatalf("resolveTransport: %v", err)
	}
	if transportType != "gcp-pubsub" {
		t.Errorf("transportType = %q, want gcp-pubsub (gated alias normalized)", transportType)
	}
}

// TestLegacyUngatedPubSub guards the regression where the retired plain
// gcp-pubsub alias started honoring gate_type: on main it wired no gate factory,
// so only that ungated alias must withhold the factory. The gated alias and the
// new --transport surface gate normally.
func TestLegacyUngatedPubSub(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{
			name: "legacy plain gcp-pubsub is ungated",
			args: []string{"--message-queue-impl=gcp-pubsub"},
			want: true,
		},
		{
			name: "legacy gated alias gates",
			args: []string{"--message-queue-impl=gcp-pubsub-gated"},
			want: false,
		},
		{
			name: "new transport surface gates",
			args: []string{
				"--transport=gcp-pubsub",
				`--transport-config={"project_id":"p","result_topic_id":"r","topics":[{"subscriber_id":"s","igw_base_url":"http://gw"}]}`,
				// A legacy impl set alongside the new flags must not flip the switch.
				"--message-queue-impl=gcp-pubsub",
			},
			want: false,
		},
		{
			name: "legacy redis impl is irrelevant",
			args: []string{"--message-queue-impl=redis-pubsub"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newTestOptions(t, tt.args...)
			if got := o.legacyUngatedPubSub(); got != tt.want {
				t.Errorf("legacyUngatedPubSub() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWarnDeprecatedFlags_LegacyUsed(t *testing.T) {
	o := newTestOptions(t,
		"--message-queue-impl=redis-sortedset",
		"--redis.url=redis://localhost:6379",
	)

	var msgs []string
	logger := funcr.New(func(prefix, args string) { msgs = append(msgs, args) }, funcr.Options{})
	o.warnDeprecatedFlags(logger)

	joined := strings.Join(msgs, "\n")
	if !strings.Contains(joined, "--message-queue-impl") || !strings.Contains(joined, "--redis.url") {
		t.Errorf("expected deprecation warnings for the set legacy flags, got:\n%s", joined)
	}
	if strings.Contains(joined, "ignored because") {
		t.Errorf("legacy-only run should warn about deprecation, not ignore, got:\n%s", joined)
	}
}

func TestWarnDeprecatedFlags_IgnoredWhenNewTransport(t *testing.T) {
	o := newTestOptions(t,
		"--transport=redis-pubsub",
		"--transport-config={\"url\":\"redis://new\",\"queues\":[]}",
		"--redis.url=redis://legacy",
	)

	var msgs []string
	logger := funcr.New(func(prefix, args string) { msgs = append(msgs, args) }, funcr.Options{})
	o.warnDeprecatedFlags(logger)

	joined := strings.Join(msgs, "\n")
	if !strings.Contains(joined, "ignored because") || !strings.Contains(joined, "--redis.url") {
		t.Errorf("expected 'ignored' warning for --redis.url when new transport used, got:\n%s", joined)
	}
}

func TestMergePolicyConfigFile_Resolution(t *testing.T) {
	t.Run("canonical flag", func(t *testing.T) {
		o := newTestOptions(t, "--request-merge-policy-config-file=/etc/new.json")
		if got := o.mergePolicyConfigFile(); got != "/etc/new.json" {
			t.Errorf("mergePolicyConfigFile() = %q, want /etc/new.json", got)
		}
	})
	t.Run("deprecated alias", func(t *testing.T) {
		o := newTestOptions(t, "--request-merge-policy-config=/etc/old.json")
		if got := o.mergePolicyConfigFile(); got != "/etc/old.json" {
			t.Errorf("mergePolicyConfigFile() = %q, want /etc/old.json", got)
		}
	})
	t.Run("canonical wins when both set", func(t *testing.T) {
		o := newTestOptions(t,
			"--request-merge-policy-config=/etc/old.json",
			"--request-merge-policy-config-file=/etc/new.json",
		)
		if got := o.mergePolicyConfigFile(); got != "/etc/new.json" {
			t.Errorf("mergePolicyConfigFile() = %q, want /etc/new.json (canonical wins)", got)
		}
	})
}

func TestWarnDeprecatedFlags_MergePolicyRenameNotIgnored(t *testing.T) {
	// Even with the new transport flags in use, the merge-policy alias still
	// works (it is orthogonal to transport), so it must not be reported "ignored".
	o := newTestOptions(t,
		"--transport-config={\"url\":\"redis://x\",\"queues\":[]}",
		"--request-merge-policy-config=/etc/old.json",
	)

	var msgs []string
	logger := funcr.New(func(prefix, args string) { msgs = append(msgs, args) }, funcr.Options{})
	o.warnDeprecatedFlags(logger)

	joined := strings.Join(msgs, "\n")
	if !strings.Contains(joined, "--request-merge-policy-config") ||
		!strings.Contains(joined, "--request-merge-policy-config-file") {
		t.Errorf("expected rename warning for merge-policy flag, got:\n%s", joined)
	}
	if strings.Contains(joined, "ignored because") {
		t.Errorf("merge-policy flag must not be reported as ignored, got:\n%s", joined)
	}
}

func TestWarnDeprecatedTransport(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantWarn bool
	}{
		{
			name:     "default transport redis-pubsub warns",
			args:     nil,
			wantWarn: true,
		},
		{
			name: "explicit redis-pubsub via new surface warns",
			args: []string{
				"--transport=redis-pubsub",
				`--transport-config={"url":"redis://x","queues":[]}`,
			},
			wantWarn: true,
		},
		{
			name:     "legacy redis-pubsub warns",
			args:     []string{"--message-queue-impl=redis-pubsub"},
			wantWarn: true,
		},
		{
			name:     "redis-sortedset does not warn",
			args:     []string{"--message-queue-impl=redis-sortedset"},
			wantWarn: false,
		},
		{
			name: "gcp-pubsub via new surface does not warn",
			args: []string{
				"--transport=gcp-pubsub",
				`--transport-config={"project_id":"p","result_topic_id":"r","topics":[]}`,
			},
			wantWarn: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newTestOptions(t, tt.args...)

			var msgs []string
			logger := funcr.New(func(prefix, args string) { msgs = append(msgs, args) }, funcr.Options{})
			o.warnDeprecatedTransport(logger)

			joined := strings.Join(msgs, "\n")
			if tt.wantWarn {
				if !strings.Contains(joined, "Deprecated transport in use") ||
					!strings.Contains(joined, "redis-pubsub") ||
					!strings.Contains(joined, "redis-sortedset") {
					t.Errorf("expected redis-pubsub deprecation warning naming redis-sortedset, got:\n%s", joined)
				}
			} else if len(msgs) != 0 {
				t.Errorf("expected no deprecation warning, got:\n%s", joined)
			}
		})
	}
}

func TestWarnDeprecatedFlags_NoneSet(t *testing.T) {
	o := newTestOptions(t, "--transport-config={\"url\":\"redis://x\",\"queues\":[]}")

	var msgs []string
	logger := funcr.New(func(prefix, args string) { msgs = append(msgs, args) }, funcr.Options{})
	o.warnDeprecatedFlags(logger)

	if len(msgs) != 0 {
		t.Errorf("expected no deprecation warnings when no deprecated flags set, got:\n%s", strings.Join(msgs, "\n"))
	}
}
