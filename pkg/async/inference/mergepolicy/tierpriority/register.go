package tierpriority

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

// MergePolicyName is the registered name for the tier-priority policy.
const MergePolicyName = "tier-priority"

// Recognized keys in MergePolicyDeps.Config:
const (
	ConfigKeyTierLabel      = "tier_label"
	ConfigKeyClassLabel     = "class_label"
	ConfigKeyTierOrder      = "tier_order"
	ConfigKeyClassOrder     = "class_order"
	ConfigKeyPriorityHeader = "priority_header"
	ConfigKeyPerSubBuffer   = "per_subscription_buffer"
)

func init() {
	pipeline.RegisterMergePolicy(MergePolicyName, build)
}

func build(deps pipeline.MergePolicyDeps) (pipeline.RequestMergePolicy, error) {
	cfg, err := parseConfigFromDeps(deps.Config)
	if err != nil {
		return nil, err
	}
	return New(cfg), nil
}

func parseConfigFromDeps(in map[string]string) (Config, error) {
	cfg := Config{
		TierLabel:      in[ConfigKeyTierLabel],
		ClassLabel:     in[ConfigKeyClassLabel],
		PriorityHeader: in[ConfigKeyPriorityHeader],
	}
	if raw := in[ConfigKeyTierOrder]; raw != "" {
		cfg.TierOrder = ParseCSV(raw)
	}
	if raw := in[ConfigKeyClassOrder]; raw != "" {
		cfg.ClassOrder = ParseCSV(raw)
	}
	if len(cfg.TierOrder) == 0 {
		cfg.TierOrder = []string{"interactive", "async", "batch"}
	}
	if len(cfg.ClassOrder) == 0 {
		cfg.ClassOrder = []string{"reserved", "overflow"}
	}
	if raw := strings.TrimSpace(in[ConfigKeyPerSubBuffer]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be an integer: %w", ConfigKeyPerSubBuffer, err)
		}
		if n < 1 {
			return Config{}, fmt.Errorf("%s must be >= 1, got %d", ConfigKeyPerSubBuffer, n)
		}
		cfg.PerSubscriptionBuffer = n
	}
	return cfg, nil
}

// ParseCSV splits a comma-separated list, trimming whitespace and
// dropping empties.
func ParseCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

