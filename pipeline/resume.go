package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	DefaultMaxResumes               = 3
	DefaultCheckpointIntervalTokens = 64
	DefaultCheckpointIntervalMs     = 2000
	DefaultMaxCheckpointTokens      = 4096

	ResumeReasonError      = "error"
	ResumeReasonEviction   = "eviction"
	ResumeReasonRedelivery = "redelivery"
	ResumeReasonShutdown   = "shutdown"

	CheckpointWriteOK     = "ok"
	CheckpointWriteFenced = "fenced"
	CheckpointWriteError  = "error"
)

// BoolPtr returns a pointer to v for optional boolean config fields.
func BoolPtr(v bool) *bool {
	return &v
}

// ResumeConfig controls streaming SSE dispatch, periodic checkpointing, and
// automatic continuation of interrupted generations.
type ResumeConfig struct {
	Enabled                  *bool `json:"enabled,omitempty"`
	MaxResumes               int   `json:"max_resumes,omitempty"`
	CheckpointIntervalTokens int   `json:"checkpoint_interval_tokens,omitempty"`
	CheckpointIntervalMs     int   `json:"checkpoint_interval_ms,omitempty"`
	MaxCheckpointTokens      int   `json:"max_checkpoint_tokens,omitempty"`
	TokenExact               *bool `json:"token_exact,omitempty"`
}

// IsEnabled reports whether streaming checkpoint/resume is enabled.
func (c ResumeConfig) IsEnabled() bool {
	return c.Enabled != nil && *c.Enabled
}

// IsTokenExact reports whether token-ID-exact continuation is enabled.
func (c ResumeConfig) IsTokenExact() bool {
	return c.TokenExact != nil && *c.TokenExact
}

// Effective returns a copy of c with zero-valued numeric knobs populated from
// package defaults. Negative values are preserved so Validate can reject them.
func (c ResumeConfig) Effective() ResumeConfig {
	out := c
	if out.MaxResumes == 0 {
		out.MaxResumes = DefaultMaxResumes
	}
	if out.CheckpointIntervalTokens == 0 {
		out.CheckpointIntervalTokens = DefaultCheckpointIntervalTokens
	}
	if out.CheckpointIntervalMs == 0 {
		out.CheckpointIntervalMs = DefaultCheckpointIntervalMs
	}
	if out.MaxCheckpointTokens == 0 {
		out.MaxCheckpointTokens = DefaultMaxCheckpointTokens
	}
	return out
}

// Validate checks that no numeric knob on c is negative.
func (c *ResumeConfig) Validate() error {
	if c == nil {
		return nil
	}
	if c.MaxResumes < 0 {
		return fmt.Errorf("resume.max_resumes must be non-negative")
	}
	if c.CheckpointIntervalTokens < 0 {
		return fmt.Errorf("resume.checkpoint_interval_tokens must be non-negative")
	}
	if c.CheckpointIntervalMs < 0 {
		return fmt.Errorf("resume.checkpoint_interval_ms must be non-negative")
	}
	if c.MaxCheckpointTokens < 0 {
		return fmt.Errorf("resume.max_checkpoint_tokens must be non-negative")
	}
	return nil
}

// MergeResumeConfig overlays per-queue overrides on top of a flow-level default
// and populates defaults for unset numeric fields.
func MergeResumeConfig(base, override *ResumeConfig) ResumeConfig {
	var merged ResumeConfig
	apply := func(src *ResumeConfig) {
		if src == nil {
			return
		}
		if src.Enabled != nil {
			v := *src.Enabled
			merged.Enabled = &v
		}
		if src.MaxResumes != 0 {
			merged.MaxResumes = src.MaxResumes
		}
		if src.CheckpointIntervalTokens != 0 {
			merged.CheckpointIntervalTokens = src.CheckpointIntervalTokens
		}
		if src.CheckpointIntervalMs != 0 {
			merged.CheckpointIntervalMs = src.CheckpointIntervalMs
		}
		if src.MaxCheckpointTokens != 0 {
			merged.MaxCheckpointTokens = src.MaxCheckpointTokens
		}
		if src.TokenExact != nil {
			v := *src.TokenExact
			merged.TokenExact = &v
		}
	}
	apply(base)
	apply(override)
	return merged.Effective()
}

// CheckpointRecord persists the partial generation state of an in-flight
// request so a subsequent attempt (or surviving worker after lease reclaim)
// can build a synthetic continuation request.
type CheckpointRecord struct {
	Endpoint               string          `json:"endpoint"`
	State                  json.RawMessage `json:"state"`
	ResumeCount            int             `json:"resume_count"`
	GeneratedTokens        int             `json:"generated_tokens"`
	RecomputedPromptTokens int             `json:"recomputed_prompt_tokens,omitempty"`
	LastInterruptReason    string          `json:"last_interrupt_reason,omitempty"`
	UpdatedAtUnixMs        int64           `json:"updated_at_unix_ms"`
}

// CheckpointWriteResult reports whether a checkpoint write succeeded or was
// rejected because the worker no longer owns the claim lease.
type CheckpointWriteResult int

const (
	CheckpointSaved CheckpointWriteResult = iota + 1
	CheckpointFenced
)

// CheckpointStore persists and loads per-request continuation checkpoints.
// Save fences writes against the active claim lease token so a stalled worker
// whose lease was reclaimed cannot overwrite a newer worker's checkpoint.
type CheckpointStore interface {
	Load(ctx context.Context, queueName, requestID, requestToken string) (*CheckpointRecord, error)
	Save(ctx context.Context, queueName, requestID, requestToken string, record CheckpointRecord) (CheckpointWriteResult, error)
}

// CheckpointStoreProvider is an optional capability for flows that persist
// lease-fenced continuation checkpoints for in-flight requests.
type CheckpointStoreProvider interface {
	CheckpointStore() CheckpointStore
}
