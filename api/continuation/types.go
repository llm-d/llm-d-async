/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package continuation provides endpoint-aware SSE accumulation, continuation
// request construction, and response/chunk rewriting for resuming interrupted
// OpenAI-compatible generations across /v1/completions, /v1/chat/completions,
// and /v1/responses.
package continuation

import (
	"encoding/json"
	"strings"
)

// Error is an immutable sentinel error returned by continuation operations.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrUnsupportedEndpoint indicates the request path is not one of the
	// supported OpenAI generation endpoints.
	ErrUnsupportedEndpoint Error = "continuation: unsupported endpoint"
	// ErrMultipleChoices indicates the request sets n > 1, which cannot be
	// deterministically continued across independent choices.
	ErrMultipleChoices Error = "continuation: n > 1 is not supported"
	// ErrInvalidRequestJSON indicates the original request body is not a valid
	// JSON object.
	ErrInvalidRequestJSON Error = "continuation: request body must be a JSON object"
	// ErrStreamError indicates the upstream SSE stream emitted an error event.
	ErrStreamError Error = "continuation: upstream stream error"
	// ErrIncompleteStream indicates the SSE stream ended before a protocol
	// terminal marker ([DONE] or response.completed/response.incomplete).
	ErrIncompleteStream Error = "continuation: stream ended before completion marker"
	// ErrMissingTokenIDs indicates a token-ID array prompt could not be
	// continued because the server did not return generated token IDs.
	ErrMissingTokenIDs Error = "continuation: token-id prompt requires generated token_ids"
	// ErrStateCapped indicates cached partial output hit MaxCheckpointTokens,
	// making live-stream migration unsafe without duplicating uncached tokens.
	ErrStateCapped Error = "continuation: partial output exceeded max checkpoint tokens"
)

// Endpoint identifies a supported OpenAI-compatible inference API route.
type Endpoint string

const (
	// EndpointCompletions represents /v1/completions.
	EndpointCompletions Endpoint = "/v1/completions"
	// EndpointChatCompletions represents /v1/chat/completions.
	EndpointChatCompletions Endpoint = "/v1/chat/completions"
	// EndpointResponses represents /v1/responses.
	EndpointResponses Endpoint = "/v1/responses"
)

// DefaultThinkEndTag is the default closing delimiter used by reasoning models
// such as Qwen3 and DeepSeek-R1 when emitting thinking blocks.
const DefaultThinkEndTag = "</think>"

// DetectEndpoint extracts a supported Endpoint from a URL or request path.
func DetectEndpoint(pathOrURL string) (Endpoint, bool) {
	clean := pathOrURL
	if idx := strings.IndexByte(clean, '?'); idx >= 0 {
		clean = clean[:idx]
	}
	if idx := strings.IndexByte(clean, '#'); idx >= 0 {
		clean = clean[:idx]
	}
	clean = strings.TrimRight(clean, "/")
	switch {
	case strings.HasSuffix(clean, string(EndpointChatCompletions)):
		return EndpointChatCompletions, true
	case strings.HasSuffix(clean, string(EndpointCompletions)):
		return EndpointCompletions, true
	case strings.HasSuffix(clean, string(EndpointResponses)):
		return EndpointResponses, true
	default:
		return "", false
	}
}

// Options configures accumulation and continuation request construction.
type Options struct {
	// MaxCheckpointTokens bounds how many generated tokens are retained in
	// CheckpointSnapshot(). Zero or negative disables the cap.
	MaxCheckpointTokens int
	// TokenExact opts into requesting return_token_ids on attempt 1 and sending
	// kv_transfer_params.prompt_token_ids on continuation attempts when token
	// IDs were returned by the model server. Off by default.
	TokenExact bool
	// ThinkEndTag overrides DefaultThinkEndTag ("</think>") when recovering
	// mid-reasoning continuations where the model server emits the closing
	// thinking tag inside content deltas.
	ThinkEndTag string
}

func (o Options) thinkEndTag() string {
	if o.ThinkEndTag != "" {
		return o.ThinkEndTag
	}
	return DefaultThinkEndTag
}

// UsageState tracks delivered and recomputed token counts across attempts.
type UsageState struct {
	OriginalPromptTokens   int `json:"original_prompt_tokens,omitempty"`
	GeneratedTokens        int `json:"generated_tokens,omitempty"`
	ReasoningTokens        int `json:"reasoning_tokens,omitempty"`
	RecomputedPromptTokens int `json:"recomputed_prompt_tokens,omitempty"`
}

// ChatToolCallFunction holds the name and streamed JSON arguments of a tool call.
type ChatToolCallFunction struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// ChatToolCall represents an OpenAI chat completion tool call item.
type ChatToolCall struct {
	Index    int                  `json:"index"`
	ID       string               `json:"id,omitempty"`
	Type     string               `json:"type,omitempty"`
	Function ChatToolCallFunction `json:"function"`
}

// ResponseContentPart represents a content or summary part inside a /v1/responses item.
type ResponseContentPart struct {
	Type        string          `json:"type"`
	Text        string          `json:"text,omitempty"`
	Refusal     string          `json:"refusal,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
}

// ResponseItem represents a completed or partial output item in /v1/responses.
type ResponseItem struct {
	ID        string                `json:"id,omitempty"`
	Type      string                `json:"type"`
	Role      string                `json:"role,omitempty"`
	Status    string                `json:"status,omitempty"`
	Summary   []ResponseContentPart `json:"summary,omitempty"`
	Content   []ResponseContentPart `json:"content,omitempty"`
	CallID    string                `json:"call_id,omitempty"`
	Name      string                `json:"name,omitempty"`
	Arguments string                `json:"arguments,omitempty"`
	Raw       json.RawMessage       `json:"raw,omitempty"`
}

// HasProgress reports whether a ResponseItem carries non-empty content.
func (it *ResponseItem) HasProgress() bool {
	if it == nil {
		return false
	}
	if len(it.Raw) > 0 || it.Arguments != "" || it.Name != "" {
		return true
	}
	for _, p := range it.Content {
		if p.Text != "" || p.Refusal != "" {
			return true
		}
	}
	for _, p := range it.Summary {
		if p.Text != "" {
			return true
		}
	}
	return false
}

// Clone returns a deep copy of ResponseItem.
func (it *ResponseItem) Clone() *ResponseItem {
	if it == nil {
		return nil
	}
	cp := *it
	if len(it.Summary) > 0 {
		cp.Summary = make([]ResponseContentPart, len(it.Summary))
		for i, p := range it.Summary {
			cp.Summary[i] = p
			if len(p.Annotations) > 0 {
				cp.Summary[i].Annotations = append(json.RawMessage(nil), p.Annotations...)
			}
		}
	}
	if len(it.Content) > 0 {
		cp.Content = make([]ResponseContentPart, len(it.Content))
		for i, p := range it.Content {
			cp.Content[i] = p
			if len(p.Annotations) > 0 {
				cp.Content[i].Annotations = append(json.RawMessage(nil), p.Annotations...)
			}
		}
	}
	if len(it.Raw) > 0 {
		cp.Raw = append(json.RawMessage(nil), it.Raw...)
	}
	return &cp
}

// State captures accumulated generation progress across one or more streaming
// attempts. It is JSON-serializable for durable checkpointing.
type State struct {
	Endpoint          Endpoint `json:"endpoint"`
	ResponseID        string   `json:"response_id,omitempty"`
	Model             string   `json:"model,omitempty"`
	Created           int64    `json:"created,omitempty"`
	SystemFingerprint string   `json:"system_fingerprint,omitempty"`
	ServiceTier       string   `json:"service_tier,omitempty"`

	Attempt          int    `json:"attempt"`
	Completed        bool   `json:"completed,omitempty"`
	Capped           bool   `json:"capped,omitempty"`
	InReasoning      bool   `json:"in_reasoning,omitempty"`
	FinishReason     string `json:"finish_reason,omitempty"`
	Status           string `json:"status,omitempty"`
	IncompleteReason string `json:"incomplete_reason,omitempty"`

	Text                   string         `json:"text,omitempty"`
	Reasoning              string         `json:"reasoning,omitempty"`
	ToolCalls              []ChatToolCall `json:"tool_calls,omitempty"`
	DroppedPartialToolCall bool           `json:"dropped_partial_tool_call,omitempty"`

	CompletedItems []ResponseItem `json:"completed_items,omitempty"`
	PartialItem    *ResponseItem  `json:"partial_item,omitempty"`

	Usage             UsageState `json:"usage"`
	PromptTokenIDs    []int      `json:"prompt_token_ids,omitempty"`
	GeneratedTokenIDs []int      `json:"generated_token_ids,omitempty"`
}

// NewState initializes an empty State for the given endpoint.
func NewState(endpoint Endpoint) *State {
	return &State{Endpoint: endpoint}
}

// HasProgress reports whether any continuable partial output has been captured.
func (s *State) HasProgress() bool {
	if s == nil {
		return false
	}
	if s.Text != "" || s.Reasoning != "" || len(s.CompletedItems) > 0 || len(s.GeneratedTokenIDs) > 0 {
		return true
	}
	return s.PartialItem.HasProgress()
}

// CanMigrateLiveStream reports whether a live-streaming proxy (such as
// llm-d-router) can safely migrate the stream without duplicating tokens or
// corrupting a partially-emitted tool call.
func (s *State) CanMigrateLiveStream() bool {
	if s == nil {
		return false
	}
	return !s.Capped && !s.DroppedPartialToolCall
}

// Clone returns a deep copy of s.
func (s *State) Clone() *State {
	if s == nil {
		return nil
	}
	cp := *s
	if len(s.ToolCalls) > 0 {
		cp.ToolCalls = make([]ChatToolCall, len(s.ToolCalls))
		copy(cp.ToolCalls, s.ToolCalls)
	}
	if len(s.CompletedItems) > 0 {
		cp.CompletedItems = make([]ResponseItem, len(s.CompletedItems))
		for i := range s.CompletedItems {
			cp.CompletedItems[i] = *s.CompletedItems[i].Clone()
		}
	}
	if s.PartialItem != nil {
		cp.PartialItem = s.PartialItem.Clone()
	}
	if len(s.PromptTokenIDs) > 0 {
		cp.PromptTokenIDs = append([]int(nil), s.PromptTokenIDs...)
	}
	if len(s.GeneratedTokenIDs) > 0 {
		cp.GeneratedTokenIDs = append([]int(nil), s.GeneratedTokenIDs...)
	}
	return &cp
}
