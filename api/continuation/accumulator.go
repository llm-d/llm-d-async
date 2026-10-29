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

package continuation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Accumulator consumes SSE frames for a single streaming attempt on top of an
// optional prior State, maintaining both the full live state (used when the
// attempt finishes normally) and a token-capped checkpoint snapshot (used when
// persisting mid-stream checkpoints or resuming after an interruption).
type Accumulator struct {
	opts EndpointOptions

	priorGenTokens        int
	priorRecomputedTokens int
	priorReasoningTokens  int
	priorInReasoning      bool
	thinkSplitOffset      int
	thinkSplitResolved    bool
	attemptSawReasoning   bool

	attemptDeltaTokens     int
	attemptReasonDeltaToks int
	attemptUsageSeen       bool
	attemptPromptToks      int
	attemptCompToks        int
	attemptReasonToks      int

	live       *State
	cappedSnap *State

	// Per-attempt tool call map (index -> ChatToolCall) for chat completions.
	chatToolCalls map[int]*ChatToolCall

	// Per-attempt open items by output_index for /v1/responses.
	openRespItems    map[int]*ResponseItem
	mergedPriorItem  bool
	respThinkPending string
}

// EndpointOptions pairs an Endpoint with continuation Options.
type EndpointOptions struct {
	Endpoint Endpoint
	Options  Options
}

// NewAccumulator creates an Accumulator for starting a new streaming attempt.
// If prior is nil, a fresh Attempt 1 State is initialized for endpoint.
func NewAccumulator(endpoint Endpoint, prior *State, opts Options) *Accumulator {
	var base *State
	if prior != nil {
		base = prior.Clone()
		if base.Endpoint == "" {
			base.Endpoint = endpoint
		}
	} else {
		base = NewState(endpoint)
	}

	priorInReasoning := base.InReasoning
	thinkOffset := len(base.Text)

	// Clear transient completion/interruption markers from prior attempt.
	base.Attempt++
	base.Completed = false
	base.FinishReason = ""
	base.Status = ""
	base.IncompleteReason = ""
	base.ToolCalls = nil
	base.DroppedPartialToolCall = false

	acc := &Accumulator{
		opts:                  EndpointOptions{Endpoint: base.Endpoint, Options: opts},
		priorGenTokens:        base.Usage.GeneratedTokens,
		priorRecomputedTokens: base.Usage.RecomputedPromptTokens,
		priorReasoningTokens:  base.Usage.ReasoningTokens,
		priorInReasoning:      priorInReasoning,
		thinkSplitOffset:      thinkOffset,
		live:                  base,
		chatToolCalls:         make(map[int]*ChatToolCall),
		openRespItems:         make(map[int]*ResponseItem),
	}
	if base.Capped {
		acc.cappedSnap = acc.buildInterruptedState(base)
	}
	return acc
}

// ConsumeStream reads all SSE events from r until EOF, invoking onEvent (if
// non-nil) after each non-comment frame is processed. It returns
// ErrIncompleteStream if the stream closes before a terminal protocol marker.
func (a *Accumulator) ConsumeStream(r io.Reader, onEvent func(ev SSEEvent) error) error {
	sr := NewSSEReader(r)
	for {
		ev, err := sr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if err := a.FeedEvent(ev); err != nil {
			return err
		}
		if onEvent != nil {
			if err := onEvent(ev); err != nil {
				return err
			}
		}
		if a.live.Completed {
			return nil
		}
	}
	if !a.live.Completed {
		return ErrIncompleteStream
	}
	return nil
}

// AttemptDeltaTokens returns the number of generated tokens observed so far in
// the current attempt (exact when usage or token_ids were reported, otherwise
// delta count).
func (a *Accumulator) AttemptDeltaTokens() int {
	if a.attemptUsageSeen {
		return a.attemptCompToks
	}
	return a.attemptDeltaTokens
}

// FinalState returns the full accumulated state. Callers should inspect
// FinalState().Completed to verify the attempt reached a terminal marker.
func (a *Accumulator) FinalState() *State {
	st := a.live.Clone()
	if !st.Completed {
		return a.buildInterruptedState(st)
	}
	if len(a.chatToolCalls) > 0 {
		indices := make([]int, 0, len(a.chatToolCalls))
		for idx := range a.chatToolCalls {
			indices = append(indices, idx)
		}
		sort.Ints(indices)
		st.ToolCalls = make([]ChatToolCall, 0, len(indices))
		for _, idx := range indices {
			st.ToolCalls = append(st.ToolCalls, *a.chatToolCalls[idx])
		}
	}
	return st
}

// CheckpointSnapshot returns a safe, interruption-clean snapshot of partial
// progress suitable for persisting to a checkpoint store or passing to
// BuildContinuationRequest after a stream failure. Partial tool calls are
// stripped and MaxCheckpointTokens is respected.
func (a *Accumulator) CheckpointSnapshot() *State {
	if a.cappedSnap != nil {
		return a.cappedSnap.Clone()
	}
	return a.buildInterruptedState(a.live)
}

func (a *Accumulator) buildInterruptedState(src *State) *State {
	snap := src.Clone()
	snap.Completed = false
	snap.ToolCalls = nil
	if len(a.chatToolCalls) > 0 {
		snap.DroppedPartialToolCall = true
	}

	if snap.Endpoint == EndpointResponses {
		// Promote any in-progress continuable item to PartialItem; discard
		// incomplete function_call/tool items per #518 guard.
		if len(a.openRespItems) > 0 {
			indices := make([]int, 0, len(a.openRespItems))
			for idx := range a.openRespItems {
				indices = append(indices, idx)
			}
			sort.Ints(indices)
			for _, idx := range indices {
				it := a.openRespItems[idx]
				if it == nil {
					continue
				}
				switch it.Type {
				case "reasoning", "message":
					if it.HasProgress() {
						if snap.PartialItem != nil && snap.PartialItem.HasProgress() && snap.PartialItem.Type != it.Type {
							prev := snap.PartialItem.Clone()
							prev.Status = "completed"
							snap.CompletedItems = append(snap.CompletedItems, *prev)
						}
						cp := it.Clone()
						cp.Status = "incomplete"
						snap.PartialItem = cp
					}
				case "function_call":
					if snap.PartialItem != nil && snap.PartialItem.HasProgress() && snap.PartialItem.Type != "function_call" {
						prev := snap.PartialItem.Clone()
						prev.Status = "completed"
						snap.CompletedItems = append(snap.CompletedItems, *prev)
						snap.PartialItem = nil
					}
					snap.DroppedPartialToolCall = true
				}
			}
		} else if snap.PartialItem != nil && snap.PartialItem.Type == "function_call" {
			snap.PartialItem = nil
			snap.DroppedPartialToolCall = true
		}
	}
	return snap
}

func (a *Accumulator) updateUsageAndCap() {
	gen := a.priorGenTokens + a.AttemptDeltaTokens()
	a.live.Usage.GeneratedTokens = gen

	reasonDelta := a.attemptReasonDeltaToks
	if a.attemptUsageSeen {
		if a.live.Attempt == 1 || a.attemptSawReasoning || (a.priorInReasoning && a.thinkSplitResolved) {
			reasonDelta = a.attemptReasonToks
		} else {
			reasonDelta = 0
		}
	}
	a.live.Usage.ReasoningTokens = a.priorReasoningTokens + reasonDelta

	if a.live.Attempt == 1 {
		if a.attemptPromptToks > 0 {
			a.live.Usage.OriginalPromptTokens = a.attemptPromptToks
		} else if len(a.live.PromptTokenIDs) > 0 && a.live.Usage.OriginalPromptTokens == 0 {
			a.live.Usage.OriginalPromptTokens = len(a.live.PromptTokenIDs)
		}
	} else if a.attemptPromptToks > 0 {
		a.live.Usage.RecomputedPromptTokens = a.priorRecomputedTokens + a.attemptPromptToks
		if a.live.Usage.OriginalPromptTokens == 0 {
			if inferred := a.attemptPromptToks - a.priorGenTokens; inferred > 0 {
				a.live.Usage.OriginalPromptTokens = inferred
			}
		}
	}

	maxTok := a.opts.Options.MaxCheckpointTokens
	if maxTok > 0 && a.cappedSnap == nil && gen >= maxTok {
		a.live.Capped = true
		a.cappedSnap = a.buildInterruptedState(a.live)
	}
}

// FeedEvent ingests one parsed SSEEvent into the accumulator.
func (a *Accumulator) FeedEvent(ev SSEEvent) error {
	switch a.opts.Endpoint {
	case EndpointCompletions:
		return a.feedCompletions(ev)
	case EndpointChatCompletions:
		return a.feedChatCompletions(ev)
	case EndpointResponses:
		return a.feedResponses(ev)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedEndpoint, a.opts.Endpoint)
	}
}

type sseErrorEnvelope struct {
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func checkStreamErrorPayload(event string, data []byte) error {
	if event == "error" {
		msg := strings.TrimSpace(string(data))
		var env sseErrorEnvelope
		if json.Unmarshal(data, &env) == nil && env.Error != nil && env.Error.Message != "" {
			msg = env.Error.Message
		}
		return fmt.Errorf("%w: %s", ErrStreamError, msg)
	}
	var env sseErrorEnvelope
	if err := json.Unmarshal(data, &env); err == nil && env.Error != nil && (env.Error.Message != "" || env.Error.Type != "") {
		return fmt.Errorf("%w: %s", ErrStreamError, env.Error.Message)
	}
	return nil
}

type streamUsage struct {
	PromptTokens            int `json:"prompt_tokens"`
	CompletionTokens        int `json:"completion_tokens"`
	InputTokens             int `json:"input_tokens"`
	OutputTokens            int `json:"output_tokens"`
	CompletionTokensDetails *struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
	OutputTokensDetails *struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

func (a *Accumulator) applyUsage(u *streamUsage) {
	if u == nil {
		return
	}
	promptTok := u.PromptTokens
	if promptTok == 0 && u.InputTokens > 0 {
		promptTok = u.InputTokens
	}
	compTok := u.CompletionTokens
	if compTok == 0 && u.OutputTokens > 0 {
		compTok = u.OutputTokens
	}
	if promptTok > 0 {
		a.attemptPromptToks = promptTok
	}
	if compTok > 0 || promptTok > 0 {
		a.attemptUsageSeen = true
		a.attemptCompToks = compTok
	}
	if u.CompletionTokensDetails != nil && u.CompletionTokensDetails.ReasoningTokens > 0 {
		a.attemptReasonToks = u.CompletionTokensDetails.ReasoningTokens
	} else if u.OutputTokensDetails != nil && u.OutputTokensDetails.ReasoningTokens > 0 {
		a.attemptReasonToks = u.OutputTokensDetails.ReasoningTokens
	}
}

type completionsChunk struct {
	ID                string       `json:"id"`
	Model             string       `json:"model"`
	Created           int64        `json:"created"`
	SystemFingerprint string       `json:"system_fingerprint"`
	ServiceTier       string       `json:"service_tier"`
	PromptTokenIDs    []int        `json:"prompt_token_ids"`
	Usage             *streamUsage `json:"usage"`
	Choices           []struct {
		Index          int     `json:"index"`
		Text           string  `json:"text"`
		FinishReason   *string `json:"finish_reason"`
		PromptTokenIDs []int   `json:"prompt_token_ids"`
		TokenIDs       []int   `json:"token_ids"`
	} `json:"choices"`
}

func (a *Accumulator) feedCompletions(ev SSEEvent) error {
	if ev.Done {
		a.live.Completed = true
		if a.live.FinishReason == "" {
			a.live.FinishReason = "stop"
		}
		return nil
	}
	if len(bytes.TrimSpace(ev.Data)) == 0 {
		return nil
	}
	if err := checkStreamErrorPayload(ev.Event, ev.Data); err != nil {
		return err
	}

	var ch completionsChunk
	if err := json.Unmarshal(ev.Data, &ch); err != nil {
		return fmt.Errorf("parse completions sse chunk: %w", err)
	}

	a.recordEnvelopeMeta(ch.ID, ch.Model, ch.Created, ch.SystemFingerprint, ch.ServiceTier, ch.PromptTokenIDs)
	for _, c := range ch.Choices {
		if c.Index != 0 {
			continue
		}
		if len(c.PromptTokenIDs) > 0 {
			a.recordEnvelopeMeta("", "", 0, "", "", c.PromptTokenIDs)
		}
		if c.Text != "" {
			a.live.Text += c.Text
		}
		if len(c.TokenIDs) > 0 {
			a.live.GeneratedTokenIDs = append(a.live.GeneratedTokenIDs, c.TokenIDs...)
			a.attemptDeltaTokens += len(c.TokenIDs)
		} else if c.Text != "" {
			a.attemptDeltaTokens++
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			a.live.FinishReason = *c.FinishReason
		}
	}
	a.applyUsage(ch.Usage)
	a.updateUsageAndCap()
	return nil
}

type chatChunk struct {
	ID                string       `json:"id"`
	Model             string       `json:"model"`
	Created           int64        `json:"created"`
	SystemFingerprint string       `json:"system_fingerprint"`
	ServiceTier       string       `json:"service_tier"`
	PromptTokenIDs    []int        `json:"prompt_token_ids"`
	Usage             *streamUsage `json:"usage"`
	Choices           []struct {
		Index int `json:"index"`
		Delta struct {
			Role             string         `json:"role"`
			Content          string         `json:"content"`
			Reasoning        string         `json:"reasoning"`
			ReasoningContent string         `json:"reasoning_content"`
			ToolCalls        []ChatToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
		TokenIDs     []int   `json:"token_ids"`
	} `json:"choices"`
}

func (a *Accumulator) feedChatCompletions(ev SSEEvent) error {
	if ev.Done {
		a.live.Completed = true
		if a.live.FinishReason == "" {
			if len(a.chatToolCalls) > 0 {
				a.live.FinishReason = "tool_calls"
			} else {
				a.live.FinishReason = "stop"
			}
		}
		return nil
	}
	if len(bytes.TrimSpace(ev.Data)) == 0 {
		return nil
	}
	if err := checkStreamErrorPayload(ev.Event, ev.Data); err != nil {
		return err
	}

	var ch chatChunk
	if err := json.Unmarshal(ev.Data, &ch); err != nil {
		return fmt.Errorf("parse chat completions sse chunk: %w", err)
	}

	a.recordEnvelopeMeta(ch.ID, ch.Model, ch.Created, ch.SystemFingerprint, ch.ServiceTier, ch.PromptTokenIDs)
	for _, c := range ch.Choices {
		if c.Index != 0 {
			continue
		}
		// vLLM copies reasoning into reasoning_content on every chunk; prefer
		// reasoning and fall back only when empty to avoid double-counting.
		rDelta := c.Delta.Reasoning
		if rDelta == "" {
			rDelta = c.Delta.ReasoningContent
		}
		emittedAny := false
		if rDelta != "" {
			a.live.Reasoning += rDelta
			a.attemptSawReasoning = true
			if a.live.Text == "" {
				a.live.InReasoning = true
			}
			if len(c.TokenIDs) > 0 && c.Delta.Content == "" {
				a.attemptReasonDeltaToks += len(c.TokenIDs)
			} else {
				a.attemptReasonDeltaToks++
			}
			emittedAny = true
		}
		if c.Delta.Content != "" {
			a.appendChatContent(c.Delta.Content)
			emittedAny = true
		}
		for _, tc := range c.Delta.ToolCalls {
			a.live.InReasoning = false
			existing, ok := a.chatToolCalls[tc.Index]
			if !ok {
				cp := tc
				a.chatToolCalls[tc.Index] = &cp
			} else {
				if tc.ID != "" {
					existing.ID = tc.ID
				}
				if tc.Type != "" {
					existing.Type = tc.Type
				}
				if tc.Function.Name != "" {
					existing.Function.Name += tc.Function.Name
				}
				if tc.Function.Arguments != "" {
					existing.Function.Arguments += tc.Function.Arguments
				}
			}
			emittedAny = true
		}
		if len(c.TokenIDs) > 0 {
			a.live.GeneratedTokenIDs = append(a.live.GeneratedTokenIDs, c.TokenIDs...)
			a.attemptDeltaTokens += len(c.TokenIDs)
		} else if emittedAny {
			a.attemptDeltaTokens++
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			a.live.FinishReason = *c.FinishReason
		}
	}
	a.applyUsage(ch.Usage)
	a.updateUsageAndCap()
	return nil
}

func (a *Accumulator) appendChatContent(delta string) {
	a.live.Text += delta
	if a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved {
		tag := a.opts.Options.thinkEndTag()
		tail := a.live.Text[a.thinkSplitOffset:]
		if idx := strings.Index(tail, tag); idx >= 0 {
			extraReasoning := tail[:idx]
			after := tail[idx+len(tag):]
			after = strings.TrimPrefix(after, "\n")
			a.live.Reasoning += extraReasoning
			a.live.Text = a.live.Text[:a.thinkSplitOffset] + after
			a.live.InReasoning = false
			a.thinkSplitResolved = true
		}
		return
	}
	a.live.InReasoning = false
}

func (a *Accumulator) recordEnvelopeMeta(id, model string, created int64, fp, tier string, promptTokIDs []int) {
	if a.live.ResponseID == "" && id != "" {
		a.live.ResponseID = id
	}
	if a.live.Model == "" && model != "" {
		a.live.Model = model
	}
	if a.live.Created == 0 && created > 0 {
		a.live.Created = created
	}
	if a.live.SystemFingerprint == "" && fp != "" {
		a.live.SystemFingerprint = fp
	}
	if a.live.ServiceTier == "" && tier != "" {
		a.live.ServiceTier = tier
	}
	if a.live.Attempt == 1 && len(a.live.PromptTokenIDs) == 0 && len(promptTokIDs) > 0 {
		a.live.PromptTokenIDs = append([]int(nil), promptTokIDs...)
	}
}

type responsesStreamFrame struct {
	Type        string          `json:"type"`
	OutputIndex int             `json:"output_index"`
	ContentIdx  int             `json:"content_index"`
	SummaryIdx  int             `json:"summary_index"`
	ItemID      string          `json:"item_id"`
	Delta       string          `json:"delta"`
	Item        json.RawMessage `json:"item"`
	Response    *struct {
		ID                string       `json:"id"`
		Model             string       `json:"model"`
		CreatedAt         int64        `json:"created_at"`
		Status            string       `json:"status"`
		ServiceTier       string       `json:"service_tier"`
		Usage             *streamUsage `json:"usage"`
		IncompleteDetails *struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"response"`
}

func (a *Accumulator) feedResponses(ev SSEEvent) error {
	if ev.Done {
		// Some OpenAI-compatible proxies emit a trailing [DONE] after
		// response.completed; ignore if already completed.
		return nil
	}
	if len(bytes.TrimSpace(ev.Data)) == 0 {
		return nil
	}

	var frame responsesStreamFrame
	if err := json.Unmarshal(ev.Data, &frame); err != nil {
		return fmt.Errorf("parse responses sse event: %w", err)
	}
	evType := ev.Event
	if evType == "" {
		evType = frame.Type
	}
	if evType == "error" || evType == "response.failed" {
		if frame.Response != nil && frame.Response.Error != nil && frame.Response.Error.Message != "" {
			return fmt.Errorf("%w: %s", ErrStreamError, frame.Response.Error.Message)
		}
		return checkStreamErrorPayload("error", ev.Data)
	}

	if frame.Response != nil {
		a.recordEnvelopeMeta(frame.Response.ID, frame.Response.Model, frame.Response.CreatedAt, "", frame.Response.ServiceTier, nil)
		a.applyUsage(frame.Response.Usage)
	}

	switch evType {
	case "response.created", "response.in_progress":
		// Envelope metadata already recorded above.

	case "response.output_item.added":
		var it ResponseItem
		if len(frame.Item) > 0 {
			if err := json.Unmarshal(frame.Item, &it); err != nil {
				return fmt.Errorf("parse response.output_item.added item: %w", err)
			}
		}
		open := a.adoptOrInitRespItem(&it)
		a.openRespItems[frame.OutputIndex] = open

	case "response.reasoning_text.delta":
		open := a.ensureOpenRespItem(frame.OutputIndex, "reasoning", frame.ItemID)
		appendRespPart(&open.Content, "reasoning_text", frame.Delta)
		a.attemptSawReasoning = true
		a.live.InReasoning = true
		if frame.Delta != "" {
			a.attemptDeltaTokens++
			a.attemptReasonDeltaToks++
		}

	case "response.reasoning_summary_text.delta":
		open := a.ensureOpenRespItem(frame.OutputIndex, "reasoning", frame.ItemID)
		appendRespPart(&open.Summary, "summary_text", frame.Delta)
		a.attemptSawReasoning = true
		a.live.InReasoning = true
		if frame.Delta != "" {
			a.attemptDeltaTokens++
			a.attemptReasonDeltaToks++
		}

	case "response.output_text.delta":
		a.handleRespOutputTextDelta(frame.OutputIndex, frame.ItemID, frame.Delta)

	case "response.function_call_arguments.delta":
		open := a.ensureOpenRespItem(frame.OutputIndex, "function_call", frame.ItemID)
		open.Arguments += frame.Delta
		a.live.InReasoning = false
		if frame.Delta != "" {
			a.attemptDeltaTokens++
		}

	case "response.output_item.done":
		if err := a.finalizeRespItem(frame.OutputIndex, frame.Item); err != nil {
			return err
		}

	case "response.completed", "response.incomplete":
		// Flush any open items not yet closed by output_item.done.
		if len(a.openRespItems) > 0 {
			indices := make([]int, 0, len(a.openRespItems))
			for idx := range a.openRespItems {
				indices = append(indices, idx)
			}
			sort.Ints(indices)
			for _, idx := range indices {
				if err := a.finalizeRespItem(idx, nil); err != nil {
					return err
				}
			}
		}
		if frame.Response != nil && frame.Response.Status != "" {
			a.live.Status = frame.Response.Status
			if frame.Response.IncompleteDetails != nil {
				a.live.IncompleteReason = frame.Response.IncompleteDetails.Reason
			}
		} else if evType == "response.incomplete" {
			a.live.Status = "incomplete"
		} else {
			a.live.Status = "completed"
		}
		a.live.Completed = true
	}

	a.updateUsageAndCap()
	return nil
}

func (a *Accumulator) adoptOrInitRespItem(added *ResponseItem) *ResponseItem {
	// If continuing an incomplete item of the same type from a prior attempt,
	// merge into PartialItem so the output keeps Attempt 1's item ID and prefix.
	if !a.mergedPriorItem && a.live.PartialItem != nil && a.live.PartialItem.Type == added.Type {
		a.mergedPriorItem = true
		merged := a.live.PartialItem.Clone()
		a.live.PartialItem = nil
		return merged
	}
	cp := added.Clone()
	if cp.Role == "" && cp.Type == "message" {
		cp.Role = "assistant"
	}
	return cp
}

func (a *Accumulator) ensureOpenRespItem(outputIdx int, itemType, itemID string) *ResponseItem {
	if open, ok := a.openRespItems[outputIdx]; ok && open.Type == itemType {
		// vLLM rotates item_id on the first message delta after reasoning; adopt
		// the updated ID unless we merged Attempt 1's PartialItem ID.
		if itemID != "" && (open.ID == "" || !a.mergedPriorItem) {
			open.ID = itemID
		}
		return open
	}
	seed := &ResponseItem{ID: itemID, Type: itemType}
	if itemType == "message" {
		seed.Role = "assistant"
	}
	open := a.adoptOrInitRespItem(seed)
	a.openRespItems[outputIdx] = open
	return open
}

func (a *Accumulator) handleRespOutputTextDelta(outputIdx int, itemID, delta string) {
	open := a.ensureOpenRespItem(outputIdx, "message", itemID)
	appendRespPart(&open.Content, "output_text", delta)
	if delta != "" {
		a.attemptDeltaTokens++
	}

	// Recover mid-reasoning split on /v1/responses when prior attempt stopped
	// inside an incomplete reasoning item and Attempt 2 emitted </think> inside
	// output_text.delta.
	if a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved {
		a.respThinkPending += delta
		tag := a.opts.Options.thinkEndTag()
		if idx := strings.Index(a.respThinkPending, tag); idx >= 0 {
			extraReasoning := a.respThinkPending[:idx]
			after := strings.TrimPrefix(a.respThinkPending[idx+len(tag):], "\n")
			if a.live.PartialItem != nil && a.live.PartialItem.Type == "reasoning" {
				rItem := a.live.PartialItem.Clone()
				appendRespPart(&rItem.Content, "reasoning_text", extraReasoning)
				rItem.Status = "completed"
				a.live.CompletedItems = append(a.live.CompletedItems, *rItem)
				a.live.PartialItem = nil
				a.mergedPriorItem = true
			}
			open.Content = []ResponseContentPart{{Type: "output_text", Text: after}}
			a.live.InReasoning = false
			a.thinkSplitResolved = true
		}
		return
	}
	a.live.InReasoning = false
}

func (a *Accumulator) finalizeRespItem(outputIdx int, rawDone json.RawMessage) error {
	open := a.openRespItems[outputIdx]
	delete(a.openRespItems, outputIdx)

	var doneItem ResponseItem
	hasDone := false
	if len(rawDone) > 0 {
		if err := json.Unmarshal(rawDone, &doneItem); err != nil {
			return fmt.Errorf("parse response.output_item.done: %w", err)
		}
		hasDone = true
	}

	if open == nil {
		if !hasDone {
			return nil
		}
		open = a.adoptOrInitRespItem(&doneItem)
		if open.Type == "function_call" || (!open.HasProgress() && doneItem.HasProgress()) {
			open = &doneItem
			open.Raw = append(json.RawMessage(nil), rawDone...)
		}
	} else if hasDone {
		if open.ID == "" && doneItem.ID != "" {
			open.ID = doneItem.ID
		}
		if open.Type == "function_call" {
			open.CallID = doneItem.CallID
			open.Name = doneItem.Name
			if doneItem.Arguments != "" {
				open.Arguments = doneItem.Arguments
			}
			open.Raw = append(json.RawMessage(nil), rawDone...)
		} else if !open.HasProgress() && doneItem.HasProgress() {
			// Fallback when a server sends full text only on output_item.done.
			open.Content = doneItem.Content
			open.Summary = doneItem.Summary
		}
	}

	if a.live.PartialItem != nil && a.live.PartialItem.HasProgress() && a.live.PartialItem.Type != open.Type {
		prev := a.live.PartialItem.Clone()
		prev.Status = "completed"
		a.live.CompletedItems = append(a.live.CompletedItems, *prev)
		a.live.PartialItem = nil
	}
	open.Status = "completed"
	if open.Type == "reasoning" {
		// Reasoning finished; next item transitions out of reasoning.
		a.live.InReasoning = false
	}
	a.live.CompletedItems = append(a.live.CompletedItems, *open)
	return nil
}

func appendRespPart(parts *[]ResponseContentPart, partType, delta string) {
	for i := range *parts {
		if (*parts)[i].Type == partType {
			(*parts)[i].Text += delta
			return
		}
	}
	*parts = append(*parts, ResponseContentPart{
		Type: partType,
		Text: delta,
	})
}
