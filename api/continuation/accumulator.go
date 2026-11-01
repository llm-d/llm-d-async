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
	endpoint Endpoint
	opts     Options

	priorGenTokens        int
	priorRecomputedTokens int
	priorReasoningTokens  int
	priorInReasoning      bool
	thinkSplitOffset      int
	thinkSplitResolved    bool
	attemptSawReasoning   bool

	attemptDeltaTokens       int
	deltasSinceUsage         int
	attemptReasonDeltaToks   int
	attemptUsageSeen         bool
	attemptReasonDetailsSeen bool
	attemptPromptToks        int
	attemptCompToks          int
	attemptReasonToks        int

	streamDone bool
	live       *State
	cappedSnap *State

	// Amortized O(1) byte buffers for growing text/reasoning across chunks.
	textBuf      []byte
	reasoningBuf []byte

	// Per-attempt tool call map (index -> openToolCall) for chat completions.
	chatToolCalls map[int]*openToolCall

	// Per-attempt open items by output_index for /v1/responses.
	openRespItems    map[int]*openRespItem
	mergedPriorItem  bool
	respThinkPending []byte
}

type openToolCall struct {
	index   int
	id      string
	typ     string
	nameBuf []byte
	argsBuf []byte
}

func (tc *openToolCall) toChatToolCall() ChatToolCall {
	return ChatToolCall{
		Index: tc.index,
		ID:    tc.id,
		Type:  tc.typ,
		Function: ChatToolCallFunction{
			Name:      string(tc.nameBuf),
			Arguments: string(tc.argsBuf),
		},
	}
}

type openRespPart struct {
	partType    string
	textBuf     []byte
	refusal     string
	annotations json.RawMessage
}

type openRespItem struct {
	id      string
	typ     string
	role    string
	status  string
	summary []openRespPart
	content []openRespPart
	callID  string
	name    string
	argsBuf []byte
	raw     json.RawMessage
}

func newOpenRespItem(it *ResponseItem) *openRespItem {
	if it == nil {
		return &openRespItem{}
	}
	open := &openRespItem{
		id:     it.ID,
		typ:    it.Type,
		role:   it.Role,
		status: it.Status,
		callID: it.CallID,
		name:   it.Name,
	}
	if it.Arguments != "" {
		open.argsBuf = []byte(it.Arguments)
	}
	if len(it.Raw) > 0 {
		open.raw = append(json.RawMessage(nil), it.Raw...)
	}
	if len(it.Summary) > 0 {
		open.summary = make([]openRespPart, len(it.Summary))
		for i, p := range it.Summary {
			open.summary[i] = openRespPart{
				partType:    p.Type,
				textBuf:     []byte(p.Text),
				refusal:     p.Refusal,
				annotations: append(json.RawMessage(nil), p.Annotations...),
			}
		}
	}
	if len(it.Content) > 0 {
		open.content = make([]openRespPart, len(it.Content))
		for i, p := range it.Content {
			open.content[i] = openRespPart{
				partType:    p.Type,
				textBuf:     []byte(p.Text),
				refusal:     p.Refusal,
				annotations: append(json.RawMessage(nil), p.Annotations...),
			}
		}
	}
	return open
}

func (o *openRespItem) appendSummary(partType, delta string) {
	appendOpenPart(&o.summary, partType, delta)
}

func (o *openRespItem) appendContent(partType, delta string) {
	appendOpenPart(&o.content, partType, delta)
}

func appendOpenPart(parts *[]openRespPart, partType, delta string) {
	for i := range *parts {
		if (*parts)[i].partType == partType {
			(*parts)[i].textBuf = append((*parts)[i].textBuf, delta...)
			return
		}
	}
	*parts = append(*parts, openRespPart{
		partType: partType,
		textBuf:  []byte(delta),
	})
}

func (o *openRespItem) toResponseItem() *ResponseItem {
	if o == nil {
		return nil
	}
	it := &ResponseItem{
		ID:        o.id,
		Type:      o.typ,
		Role:      o.role,
		Status:    o.status,
		CallID:    o.callID,
		Name:      o.name,
		Arguments: string(o.argsBuf),
	}
	if len(o.raw) > 0 {
		it.Raw = append(json.RawMessage(nil), o.raw...)
	}
	if len(o.summary) > 0 {
		it.Summary = make([]ResponseContentPart, len(o.summary))
		for i, p := range o.summary {
			it.Summary[i] = ResponseContentPart{
				Type:        p.partType,
				Text:        string(p.textBuf),
				Refusal:     p.refusal,
				Annotations: append(json.RawMessage(nil), p.annotations...),
			}
		}
	}
	if len(o.content) > 0 {
		it.Content = make([]ResponseContentPart, len(o.content))
		for i, p := range o.content {
			it.Content[i] = ResponseContentPart{
				Type:        p.partType,
				Text:        string(p.textBuf),
				Refusal:     p.refusal,
				Annotations: append(json.RawMessage(nil), p.annotations...),
			}
		}
	}
	return it
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
	var textBuf, reasoningBuf []byte
	if base.Text != "" {
		textBuf = []byte(base.Text)
	}
	if base.Reasoning != "" {
		reasoningBuf = []byte(base.Reasoning)
	}

	// Clear transient completion/interruption/cap markers from prior attempt so
	// the new attempt can advance checkpoints by up to MaxCheckpointTokens.
	base.Attempt++
	base.Completed = false
	base.Capped = false
	base.FinishReason = ""
	base.Status = ""
	base.IncompleteReason = ""
	base.ToolCalls = nil
	base.DroppedPartialToolCall = false

	return &Accumulator{
		endpoint:              base.Endpoint,
		opts:                  opts,
		priorGenTokens:        base.Usage.GeneratedTokens,
		priorRecomputedTokens: base.Usage.RecomputedPromptTokens,
		priorReasoningTokens:  base.Usage.ReasoningTokens,
		priorInReasoning:      priorInReasoning,
		thinkSplitOffset:      thinkOffset,
		live:                  base,
		textBuf:               textBuf,
		reasoningBuf:          reasoningBuf,
		chatToolCalls:         make(map[int]*openToolCall),
		openRespItems:         make(map[int]*openRespItem),
	}
}

// ConsumeStream reads all SSE events from r until a terminal protocol frame or
// EOF, invoking onEvent (if non-nil) after each non-comment frame is processed.
// It returns ErrIncompleteStream if the stream closes before generation
// completed.
func (a *Accumulator) ConsumeStream(r io.Reader, onEvent func(ev SSEEvent) error) error {
	sr := NewSSEReader(r)
	for {
		ev, err := sr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
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
		if a.streamDone {
			return nil
		}
	}
	if !a.live.Completed {
		return ErrIncompleteStream
	}
	return nil
}

// AttemptDeltaTokens returns the number of generated tokens observed so far in
// the current attempt (exact when usage with completion_tokens > 0 or token_ids
// were reported, plus any subsequent chunks lacking usage).
func (a *Accumulator) AttemptDeltaTokens() int {
	if a.attemptUsageSeen {
		return a.attemptCompToks + a.deltasSinceUsage
	}
	return a.attemptDeltaTokens
}

// FinalState returns the full accumulated state. Callers should inspect
// FinalState().Completed to verify the attempt reached a terminal marker.
func (a *Accumulator) FinalState() *State {
	if !a.live.Completed {
		return a.buildInterruptedState()
	}
	st := a.live.Clone()
	st.Text = string(a.textBuf)
	st.Reasoning = string(a.reasoningBuf)

	// If a chat continuation stopped naturally (e.g. finish_reason="length")
	// while still inside an unclosed thinking block emitted in delta.content,
	// keep those tokens in Reasoning rather than leaking them into Text.
	if st.Endpoint == EndpointChatCompletions &&
		a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved &&
		len(a.textBuf) > a.thinkSplitOffset {
		unclosed := trimPartialTagSuffix(string(a.textBuf[a.thinkSplitOffset:]), a.opts.thinkEndTag())
		st.Reasoning = string(a.reasoningBuf) + unclosed
		st.Text = string(a.textBuf[:a.thinkSplitOffset])
		st.InReasoning = true
	}

	if len(a.chatToolCalls) > 0 {
		indices := make([]int, 0, len(a.chatToolCalls))
		for idx := range a.chatToolCalls {
			indices = append(indices, idx)
		}
		sort.Ints(indices)
		st.ToolCalls = make([]ChatToolCall, 0, len(indices))
		for _, idx := range indices {
			st.ToolCalls = append(st.ToolCalls, a.chatToolCalls[idx].toChatToolCall())
		}
	}
	return st
}

// CheckpointSnapshot returns a safe, interruption-clean snapshot of partial
// progress suitable for persisting to a checkpoint store or passing to
// BuildContinuationRequest after a stream failure. Partial tool calls are
// stripped and MaxCheckpointTokens is respected per attempt.
func (a *Accumulator) CheckpointSnapshot() *State {
	if a.cappedSnap != nil {
		return a.cappedSnap.Clone()
	}
	return a.buildInterruptedState()
}

func (a *Accumulator) buildInterruptedState() *State {
	snap := a.live.Clone()
	snap.Text = string(a.textBuf)
	snap.Reasoning = string(a.reasoningBuf)
	snap.Completed = false
	snap.ToolCalls = nil
	if len(a.chatToolCalls) > 0 {
		snap.DroppedPartialToolCall = true
	}

	// If a chat continuation resumed mid-reasoning and emitted remaining
	// reasoning tokens inside delta.content without yet reaching </think>, fold
	// those unclosed tokens back into Reasoning so a subsequent attempt keeps
	// continuing inside the thinking block instead of leaking reasoning into
	// assistant content.
	if snap.Endpoint == EndpointChatCompletions &&
		a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved &&
		len(a.textBuf) > a.thinkSplitOffset {
		unclosed := trimPartialTagSuffix(string(a.textBuf[a.thinkSplitOffset:]), a.opts.thinkEndTag())
		snap.Reasoning = string(a.reasoningBuf) + unclosed
		snap.Text = string(a.textBuf[:a.thinkSplitOffset])
		snap.InReasoning = true
	}

	if snap.Endpoint == EndpointResponses {
		unclosedThinkInMsg := a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved &&
			snap.PartialItem != nil && snap.PartialItem.Type == "reasoning"
		if unclosedThinkInMsg && len(a.respThinkPending) > 0 {
			extra := trimPartialTagSuffix(string(a.respThinkPending), a.opts.thinkEndTag())
			if extra != "" {
				appendRespPart(&snap.PartialItem.Content, "reasoning_text", extra)
			}
			snap.PartialItem.Status = "incomplete"
			snap.InReasoning = true
		}

		// Promote any in-progress continuable item to PartialItem; discard
		// incomplete function_call/tool items per #518 guard.
		if len(a.openRespItems) > 0 {
			indices := make([]int, 0, len(a.openRespItems))
			for idx := range a.openRespItems {
				indices = append(indices, idx)
			}
			sort.Ints(indices)
			for _, idx := range indices {
				open := a.openRespItems[idx]
				if open == nil {
					continue
				}
				switch open.typ {
				case "reasoning", "message":
					if unclosedThinkInMsg && open.typ == "message" {
						// Synthetic message item only buffered unclosed reasoning
						// tokens already folded into snap.PartialItem above.
						continue
					}
					it := open.toResponseItem()
					if it.HasProgress() {
						if snap.PartialItem != nil && snap.PartialItem.HasProgress() && snap.PartialItem.Type != it.Type {
							prev := snap.PartialItem.Clone()
							prev.Status = "completed"
							snap.CompletedItems = append(snap.CompletedItems, *prev)
						}
						it.Status = "incomplete"
						snap.PartialItem = it
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

// trimPartialTagSuffix strips any trailing proper prefix of tag (such as "</th"
// when tag is "</think>") so an interruption mid-delimiter does not split the
// closing tag across attempts.
func trimPartialTagSuffix(s, tag string) string {
	maxCheck := len(tag) - 1
	if maxCheck > len(s) {
		maxCheck = len(s)
	}
	for k := maxCheck; k >= 1; k-- {
		if strings.HasSuffix(s, tag[:k]) {
			return s[:len(s)-k]
		}
	}
	return s
}

func (a *Accumulator) updateUsageAndCap() {
	attemptDelta := a.AttemptDeltaTokens()
	gen := a.priorGenTokens + attemptDelta
	a.live.Usage.GeneratedTokens = gen

	reasonDelta := a.attemptReasonDeltaToks
	if a.attemptReasonDetailsSeen {
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

	maxTok := a.opts.MaxCheckpointTokens
	if maxTok > 0 && a.cappedSnap == nil && attemptDelta >= maxTok {
		a.live.Capped = true
		a.cappedSnap = a.buildInterruptedState()
	}
}

// FeedEvent ingests one parsed SSEEvent into the accumulator.
func (a *Accumulator) FeedEvent(ev SSEEvent) error {
	switch a.endpoint {
	case EndpointCompletions:
		return a.feedCompletions(ev)
	case EndpointChatCompletions:
		return a.feedChatCompletions(ev)
	case EndpointResponses:
		return a.feedResponses(ev)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedEndpoint, a.endpoint)
	}
}

type sseErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

type sseErrorEnvelope struct {
	Error *sseErrorDetail `json:"error"`
}

func formatStreamErrorEvent(data []byte) error {
	msg := strings.TrimSpace(string(data))
	var env sseErrorEnvelope
	if json.Unmarshal(data, &env) == nil && env.Error != nil && env.Error.Message != "" {
		msg = env.Error.Message
	}
	return fmt.Errorf("%w: %s", ErrStreamError, msg)
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

func (a *Accumulator) applyUsage(u *streamUsage, chunkDeltas int) {
	if u == nil {
		a.deltasSinceUsage += chunkDeltas
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
	if compTok > 0 {
		a.attemptUsageSeen = true
		a.attemptCompToks = compTok
		a.deltasSinceUsage = 0
	} else {
		a.deltasSinceUsage += chunkDeltas
	}
	if u.CompletionTokensDetails != nil {
		a.attemptReasonDetailsSeen = true
		a.attemptReasonToks = u.CompletionTokensDetails.ReasoningTokens
	} else if u.OutputTokensDetails != nil {
		a.attemptReasonDetailsSeen = true
		a.attemptReasonToks = u.OutputTokensDetails.ReasoningTokens
	}
}

type completionsChunk struct {
	ID                string          `json:"id"`
	Model             string          `json:"model"`
	Created           int64           `json:"created"`
	SystemFingerprint string          `json:"system_fingerprint"`
	ServiceTier       string          `json:"service_tier"`
	PromptTokenIDs    []int           `json:"prompt_token_ids"`
	Usage             *streamUsage    `json:"usage"`
	Error             *sseErrorDetail `json:"error"`
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
		a.streamDone = true
		if a.live.FinishReason == "" {
			a.live.FinishReason = "stop"
		}
		return nil
	}
	if ev.Event == "error" {
		return formatStreamErrorEvent(ev.Data)
	}
	if len(bytes.TrimSpace(ev.Data)) == 0 {
		return nil
	}

	var ch completionsChunk
	if err := json.Unmarshal(ev.Data, &ch); err != nil {
		return fmt.Errorf("parse completions sse chunk: %w", err)
	}
	if ch.Error != nil && (ch.Error.Message != "" || ch.Error.Type != "") {
		return fmt.Errorf("%w: %s", ErrStreamError, ch.Error.Message)
	}

	a.recordEnvelopeMeta(ch.ID, ch.Model, ch.Created, ch.SystemFingerprint, ch.ServiceTier, ch.PromptTokenIDs)
	chunkDeltas := 0
	for _, c := range ch.Choices {
		if c.Index != 0 {
			continue
		}
		if len(c.PromptTokenIDs) > 0 {
			a.recordEnvelopeMeta("", "", 0, "", "", c.PromptTokenIDs)
		}
		if c.Text != "" {
			a.textBuf = append(a.textBuf, c.Text...)
		}
		if len(c.TokenIDs) > 0 {
			a.live.GeneratedTokenIDs = append(a.live.GeneratedTokenIDs, c.TokenIDs...)
			chunkDeltas += len(c.TokenIDs)
		} else if c.Text != "" {
			chunkDeltas++
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			a.live.FinishReason = *c.FinishReason
			a.live.Completed = true
		}
	}
	a.attemptDeltaTokens += chunkDeltas
	a.applyUsage(ch.Usage, chunkDeltas)
	a.updateUsageAndCap()
	return nil
}

type chatChunk struct {
	ID                string          `json:"id"`
	Model             string          `json:"model"`
	Created           int64           `json:"created"`
	SystemFingerprint string          `json:"system_fingerprint"`
	ServiceTier       string          `json:"service_tier"`
	PromptTokenIDs    []int           `json:"prompt_token_ids"`
	Usage             *streamUsage    `json:"usage"`
	Error             *sseErrorDetail `json:"error"`
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
		a.streamDone = true
		if a.live.FinishReason == "" {
			if len(a.chatToolCalls) > 0 {
				a.live.FinishReason = "tool_calls"
			} else {
				a.live.FinishReason = "stop"
			}
		}
		return nil
	}
	if ev.Event == "error" {
		return formatStreamErrorEvent(ev.Data)
	}
	if len(bytes.TrimSpace(ev.Data)) == 0 {
		return nil
	}

	var ch chatChunk
	if err := json.Unmarshal(ev.Data, &ch); err != nil {
		return fmt.Errorf("parse chat completions sse chunk: %w", err)
	}
	if ch.Error != nil && (ch.Error.Message != "" || ch.Error.Type != "") {
		return fmt.Errorf("%w: %s", ErrStreamError, ch.Error.Message)
	}

	a.recordEnvelopeMeta(ch.ID, ch.Model, ch.Created, ch.SystemFingerprint, ch.ServiceTier, ch.PromptTokenIDs)
	chunkDeltas := 0
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
			a.reasoningBuf = append(a.reasoningBuf, rDelta...)
			a.attemptSawReasoning = true
			if len(a.textBuf) == a.thinkSplitOffset {
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
			a.appendChatContent(c.Delta.Content, len(c.TokenIDs))
			emittedAny = true
		}
		for _, tc := range c.Delta.ToolCalls {
			a.live.InReasoning = false
			existing, ok := a.chatToolCalls[tc.Index]
			if !ok {
				existing = &openToolCall{
					index: tc.Index,
					id:    tc.ID,
					typ:   tc.Type,
				}
				if tc.Function.Name != "" {
					existing.nameBuf = append(existing.nameBuf, tc.Function.Name...)
				}
				if tc.Function.Arguments != "" {
					existing.argsBuf = append(existing.argsBuf, tc.Function.Arguments...)
				}
				a.chatToolCalls[tc.Index] = existing
			} else {
				if tc.ID != "" {
					existing.id = tc.ID
				}
				if tc.Type != "" {
					existing.typ = tc.Type
				}
				if tc.Function.Name != "" {
					existing.nameBuf = append(existing.nameBuf, tc.Function.Name...)
				}
				if tc.Function.Arguments != "" {
					existing.argsBuf = append(existing.argsBuf, tc.Function.Arguments...)
				}
			}
			emittedAny = true
		}
		if len(c.TokenIDs) > 0 {
			a.live.GeneratedTokenIDs = append(a.live.GeneratedTokenIDs, c.TokenIDs...)
			chunkDeltas += len(c.TokenIDs)
		} else if emittedAny {
			chunkDeltas++
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			a.live.FinishReason = *c.FinishReason
			a.live.Completed = true
		}
	}
	a.attemptDeltaTokens += chunkDeltas
	a.applyUsage(ch.Usage, chunkDeltas)
	a.updateUsageAndCap()
	return nil
}

func (a *Accumulator) appendChatContent(delta string, tokenIDCount int) {
	a.textBuf = append(a.textBuf, delta...)
	if a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved {
		tag := []byte(a.opts.thinkEndTag())
		tail := a.textBuf[a.thinkSplitOffset:]
		if idx := bytes.Index(tail, tag); idx >= 0 {
			a.reasoningBuf = append(a.reasoningBuf, tail[:idx]...)
			after := bytes.TrimPrefix(tail[idx+len(tag):], []byte("\n"))
			a.textBuf = append(a.textBuf[:a.thinkSplitOffset], after...)
			a.live.InReasoning = false
			a.thinkSplitResolved = true
		} else {
			a.live.InReasoning = true
			if tokenIDCount > 0 {
				a.attemptReasonDeltaToks += tokenIDCount
			} else {
				a.attemptReasonDeltaToks++
			}
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
	Error       *sseErrorDetail `json:"error"`
	Response    *struct {
		ID                string          `json:"id"`
		Model             string          `json:"model"`
		CreatedAt         int64           `json:"created_at"`
		Status            string          `json:"status"`
		ServiceTier       string          `json:"service_tier"`
		Usage             *streamUsage    `json:"usage"`
		Error             *sseErrorDetail `json:"error"`
		IncompleteDetails *struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
	} `json:"response"`
}

func (a *Accumulator) feedResponses(ev SSEEvent) error {
	if ev.Done {
		// Some OpenAI-compatible proxies emit a trailing [DONE] after
		// response.completed; ignore if already completed.
		if a.live.Completed {
			a.streamDone = true
		}
		return nil
	}
	if ev.Event == "error" {
		return formatStreamErrorEvent(ev.Data)
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
		if frame.Error != nil && frame.Error.Message != "" {
			return fmt.Errorf("%w: %s", ErrStreamError, frame.Error.Message)
		}
		return formatStreamErrorEvent(ev.Data)
	}
	if frame.Error != nil && (frame.Error.Message != "" || frame.Error.Type != "") {
		return fmt.Errorf("%w: %s", ErrStreamError, frame.Error.Message)
	}

	var frameUsage *streamUsage
	if frame.Response != nil {
		a.recordEnvelopeMeta(frame.Response.ID, frame.Response.Model, frame.Response.CreatedAt, "", frame.Response.ServiceTier, nil)
		frameUsage = frame.Response.Usage
	}

	chunkDeltas := 0
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
		open.appendContent("reasoning_text", frame.Delta)
		a.attemptSawReasoning = true
		a.live.InReasoning = true
		if frame.Delta != "" {
			chunkDeltas++
			a.attemptReasonDeltaToks++
		}

	case "response.reasoning_summary_text.delta":
		open := a.ensureOpenRespItem(frame.OutputIndex, "reasoning", frame.ItemID)
		open.appendSummary("summary_text", frame.Delta)
		a.attemptSawReasoning = true
		a.live.InReasoning = true
		if frame.Delta != "" {
			chunkDeltas++
			a.attemptReasonDeltaToks++
		}

	case "response.output_text.delta":
		if frame.Delta != "" {
			chunkDeltas++
		}
		a.handleRespOutputTextDelta(frame.OutputIndex, frame.ItemID, frame.Delta)

	case "response.function_call_arguments.delta":
		open := a.ensureOpenRespItem(frame.OutputIndex, "function_call", frame.ItemID)
		open.argsBuf = append(open.argsBuf, frame.Delta...)
		a.live.InReasoning = false
		if frame.Delta != "" {
			chunkDeltas++
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
		a.streamDone = true
	}

	a.attemptDeltaTokens += chunkDeltas
	a.applyUsage(frameUsage, chunkDeltas)
	a.updateUsageAndCap()
	return nil
}

func (a *Accumulator) adoptOrInitRespItem(added *ResponseItem) *openRespItem {
	// If continuing an incomplete item of the same type from a prior attempt,
	// merge into PartialItem so the output keeps Attempt 1's item ID and prefix.
	if !a.mergedPriorItem && a.live.PartialItem != nil && a.live.PartialItem.Type == added.Type {
		a.mergedPriorItem = true
		merged := newOpenRespItem(a.live.PartialItem)
		a.live.PartialItem = nil
		return merged
	}
	open := newOpenRespItem(added)
	if open.role == "" && open.typ == "message" {
		open.role = "assistant"
	}
	return open
}

func (a *Accumulator) ensureOpenRespItem(outputIdx int, itemType, itemID string) *openRespItem {
	if open, ok := a.openRespItems[outputIdx]; ok && open.typ == itemType {
		// vLLM rotates item_id on the first message delta after reasoning; adopt
		// the updated ID unless we merged Attempt 1's PartialItem ID.
		if itemID != "" && (open.id == "" || !a.mergedPriorItem) {
			open.id = itemID
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
	open.appendContent("output_text", delta)

	// Recover mid-reasoning split on /v1/responses when prior attempt stopped
	// inside an incomplete reasoning item and Attempt 2 emitted </think> inside
	// output_text.delta.
	if a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved {
		a.respThinkPending = append(a.respThinkPending, delta...)
		tag := []byte(a.opts.thinkEndTag())
		if idx := bytes.Index(a.respThinkPending, tag); idx >= 0 {
			extraReasoning := string(a.respThinkPending[:idx])
			after := bytes.TrimPrefix(a.respThinkPending[idx+len(tag):], []byte("\n"))
			if a.live.PartialItem != nil && a.live.PartialItem.Type == "reasoning" {
				rItem := a.live.PartialItem.Clone()
				if extraReasoning != "" {
					appendRespPart(&rItem.Content, "reasoning_text", extraReasoning)
				}
				rItem.Status = "completed"
				a.live.CompletedItems = append(a.live.CompletedItems, *rItem)
				a.live.PartialItem = nil
				a.mergedPriorItem = true
			}
			open.content = []openRespPart{{
				partType: "output_text",
				textBuf:  append([]byte(nil), after...),
			}}
			a.respThinkPending = nil
			a.live.InReasoning = false
			a.thinkSplitResolved = true
		} else {
			a.live.InReasoning = true
			if delta != "" {
				a.attemptReasonDeltaToks++
			}
		}
		return
	}
	a.live.InReasoning = false
}

func (a *Accumulator) finalizeRespItem(outputIdx int, rawDone json.RawMessage) error {
	open := a.openRespItems[outputIdx]
	delete(a.openRespItems, outputIdx)

	// If the attempt finished (e.g. max_output_tokens) before </think> was
	// emitted while continuing an incomplete reasoning item, fold the unclosed
	// output_text buffer back into the reasoning PartialItem instead of emitting
	// a message item containing raw thinking text.
	if open != nil && open.typ == "message" &&
		a.priorInReasoning && !a.attemptSawReasoning && !a.thinkSplitResolved &&
		a.live.PartialItem != nil && a.live.PartialItem.Type == "reasoning" {
		extra := trimPartialTagSuffix(string(a.respThinkPending), a.opts.thinkEndTag())
		if extra != "" {
			appendRespPart(&a.live.PartialItem.Content, "reasoning_text", extra)
		}
		a.respThinkPending = nil
		return nil
	}

	var doneItem ResponseItem
	hasDone := false
	if len(rawDone) > 0 {
		if err := json.Unmarshal(rawDone, &doneItem); err != nil {
			return fmt.Errorf("parse response.output_item.done: %w", err)
		}
		hasDone = true
	}

	var finalized *ResponseItem
	if open == nil {
		if !hasDone {
			return nil
		}
		adopted := a.adoptOrInitRespItem(&doneItem).toResponseItem()
		if adopted.Type == "function_call" || (!adopted.HasProgress() && doneItem.HasProgress()) {
			cp := doneItem
			cp.Raw = append(json.RawMessage(nil), rawDone...)
			finalized = &cp
		} else {
			finalized = adopted
		}
	} else {
		finalized = open.toResponseItem()
		if hasDone {
			if finalized.ID == "" && doneItem.ID != "" {
				finalized.ID = doneItem.ID
			}
			if finalized.Type == "function_call" {
				finalized.CallID = doneItem.CallID
				finalized.Name = doneItem.Name
				if doneItem.Arguments != "" {
					finalized.Arguments = doneItem.Arguments
				}
				finalized.Raw = append(json.RawMessage(nil), rawDone...)
			} else if !finalized.HasProgress() && doneItem.HasProgress() {
				// Fallback when a server sends full text only on output_item.done.
				finalized.Content = doneItem.Content
				finalized.Summary = doneItem.Summary
			}
		}
	}

	if a.live.PartialItem != nil && a.live.PartialItem.HasProgress() && a.live.PartialItem.Type != finalized.Type {
		prev := a.live.PartialItem.Clone()
		prev.Status = "completed"
		a.live.CompletedItems = append(a.live.CompletedItems, *prev)
		a.live.PartialItem = nil
	}
	finalized.Status = "completed"
	if finalized.Type == "reasoning" {
		// Reasoning finished; next item transitions out of reasoning.
		a.live.InReasoning = false
	}
	a.live.CompletedItems = append(a.live.CompletedItems, *finalized)
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
