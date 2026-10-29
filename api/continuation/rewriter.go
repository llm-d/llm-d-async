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
	"fmt"
)

// AssembleResponse builds a standard non-streaming OpenAI JSON response body
// from state, preserving Attempt 1's response ID, concatenating output across
// all attempts, and reporting delivered token usage
// (original_prompt_tokens + sum(completion_tokens)).
func AssembleResponse(state *State) ([]byte, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: nil state", ErrUnsupportedEndpoint)
	}
	switch state.Endpoint {
	case EndpointCompletions:
		return assembleCompletionsResponse(state)
	case EndpointChatCompletions:
		return assembleChatResponse(state)
	case EndpointResponses:
		return assembleResponsesResponse(state)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedEndpoint, state.Endpoint)
	}
}

// MarkBudgetExhausted updates state to reflect that the request's configured
// output token budget was already reached before starting another attempt, so
// AssembleResponse emits a length/incomplete finish reason.
func MarkBudgetExhausted(state *State) {
	if state == nil {
		return
	}
	state.Completed = true
	switch state.Endpoint {
	case EndpointCompletions, EndpointChatCompletions:
		state.FinishReason = "length"
	case EndpointResponses:
		if state.PartialItem != nil && state.PartialItem.HasProgress() {
			cp := state.PartialItem.Clone()
			cp.Status = "incomplete"
			state.CompletedItems = append(state.CompletedItems, *cp)
			state.PartialItem = nil
		}
		state.Status = "incomplete"
		state.IncompleteReason = "max_output_tokens"
	}
}

func assembleCompletionsResponse(state *State) ([]byte, error) {
	id := state.ResponseID
	if id == "" {
		id = "cmpl-continued"
	}
	finish := state.FinishReason
	if finish == "" {
		finish = "stop"
	}
	resp := map[string]any{
		"id":      id,
		"object":  "text_completion",
		"created": state.Created,
		"model":   state.Model,
		"choices": []map[string]any{
			{
				"index":         0,
				"text":          state.Text,
				"logprobs":      nil,
				"finish_reason": finish,
			},
		},
		"usage": buildChatOrCompletionsUsage(state.Usage),
	}
	if state.SystemFingerprint != "" {
		resp["system_fingerprint"] = state.SystemFingerprint
	}
	if state.ServiceTier != "" {
		resp["service_tier"] = state.ServiceTier
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal completions response: %w", err)
	}
	return b, nil
}

func assembleChatResponse(state *State) ([]byte, error) {
	id := state.ResponseID
	if id == "" {
		id = "chatcmpl-continued"
	}
	finish := state.FinishReason
	if finish == "" {
		if len(state.ToolCalls) > 0 {
			finish = "tool_calls"
		} else {
			finish = "stop"
		}
	}
	msg := map[string]any{
		"role": "assistant",
	}
	if len(state.ToolCalls) > 0 && state.Text == "" {
		msg["content"] = nil
	} else {
		msg["content"] = state.Text
	}
	if state.Reasoning != "" {
		msg["reasoning"] = state.Reasoning
		msg["reasoning_content"] = state.Reasoning
	}
	if len(state.ToolCalls) > 0 {
		msg["tool_calls"] = state.ToolCalls
	}

	resp := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": state.Created,
		"model":   state.Model,
		"choices": []map[string]any{
			{
				"index":         0,
				"message":       msg,
				"logprobs":      nil,
				"finish_reason": finish,
			},
		},
		"usage": buildChatOrCompletionsUsage(state.Usage),
	}
	if state.SystemFingerprint != "" {
		resp["system_fingerprint"] = state.SystemFingerprint
	}
	if state.ServiceTier != "" {
		resp["service_tier"] = state.ServiceTier
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal chat completions response: %w", err)
	}
	return b, nil
}

func buildChatOrCompletionsUsage(u UsageState) map[string]any {
	out := map[string]any{
		"prompt_tokens":     u.OriginalPromptTokens,
		"completion_tokens": u.GeneratedTokens,
		"total_tokens":      u.OriginalPromptTokens + u.GeneratedTokens,
	}
	if u.ReasoningTokens > 0 {
		out["completion_tokens_details"] = map[string]any{
			"reasoning_tokens": u.ReasoningTokens,
		}
	}
	return out
}

func assembleResponsesResponse(state *State) ([]byte, error) {
	id := state.ResponseID
	if id == "" {
		id = "resp_continued"
	}
	status := state.Status
	if status == "" {
		status = "completed"
	}

	outputItems := make([]json.RawMessage, 0, len(state.CompletedItems)+1)
	for i := range state.CompletedItems {
		itemStatus := state.CompletedItems[i].Status
		if itemStatus == "" {
			itemStatus = "completed"
		}
		raw, err := marshalResponsesOutputItem(&state.CompletedItems[i], itemStatus)
		if err != nil {
			return nil, err
		}
		outputItems = append(outputItems, raw)
	}
	if state.PartialItem != nil && state.PartialItem.HasProgress() {
		pStatus := "completed"
		if status == "incomplete" {
			pStatus = "incomplete"
		}
		raw, err := marshalResponsesOutputItem(state.PartialItem, pStatus)
		if err != nil {
			return nil, err
		}
		outputItems = append(outputItems, raw)
	}

	usage := map[string]any{
		"input_tokens":  state.Usage.OriginalPromptTokens,
		"output_tokens": state.Usage.GeneratedTokens,
		"total_tokens":  state.Usage.OriginalPromptTokens + state.Usage.GeneratedTokens,
	}
	if state.Usage.ReasoningTokens > 0 {
		usage["output_tokens_details"] = map[string]any{
			"reasoning_tokens": state.Usage.ReasoningTokens,
		}
	}

	resp := map[string]any{
		"id":         id,
		"object":     "response",
		"created_at": state.Created,
		"status":     status,
		"model":      state.Model,
		"output":     outputItems,
		"usage":      usage,
	}
	if status == "incomplete" && state.IncompleteReason != "" {
		resp["incomplete_details"] = map[string]any{
			"reason": state.IncompleteReason,
		}
	} else {
		resp["incomplete_details"] = nil
	}
	if state.ServiceTier != "" {
		resp["service_tier"] = state.ServiceTier
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal responses response: %w", err)
	}
	return b, nil
}

func marshalResponsesOutputItem(it *ResponseItem, status string) (json.RawMessage, error) {
	if it.Type == "message" {
		id := it.ID
		if id == "" {
			id = "msg_continued"
		}
		role := it.Role
		if role == "" {
			role = "assistant"
		}
		parts := make([]map[string]any, 0, len(it.Content))
		for _, p := range it.Content {
			annotations := p.Annotations
			if len(annotations) == 0 {
				annotations = json.RawMessage("[]")
			}
			partMap := map[string]any{
				"type":        p.Type,
				"text":        p.Text,
				"annotations": annotations,
			}
			if p.Refusal != "" {
				partMap["refusal"] = p.Refusal
			}
			parts = append(parts, partMap)
		}
		b, err := json.Marshal(map[string]any{
			"id":      id,
			"type":    "message",
			"role":    role,
			"status":  status,
			"content": parts,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal responses message output: %w", err)
		}
		return b, nil
	}
	return marshalResponsesInputItem(it, status)
}

// RewriteStreamChunk rewrites a single SSE event emitted on Attempt >= 1 for
// callers that stream SSE chunks directly to a downstream client (e.g.
// llm-d-router). Callers should invoke Accumulator.FeedEvent(ev) BEFORE calling
// RewriteStreamChunk(acc.FinalState(), ev) so state reflects the latest usage
// and completion metadata.
func RewriteStreamChunk(state *State, ev SSEEvent) (out SSEEvent, suppress bool, err error) {
	if state == nil || ev.Done || state.Attempt <= 1 {
		return ev, false, nil
	}
	if len(bytes.TrimSpace(ev.Data)) == 0 {
		return ev, false, nil
	}

	switch state.Endpoint {
	case EndpointCompletions:
		return rewriteCompletionsChunk(state, ev)
	case EndpointChatCompletions:
		return rewriteChatChunk(state, ev)
	case EndpointResponses:
		return rewriteResponsesChunk(state, ev)
	default:
		return ev, false, fmt.Errorf("%w: %q", ErrUnsupportedEndpoint, state.Endpoint)
	}
}

func rewriteCompletionsChunk(state *State, ev SSEEvent) (SSEEvent, bool, error) {
	doc, err := parseJSONObject(ev.Data)
	if err != nil {
		return ev, false, err
	}
	if err := overwriteStringField(doc, "id", state.ResponseID); err != nil {
		return ev, false, err
	}
	if hasNonNullField(doc, "usage") {
		uBytes, err := json.Marshal(buildChatOrCompletionsUsage(state.Usage))
		if err != nil {
			return ev, false, fmt.Errorf("marshal rewritten usage: %w", err)
		}
		doc["usage"] = uBytes
	}
	outBytes, err := json.Marshal(doc)
	if err != nil {
		return ev, false, fmt.Errorf("marshal rewritten completions chunk: %w", err)
	}
	return SSEEvent{Event: ev.Event, Data: outBytes}, false, nil
}

func rewriteChatChunk(state *State, ev SSEEvent) (SSEEvent, bool, error) {
	var parsed chatChunk
	if err := json.Unmarshal(ev.Data, &parsed); err != nil {
		return ev, false, fmt.Errorf("parse chat chunk for rewrite: %w", err)
	}
	// Suppress initial role-only chunk on continuation attempts so downstream
	// SSE clients do not observe duplicate role headers.
	if parsed.Usage == nil && len(parsed.Choices) == 1 {
		c := parsed.Choices[0]
		if c.FinishReason == nil &&
			c.Delta.Role != "" &&
			c.Delta.Content == "" &&
			c.Delta.Reasoning == "" &&
			c.Delta.ReasoningContent == "" &&
			len(c.Delta.ToolCalls) == 0 {
			return SSEEvent{}, true, nil
		}
	}
	return rewriteCompletionsChunk(state, ev)
}

func rewriteResponsesChunk(state *State, ev SSEEvent) (SSEEvent, bool, error) {
	doc, err := parseJSONObject(ev.Data)
	if err != nil {
		return ev, false, err
	}
	evType := ev.Event
	if evType == "" {
		if rawT, ok := doc["type"]; ok {
			_ = json.Unmarshal(rawT, &evType)
		}
	}
	switch evType {
	case "response.created", "response.in_progress":
		return SSEEvent{}, true, nil
	case "response.completed", "response.incomplete":
		assembled, err := AssembleResponse(state)
		if err != nil {
			return ev, false, err
		}
		doc["response"] = assembled
		outBytes, err := json.Marshal(doc)
		if err != nil {
			return ev, false, fmt.Errorf("marshal rewritten responses terminal event: %w", err)
		}
		return SSEEvent{Event: ev.Event, Data: outBytes}, false, nil
	default:
		return ev, false, nil
	}
}

func overwriteStringField(doc map[string]json.RawMessage, key, val string) error {
	if val == "" {
		return nil
	}
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshal field %s: %w", key, err)
	}
	doc[key] = b
	return nil
}
