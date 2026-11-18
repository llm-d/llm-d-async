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
