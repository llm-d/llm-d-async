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

// CheckEligibility verifies that endpoint and rawBody support streaming
// continuation. Requests with n > 1 return ErrMultipleChoices so callers can
// fall back to non-streaming execution.
func CheckEligibility(endpoint Endpoint, rawBody []byte) error {
	switch endpoint {
	case EndpointCompletions, EndpointChatCompletions, EndpointResponses:
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedEndpoint, endpoint)
	}
	doc, err := parseJSONObject(rawBody)
	if err != nil {
		return err
	}
	if rawN, ok := doc["n"]; ok && !isJSONNull(rawN) {
		var n int
		if err := json.Unmarshal(rawN, &n); err != nil {
			return fmt.Errorf("%w: invalid field \"n\": %w", ErrInvalidRequestJSON, err)
		}
		if n > 1 {
			return ErrMultipleChoices
		}
	}
	return nil
}

// PrepareStreamRequest rewrites an eligible request body for streaming SSE
// dispatch (setting stream: true, stream_options.include_usage: true on
// completions/chat, and return_token_ids: true when opts.TokenExact is set),
// preserving all other request fields.
func PrepareStreamRequest(endpoint Endpoint, rawBody []byte, opts Options) ([]byte, error) {
	if err := CheckEligibility(endpoint, rawBody); err != nil {
		return nil, err
	}
	doc, err := parseJSONObject(rawBody)
	if err != nil {
		return nil, err
	}
	if err := applyStreamFields(endpoint, doc, opts); err != nil {
		return nil, err
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal stream request: %w", err)
	}
	return out, nil
}

// BuildContinuationRequest constructs a continuation request body from the
// original user request JSON and accumulated partial State.
//
// If the original request specified a max token budget and state has already
// generated at least that many tokens, budgetExhausted is returned as true
// (with err == nil) so the caller can finalize the response as length-limited
// instead of sending a 0-budget continuation that model servers reject.
func BuildContinuationRequest(originalBody []byte, state *State, opts Options) (body []byte, budgetExhausted bool, err error) {
	if state == nil {
		return nil, false, fmt.Errorf("%w: nil state", ErrUnsupportedEndpoint)
	}
	if err := CheckEligibility(state.Endpoint, originalBody); err != nil {
		return nil, false, err
	}
	if !state.HasProgress() {
		req, err := PrepareStreamRequest(state.Endpoint, originalBody, opts)
		return req, false, err
	}

	doc, err := parseJSONObject(originalBody)
	if err != nil {
		return nil, false, err
	}
	if err := applyStreamFields(state.Endpoint, doc, opts); err != nil {
		return nil, false, err
	}

	exhausted, err := adjustTokenBudget(state.Endpoint, doc, state.Usage.GeneratedTokens)
	if err != nil {
		return nil, false, err
	}
	if exhausted {
		return nil, true, nil
	}

	switch state.Endpoint {
	case EndpointCompletions:
		if err := buildCompletionsContinuation(doc, state, opts); err != nil {
			return nil, false, err
		}
	case EndpointChatCompletions:
		if err := buildChatContinuation(doc, state, opts); err != nil {
			return nil, false, err
		}
	case EndpointResponses:
		if err := buildResponsesContinuation(doc, state); err != nil {
			return nil, false, err
		}
	default:
		return nil, false, fmt.Errorf("%w: %q", ErrUnsupportedEndpoint, state.Endpoint)
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return nil, false, fmt.Errorf("marshal continuation request: %w", err)
	}
	return out, false, nil
}

func parseJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, ErrInvalidRequestJSON
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &doc); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequestJSON, err)
	}
	if doc == nil {
		return nil, ErrInvalidRequestJSON
	}
	return doc, nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func applyStreamFields(endpoint Endpoint, doc map[string]json.RawMessage, opts Options) error {
	doc["stream"] = json.RawMessage("true")

	if endpoint == EndpointCompletions || endpoint == EndpointChatCompletions {
		streamOpts := make(map[string]json.RawMessage)
		if existing, ok := doc["stream_options"]; ok && !isJSONNull(existing) {
			if err := json.Unmarshal(existing, &streamOpts); err != nil {
				return fmt.Errorf("%w: invalid stream_options: %w", ErrInvalidRequestJSON, err)
			}
		}
		streamOpts["include_usage"] = json.RawMessage("true")
		encoded, err := json.Marshal(streamOpts)
		if err != nil {
			return fmt.Errorf("marshal stream_options: %w", err)
		}
		doc["stream_options"] = encoded

		if opts.TokenExact {
			doc["return_token_ids"] = json.RawMessage("true")
		}
	}
	return nil
}

func adjustTokenBudget(endpoint Endpoint, doc map[string]json.RawMessage, generated int) (exhausted bool, err error) {
	if generated <= 0 {
		return false, nil
	}
	switch endpoint {
	case EndpointCompletions:
		return reduceIntField(doc, "max_tokens", generated)
	case EndpointChatCompletions:
		hasMaxComp := hasNonNullField(doc, "max_completion_tokens")
		hasMaxTok := hasNonNullField(doc, "max_tokens")
		if hasMaxComp {
			ex, err := reduceIntField(doc, "max_completion_tokens", generated)
			if err != nil || ex {
				return ex, err
			}
		}
		if hasMaxTok {
			return reduceIntField(doc, "max_tokens", generated)
		}
		return false, nil
	case EndpointResponses:
		return reduceIntField(doc, "max_output_tokens", generated)
	default:
		return false, nil
	}
}

func hasNonNullField(doc map[string]json.RawMessage, key string) bool {
	raw, ok := doc[key]
	return ok && !isJSONNull(raw)
}

func reduceIntField(doc map[string]json.RawMessage, key string, generated int) (exhausted bool, err error) {
	raw, ok := doc[key]
	if !ok || isJSONNull(raw) {
		return false, nil
	}
	var orig int
	if err := json.Unmarshal(raw, &orig); err != nil {
		return false, fmt.Errorf("%w: invalid %s: %w", ErrInvalidRequestJSON, key, err)
	}
	if orig <= 0 {
		return false, nil
	}
	rem := orig - generated
	if rem <= 0 {
		return true, nil
	}
	b, err := json.Marshal(rem)
	if err != nil {
		return false, fmt.Errorf("marshal reduced %s: %w", key, err)
	}
	doc[key] = b
	return false, nil
}

func buildCompletionsContinuation(doc map[string]json.RawMessage, state *State, opts Options) error {
	rawPrompt, ok := doc["prompt"]
	if !ok || isJSONNull(rawPrompt) {
		return fmt.Errorf("%w: missing prompt", ErrInvalidRequestJSON)
	}
	trimmed := bytes.TrimSpace(rawPrompt)
	switch {
	case len(trimmed) > 0 && trimmed[0] == '"':
		var promptStr string
		if err := json.Unmarshal(trimmed, &promptStr); err != nil {
			return fmt.Errorf("%w: invalid string prompt: %w", ErrInvalidRequestJSON, err)
		}
		updated, err := json.Marshal(promptStr + state.Text)
		if err != nil {
			return fmt.Errorf("marshal continuation prompt: %w", err)
		}
		doc["prompt"] = updated

	case len(trimmed) > 0 && trimmed[0] == '[':
		var promptIDs []int
		if err := json.Unmarshal(trimmed, &promptIDs); err != nil {
			return fmt.Errorf("%w: prompt array must be token IDs ([]int): %w", ErrInvalidRequestJSON, err)
		}
		if len(state.GeneratedTokenIDs) == 0 {
			return ErrMissingTokenIDs
		}
		combined := make([]int, 0, len(promptIDs)+len(state.GeneratedTokenIDs))
		combined = append(combined, promptIDs...)
		combined = append(combined, state.GeneratedTokenIDs...)
		updated, err := json.Marshal(combined)
		if err != nil {
			return fmt.Errorf("marshal continuation token_ids prompt: %w", err)
		}
		doc["prompt"] = updated

	default:
		return fmt.Errorf("%w: unsupported prompt format", ErrInvalidRequestJSON)
	}

	if opts.TokenExact && len(state.PromptTokenIDs) > 0 && len(state.GeneratedTokenIDs) > 0 {
		if err := attachKVPromptTokenIDs(doc, state.PromptTokenIDs, state.GeneratedTokenIDs); err != nil {
			return err
		}
	}
	return nil
}

func buildChatContinuation(doc map[string]json.RawMessage, state *State, opts Options) error {
	rawMsgs, ok := doc["messages"]
	if !ok || isJSONNull(rawMsgs) {
		return fmt.Errorf("%w: missing messages", ErrInvalidRequestJSON)
	}
	var msgs []json.RawMessage
	if err := json.Unmarshal(rawMsgs, &msgs); err != nil {
		return fmt.Errorf("%w: invalid messages array: %w", ErrInvalidRequestJSON, err)
	}

	// Always include content (even "" when still inside reasoning) so vLLM's
	// _parse_chat_message_content builds a ConversationMessage to attach
	// reasoning onto.
	assistantMsg := map[string]any{
		"role":    "assistant",
		"content": state.Text,
	}
	if state.Reasoning != "" {
		assistantMsg["reasoning"] = state.Reasoning
	}
	encodedAssistant, err := json.Marshal(assistantMsg)
	if err != nil {
		return fmt.Errorf("marshal assistant continuation message: %w", err)
	}
	msgs = append(msgs, encodedAssistant)

	encodedMsgs, err := json.Marshal(msgs)
	if err != nil {
		return fmt.Errorf("marshal continuation messages: %w", err)
	}
	doc["messages"] = encodedMsgs
	doc["continue_final_message"] = json.RawMessage("true")
	doc["add_generation_prompt"] = json.RawMessage("false")

	if opts.TokenExact && len(state.PromptTokenIDs) > 0 && len(state.GeneratedTokenIDs) > 0 {
		if err := attachKVPromptTokenIDs(doc, state.PromptTokenIDs, state.GeneratedTokenIDs); err != nil {
			return err
		}
	}
	return nil
}

func attachKVPromptTokenIDs(doc map[string]json.RawMessage, promptIDs, genIDs []int) error {
	kv := make(map[string]json.RawMessage)
	if rawKV, ok := doc["kv_transfer_params"]; ok && !isJSONNull(rawKV) {
		if err := json.Unmarshal(rawKV, &kv); err != nil {
			return fmt.Errorf("%w: invalid kv_transfer_params: %w", ErrInvalidRequestJSON, err)
		}
	}
	combined := make([]int, 0, len(promptIDs)+len(genIDs))
	combined = append(combined, promptIDs...)
	combined = append(combined, genIDs...)
	encodedIDs, err := json.Marshal(combined)
	if err != nil {
		return fmt.Errorf("marshal prompt_token_ids: %w", err)
	}
	kv["prompt_token_ids"] = encodedIDs
	encodedKV, err := json.Marshal(kv)
	if err != nil {
		return fmt.Errorf("marshal kv_transfer_params: %w", err)
	}
	doc["kv_transfer_params"] = encodedKV
	return nil
}

func buildResponsesContinuation(doc map[string]json.RawMessage, state *State) error {
	rawInput, ok := doc["input"]
	if !ok || isJSONNull(rawInput) {
		return fmt.Errorf("%w: missing input", ErrInvalidRequestJSON)
	}

	var items []json.RawMessage
	trimmed := bytes.TrimSpace(rawInput)
	switch {
	case len(trimmed) > 0 && trimmed[0] == '"':
		var inputStr string
		if err := json.Unmarshal(trimmed, &inputStr); err != nil {
			return fmt.Errorf("%w: invalid string input: %w", ErrInvalidRequestJSON, err)
		}
		userItem, err := json.Marshal(map[string]any{
			"role":    "user",
			"content": inputStr,
		})
		if err != nil {
			return fmt.Errorf("marshal normalized user input: %w", err)
		}
		items = []json.RawMessage{userItem}

	case len(trimmed) > 0 && trimmed[0] == '[':
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return fmt.Errorf("%w: invalid input array: %w", ErrInvalidRequestJSON, err)
		}

	default:
		return fmt.Errorf("%w: unsupported input format", ErrInvalidRequestJSON)
	}

	for i := range state.CompletedItems {
		rawItem, err := marshalResponsesInputItem(&state.CompletedItems[i], "completed")
		if err != nil {
			return err
		}
		items = append(items, rawItem)
	}

	if state.PartialItem != nil && state.PartialItem.HasProgress() {
		rawPartial, err := marshalResponsesInputItem(state.PartialItem, "incomplete")
		if err != nil {
			return err
		}
		items = append(items, rawPartial)
	} else if len(state.CompletedItems) > 0 {
		// vLLM's should_continue_final_message inspects only the LAST item in
		// input for status in ("in_progress", "incomplete"). If the stream was
		// interrupted right after a reasoning item completed (or mid-tool-call
		// after reasoning), append an incomplete empty assistant message so
		// vLLM continues the assistant turn instead of opening a new one.
		trailing, err := marshalResponsesInputItem(&ResponseItem{
			ID:     "msg_cont",
			Type:   "message",
			Role:   "assistant",
			Status: "incomplete",
			Content: []ResponseContentPart{
				{Type: "output_text", Text: ""},
			},
		}, "incomplete")
		if err != nil {
			return err
		}
		items = append(items, trailing)
	}

	encodedInput, err := json.Marshal(items)
	if err != nil {
		return fmt.Errorf("marshal continuation input: %w", err)
	}
	doc["input"] = encodedInput
	return nil
}

func marshalResponsesInputItem(it *ResponseItem, status string) (json.RawMessage, error) {
	switch it.Type {
	case "reasoning":
		id := it.ID
		if id == "" {
			id = "rs_cont"
		}
		summary := it.Summary
		if summary == nil {
			summary = []ResponseContentPart{}
		}
		content := it.Content
		if content == nil {
			content = []ResponseContentPart{}
		}
		b, err := json.Marshal(map[string]any{
			"id":      id,
			"type":    "reasoning",
			"status":  status,
			"summary": summary,
			"content": content,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal reasoning input item: %w", err)
		}
		return b, nil

	case "message":
		id := it.ID
		if id == "" {
			id = "msg_cont"
		}
		role := it.Role
		if role == "" {
			role = "assistant"
		}
		content := it.Content
		if len(content) == 0 {
			content = []ResponseContentPart{{Type: "output_text", Text: ""}}
		}
		b, err := json.Marshal(map[string]any{
			"id":      id,
			"type":    "message",
			"role":    role,
			"status":  status,
			"content": content,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal message input item: %w", err)
		}
		return b, nil

	default:
		if len(it.Raw) > 0 {
			return append(json.RawMessage(nil), it.Raw...), nil
		}
		b, err := json.Marshal(it)
		if err != nil {
			return nil, fmt.Errorf("marshal response item: %w", err)
		}
		return b, nil
	}
}
