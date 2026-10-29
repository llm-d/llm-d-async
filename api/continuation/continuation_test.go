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
	"io"
	"strings"
	"testing"
)

// trickleReader splits reads into small chunks to exercise SSEReader across
// arbitrary packet/chunk boundaries.
type trickleReader struct {
	data []byte
	pos  int
	step int
}

func (tr *trickleReader) Read(p []byte) (int, error) {
	if tr.pos >= len(tr.data) {
		return 0, io.EOF
	}
	n := tr.step
	if n <= 0 || n > len(p) {
		n = len(p)
	}
	if rem := len(tr.data) - tr.pos; rem < n {
		n = rem
	}
	copy(p, tr.data[tr.pos:tr.pos+n])
	tr.pos += n
	return n, nil
}

func TestDetectEndpoint(t *testing.T) {
	cases := []struct {
		in   string
		want Endpoint
		ok   bool
	}{
		{"http://epp:80/v1/completions", EndpointCompletions, true},
		{"/v1/chat/completions?foo=1", EndpointChatCompletions, true},
		{"https://host/prefix/v1/responses/", EndpointResponses, true},
		{"/v1/embeddings", "", false},
		{"/v1/audio/transcriptions", "", false},
	}
	for _, tc := range cases {
		got, ok := DetectEndpoint(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("DetectEndpoint(%q) = (%q, %v), want (%q, %v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestGuardsAndEligibility(t *testing.T) {
	if err := CheckEligibility(EndpointChatCompletions, []byte(`{"model":"m","messages":[],"n":2}`)); !errors.Is(err, ErrMultipleChoices) {
		t.Fatalf("expected ErrMultipleChoices, got %v", err)
	}
	if err := CheckEligibility(Endpoint("/v1/embeddings"), []byte(`{}`)); !errors.Is(err, ErrUnsupportedEndpoint) {
		t.Fatalf("expected ErrUnsupportedEndpoint, got %v", err)
	}
	if err := CheckEligibility(EndpointCompletions, []byte(`not-json`)); !errors.Is(err, ErrInvalidRequestJSON) {
		t.Fatalf("expected ErrInvalidRequestJSON, got %v", err)
	}
}

func TestCompletionsMultiResumeAndRewrite(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","prompt":"Count: ","max_tokens":10,"temperature":0}`)

	prep, err := PrepareStreamRequest(EndpointCompletions, origReq, Options{})
	if err != nil {
		t.Fatalf("PrepareStreamRequest: %v", err)
	}
	var prepDoc map[string]any
	if err := json.Unmarshal(prep, &prepDoc); err != nil {
		t.Fatalf("unmarshal prep: %v", err)
	}
	if prepDoc["stream"] != true {
		t.Fatalf("expected stream=true, got %v", prepDoc["stream"])
	}

	// Attempt 1: interrupted before [DONE] after 2 chunks.
	sse1 := ": keepalive\n\n" +
		`data: {"id":"cmpl-first","created":1700,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"text":"1, ","finish_reason":null}]}` + "\n\n" +
		`data: {"id":"cmpl-first","created":1700,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"text":"2, ","finish_reason":null}]}` + "\n\n"

	acc1 := NewAccumulator(EndpointCompletions, nil, Options{})
	err = acc1.ConsumeStream(&trickleReader{data: []byte(sse1), step: 7}, nil)
	if !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("expected ErrIncompleteStream, got %v", err)
	}
	snap1 := acc1.CheckpointSnapshot()
	if snap1.Text != "1, 2, " || snap1.ResponseID != "cmpl-first" || snap1.Usage.GeneratedTokens != 2 {
		t.Fatalf("unexpected snap1: %+v", snap1)
	}

	// Round-trip checkpoint through JSON as Redis would.
	rawSnap, err := json.Marshal(snap1)
	if err != nil {
		t.Fatalf("marshal snap1: %v", err)
	}
	var loadedSnap State
	if err := json.Unmarshal(rawSnap, &loadedSnap); err != nil {
		t.Fatalf("unmarshal snap1: %v", err)
	}

	contReq, exhausted, err := BuildContinuationRequest(origReq, &loadedSnap, Options{})
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: exhausted=%v err=%v", exhausted, err)
	}
	var contDoc map[string]any
	if err := json.Unmarshal(contReq, &contDoc); err != nil {
		t.Fatalf("unmarshal contReq: %v", err)
	}
	if contDoc["prompt"] != "Count: 1, 2, " {
		t.Fatalf("expected continued prompt, got %v", contDoc["prompt"])
	}
	if int(contDoc["max_tokens"].(float64)) != 8 {
		t.Fatalf("expected reduced max_tokens=8, got %v", contDoc["max_tokens"])
	}

	// Attempt 2: completes with a different upstream id and usage chunk.
	sse2 := `data: {"id":"cmpl-second","created":1701,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"text":"3.","finish_reason":"stop"}]}` + "\n\n" +
		`data: {"id":"cmpl-second","created":1701,"model":"Qwen/Qwen3-8B","choices":[],"usage":{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}}` + "\n\n" +
		"data: [DONE]\n\n"

	acc2 := NewAccumulator(EndpointCompletions, &loadedSnap, Options{})
	var rewrittenChunks []SSEEvent
	err = acc2.ConsumeStream(strings.NewReader(sse2), func(ev SSEEvent) error {
		rw, suppress, rerr := RewriteStreamChunk(acc2.FinalState(), ev)
		if rerr != nil {
			return rerr
		}
		if !suppress {
			rewrittenChunks = append(rewrittenChunks, rw)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("acc2 ConsumeStream: %v", err)
	}
	final := acc2.FinalState()
	// Attempt 1 didn't get a usage chunk (interrupted mid-stream); set original
	// prompt tokens to verify delivered usage accounting when known.
	final.Usage.OriginalPromptTokens = 5

	assembled, err := AssembleResponse(final)
	if err != nil {
		t.Fatalf("AssembleResponse: %v", err)
	}
	var resp struct {
		ID      string `json:"id"`
		Choices []struct {
			Text         string `json:"text"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(assembled, &resp); err != nil {
		t.Fatalf("unmarshal assembled: %v", err)
	}
	if resp.ID != "cmpl-first" {
		t.Fatalf("expected preserved id cmpl-first, got %q", resp.ID)
	}
	if resp.Choices[0].Text != "1, 2, 3." || resp.Choices[0].FinishReason != "stop" {
		t.Fatalf("unexpected choices: %+v", resp.Choices)
	}
	if resp.Usage.PromptTokens != 5 || resp.Usage.CompletionTokens != 4 || resp.Usage.TotalTokens != 9 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
	if final.Usage.RecomputedPromptTokens != 9 {
		t.Fatalf("expected recomputed prompt tokens=9, got %d", final.Usage.RecomputedPromptTokens)
	}
	if len(rewrittenChunks) != 3 || !bytes.Contains(rewrittenChunks[0].Data, []byte(`"cmpl-first"`)) {
		t.Fatalf("expected rewritten chunks to carry cmpl-first, got %s", rewrittenChunks[0].Data)
	}
}

func TestCompletionsTokenIDPromptAndTokenExact(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","prompt":[101,102],"max_tokens":20}`)
	opts := Options{TokenExact: true}

	prep, err := PrepareStreamRequest(EndpointCompletions, origReq, opts)
	if err != nil {
		t.Fatalf("PrepareStreamRequest: %v", err)
	}
	if !bytes.Contains(prep, []byte(`"return_token_ids":true`)) {
		t.Fatalf("expected return_token_ids:true in %s", prep)
	}

	sse1 := `data: {"id":"cmpl-tok","prompt_token_ids":[101,102],"choices":[{"index":0,"text":"Hi","token_ids":[201,202]}]}` + "\n\n"
	acc1 := NewAccumulator(EndpointCompletions, nil, opts)
	_ = acc1.ConsumeStream(strings.NewReader(sse1), nil)
	snap := acc1.CheckpointSnapshot()

	contReq, exhausted, err := BuildContinuationRequest(origReq, snap, opts)
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	var doc struct {
		Prompt           []int `json:"prompt"`
		KVTransferParams struct {
			PromptTokenIDs []int `json:"prompt_token_ids"`
		} `json:"kv_transfer_params"`
	}
	if err := json.Unmarshal(contReq, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := []int{101, 102, 201, 202}
	if len(doc.Prompt) != 4 || doc.Prompt[3] != 202 {
		t.Fatalf("prompt = %v, want %v", doc.Prompt, want)
	}
	if len(doc.KVTransferParams.PromptTokenIDs) != 4 {
		t.Fatalf("kv prompt_token_ids = %v, want %v", doc.KVTransferParams.PromptTokenIDs, want)
	}
}

func TestChatCompletionsReasoningMidThinkSplitAndDedup(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"What is 6*7?"}],"max_completion_tokens":50}`)

	// Attempt 1 emits BOTH reasoning and reasoning_content (as vLLM does) and
	// drops mid-reasoning before </think>.
	sse1 := `data: {"id":"chatcmpl-r1","created":1800,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}` + "\n\n" +
		`data: {"id":"chatcmpl-r1","created":1800,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"reasoning":"Let me ","reasoning_content":"Let me "}}]}` + "\n\n" +
		`data: {"id":"chatcmpl-r1","created":1800,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"reasoning":"compute 6*7. ","reasoning_content":"compute 6*7. "}}]}` + "\n\n"

	acc1 := NewAccumulator(EndpointChatCompletions, nil, Options{})
	if err := acc1.ConsumeStream(strings.NewReader(sse1), nil); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("expected ErrIncompleteStream, got %v", err)
	}
	snap1 := acc1.CheckpointSnapshot()
	if snap1.Reasoning != "Let me compute 6*7. " {
		t.Fatalf("reasoning was duplicated or mangled: %q", snap1.Reasoning)
	}
	if !snap1.InReasoning {
		t.Fatalf("expected InReasoning=true")
	}

	contReq, exhausted, err := BuildContinuationRequest(origReq, snap1, Options{})
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	var contDoc struct {
		ContinueFinalMessage bool             `json:"continue_final_message"`
		AddGenerationPrompt  bool             `json:"add_generation_prompt"`
		MaxCompletionTokens  int              `json:"max_completion_tokens"`
		Messages             []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(contReq, &contDoc); err != nil {
		t.Fatalf("unmarshal contReq: %v", err)
	}
	if !contDoc.ContinueFinalMessage || contDoc.AddGenerationPrompt {
		t.Fatalf("expected continue_final_message=true, add_generation_prompt=false: %+v", contDoc)
	}
	lastMsg := contDoc.Messages[len(contDoc.Messages)-1]
	if lastMsg["role"] != "assistant" || lastMsg["content"] != "" || lastMsg["reasoning"] != "Let me compute 6*7. " {
		t.Fatalf("unexpected continuation assistant msg: %+v", lastMsg)
	}

	// Attempt 2: simulates vLLM's BaseThinkingReasoningParser emitting the
	// remaining reasoning + "</think>\n" inside delta.content!
	sse2 := `data: {"id":"chatcmpl-r2","created":1801,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}` + "\n\n" +
		`data: {"id":"chatcmpl-r2","created":1801,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"content":"It is 42.</th"}}]}` + "\n\n" +
		`data: {"id":"chatcmpl-r2","created":1801,"model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"content":"ink>\nThe answer is 42."},"finish_reason":"stop"}]}` + "\n\n" +
		`data: {"id":"chatcmpl-r2","created":1801,"model":"Qwen/Qwen3-8B","choices":[],"usage":{"prompt_tokens":30,"completion_tokens":10,"total_tokens":40}}` + "\n\n" +
		"data: [DONE]\n\n"

	acc2 := NewAccumulator(EndpointChatCompletions, snap1, Options{})
	suppressedCount := 0
	if err := acc2.ConsumeStream(strings.NewReader(sse2), func(ev SSEEvent) error {
		_, sup, rerr := RewriteStreamChunk(acc2.FinalState(), ev)
		if sup {
			suppressedCount++
		}
		return rerr
	}); err != nil {
		t.Fatalf("acc2 ConsumeStream: %v", err)
	}
	if suppressedCount != 1 {
		t.Fatalf("expected role-only chunk to be suppressed on attempt 2, got %d", suppressedCount)
	}

	final := acc2.FinalState()
	if final.Reasoning != "Let me compute 6*7. It is 42." {
		t.Fatalf("expected recovered reasoning, got %q", final.Reasoning)
	}
	if final.Text != "The answer is 42." {
		t.Fatalf("expected clean post-think content, got %q", final.Text)
	}

	assembled, err := AssembleResponse(final)
	if err != nil {
		t.Fatalf("AssembleResponse: %v", err)
	}
	if !bytes.Contains(assembled, []byte(`"chatcmpl-r1"`)) || !bytes.Contains(assembled, []byte(`"The answer is 42."`)) {
		t.Fatalf("unexpected assembled chat response: %s", assembled)
	}
}

func TestChatPartialToolCallDroppedOnInterruption(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"Weather?"}]}`)
	sse := `data: {"id":"chatcmpl-tc","choices":[{"index":0,"delta":{"reasoning":"Need weather tool."}}]}` + "\n\n" +
		`data: {"id":"chatcmpl-tc","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"loc\":"}}]}}]}` + "\n\n"

	acc := NewAccumulator(EndpointChatCompletions, nil, Options{})
	_ = acc.ConsumeStream(strings.NewReader(sse), nil)
	snap := acc.CheckpointSnapshot()
	if !snap.DroppedPartialToolCall || len(snap.ToolCalls) != 0 {
		t.Fatalf("expected partial tool call to be dropped, got %+v", snap)
	}
	if snap.CanMigrateLiveStream() {
		t.Fatalf("expected CanMigrateLiveStream=false when partial tool call dropped")
	}
	contReq, _, err := BuildContinuationRequest(origReq, snap, Options{})
	if err != nil {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	if bytes.Contains(contReq, []byte("get_weather")) {
		t.Fatalf("partial tool call must not appear in continuation request: %s", contReq)
	}
}

func TestResponsesContinuationItemIDRotationAndReasoningSplit(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","input":"Explain gravity briefly.","max_output_tokens":100,"store":false}`)

	// Attempt 1: starts reasoning item and interrupts mid-reasoning.
	sse1 := "event: response.created\n" +
		`data: {"type":"response.created","response":{"id":"resp_orig","created_at":1900,"model":"Qwen/Qwen3-8B","status":"in_progress"}}` + "\n\n" +
		"event: response.output_item.added\n" +
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","status":"in_progress","content":[]}}` + "\n\n" +
		"event: response.reasoning_text.delta\n" +
		`data: {"type":"response.reasoning_text.delta","output_index":0,"item_id":"rs_1","delta":"Mass curves "}` + "\n\n"

	acc1 := NewAccumulator(EndpointResponses, nil, Options{})
	if err := acc1.ConsumeStream(strings.NewReader(sse1), nil); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("expected ErrIncompleteStream, got %v", err)
	}
	snap1 := acc1.CheckpointSnapshot()
	if snap1.PartialItem == nil || snap1.PartialItem.Type != "reasoning" || snap1.PartialItem.Status != "incomplete" {
		t.Fatalf("unexpected PartialItem: %+v", snap1.PartialItem)
	}

	contReq, exhausted, err := BuildContinuationRequest(origReq, snap1, Options{})
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	var contDoc struct {
		Store           bool             `json:"store"`
		MaxOutputTokens int              `json:"max_output_tokens"`
		Input           []map[string]any `json:"input"`
	}
	if err := json.Unmarshal(contReq, &contDoc); err != nil {
		t.Fatalf("unmarshal contReq: %v", err)
	}
	if contDoc.Store != false || contDoc.MaxOutputTokens != 99 || len(contDoc.Input) != 2 {
		t.Fatalf("unexpected normalized /v1/responses continuation: %+v", contDoc)
	}
	if contDoc.Input[0]["role"] != "user" || contDoc.Input[1]["type"] != "reasoning" || contDoc.Input[1]["status"] != "incomplete" {
		t.Fatalf("unexpected input items: %+v", contDoc.Input)
	}

	// Attempt 2: simulates vLLM rotating item_id on message delta AND emitting
	// remaining reasoning + </think> in output_text.delta!
	sse2 := "event: response.created\n" +
		`data: {"type":"response.created","response":{"id":"resp_new","created_at":1901,"model":"Qwen/Qwen3-8B","status":"in_progress"}}` + "\n\n" +
		"event: response.output_item.added\n" +
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_stale","type":"message","role":"assistant","status":"in_progress","content":[]}}` + "\n\n" +
		"event: response.output_text.delta\n" +
		`data: {"type":"response.output_text.delta","output_index":0,"item_id":"msg_real","delta":"spacetime.</think>\nGravity is spacetime curvature."}` + "\n\n" +
		"event: response.output_item.done\n" +
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"msg_real","type":"message","role":"assistant","status":"completed"}}` + "\n\n" +
		"event: response.completed\n" +
		`data: {"type":"response.completed","response":{"id":"resp_new","status":"completed","usage":{"input_tokens":22,"output_tokens":12,"total_tokens":34}}}` + "\n\n"

	acc2 := NewAccumulator(EndpointResponses, snap1, Options{})
	if err := acc2.ConsumeStream(strings.NewReader(sse2), nil); err != nil {
		t.Fatalf("acc2 ConsumeStream: %v", err)
	}
	final := acc2.FinalState()
	final.Usage.OriginalPromptTokens = 10

	assembled, err := AssembleResponse(final)
	if err != nil {
		t.Fatalf("AssembleResponse: %v", err)
	}
	var resp struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Output []struct {
			ID      string                `json:"id"`
			Type    string                `json:"type"`
			Status  string                `json:"status"`
			Content []ResponseContentPart `json:"content"`
		} `json:"output"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(assembled, &resp); err != nil {
		t.Fatalf("unmarshal assembled responses: %v", err)
	}
	if resp.ID != "resp_orig" || resp.Status != "completed" || len(resp.Output) != 2 {
		t.Fatalf("unexpected assembled responses: %+v", resp)
	}
	if resp.Output[0].Type != "reasoning" || resp.Output[0].Content[0].Text != "Mass curves spacetime." {
		t.Fatalf("unexpected reasoning output item: %+v", resp.Output[0])
	}
	if resp.Output[1].Type != "message" || resp.Output[1].ID != "msg_real" || resp.Output[1].Content[0].Text != "Gravity is spacetime curvature." {
		t.Fatalf("unexpected message output item: %+v", resp.Output[1])
	}
	if resp.Usage.InputTokens != 10 || resp.Usage.OutputTokens != 13 || resp.Usage.TotalTokens != 23 {
		t.Fatalf("unexpected usage: %+v", resp.Usage)
	}
}

func TestBudgetExhaustionAndMaxCheckpointCap(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","prompt":"Hi","max_tokens":3}`)
	opts := Options{MaxCheckpointTokens: 2}

	sse := `data: {"id":"cmpl-cap","choices":[{"index":0,"text":"A"}]}` + "\n\n" +
		`data: {"id":"cmpl-cap","choices":[{"index":0,"text":"B"}]}` + "\n\n" +
		`data: {"id":"cmpl-cap","choices":[{"index":0,"text":"C"}]}` + "\n\n"

	acc := NewAccumulator(EndpointCompletions, nil, opts)
	_ = acc.ConsumeStream(strings.NewReader(sse), nil)

	// CheckpointSnapshot must cap at 2 tokens ("AB"), while live state saw 3 ("ABC").
	snap := acc.CheckpointSnapshot()
	if !snap.Capped || snap.Text != "AB" || snap.Usage.GeneratedTokens != 2 {
		t.Fatalf("expected capped snapshot at 2 tokens ('AB'), got %+v", snap)
	}
	if snap.CanMigrateLiveStream() {
		t.Fatalf("expected CanMigrateLiveStream=false when capped")
	}

	// Exhausted budget check when GeneratedTokens >= max_tokens (3 >= 3):
	uncapped := acc.FinalState()
	_, exhausted, err := BuildContinuationRequest(origReq, uncapped, Options{})
	if err != nil || !exhausted {
		t.Fatalf("expected budgetExhausted=true, got exhausted=%v err=%v", exhausted, err)
	}
	MarkBudgetExhausted(uncapped)
	assembled, err := AssembleResponse(uncapped)
	if err != nil {
		t.Fatalf("AssembleResponse: %v", err)
	}
	if !bytes.Contains(assembled, []byte(`"finish_reason":"length"`)) {
		t.Fatalf("expected finish_reason=length: %s", assembled)
	}
}

func TestResponsesCompletedReasoningBoundaryAndPartialFunctionCall(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","input":[{"role":"user","content":"Check weather"}],"max_output_tokens":60}`)

	sse := "event: response.created\r\n" +
		`data: {"type":"response.created","response":{"id":"resp_fc","status":"in_progress"}}` + "\r\n\r\n" +
		"event: response.output_item.added\r\n" +
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_fc","type":"reasoning","status":"in_progress"}}` + "\r\n\r\n" +
		"event: response.reasoning_text.delta\r\n" +
		`data: {"type":"response.reasoning_text.delta","output_index":0,"item_id":"rs_fc","delta":"Need tool."}` + "\r\n\r\n" +
		"event: response.output_item.done\r\n" +
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"rs_fc","type":"reasoning","status":"completed"}}` + "\r\n\r\n" +
		"event: response.output_item.added\r\n" +
		`data: {"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","name":"weather","status":"in_progress"}}` + "\r\n\r\n" +
		"event: response.function_call_arguments.delta\r\n" +
		`data: {"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{\"city\":"}` + "\r\n\r\n"

	acc := NewAccumulator(EndpointResponses, nil, Options{})
	if err := acc.ConsumeStream(strings.NewReader(sse), nil); !errors.Is(err, ErrIncompleteStream) {
		t.Fatalf("expected ErrIncompleteStream, got %v", err)
	}
	snap := acc.CheckpointSnapshot()
	if !snap.DroppedPartialToolCall || snap.PartialItem != nil || len(snap.CompletedItems) != 1 {
		t.Fatalf("expected completed reasoning + dropped partial function_call: %+v", snap)
	}

	contReq, exhausted, err := BuildContinuationRequest(origReq, snap, Options{})
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	var doc struct {
		Input []map[string]any `json:"input"`
	}
	if err := json.Unmarshal(contReq, &doc); err != nil {
		t.Fatalf("unmarshal contReq: %v", err)
	}
	// Expect [user, completed reasoning, trailing incomplete assistant message].
	if len(doc.Input) != 3 {
		t.Fatalf("expected 3 input items so last item is incomplete, got %d: %+v", len(doc.Input), doc.Input)
	}
	if doc.Input[1]["status"] != "completed" || doc.Input[2]["type"] != "message" || doc.Input[2]["status"] != "incomplete" {
		t.Fatalf("unexpected continuation input items: %+v", doc.Input)
	}
}

func TestThreeAttemptChainAndStreamError(t *testing.T) {
	origReq := []byte(`{"model":"Qwen/Qwen3-8B","messages":[{"role":"user","content":"Spell ABC"}],"max_tokens":20}`)
	opts := Options{TokenExact: true}

	// Attempt 1 -> "A"
	acc1 := NewAccumulator(EndpointChatCompletions, nil, opts)
	_ = acc1.ConsumeStream(strings.NewReader(`data: {"id":"chat-3x","prompt_token_ids":[1,2],"choices":[{"index":0,"delta":{"content":"A"},"token_ids":[10]}]}`+"\n\n"), nil)
	s1 := acc1.CheckpointSnapshot()

	// Attempt 2 -> mid-stream error after "B"
	acc2 := NewAccumulator(EndpointChatCompletions, s1, opts)
	err := acc2.ConsumeStream(strings.NewReader(
		`data: {"id":"chat-3x-2","choices":[{"index":0,"delta":{"content":"B"},"token_ids":[11]}]}`+"\n\n"+
			`data: {"error":{"message":"cuda error","type":"server_error"}}`+"\n\n",
	), nil)
	if !errors.Is(err, ErrStreamError) {
		t.Fatalf("expected ErrStreamError, got %v", err)
	}
	s2 := acc2.CheckpointSnapshot()
	if s2.Text != "AB" || len(s2.GeneratedTokenIDs) != 2 {
		t.Fatalf("unexpected s2: %+v", s2)
	}

	contReq3, exhausted, err := BuildContinuationRequest(origReq, s2, opts)
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	if !bytes.Contains(contReq3, []byte(`[1,2,10,11]`)) {
		t.Fatalf("expected kv_transfer_params.prompt_token_ids [1,2,10,11] in %s", contReq3)
	}

	// Attempt 3 -> "C" + [DONE]
	acc3 := NewAccumulator(EndpointChatCompletions, s2, opts)
	if err := acc3.ConsumeStream(strings.NewReader(
		`data: {"id":"chat-3x-3","choices":[{"index":0,"delta":{"content":"C"},"token_ids":[12],"finish_reason":"stop"}]}`+"\n\n"+
			"data: [DONE]\n\n",
	), nil); err != nil {
		t.Fatalf("acc3: %v", err)
	}
	final := acc3.FinalState()
	if final.ResponseID != "chat-3x" || final.Text != "ABC" || final.Attempt != 3 {
		t.Fatalf("unexpected final state: %+v", final)
	}
}

func TestUninterruptedStreamPastCheckpointCapReturnsFullOutput(t *testing.T) {
	sse := `data: {"id":"cmpl-full","choices":[{"index":0,"text":"Hello "}]}` + "\n\n" +
		`data: {"id":"cmpl-full","choices":[{"index":0,"text":"brave "}]}` + "\n\n" +
		`data: {"id":"cmpl-full","choices":[{"index":0,"text":"world!","finish_reason":"stop"}]}` + "\n\n" +
		"data: [DONE]\n\n"

	acc := NewAccumulator(EndpointCompletions, nil, Options{MaxCheckpointTokens: 1})
	if err := acc.ConsumeStream(strings.NewReader(sse), nil); err != nil {
		t.Fatalf("ConsumeStream: %v", err)
	}
	if snap := acc.CheckpointSnapshot(); snap.Text != "Hello " || !snap.Capped {
		t.Fatalf("expected checkpoint capped at 1 chunk, got %+v", snap)
	}
	assembled, err := AssembleResponse(acc.FinalState())
	if err != nil {
		t.Fatalf("AssembleResponse: %v", err)
	}
	if !bytes.Contains(assembled, []byte(`"Hello brave world!"`)) {
		t.Fatalf("expected full uninterrupted text in final response, got %s", assembled)
	}
}

func TestVLLMQuirks_ChoicePromptTokenIDsAndUsageRecovery(t *testing.T) {
	// 1. /v1/completions in vLLM emits prompt_token_ids inside choices[0].
	opts := Options{TokenExact: true}
	acc1 := NewAccumulator(EndpointCompletions, nil, opts)
	_ = acc1.ConsumeStream(strings.NewReader(
		`data: {"id":"cmpl-vllm","model":"Qwen/Qwen3-8B","choices":[{"index":0,"text":" 3,","prompt_token_ids":[101,102,103],"token_ids":[201,202]}]}`+"\n\n",
	), nil)
	snap := acc1.CheckpointSnapshot()
	if len(snap.PromptTokenIDs) != 3 || snap.Usage.OriginalPromptTokens != 3 {
		t.Fatalf("expected choice-level prompt_token_ids [101,102,103] and OriginalPromptTokens=3, got %+v", snap)
	}
	contReq, exhausted, err := BuildContinuationRequest([]byte(`{"model":"Qwen/Qwen3-8B","prompt":"1, 2,","max_tokens":20}`), snap, opts)
	if err != nil || exhausted {
		t.Fatalf("BuildContinuationRequest: %v", err)
	}
	if !bytes.Contains(contReq, []byte(`"prompt_token_ids":[101,102,103,201,202]`)) {
		t.Fatalf("expected kv_transfer_params.prompt_token_ids in %s", contReq)
	}

	// 2. Post-reasoning continuation in text mode where Attempt 1 aborts before
	// usage (3 reasoning tokens + 2 content tokens = 5 generated) and Attempt 2
	// emits only content while vLLM misreports reasoning_tokens=3 in usage.
	chatAcc1 := NewAccumulator(EndpointChatCompletions, nil, Options{})
	_ = chatAcc1.ConsumeStream(strings.NewReader(
		`data: {"id":"chat-qwen","model":"Qwen/Qwen3-8B","choices":[{"index":0,"delta":{"role":"assistant","reasoning":"t1 "}}]}`+"\n\n"+
			`data: {"id":"chat-qwen","choices":[{"index":0,"delta":{"reasoning":"t2 "}}]}`+"\n\n"+
			`data: {"id":"chat-qwen","choices":[{"index":0,"delta":{"reasoning":"t3"}}]}`+"\n\n"+
			`data: {"id":"chat-qwen","choices":[{"index":0,"delta":{"content":"Hello "}}]}`+"\n\n"+
			`data: {"id":"chat-qwen","choices":[{"index":0,"delta":{"content":"world"}} ]}`+"\n\n",
	), nil)
	chatSnap := chatAcc1.CheckpointSnapshot()
	if chatSnap.Usage.GeneratedTokens != 5 || chatSnap.Usage.ReasoningTokens != 3 {
		t.Fatalf("expected 5 generated and 3 reasoning tokens on interrupted attempt 1, got %+v", chatSnap.Usage)
	}

	chatAcc2 := NewAccumulator(EndpointChatCompletions, chatSnap, Options{})
	if err := chatAcc2.ConsumeStream(strings.NewReader(
		`data: {"id":"chat-qwen-2","choices":[{"index":0,"delta":{"content":"!"}}]}`+"\n\n"+
			`data: {"id":"chat-qwen-2","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`+"\n\n"+
			`data: {"id":"chat-qwen-2","choices":[],"usage":{"prompt_tokens":32,"completion_tokens":4,"total_tokens":36,"completion_tokens_details":{"reasoning_tokens":3}}}`+"\n\n"+
			"data: [DONE]\n\n",
	), nil); err != nil {
		t.Fatalf("chatAcc2: %v", err)
	}
	final := chatAcc2.FinalState()
	// OriginalPromptTokens inferred as 32 - 5 = 27; ReasoningTokens stays 3 (Attempt 2 emitted 0 reasoning deltas).
	if final.Usage.OriginalPromptTokens != 27 || final.Usage.GeneratedTokens != 9 || final.Usage.ReasoningTokens != 3 {
		t.Fatalf("unexpected final usage: %+v", final.Usage)
	}
}
