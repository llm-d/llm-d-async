package asyncworker

import (
	"encoding/json"
	"net/url"
	"strings"
)

// ParseUsage extracts the OpenAI usage object from a response body. ok is
// false when requestURL does not point at an OpenAI-style completions
// endpoint, when the body is not valid JSON, or when it carries no usage
// object; such bodies are silently ignored (best-effort parse on a hot
// path). Negative token counts are clamped to 0 so they can never panic a
// counter Add.
func ParseUsage(body []byte, requestURL string) (input, output int64, ok bool) {
	u, err := url.Parse(requestURL)
	if err != nil {
		return 0, 0, false
	}
	isResponses := matchPathSuffix(u.Path, "v1/responses")
	if !isResponses && !matchPathSuffix(u.Path, "v1/completions") && !matchPathSuffix(u.Path, "v1/chat/completions") {
		return 0, 0, false
	}
	var resp struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			InputTokens      int64 `json:"input_tokens"`
			OutputTokens     int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Usage == nil {
		return 0, 0, false
	}
	if isResponses {
		input, output = resp.Usage.InputTokens, resp.Usage.OutputTokens
	} else {
		input, output = resp.Usage.PromptTokens, resp.Usage.CompletionTokens
	}
	if input < 0 {
		input = 0
	}
	if output < 0 {
		output = 0
	}
	return input, output, true
}

// matchPathSuffix reports whether path matches suffix, replicating
// llm-d-router's MatchPathSuffix: both sides are slash-trimmed so the match
// is independent of leading/trailing slashes.
func matchPathSuffix(path, suffix string) bool {
	path = strings.TrimSuffix(strings.TrimSpace(path), "/")
	suffix = strings.Trim(strings.TrimSpace(suffix), "/")
	return strings.HasSuffix(path, suffix)
}
