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
	if !openAICompletionsEndpoint(requestURL) {
		return 0, 0, false
	}
	var resp struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Usage == nil {
		return 0, 0, false
	}
	if resp.Usage.PromptTokens < 0 {
		resp.Usage.PromptTokens = 0
	}
	if resp.Usage.CompletionTokens < 0 {
		resp.Usage.CompletionTokens = 0
	}
	return resp.Usage.PromptTokens, resp.Usage.CompletionTokens, true
}

// openAICompletionsEndpoint reports whether requestURL points at an
// OpenAI-style completions endpoint. It mirrors llm-d-router's parser path
// suffix matching (pkg/epp/framework/common/request/headers.go MatchPathSuffix),
// which claims the bare suffixes "v1/completions" and "v1/chat/completions",
// so gateways behind custom base paths (and per-request endpoint overrides)
// keep working while the parse is skipped everywhere else.
func openAICompletionsEndpoint(requestURL string) bool {
	u, err := url.Parse(requestURL)
	if err != nil {
		return false
	}
	return matchPathSuffix(u.Path, "v1/completions") || matchPathSuffix(u.Path, "v1/chat/completions")
}

// matchPathSuffix reports whether path matches suffix, replicating
// llm-d-router's MatchPathSuffix: both sides are slash-trimmed so the match
// is independent of leading/trailing slashes.
func matchPathSuffix(path, suffix string) bool {
	path = strings.TrimSuffix(strings.TrimSpace(path), "/")
	suffix = strings.Trim(strings.TrimSpace(suffix), "/")
	return strings.HasSuffix(path, suffix)
}
