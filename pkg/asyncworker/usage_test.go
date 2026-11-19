package asyncworker

import "testing"

func TestParseUsage(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		url       string
		wantInput int64
		wantOut   int64
		wantOK    bool
	}{
		{name: "both tokens", body: `{"usage":{"prompt_tokens":25,"completion_tokens":13}}`, url: "https://igw.example/v1/completions", wantInput: 25, wantOut: 13, wantOK: true},
		{name: "prompt only", body: `{"usage":{"prompt_tokens":7}}`, url: "https://igw.example/v1/completions", wantInput: 7, wantOut: 0, wantOK: true},
		{name: "completion only", body: `{"usage":{"completion_tokens":9}}`, url: "https://igw.example/v1/chat/completions", wantInput: 0, wantOut: 9, wantOK: true},
		{name: "no usage", body: `{"choices":[{"text":"hi"}]}`, url: "https://igw.example/v1/completions", wantOK: false},
		{name: "usage null", body: `{"usage":null}`, url: "https://igw.example/v1/completions", wantOK: false},
		{name: "non-json", body: `not json`, url: "https://igw.example/v1/completions", wantOK: false},
		{name: "empty", body: ``, url: "https://igw.example/v1/completions", wantOK: false},
		{name: "negative clamped", body: `{"usage":{"prompt_tokens":-5,"completion_tokens":3}}`, url: "https://igw.example/v1/completions", wantInput: 0, wantOut: 3, wantOK: true},
		{name: "custom base path", body: `{"usage":{"prompt_tokens":2,"completion_tokens":4}}`, url: "https://igw.example/gateway/v1/chat/completions?model=gpt-4o", wantInput: 2, wantOut: 4, wantOK: true},
		{name: "trailing slash", body: `{"usage":{"prompt_tokens":1,"completion_tokens":1}}`, url: "https://igw.example/v1/completions/", wantInput: 1, wantOut: 1, wantOK: true},
		{name: "bare relative path (router parity)", body: `{"usage":{"prompt_tokens":25,"completion_tokens":13}}`, url: "v1/completions", wantInput: 25, wantOut: 13, wantOK: true},
		{name: "non-openai endpoint", body: `{"usage":{"prompt_tokens":25,"completion_tokens":13}}`, url: "https://igw.example/v1/embeddings", wantOK: false},
		{name: "malformed url", body: `{"usage":{"prompt_tokens":25,"completion_tokens":13}}`, url: "://bad", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input, output, ok := ParseUsage([]byte(tt.body), tt.url)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if input != tt.wantInput || output != tt.wantOut {
				t.Errorf("ParseUsage = (%d, %d), want (%d, %d)", input, output, tt.wantInput, tt.wantOut)
			}
		})
	}
}
