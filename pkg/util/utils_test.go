package util

import "testing"

func TestNormalizeURLPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{"empty string", "", "/"},
		{"no leading slash", "foo", "/foo"},
		{"has leading slash", "/foo", "/foo"},
		{"multiple segments no leading slash", "foo/bar", "/foo/bar"},
		{"multiple segments with leading slash", "/foo/bar", "/foo/bar"},
		{"just a slash", "/", "/"},
		{"double leading slash", "//foo", "//foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeURLPath(tt.path)
			if got != tt.want {
				t.Errorf("NormalizeURLPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestNormalizeBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		want    string
	}{
		{"empty string", "", ""},
		{"no trailing slash", "http://example.com", "http://example.com"},
		{"single trailing slash", "http://example.com/", "http://example.com"},
		{"multiple trailing slashes", "http://example.com///", "http://example.com"},
		{"path with trailing slash", "http://example.com/v1/", "http://example.com/v1"},
		{"just a scheme and host", "https://host", "https://host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeBaseURL(tt.baseURL)
			if got != tt.want {
				t.Errorf("NormalizeBaseURL(%q) = %q, want %q", tt.baseURL, got, tt.want)
			}
		})
	}
}
