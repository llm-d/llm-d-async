package e2e

import (
	"os"
	"testing"
)

func TestGetEnvString(t *testing.T) {
	const key = "LLMD_E2E_TEST_GET_ENV_STRING"
	t.Run("unset", func(t *testing.T) {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		if got := getEnvString(key, "fallback"); got != "fallback" {
			t.Fatalf("got %q, want fallback", got)
		}
	})
	t.Run("empty", func(t *testing.T) {
		t.Setenv(key, "")
		if got := getEnvString(key, "fallback"); got != "" {
			t.Fatalf("got %q, want empty string", got)
		}
	})
	t.Run("set", func(t *testing.T) {
		t.Setenv(key, "custom")
		if got := getEnvString(key, "fallback"); got != "custom" {
			t.Fatalf("got %q, want custom", got)
		}
	})
}
