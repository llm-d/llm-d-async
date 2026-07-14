package mergepolicy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadMergePolicyConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "merge-policy-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	t.Run("valid config", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "valid.json")
		content := `{
			"type": "tier-priority",
			"parameters": {
				"priority_header": "x-gateway-priority"
			}
		}`
		if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		spec, err := LoadMergePolicyConfig(configPath)
		if err != nil {
			t.Fatalf("unexpected error loading config: %v", err)
		}

		if spec.Type != "tier-priority" {
			t.Errorf("expected type 'tier-priority', got %q", spec.Type)
		}

		var params map[string]string
		if err := json.Unmarshal(spec.Parameters, &params); err != nil {
			t.Fatalf("failed to unmarshal parameters: %v", err)
		}

		if params["priority_header"] != "x-gateway-priority" {
			t.Errorf("expected priority_header 'x-gateway-priority', got %q", params["priority_header"])
		}
	})

	t.Run("invalid unknown fields", func(t *testing.T) {
		configPath := filepath.Join(tmpDir, "invalid.json")
		content := `{
			"type": "tier-priority",
			"unknown_field": "error"
		}`
		if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		_, err := LoadMergePolicyConfig(configPath)
		if err == nil {
			t.Error("expected error due to unknown field, got nil")
		}
	})
}
