package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempFile(t *testing.T, dir, filename, content string) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}
	return path
}

func TestLoadPools(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		jsonContent string
		wantErr     bool
		errMsg      string
		wantCount   int
	}{
		{
			name: "valid pools config",
			jsonContent: `[
				{"id": "pool-1", "workers": 2},
				{"id": "pool-2", "workers": 4}
			]`,
			wantErr:   false,
			wantCount: 2,
		},
		{
			name: "empty pool ID",
			jsonContent: `[
				{"id": "pool-1", "workers": 2},
				{"id": "", "workers": 4}
			]`,
			wantErr: true,
			errMsg:  "pool config has empty ID",
		},
		{
			name: "duplicate pool ID",
			jsonContent: `[
				{"id": "pool-1", "workers": 2},
				{"id": "pool-1", "workers": 4}
			]`,
			wantErr: true,
			errMsg:  `duplicate pool ID: "pool-1"`,
		},
		{
			name:        "invalid json",
			jsonContent: `invalid`,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempFile(t, tmpDir, tt.name+".json", tt.jsonContent)
			pools, err := LoadPools(path)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadPools() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if tt.errMsg != "" && (err == nil || err.Error() != tt.errMsg) {
					t.Errorf("LoadPools() error message = %v, want %q", err, tt.errMsg)
				}
				return
			}
			if len(pools) != tt.wantCount {
				t.Errorf("LoadPools() returned %d pools, want %d", len(pools), tt.wantCount)
			}
		})
	}
}
