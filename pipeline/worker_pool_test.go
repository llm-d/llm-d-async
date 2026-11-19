package pipeline

import (
	"os"
	"path/filepath"
	"strings"
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

func TestLoadWorkerPools(t *testing.T) {
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
			errMsg:  "pool config at index 1 has an empty ID",
		},
		{
			name: "duplicate pool ID",
			jsonContent: `[
				{"id": "pool-1", "workers": 2},
				{"id": "pool-1", "workers": 4}
			]`,
			wantErr: true,
			errMsg:  `duplicate pool ID found: "pool-1"`,
		},
		{
			name: "invalid worker count (zero)",
			jsonContent: `[
				{"id": "pool-1", "workers": 0}
			]`,
			wantErr: true,
			errMsg:  `pool "pool-1" must have at least 1 worker`,
		},
		{
			name: "invalid worker count (negative)",
			jsonContent: `[
				{"id": "pool-1", "workers": -5}
			]`,
			wantErr: true,
			errMsg:  `pool "pool-1" must have at least 1 worker`,
		},
		{
			name:        "invalid json",
			jsonContent: `invalid`,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTempFile(t, tmpDir, strings.ReplaceAll(tt.name, " ", "_")+".json", tt.jsonContent)
			pools, err := LoadWorkerPools(path)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadWorkerPools() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if tt.errMsg != "" && (err == nil || !strings.Contains(err.Error(), tt.errMsg)) {
					t.Errorf("LoadWorkerPools() error = %v, want error containing %q", err, tt.errMsg)
				}
				return
			}
			if len(pools) != tt.wantCount {
				t.Errorf("LoadWorkerPools() returned %d pools, want %d", len(pools), tt.wantCount)
			}
		})
	}
}
