package redis

import (
	"strings"
	"testing"
)

// TestREDIS_URL_IsDefaultNotOverride guards the regression where REDIS_URL
// overwrote an explicit url from the transport config on every load. It must be
// a fallback default: an explicit url wins, and REDIS_URL only fills an empty one.
func TestREDIS_URL_IsDefaultNotOverride(t *testing.T) {
	const explicit = `{"url":"redis://from-config:6379","queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`
	const noURL = `{"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`

	t.Run("pubsub explicit url wins over REDIS_URL", func(t *testing.T) {
		t.Setenv("REDIS_URL", "redis://from-env:6379")
		cfg, err := LoadPubSubConfig([]byte(explicit))
		if err != nil {
			t.Fatalf("LoadPubSubConfig: %v", err)
		}
		if cfg.URL != "redis://from-config:6379" {
			t.Errorf("URL = %q, want the explicit config value (env must not override)", cfg.URL)
		}
	})

	t.Run("pubsub REDIS_URL fills empty url", func(t *testing.T) {
		t.Setenv("REDIS_URL", "redis://from-env:6379")
		cfg, err := LoadPubSubConfig([]byte(noURL))
		if err != nil {
			t.Fatalf("LoadPubSubConfig: %v", err)
		}
		if cfg.URL != "redis://from-env:6379" {
			t.Errorf("URL = %q, want the REDIS_URL fallback", cfg.URL)
		}
	})

	t.Run("sortedset explicit url wins over REDIS_URL", func(t *testing.T) {
		t.Setenv("REDIS_URL", "redis://from-env:6379")
		cfg, err := LoadSortedSetConfig([]byte(explicit))
		if err != nil {
			t.Fatalf("LoadSortedSetConfig: %v", err)
		}
		if cfg.URL != "redis://from-config:6379" {
			t.Errorf("URL = %q, want the explicit config value (env must not override)", cfg.URL)
		}
	})

	t.Run("sortedset REDIS_URL fills empty url", func(t *testing.T) {
		t.Setenv("REDIS_URL", "redis://from-env:6379")
		cfg, err := LoadSortedSetConfig([]byte(noURL))
		if err != nil {
			t.Fatalf("LoadSortedSetConfig: %v", err)
		}
		if cfg.URL != "redis://from-env:6379" {
			t.Errorf("URL = %q, want the REDIS_URL fallback", cfg.URL)
		}
	})
}

func TestSortedSetConfig_ValidateNegativeValues(t *testing.T) {
	cases := []struct {
		name    string
		json    string
		wantErr string
	}{
		{
			name:    "negative claim_lease_ttl_seconds",
			json:    `{"url":"redis://localhost:6379","claim_lease_ttl_seconds":-1,"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`,
			wantErr: "claim_lease_ttl_seconds must be non-negative",
		},
		{
			name:    "negative claim_reclaim_interval_ms",
			json:    `{"url":"redis://localhost:6379","claim_reclaim_interval_ms":-500,"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`,
			wantErr: "claim_reclaim_interval_ms must be non-negative",
		},
		{
			name:    "negative poll_interval_ms",
			json:    `{"url":"redis://localhost:6379","poll_interval_ms":-10,"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`,
			wantErr: "poll_interval_ms must be non-negative",
		},
		{
			name:    "negative batch_size",
			json:    `{"url":"redis://localhost:6379","batch_size":-5,"queues":[{"queue_name":"q","igw_base_url":"http://gw"}]}`,
			wantErr: "batch_size must be non-negative",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadSortedSetConfig([]byte(tc.json))
			if err == nil {
				t.Fatalf("LoadSortedSetConfig expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("LoadSortedSetConfig error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestSortedSetConfigEmptyQueuesOnlyAllowedForReload(t *testing.T) {
	data := []byte(`{"url":"redis://localhost:6379","queues":[]}`)
	if _, err := LoadSortedSetConfig(data); err == nil {
		t.Fatal("startup loader accepted an empty queue set")
	}
	if _, err := LoadSortedSetConfigAllowEmptyQueues(data); err != nil {
		t.Fatalf("reload loader rejected empty queue set: %v", err)
	}
}
