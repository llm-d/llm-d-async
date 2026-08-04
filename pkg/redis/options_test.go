package redis

import "testing"

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
