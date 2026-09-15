package sqlflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigAppliesDefaults(t *testing.T) {
	t.Setenv("SQL_URL", "")
	cfg, err := LoadConfig([]byte(`{"url":"sqlite://q.db","queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`))
	require.NoError(t, err)
	assert.Equal(t, "result-sql", cfg.ResultQueueName)
	assert.Equal(t, 1000, cfg.PollIntervalMs)
	assert.Equal(t, 10, cfg.BatchSize)
	assert.EqualValues(t, 30, cfg.LeaseTTLSeconds)
	assert.EqualValues(t, 900, cfg.HandoffTimeoutSeconds)
	require.Len(t, cfg.Queues, 1)
	assert.Equal(t, "q", cfg.Queues[0].ID)
	assert.Equal(t, "default", cfg.Queues[0].WorkerPoolID)
	assert.Equal(t, "/v1/completions", cfg.Queues[0].RequestPathURL)
}

func TestLoadConfigEnvOverridesURL(t *testing.T) {
	t.Setenv("SQL_URL", "postgres://env@host/db")
	cfg, err := LoadConfig([]byte(`{"url":"sqlite://q.db","queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`))
	require.NoError(t, err)
	assert.Equal(t, "postgres://env@host/db", cfg.URL)
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	t.Setenv("SQL_URL", "")
	for name, raw := range map[string]string{
		"missing url":      `{"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"no queues":        `{"url":"sqlite://q.db","queues":[]}`,
		"missing igw":      `{"url":"sqlite://q.db","queues":[{"queue_name":"q"}]}`,
		"duplicate queue":  `{"url":"sqlite://q.db","queues":[{"queue_name":"q","igw_base_url":"http://igw"},{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative batch":   `{"url":"sqlite://q.db","batch_size":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative lease":   `{"url":"sqlite://q.db","lease_ttl_seconds":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative handoff": `{"url":"sqlite://q.db","handoff_timeout_seconds":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"malformed json":   `{`,
	} {
		_, err := LoadConfig([]byte(raw))
		assert.Error(t, err, name)
	}
}
