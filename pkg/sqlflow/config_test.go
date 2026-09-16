package sqlflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigAppliesDefaults(t *testing.T) {
	t.Setenv("SQL_URL", "")
	cfg, err := LoadConfig([]byte(`{"url":"postgres://u@h/db","queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`))
	require.NoError(t, err)
	assert.Equal(t, "result-sql", cfg.ResultQueueName)
	assert.Equal(t, 1000, cfg.PollIntervalMs)
	assert.Equal(t, 10, cfg.BatchSize)
	assert.Equal(t, 32, cfg.ResultBatchSize)
	assert.Equal(t, 256, cfg.CancelCheckBatchSize)
	assert.Equal(t, 5, cfg.CancelCheckLingerMs)
	assert.EqualValues(t, 30, cfg.LeaseTTLSeconds)
	assert.EqualValues(t, 900, cfg.HandoffTimeoutSeconds)
	require.Len(t, cfg.Queues, 1)
	assert.Equal(t, "q", cfg.Queues[0].ID)
	assert.Equal(t, "default", cfg.Queues[0].WorkerPoolID)
	assert.Equal(t, "/v1/completions", cfg.Queues[0].RequestPathURL)
}

func TestLoadConfigEnvOverridesURL(t *testing.T) {
	t.Setenv("SQL_URL", "postgres://env@host/db")
	cfg, err := LoadConfig([]byte(`{"url":"postgres://u@h/db","queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`))
	require.NoError(t, err)
	assert.Equal(t, "postgres://env@host/db", cfg.URL)
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	t.Setenv("SQL_URL", "")
	for name, raw := range map[string]string{
		"missing url":            `{"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"no queues":              `{"url":"postgres://u@h/db","queues":[]}`,
		"missing igw":            `{"url":"postgres://u@h/db","queues":[{"queue_name":"q"}]}`,
		"duplicate queue":        `{"url":"postgres://u@h/db","queues":[{"queue_name":"q","igw_base_url":"http://igw"},{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative batch":         `{"url":"postgres://u@h/db","batch_size":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative result batch":  `{"url":"postgres://u@h/db","result_batch_size":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative lease":         `{"url":"postgres://u@h/db","lease_ttl_seconds":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"zero cancel batch":      `{"url":"postgres://u@h/db","cancel_check_batch_size":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative cancel linger": `{"url":"postgres://u@h/db","cancel_check_linger_ms":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"negative handoff":       `{"url":"postgres://u@h/db","handoff_timeout_seconds":-1,"queues":[{"queue_name":"q","igw_base_url":"http://igw"}]}`,
		"malformed json":         `{`,
	} {
		_, err := LoadConfig([]byte(raw))
		assert.Error(t, err, name)
	}
}
