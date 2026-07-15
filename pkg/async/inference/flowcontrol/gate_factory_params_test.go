/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParamString(t *testing.T) {
	params := map[string]any{
		"str":   "hello",
		"num":   42.5,
		"bool":  true,
		"nil":   nil,
		"slice": []any{"a"},
	}
	assert.Equal(t, "hello", paramString(params, "str", ""))
	assert.Equal(t, "42.5", paramString(params, "num", ""))
	assert.Equal(t, "true", paramString(params, "bool", ""))
	assert.Equal(t, "default", paramString(params, "nil", "default"))
	assert.Equal(t, "default", paramString(params, "missing", "default"))
	assert.Equal(t, "default", paramString(params, "slice", "default"))
}

func TestParamFloat(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		def     float64
		want    float64
		wantErr string
	}{
		{"native float", map[string]any{"v": 0.7}, "v", 0, 0.7, ""},
		{"string float", map[string]any{"v": "0.7"}, "v", 0, 0.7, ""},
		{"int as float", map[string]any{"v": 5}, "v", 0, 5.0, ""},
		{"missing key", map[string]any{}, "v", 1.0, 1.0, ""},
		{"nil value", map[string]any{"v": nil}, "v", 1.0, 1.0, ""},
		{"empty string", map[string]any{"v": ""}, "v", 1.0, 1.0, ""},
		{"invalid string", map[string]any{"v": "abc"}, "v", 0, 0, "invalid v value"},
		{"unsupported type", map[string]any{"v": true}, "v", 0, 0, "unsupported type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := paramFloat(tt.params, tt.key, tt.def)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.InDelta(t, tt.want, got, 0.001)
			}
		})
	}
}

func TestParamInt(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		def     int
		want    int
		wantErr string
	}{
		{"native int", map[string]any{"v": 5}, "v", 0, 5, ""},
		{"float64 integer", map[string]any{"v": 10.0}, "v", 0, 10, ""},
		{"string int", map[string]any{"v": "42"}, "v", 0, 42, ""},
		{"missing key", map[string]any{}, "v", 7, 7, ""},
		{"nil value", map[string]any{"v": nil}, "v", 7, 7, ""},
		{"empty string", map[string]any{"v": ""}, "v", 7, 7, ""},
		{"non-integer float", map[string]any{"v": 2.9}, "v", 0, 0, "is not an integer"},
		{"invalid string", map[string]any{"v": "abc"}, "v", 0, 0, "invalid v value"},
		{"unsupported type", map[string]any{"v": true}, "v", 0, 0, "unsupported type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := paramInt(tt.params, tt.key, tt.def)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParamDuration(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]any
		key     string
		def     time.Duration
		want    time.Duration
		wantErr string
	}{
		{"string duration", map[string]any{"v": "5s"}, "v", 0, 5 * time.Second, ""},
		{"missing key", map[string]any{}, "v", time.Minute, time.Minute, ""},
		{"nil value", map[string]any{"v": nil}, "v", time.Minute, time.Minute, ""},
		{"invalid string", map[string]any{"v": "xyz"}, "v", 0, 0, "invalid v value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := paramDuration(tt.params, tt.key, tt.def)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParamStringMap(t *testing.T) {
	t.Run("json string", func(t *testing.T) {
		params := map[string]any{"labels": `{"k":"v"}`}
		m, err := paramStringMap(params, "labels")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"k": "v"}, m)
	})

	t.Run("structured map", func(t *testing.T) {
		params := map[string]any{"labels": map[string]any{"k": "v", "n": 42.0}}
		m, err := paramStringMap(params, "labels")
		require.NoError(t, err)
		assert.Equal(t, "v", m["k"])
		assert.Equal(t, "42", m["n"])
	})

	t.Run("map[string]string passthrough", func(t *testing.T) {
		params := map[string]any{"labels": map[string]string{"k": "v"}}
		m, err := paramStringMap(params, "labels")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"k": "v"}, m)
	})

	t.Run("missing key", func(t *testing.T) {
		m, err := paramStringMap(map[string]any{}, "labels")
		require.NoError(t, err)
		assert.Nil(t, m)
	})

	t.Run("unsupported type", func(t *testing.T) {
		params := map[string]any{"labels": 42}
		_, err := paramStringMap(params, "labels")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")
	})
}

func TestParamGateConfigs_Structured(t *testing.T) {
	params := map[string]any{
		"gates": []any{
			map[string]any{
				"gate_type":   "constant",
				"gate_params": map[string]any{},
			},
			map[string]any{
				"gate_type":   "redis",
				"gate_params": map[string]any{"address": "localhost:6379"},
			},
		},
	}
	configs, err := paramGateConfigs(params, "gates")
	require.NoError(t, err)
	require.Len(t, configs, 2)
	assert.Equal(t, "constant", configs[0].GateType)
	assert.Equal(t, "redis", configs[1].GateType)
}

func TestParamGateConfigs_TypedSlice(t *testing.T) {
	params := map[string]any{
		"gates": []pipeline.GateConfig{
			{GateType: "constant"},
		},
	}
	configs, err := paramGateConfigs(params, "gates")
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.Equal(t, "constant", configs[0].GateType)
}

func TestParamGateConfigs_UnsupportedType(t *testing.T) {
	params := map[string]any{"gates": 42}
	_, err := paramGateConfigs(params, "gates")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

func TestParamGateConfig_Structured(t *testing.T) {
	params := map[string]any{
		"gate": map[string]any{
			"gate_type":   "constant",
			"gate_params": map[string]any{},
		},
	}
	cfg, err := paramGateConfig(params, "gate")
	require.NoError(t, err)
	assert.Equal(t, "constant", cfg.GateType)
}

func TestParamGateConfig_TypedStruct(t *testing.T) {
	params := map[string]any{
		"gate": pipeline.GateConfig{GateType: "redis", GateParams: map[string]any{"address": "localhost:6379"}},
	}
	cfg, err := paramGateConfig(params, "gate")
	require.NoError(t, err)
	assert.Equal(t, "redis", cfg.GateType)
}

func TestParamGateConfig_UnsupportedType(t *testing.T) {
	params := map[string]any{"gate": 42}
	_, err := paramGateConfig(params, "gate")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}
