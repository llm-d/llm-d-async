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
	"context"
	"testing"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestGateFactory_CreateCompositeGate(t *testing.T) {
	factory := NewGateFactory("http://localhost:9090")

	t.Run("Valid composite gate", func(t *testing.T) {
		gatesJSON := `[
			{"gate_type": "constant", "gate_params": {}},
			{"gate_type": "constant", "gate_params": {}}
		]`
		gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "composite", GateParams: map[string]any{
			"gates": gatesJSON,
		}})

		assert.NoError(t, err)
		assert.NotNil(t, gate)

		budget := gate.Budget(context.Background())
		assert.Equal(t, 1.0, budget)
	})

	t.Run("Missing gates parameter", func(t *testing.T) {
		gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "composite", GateParams: map[string]any{}})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "requires 'gates' parameter")
		assert.Nil(t, gate)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "composite", GateParams: map[string]any{
			"gates": `[invalid json`,
		}})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse 'gates' parameter")
		assert.Nil(t, gate)
	})

	t.Run("Invalid inner gate", func(t *testing.T) {
		gatesJSON := `[
			{"gate_type": "prometheus-saturation", "gate_params": {}}
		]`
		// Missing 'pool' will cause prometheus-saturation to fail
		gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "composite", GateParams: map[string]any{
			"gates": gatesJSON,
		}})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create inner gate \"prometheus-saturation\"")
		assert.Nil(t, gate)
	})
}
