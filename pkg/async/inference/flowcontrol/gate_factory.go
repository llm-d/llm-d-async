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
	"fmt"
	"strconv"

	asyncapi "github.com/llm-d-incubation/llm-d-async/pkg/async/api"
	redisgate "github.com/llm-d-incubation/llm-d-async/pkg/redis"
	promapi "github.com/prometheus/client_golang/api"
	goredis "github.com/redis/go-redis/v9"
)

var _ asyncapi.GateFactory = (*GateFactory)(nil)

// GateFactory creates DispatchGate instances based on configuration.
type GateFactory struct {
	prometheusURL string
	redisClients  map[string]*goredis.Client
}

// NewGateFactory creates a new GateFactory with an optional Prometheus URL.
// If prometheusURL is empty, Prometheus gates will fail at creation time.
func NewGateFactory(prometheusURL string) *GateFactory {
	return &GateFactory{
		prometheusURL: prometheusURL,
		redisClients:  make(map[string]*goredis.Client),
	}
}

// CreateGate creates a DispatchGate based on the gate type and parameters.
// Supported gate types:
//   - "constant": Always returns budget 1.0 (fully open)
//   - "redis": Queries Redis for dispatch budget
//   - "prometheus-saturation": Queries Prometheus for pool saturation metric
//     Optional params: threshold (default 0.8), fallback (default 0.0)
//
// For unsupported or unknown gate types, returns ConstOpenGate as a safe default.
func (f *GateFactory) CreateGate(gateType string, params map[string]string) (asyncapi.DispatchGate, error) {
	switch gateType {
	case "constant":
		return ConstOpenGate(), nil

	case "redis":
		addr := params["address"]
		if addr == "" {
			return nil, fmt.Errorf("redis gate requires an 'address' in gate_params")
		}
		client, ok := f.redisClients[addr]
		if !ok {
			client = goredis.NewClient(&goredis.Options{Addr: addr})
			f.redisClients[addr] = client
		}
		budgetKey := params["budget_key"]
		if budgetKey == "" {
			budgetKey = "dispatch-gate-budget"
		}
		return redisgate.NewRedisDispatchGate(client, budgetKey), nil

	case "prometheus-saturation":
		if f.prometheusURL == "" {
			return nil, fmt.Errorf("prometheus-saturation gate type requires --prometheus-url flag to be set")
		}

		pool := params["pool"]

		threshold := 0.8 // default threshold
		if thresholdStr := params["threshold"]; thresholdStr != "" {
			t, err := strconv.ParseFloat(thresholdStr, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid threshold value '%s': %w", thresholdStr, err)
			}
			threshold = t
		}

		fallback := 0.0 // default fallback saturation
		if fallbackStr := params["fallback"]; fallbackStr != "" {
			fb, err := strconv.ParseFloat(fallbackStr, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid fallback value '%s': %w", fallbackStr, err)
			}
			fallback = fb
		}

		queryExpr := params["query"]
		if queryExpr == "" {
			labels := map[string]string{}
			if pool != "" {
				labels["inference_pool"] = pool
			}
			queryExpr = buildPromQL("inference_extension_flow_control_pool_saturation", labels)
		}

		source, err := NewPromQLMetricSource(promapi.Config{Address: f.prometheusURL}, queryExpr)
		if err != nil {
			return nil, fmt.Errorf("failed to create Prometheus metric source: %w", err)
		}

		return NewSaturationMetricDispatchGateWithSource(source, threshold, fallback), nil

	default:
		// Unknown gate types default to open gate
		return ConstOpenGate(), nil
	}
}
