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

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

var _ pipeline.DispatchGate = DispatchGateFunc(nil)

// DispatchGateFunc is a function type that implements DispatchGate.
// This allows any function with the signature func(context.Context) float64
// to be used as a DispatchGate.
type DispatchGateFunc func(context.Context) float64

// Budget implements DispatchGate by calling the function itself.
func (f DispatchGateFunc) Budget(ctx context.Context) float64 {
	return f(ctx)
}

func ConstOpenGate() pipeline.DispatchGate {
	return DispatchGateFunc(func(ctx context.Context) float64 { return 1.0 })
}
