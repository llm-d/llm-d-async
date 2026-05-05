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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCascadeMetricSource_RequiresAtLeastTwoSources(t *testing.T) {
	assert.Panics(t, func() { NewCascadeMetricSource() })
	assert.Panics(t, func() {
		NewCascadeMetricSource(&mockMetricSource{samples: []Sample{{Value: 1}}})
	})
}

func TestCascadeMetricSource_UsesPrimary(t *testing.T) {
	primary := &mockMetricSource{samples: []Sample{{Value: 0.7}}}
	secondary := &mockMetricSource{samples: []Sample{{Value: 0.3}}}
	cascade := NewCascadeMetricSource(primary, secondary)

	samples, err := cascade.Query(context.Background())
	require.NoError(t, err)
	require.Len(t, samples, 1)
	assert.Equal(t, 0.7, samples[0].Value)
}

func TestCascadeMetricSource_FallsBackOnPrimaryError(t *testing.T) {
	primary := &mockMetricSource{err: errors.New("unavailable")}
	secondary := &mockMetricSource{samples: []Sample{{Value: 0.3}}}
	cascade := NewCascadeMetricSource(primary, secondary)

	samples, err := cascade.Query(context.Background())
	require.NoError(t, err)
	require.Len(t, samples, 1)
	assert.Equal(t, 0.3, samples[0].Value)
}

func TestCascadeMetricSource_FallsBackOnEmptySamples(t *testing.T) {
	primary := &mockMetricSource{samples: []Sample{}}
	secondary := &mockMetricSource{samples: []Sample{{Value: 0.5}}}
	cascade := NewCascadeMetricSource(primary, secondary)

	samples, err := cascade.Query(context.Background())
	require.NoError(t, err)
	require.Len(t, samples, 1)
	assert.Equal(t, 0.5, samples[0].Value)
}

func TestCascadeMetricSource_AllUnavailable(t *testing.T) {
	primary := &mockMetricSource{err: errors.New("primary down")}
	secondary := &mockMetricSource{err: errors.New("secondary down")}
	cascade := NewCascadeMetricSource(primary, secondary)

	samples, err := cascade.Query(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all metric sources unavailable")
	assert.Nil(t, samples)
}

func TestCascadeMetricSource_FallbackLogsOnce(t *testing.T) {
	primary := &switchableMetricSource{err: errors.New("unavailable")}
	secondary := &switchableMetricSource{samples: []Sample{{Value: 0.5}}}
	tertiary := &switchableMetricSource{samples: []Sample{{Value: 0.2}}}
	cascade := NewCascadeMetricSource(primary, secondary, tertiary)
	ctx := context.Background()

	// First call cascades to secondary — activeIndex becomes 1.
	samples, err := cascade.Query(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.5, samples[0].Value)
	assert.Equal(t, int32(1), cascade.activeIndex.Load(), "should track fallback index")

	// Subsequent calls stay on secondary — no duplicate log.
	samples, err = cascade.Query(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.5, samples[0].Value)
	assert.Equal(t, int32(1), cascade.activeIndex.Load())

	// Secondary also fails — cascades to tertiary, activeIndex becomes 2.
	secondary.set(nil, errors.New("also down"))
	samples, err = cascade.Query(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.2, samples[0].Value)
	assert.Equal(t, int32(2), cascade.activeIndex.Load(), "should track switch to tertiary")

	// Primary recovers — activeIndex back to 0.
	primary.set([]Sample{{Value: 0.8}}, nil)
	samples, err = cascade.Query(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.8, samples[0].Value)
	assert.Equal(t, int32(0), cascade.activeIndex.Load(), "should recover to primary")

	// Primary fails again — activeIndex becomes 2 (secondary still down).
	primary.set(nil, errors.New("unavailable again"))
	samples, err = cascade.Query(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0.2, samples[0].Value)
	assert.Equal(t, int32(2), cascade.activeIndex.Load(), "should re-enter fallback")
}
