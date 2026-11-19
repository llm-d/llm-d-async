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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countingMetricSource counts how many times Query is called and returns
// configurable samples/errors.
type countingMetricSource struct {
	calls   atomic.Int32
	samples []Sample
	err     error
}

func (c *countingMetricSource) Query(_ context.Context) ([]Sample, error) {
	c.calls.Add(1)
	return c.samples, c.err
}

type blockingMetricSource struct {
	calls   atomic.Int32
	started chan struct{}
	resume  chan struct{}
}

var _ MetricSource = (*blockingMetricSource)(nil)

func (s *blockingMetricSource) Query(ctx context.Context) ([]Sample, error) {
	if s.calls.Add(1) == 1 {
		close(s.started)
	}
	select {
	case <-s.resume:
		return []Sample{{Value: 42.0}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCachedMetricSource_CancelledWaiter(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancelled"
		if deadline {
			name = "deadline exceeded"
		}
		t.Run(name, func(t *testing.T) {
			inner := &blockingMetricSource{
				started: make(chan struct{}),
				resume:  make(chan struct{}),
			}
			cached := NewCachedMetricSource(inner, time.Hour)
			firstDone := make(chan error, 1)
			go func() {
				_, err := cached.Query(context.Background())
				firstDone <- err
			}()
			<-inner.started

			ctx, cancel := context.WithCancel(context.Background())
			expectedErr := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				expectedErr = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()

			waiterDone := make(chan error, 1)
			go func() {
				_, err := cached.Query(ctx)
				waiterDone <- err
			}()
			select {
			case err := <-waiterDone:
				if !errors.Is(err, expectedErr) {
					t.Errorf("Query error = %v, want %v", err, expectedErr)
				}
			case <-time.After(time.Second):
				t.Error("cancelled Query is blocked by another caller's source query")
			}

			close(inner.resume)
			require.NoError(t, <-firstDone)
			samples, err := cached.Query(context.Background())
			require.NoError(t, err)
			require.Equal(t, []Sample{{Value: 42.0}}, samples)
			require.Equal(t, int32(1), inner.calls.Load())
		})
	}
}

func TestCachedMetricSource_CachesWithinTTL(t *testing.T) {
	inner := &countingMetricSource{
		samples: []Sample{{Value: 42.0}},
	}
	cached := NewCachedMetricSource(inner, 1*time.Hour)
	ctx := context.Background()

	// First call should hit the source.
	s1, err := cached.Query(ctx)
	require.NoError(t, err)
	require.Len(t, s1, 1)
	require.Equal(t, 42.0, s1[0].Value)
	require.Equal(t, int32(1), inner.calls.Load())

	// Second call within TTL should return cached result.
	s2, err := cached.Query(ctx)
	require.NoError(t, err)
	require.Equal(t, s1, s2)
	require.Equal(t, int32(1), inner.calls.Load())
}

func TestCachedMetricSource_RefreshesAfterTTL(t *testing.T) {
	inner := &countingMetricSource{
		samples: []Sample{{Value: 1.0}},
	}
	cached := NewCachedMetricSource(inner, 10*time.Millisecond)
	ctx := context.Background()

	_, err := cached.Query(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(1), inner.calls.Load())

	// Wait for TTL to expire.
	time.Sleep(20 * time.Millisecond)

	_, err = cached.Query(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(2), inner.calls.Load())
}

func TestCachedMetricSource_CachesErrors(t *testing.T) {
	expectedErr := errors.New("prometheus down")
	inner := &countingMetricSource{
		err: expectedErr,
	}
	cached := NewCachedMetricSource(inner, 1*time.Hour)
	ctx := context.Background()

	_, err := cached.Query(ctx)
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, int32(1), inner.calls.Load())

	// Error should also be cached.
	_, err = cached.Query(ctx)
	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, int32(1), inner.calls.Load())
}

func TestCachedMetricSource_WorksWithGates(t *testing.T) {
	inner := &countingMetricSource{
		samples: []Sample{{Value: 0.0}},
	}
	cached := NewCachedMetricSource(inner, 1*time.Hour)
	ctx := context.Background()

	// CachedMetricSource implements MetricSource, so it should work
	// transparently with any gate.
	binaryGate := NewBinaryMetricDispatchGateWithSource(cached)
	require.Equal(t, 1.0, binaryGate.Budget(ctx))
	require.Equal(t, 1.0, binaryGate.Budget(ctx))
	require.Equal(t, int32(1), inner.calls.Load())

	satGate := NewSaturationDispatchGate(cached, 0.8, 0.0)
	// Source returns 0.0; NewSaturationDispatchGate converts threshold 0.8 → budget 0.2.
	// Since 0.0 <= 0.2, the gate returns 0.0 (closed).
	require.Equal(t, 0.0, satGate.Budget(ctx))
	// Still only 1 call since the cache hasn't expired.
	require.Equal(t, int32(1), inner.calls.Load())
}
