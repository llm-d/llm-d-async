/*
Copyright 2026 The llm-d Authors
*/

package flowcontrol

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func TestLocalMaxConcurrencyGate_AdmitsUpToMax(t *testing.T) {
	g := NewLocalMaxConcurrencyGate(3)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		msg := &pipeline.EmbelishedRequestMessage{}
		v, err := g.Apply(ctx, msg)
		if err != nil {
			t.Fatalf("slot %d Apply err: %v", i, err)
		}
		if v.Terminate {
			t.Fatalf("slot %d should Continue, got %+v", i, v)
		}
	}
}

func TestLocalMaxConcurrencyGate_ReleaseRestoresSlot(t *testing.T) {
	g := NewLocalMaxConcurrencyGate(2)
	ctx := context.Background()
	msg1 := &pipeline.EmbelishedRequestMessage{}
	msg2 := &pipeline.EmbelishedRequestMessage{}
	if _, err := g.Apply(ctx, msg1); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := g.Apply(ctx, msg2); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	// Fire msg1's release; a third Apply should succeed.
	msg1.FireReleases()
	msg3 := &pipeline.EmbelishedRequestMessage{}
	if _, err := g.Apply(ctx, msg3); err != nil {
		t.Fatalf("third Apply after release: %v", err)
	}
}

func TestLocalMaxConcurrencyGate_ConcurrentAcquireRelease(t *testing.T) {
	const max = 4
	const workers = 32
	const iterations = 100

	g := NewLocalMaxConcurrencyGate(max)
	var inFlight, peak atomic.Int64
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				msg := &pipeline.EmbelishedRequestMessage{}
				if _, err := g.Apply(ctx, msg); err != nil {
					t.Errorf("Apply: %v", err)
					return
				}
				current := inFlight.Add(1)
				for {
					p := peak.Load()
					if current <= p || peak.CompareAndSwap(p, current) {
						break
					}
				}
				inFlight.Add(-1)
				msg.FireReleases()
			}
		}()
	}
	wg.Wait()

	if peak.Load() > max {
		t.Errorf("observed peak in-flight %d exceeded max %d", peak.Load(), max)
	}
}

func TestLocalMaxConcurrencyGate_ZeroOrNegativeMaxClampsToOne(t *testing.T) {
	for _, max := range []int{0, -5} {
		g := NewLocalMaxConcurrencyGate(max)
		ctx := context.Background()
		msg1 := &pipeline.EmbelishedRequestMessage{}
		if _, err := g.Apply(ctx, msg1); err != nil {
			t.Errorf("max=%d: first Apply: %v", max, err)
		}
		// Second Apply should block; verify it returns on ctx cancel.
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		msg2 := &pipeline.EmbelishedRequestMessage{}
		v, err := g.Apply(cctx, msg2)
		if err == nil {
			t.Errorf("max=%d: expected ctx error", max)
		}
		if !v.Terminate || !v.Redeliver {
			t.Errorf("max=%d: expected Refuse on ctx cancel, got %+v", max, v)
		}
	}
}
