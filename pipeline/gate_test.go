package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/llm-d-incubation/llm-d-async/api"
)

func TestVerdictConstructors(t *testing.T) {
	if Continue.Terminate {
		t.Errorf("Continue should not Terminate")
	}
	d := Drop(nil)
	if !d.Terminate || d.Redeliver || d.Result != nil {
		t.Errorf("Drop(nil) = %+v, want Terminate=true Redeliver=false Result=nil", d)
	}
	rm := &api.ResultMessage{ID: "x"}
	dr := Drop(rm)
	if !dr.Terminate || dr.Redeliver || dr.Result != rm {
		t.Errorf("Drop(rm) = %+v, want Terminate=true Redeliver=false Result=rm", dr)
	}
	r := Refuse()
	if !r.Terminate || !r.Redeliver || r.Result != nil {
		t.Errorf("Refuse() = %+v, want Terminate=true Redeliver=true Result=nil", r)
	}
}

func TestAlwaysContinue(t *testing.T) {
	v, err := AlwaysContinue.Apply(context.Background(), &EmbelishedRequestMessage{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if v.Terminate {
		t.Errorf("verdict = %+v, want Continue", v)
	}
}

func TestGateFuncMutatesLabels(t *testing.T) {
	g := GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
		msg.Labels.Set("class", "reserved")
		return Continue, nil
	})
	msg := &EmbelishedRequestMessage{Labels: Labels{}}
	if _, err := g.Apply(context.Background(), msg); err != nil {
		t.Fatalf("err: %v", err)
	}
	if got := msg.Labels.Get("class"); got != "reserved" {
		t.Errorf("class = %q, want reserved", got)
	}
}

// TestApplyChainSnapshotTruncate verifies that releases attached upstream
// of a chain stay intact when the chain short-circuits, while releases
// attached by the chain itself fire and are truncated.
func TestApplyChainSnapshotTruncate(t *testing.T) {
	var fired []string
	upstream := func() { fired = append(fired, "upstream") }
	chainA := func() { fired = append(fired, "chainA") }
	chainB := func() { fired = append(fired, "chainB") }

	msg := &EmbelishedRequestMessage{Labels: Labels{}}
	msg.AttachRelease(upstream)

	gates := []Gate{
		GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
			msg.AttachRelease(chainA)
			return Continue, nil
		}),
		GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
			msg.AttachRelease(chainB)
			return Refuse(), nil
		}),
	}
	v, err := ApplyChain(context.Background(), msg, gates)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.Terminate || !v.Redeliver {
		t.Errorf("verdict = %+v, want Refuse", v)
	}
	// chainB then chainA fire (LIFO); upstream stays attached.
	want := []string{"chainB", "chainA"}
	if len(fired) != len(want) {
		t.Fatalf("fired = %v, want %v", fired, want)
	}
	for i := range want {
		if fired[i] != want[i] {
			t.Errorf("fired[%d] = %q, want %q", i, fired[i], want[i])
		}
	}
	if len(msg.releases) != 1 {
		t.Errorf("releases after chain = %d, want 1 (upstream)", len(msg.releases))
	}
}

func TestApplyChainContinueKeepsReleases(t *testing.T) {
	msg := &EmbelishedRequestMessage{Labels: Labels{}}
	gates := []Gate{
		GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
			msg.AttachRelease(func() {})
			return Continue, nil
		}),
		GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
			msg.AttachRelease(func() {})
			return Continue, nil
		}),
	}
	v, err := ApplyChain(context.Background(), msg, gates)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if v.Terminate {
		t.Errorf("verdict = %+v, want Continue", v)
	}
	if len(msg.releases) != 2 {
		t.Errorf("releases after chain Continue = %d, want 2", len(msg.releases))
	}
}

func TestApplyChainErrorFiresChainReleases(t *testing.T) {
	var fired bool
	msg := &EmbelishedRequestMessage{Labels: Labels{}}
	gates := []Gate{
		GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
			msg.AttachRelease(func() { fired = true })
			return Continue, nil
		}),
		GateFunc(func(ctx context.Context, msg *EmbelishedRequestMessage) (Verdict, error) {
			return Verdict{}, errors.New("boom")
		}),
	}
	v, err := ApplyChain(context.Background(), msg, gates)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !v.Terminate || !v.Redeliver {
		t.Errorf("verdict on err = %+v, want Refuse", v)
	}
	if !fired {
		t.Errorf("chain release did not fire on err")
	}
	if len(msg.releases) != 0 {
		t.Errorf("releases after err = %d, want 0", len(msg.releases))
	}
}
