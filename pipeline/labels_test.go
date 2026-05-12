package pipeline

import "testing"

func TestLabelsGetHasOnNil(t *testing.T) {
	var l Labels
	if got := l.Get("x"); got != "" {
		t.Errorf("nil Labels.Get returned %q, want \"\"", got)
	}
	if l.Has("x") {
		t.Errorf("nil Labels.Has returned true, want false")
	}
}

func TestLabelsSetAndGet(t *testing.T) {
	l := Labels{}
	l.Set("tier", "async")
	if got := l.Get("tier"); got != "async" {
		t.Errorf("Get returned %q, want async", got)
	}
	if !l.Has("tier") {
		t.Errorf("Has returned false for present key")
	}
}

func TestLabelsMerge(t *testing.T) {
	dst := Labels{"team": "A", "tier": "async"}
	src := Labels{"tier": "interactive", "model": "K2.6"}
	dst.Merge(src)
	if dst.Get("team") != "A" {
		t.Errorf("team mutated to %q", dst.Get("team"))
	}
	if dst.Get("tier") != "interactive" {
		t.Errorf("tier not overwritten, got %q", dst.Get("tier"))
	}
	if dst.Get("model") != "K2.6" {
		t.Errorf("model not added, got %q", dst.Get("model"))
	}
}

func TestLabelsCloneNil(t *testing.T) {
	var l Labels
	if got := l.Clone(); got != nil {
		t.Errorf("Clone of nil Labels returned %v, want nil", got)
	}
}

func TestLabelsCloneIndependent(t *testing.T) {
	src := Labels{"a": "1"}
	cp := src.Clone()
	cp.Set("a", "2")
	if src.Get("a") != "1" {
		t.Errorf("Clone shared backing map; src mutated to %q", src.Get("a"))
	}
}
