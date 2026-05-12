package pipeline

import "testing"

func TestAttachReleaseNilNoop(t *testing.T) {
	m := &EmbelishedRequestMessage{}
	m.AttachRelease(nil)
	if len(m.releases) != 0 {
		t.Errorf("nil release was attached: %d entries", len(m.releases))
	}
}

func TestFireReleasesLIFO(t *testing.T) {
	m := &EmbelishedRequestMessage{}
	var order []int
	m.AttachRelease(func() { order = append(order, 1) })
	m.AttachRelease(func() { order = append(order, 2) })
	m.AttachRelease(func() { order = append(order, 3) })
	m.FireReleases()
	if len(order) != 3 || order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Errorf("LIFO order broken: got %v, want [3 2 1]", order)
	}
}

func TestFireReleasesIdempotent(t *testing.T) {
	m := &EmbelishedRequestMessage{}
	calls := 0
	m.AttachRelease(func() { calls++ })
	m.FireReleases()
	m.FireReleases()
	if calls != 1 {
		t.Errorf("release fired %d times, want 1", calls)
	}
}

func TestFireReleasesEmptyOk(t *testing.T) {
	m := &EmbelishedRequestMessage{}
	m.FireReleases() // should not panic
}
