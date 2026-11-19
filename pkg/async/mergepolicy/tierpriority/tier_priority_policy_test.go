package tierpriority

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
)

func irWithLabels(id string, labels map[string]string) *api.InternalRequest {
	ir := api.NewInternalRequest(api.InternalRouting{
		Labels: labels,
	}, &api.RequestMessage{
		ID:       id,
		Created:  1,
		Deadline: 9999999999,
	})
	return ir
}

func TestTierPriorityOrdering(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 10),
		WorkerPoolID: "pool-p",
		IGWBaseURL:   "http://gw",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-p": {ID: "pool-p", Workers: 1},
	}
	policy := NewTierPriorityPolicy("test-policy", Config{PriorityHeader: "x-gateway-priority", TierLabel: "tier"})

	// Message 1: overflow + batch => Priority 5
	m1 := irWithLabels("msg-5", map[string]string{
		"tier":                  "batch",
		api.LabelClassification: string(api.ClassificationOverflow),
	})
	// Message 2: reserved + interactive => Priority 0
	m2 := irWithLabels("msg-0", map[string]string{
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})
	// Message 3: reserved + async => Priority 1
	m3 := irWithLabels("msg-1", map[string]string{
		"tier":                  "async",
		api.LabelClassification: string(api.ClassificationReserved),
	})
	// Message 4: overflow + interactive => Priority 3
	m4 := irWithLabels("msg-3", map[string]string{
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationOverflow),
	})

	// Send them in mixed order
	ch.Channel <- m1
	ch.Channel <- m2
	ch.Channel <- m3
	ch.Channel <- m4
	close(ch.Channel)

	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	merged := dispatch.Channels["pool-p"]

	expectedOrder := []struct {
		id       string
		priority string
	}{
		{"msg-0", "0"},
		{"msg-1", "1"},
		{"msg-3", "3"},
		{"msg-5", "5"},
	}

	deadline := time.After(3 * time.Second)
	for i, expected := range expectedOrder {
		select {
		case msg := <-merged:
			if msg.PublicRequest.ReqID() != expected.id {
				t.Errorf("[%d] expected request ID %q, got %q", i, expected.id, msg.PublicRequest.ReqID())
			}
			pHeader := msg.HttpHeaders["x-gateway-priority"]
			if pHeader != expected.priority {
				t.Errorf("[%d] expected priority header %q, got %q", i, expected.priority, pHeader)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestSchedulerRoundRobin(t *testing.T) {
	s := newScheduler(10, 2, "tier")

	a1 := irWithLabels("a1", map[string]string{
		"team":                  "team-a",
		"model":                 "model-1",
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})
	a2 := irWithLabels("a2", map[string]string{
		"team":                  "team-a",
		"model":                 "model-1",
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})
	b1 := irWithLabels("b1", map[string]string{
		"team":                  "team-b",
		"model":                 "model-2",
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})
	b2 := irWithLabels("b2", map[string]string{
		"team":                  "team-b",
		"model":                 "model-2",
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})

	chA := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)}
	chB := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)}
	s.Push(a1, chA)
	s.Push(a2, chA)
	s.Push(b1, chB)
	s.Push(b2, chB)

	var order []string
	for range 4 {
		mm, ok := s.Pop()
		if !ok {
			t.Fatal("expected message")
		}
		order = append(order, mm.ir.PublicRequest.ReqID())
	}

	expected := []string{"a1", "b1", "a2", "b2"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d elements, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("at index %d: expected %q, got %q", i, v, order[i])
		}
	}
}

func TestTierPriorityFallback(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 2),
		WorkerPoolID: "pool-fb",
		IGWBaseURL:   "http://gw",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-fb": {ID: "pool-fb", Workers: 1},
	}
	policy := NewTierPriorityPolicy("test-policy", Config{PriorityHeader: "x-gateway-priority", TierLabel: "tier"})

	// Message with missing labels should map to lowest priority (5)
	m := irWithLabels("missing-labels", nil)
	ch.Channel <- m
	close(ch.Channel)

	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	merged := dispatch.Channels["pool-fb"]

	select {
	case msg := <-merged:
		if msg.PublicRequest.ReqID() != "missing-labels" {
			t.Errorf("expected missing-labels, got %q", msg.PublicRequest.ReqID())
		}
		pHeader := msg.HttpHeaders["x-gateway-priority"]
		if pHeader != strconv.Itoa(5) {
			t.Errorf("expected priority header 5 for fallback, got %q", pHeader)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestTierPriorityCustomLabel(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 2),
		WorkerPoolID: "pool-fb",
		IGWBaseURL:   "http://gw",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-fb": {ID: "pool-fb", Workers: 1},
	}
	policy := NewTierPriorityPolicy("test-policy", Config{PriorityHeader: "x-gateway-priority", TierLabel: "my_custom_tier"})

	// Message with my_custom_tier label
	m := irWithLabels("custom-label-msg", map[string]string{
		"my_custom_tier":        "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})
	ch.Channel <- m
	close(ch.Channel)

	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	merged := dispatch.Channels["pool-fb"]

	select {
	case msg := <-merged:
		if msg.PublicRequest.ReqID() != "custom-label-msg" {
			t.Errorf("expected custom-label-msg, got %q", msg.PublicRequest.ReqID())
		}
		pHeader := msg.HttpHeaders["x-gateway-priority"]
		if pHeader != "0" {
			t.Errorf("expected priority header 0, got %q", pHeader)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func TestBucketKeyCleanup(t *testing.T) {
	b := newBucket()
	chA := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)}
	chB := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)}
	keyA := fmt.Sprintf("%p", chA.Channel)
	keyB := fmt.Sprintf("%p", chB.Channel)

	irA := irWithLabels("a1", nil)
	irB := irWithLabels("b1", nil)

	b.push(keyA, msgAndMeta{ir: irA, chMeta: chA})
	b.push(keyB, msgAndMeta{ir: irB, chMeta: chB})

	if len(b.keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(b.keys))
	}

	_, ok := b.pop()
	if !ok {
		t.Fatal("expected to pop item")
	}
	if len(b.keys) != 1 {
		t.Errorf("expected 1 key remaining after pop of empty queue, got %d", len(b.keys))
	}

	_, ok = b.pop()
	if !ok {
		t.Fatal("expected to pop second item")
	}
	if len(b.keys) != 0 {
		t.Errorf("expected 0 keys remaining, got %d", len(b.keys))
	}
	if len(b.queues) != 0 {
		t.Errorf("expected map to be empty, got %v", b.queues)
	}
}

func TestPerBucketLimitsNonBlocking(t *testing.T) {
	// scheduler with capacity 1 per bucket
	s := newScheduler(1, 1, "tier")

	chA := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)}
	chB := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest)}

	// Message 1: overflow + batch => Priority 5
	m1 := irWithLabels("msg-5", map[string]string{
		"tier":                  "batch",
		api.LabelClassification: string(api.ClassificationOverflow),
	})

	// Message 2: reserved + interactive => Priority 0
	m2 := irWithLabels("msg-0", map[string]string{
		"tier":                  "interactive",
		api.LabelClassification: string(api.ClassificationReserved),
	})

	// Pushing m1 to bucket 5 succeeds
	if ok := s.Push(m1, chA); !ok {
		t.Fatal("expected push to succeed")
	}

	// Pushing another message to bucket 5 would block because capacity is 1.
	// But pushing m2 to bucket 0 should succeed immediately because bucket 0 is empty.
	done := make(chan bool)
	go func() {
		if ok := s.Push(m2, chB); !ok {
			t.Error("expected push of m2 to succeed")
		}
		done <- true
	}()

	select {
	case <-done:
		// Succeeded immediately without blocking!
	case <-time.After(500 * time.Millisecond):
		t.Fatal("push to empty bucket blocked")
	}
}

func TestTierPriorityStamping(t *testing.T) {
	ch := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 4),
		WorkerPoolID: "pool-s",
		IGWBaseURL:   "http://gw",
	}
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-s": {ID: "pool-s", Workers: 1},
	}
	policy := NewTierPriorityPolicy("test-stamping", Config{
		PriorityHeader: "x-gateway-priority",
		TierLabel:      "tier",
		LaneObjectives: map[string]string{
			"reserved-interactive": "interactive-reserved",
			"overflow-interactive": "interactive-overflow",
		},
	})

	mk := func(id, tier, class, team string) *api.InternalRequest {
		ir := api.NewInternalRequest(api.InternalRouting{
			Labels: map[string]string{"tier": tier, api.LabelClassification: class},
		}, &api.RequestMessage{
			ID: id, Created: 1, Deadline: 9999999999,
			Metadata: map[string]string{"team": team},
		})
		return ir
	}
	ch.Channel <- mk("s-reserved", "interactive", string(api.ClassificationReserved), "team-a")
	ch.Channel <- mk("s-overflow", "interactive", string(api.ClassificationOverflow), "team-b")
	close(ch.Channel)

	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch}, pools)
	merged := dispatch.Channels["pool-s"]

	expected := map[string]string{
		"s-reserved": "interactive-reserved",
		"s-overflow": "interactive-overflow",
	}
	deadline := time.After(3 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-merged:
			want := expected[msg.PublicRequest.ReqID()]
			if got := msg.HttpHeaders["x-llm-d-inference-objective"]; got != want {
				t.Errorf("%s: objective header = %q, want %q", msg.PublicRequest.ReqID(), got, want)
			}
		case <-deadline:
			t.Fatal("timed out waiting for stamped messages")
		}
	}
}

func TestAddRequestChannels_NewSourceJoinsScheduler(t *testing.T) {
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-d": {ID: "pool-d", Workers: 1},
	}
	ch1 := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 2),
		WorkerPoolID: "pool-d",
		IGWBaseURL:   "http://gw",
	}
	policy := NewTierPriorityPolicy("test-dynamic", Config{TierLabel: "tier"})
	dispatch := policy.MergeRequestChannels([]pipeline.RequestChannel{ch1}, pools)
	merged := dispatch.Channels["pool-d"]

	ch2 := pipeline.RequestChannel{
		Channel:      make(chan *api.InternalRequest, 2),
		WorkerPoolID: "pool-d",
		IGWBaseURL:   "http://gw2",
	}
	if err := policy.AddRequestChannels([]pipeline.RequestChannel{ch2}, pools); err != nil {
		t.Fatalf("AddRequestChannels failed: %v", err)
	}

	ch1.Channel <- irWithLabels("dyn-1", nil)
	ch2.Channel <- irWithLabels("dyn-2", nil)

	got := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case msg := <-merged:
			got[msg.PublicRequest.ReqID()] = true
		case <-deadline:
			t.Fatalf("timed out, only got %v", got)
		}
	}
	if !got["dyn-1"] || !got["dyn-2"] {
		t.Fatalf("expected messages from both channels, got %v", got)
	}

	// Closing the added channel removes exactly its reader; the remaining
	// source keeps feeding the same merged channel.
	close(ch2.Channel)
	deadline = time.After(3 * time.Second)
	for {
		select {
		case msg := <-merged:
			if msg.PublicRequest.ReqID() == "dyn-3" {
				return
			}
		case ch1.Channel <- irWithLabels("dyn-3", nil):
		case <-deadline:
			t.Fatal("merged channel stopped after source closure")
		}
	}
}

func TestAddRequestChannels_UnknownPool(t *testing.T) {
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-d": {ID: "pool-d", Workers: 1},
	}
	policy := NewTierPriorityPolicy("test-err", Config{})
	policy.MergeRequestChannels(nil, pools)

	if err := policy.AddRequestChannels([]pipeline.RequestChannel{
		{Channel: make(chan *api.InternalRequest), WorkerPoolID: "ghost"},
	}, pools); err == nil {
		t.Fatal("expected error for unknown pool")
	}
}

func TestAddRequestChannels_Idempotent(t *testing.T) {
	pools := map[string]pipeline.WorkerPoolConfig{
		"pool-d": {ID: "pool-d", Workers: 1},
	}
	policy := NewTierPriorityPolicy("test-idem", Config{})
	dispatch := policy.MergeRequestChannels(nil, pools)
	merged := dispatch.Channels["pool-d"]

	ch := pipeline.RequestChannel{Channel: make(chan *api.InternalRequest, 1), WorkerPoolID: "pool-d", IGWBaseURL: "http://gw"}
	for range 3 {
		if err := policy.AddRequestChannels([]pipeline.RequestChannel{ch}, pools); err != nil {
			t.Fatalf("AddRequestChannels failed: %v", err)
		}
	}

	ch.Channel <- irWithLabels("once", nil)
	select {
	case msg := <-merged:
		if msg.PublicRequest.ReqID() != "once" {
			t.Fatalf("unexpected id %q", msg.PublicRequest.ReqID())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}
	select {
	case msg := <-merged:
		t.Fatalf("duplicate delivery of registered channel: %q", msg.PublicRequest.ReqID())
	case <-time.After(300 * time.Millisecond):
	}
}
