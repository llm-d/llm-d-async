package tierpriority

import (
	"strconv"
	"testing"
	"time"

	"github.com/llm-d-incubation/llm-d-async/api"
	"github.com/llm-d-incubation/llm-d-async/pipeline"
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
	policy := NewTierPriorityPolicy("test-policy", "x-gateway-priority")

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
	s := newScheduler(10, 2)

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
	policy := NewTierPriorityPolicy("test-policy", "x-gateway-priority")

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
