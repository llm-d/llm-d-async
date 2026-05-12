package pubsub

import (
	"testing"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
)

func TestPubSubMQFlow_Pools_EmptyByDefault(t *testing.T) {
	flow := &PubSubMQFlow{}
	pools := flow.Pools()
	if len(pools) != 0 {
		t.Errorf("expected empty pools, got %d", len(pools))
	}
}

func TestPubSubMQFlow_Pools_ReturnsCopy(t *testing.T) {
	flow := &PubSubMQFlow{
		pools: []pipeline.Pool{
			{ID: "pool-a", GatewayURL: "http://a"},
			{ID: "pool-b", GatewayURL: "http://b"},
		},
	}
	pools := flow.Pools()
	if len(pools) != 2 {
		t.Fatalf("expected 2 pools, got %d", len(pools))
	}
	if pools[0].ID != "pool-a" || pools[1].ID != "pool-b" {
		t.Errorf("unexpected pools: %v", pools)
	}
}

func TestBytesTrimLeftSpace(t *testing.T) {
	tests := []struct {
		input []byte
		want  byte
	}{
		{[]byte("  ["), '['},
		{[]byte("\t{"), '{'},
		{[]byte("["), '['},
		{[]byte(""), 0},
	}
	for _, tt := range tests {
		got := bytesTrimLeftSpace(tt.input)
		if tt.want == 0 {
			if len(got) != 0 {
				t.Errorf("input %q: expected empty, got %q", tt.input, got)
			}
		} else {
			if len(got) == 0 || got[0] != tt.want {
				t.Errorf("input %q: expected first byte %q, got %q", tt.input, tt.want, got)
			}
		}
	}
}
