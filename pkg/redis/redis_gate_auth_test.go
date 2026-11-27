package redis_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	redis "github.com/llm-d/llm-d-async/pkg/redis"
)

const testPassword = "test-password"

// newGatedFlow starts a password-protected miniredis and a sorted-set flow on
// database db whose single queue has a redis gate pointed at the same address.
func newGatedFlow(t *testing.T, db string) (*miniredis.Miniredis, pipeline.Gate, *flowcontrol.GateFactory) {
	t.Helper()
	server := miniredis.RunT(t)
	server.RequireAuth(testPassword)

	cfg := redis.SortedSetConfig{
		URL:            "redis://default:" + testPassword + "@" + server.Addr() + db,
		PollIntervalMs: 10,
		BatchSize:      1,
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:    "requests",
			WorkerPoolID: "default",
			IGWBaseURL:   "http://gateway",
			GateConfig: pipeline.GateConfig{
				GateType:   "redis",
				GateParams: map[string]any{"address": server.Addr()},
			},
		}},
	}
	cfg.ApplyDefaults()

	gateFactory := flowcontrol.NewGateFactory("")
	flow, err := redis.NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, gateFactory)
	if err != nil {
		t.Fatalf("create sorted-set flow: %v", err)
	}
	t.Cleanup(func() {
		flow.StopConsuming()
		flow.Shutdown()
		if err := gateFactory.Close(); err != nil {
			t.Errorf("close gate factory: %v", err)
		}
	})

	channels := flow.RequestChannels()
	if len(channels) != 1 {
		t.Fatalf("request channels = %d, want 1", len(channels))
	}
	return server, channels[0].Gate, gateFactory
}

func TestRedisSortedSetFlowUsesAuthenticatedClientForRedisGate(t *testing.T) {
	server, gate, gateFactory := newGatedFlow(t, "")
	ctx := context.Background()

	if err := server.Set("dispatch-gate-budget", "0.0"); err != nil {
		t.Fatalf("set closed budget: %v", err)
	}
	if got := gate.Budget(ctx); got != 0.0 {
		t.Fatalf("closed gate budget = %v, want 0.0", got)
	}

	if err := server.Set("dispatch-gate-budget", "1.0"); err != nil {
		t.Fatalf("set open budget: %v", err)
	}
	if got := gate.Budget(ctx); got != 1.0 {
		t.Fatalf("open gate budget = %v, want 1.0", got)
	}

	if err := gateFactory.Close(); err != nil {
		t.Fatalf("close gate factory: %v", err)
	}
	if got := gate.Budget(ctx); got != 1.0 {
		t.Fatalf("gate budget after factory close = %v, want borrowed transport client to remain open", got)
	}
}

func TestRedisGateFollowsTransportDatabase(t *testing.T) {
	server, gate, _ := newGatedFlow(t, "/1")
	if err := server.DB(0).Set("dispatch-gate-budget", "0.2"); err != nil {
		t.Fatalf("set DB 0 budget: %v", err)
	}
	if err := server.DB(1).Set("dispatch-gate-budget", "0.8"); err != nil {
		t.Fatalf("set DB 1 budget: %v", err)
	}

	if got := gate.Budget(context.Background()); got != 0.8 {
		t.Fatalf("gate budget = %v, want DB 1 budget 0.8 selected by the transport URL", got)
	}
}

func TestRedisQuotaAndLeasedRateGatesUseTransportClient(t *testing.T) {
	server, _, gateFactory := newGatedFlow(t, "/1")
	ctx := context.Background()

	t.Run("redis-quota", func(t *testing.T) {
		gate, err := gateFactory.CreateGate(pipeline.GateConfig{
			GateType:   "redis-quota",
			GateParams: map[string]any{"address": server.Addr(), "limit": 5},
		})
		if err != nil {
			t.Fatalf("create redis-quota gate: %v", err)
		}
		msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "alice"}})
		verdict, err := gate.Apply(ctx, msg, &[]pipeline.GateReleaseFunc{})
		if err != nil {
			t.Fatalf("apply redis-quota gate: %v", err)
		}
		if verdict != pipeline.Continue() {
			t.Fatalf("redis-quota verdict = %+v, want Continue", verdict)
		}
		if !server.DB(1).Exists("quota:userid:alice") {
			t.Fatalf("quota counter not written to the transport's DB 1; keys: %v", server.DB(1).Keys())
		}
	})

	t.Run("redis-leased-rate", func(t *testing.T) {
		server.DB(1).HSet("control:default",
			"api_version", api.DispatchRateLimitAPIVersion,
			"pool_id", "default",
			"max_admission_rps", "5",
			"valid_until_unix_ms", strconv.FormatInt(time.Now().Add(time.Minute).UnixMilli(), 10),
			"decision_id", "decision-1",
		)
		gate, err := gateFactory.CreateGate(pipeline.GateConfig{
			GateType:   "redis-leased-rate",
			GateParams: map[string]any{"address": server.Addr(), "pool_id": "default", "control_key": "control:default"},
		})
		if err != nil {
			t.Fatalf("create redis-leased-rate gate: %v", err)
		}
		if got := gate.Budget(ctx); got != 1 {
			t.Fatalf("redis-leased-rate budget = %v, want 1 from the lease in the transport's DB 1", got)
		}
	})
}
