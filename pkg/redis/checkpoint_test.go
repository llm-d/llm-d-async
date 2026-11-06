package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func newCheckpointTestFlow(t *testing.T, mr *miniredis.Miniredis, cluster bool, queueName string) *RedisSortedSetFlow {
	t.Helper()
	mode := ""
	var addrs []string
	url := "redis://" + mr.Addr()
	if cluster {
		mode = ModeCluster
		addrs = []string{mr.Addr()}
		url = ""
	}
	cfg := SortedSetConfig{
		URL:                    url,
		Mode:                   mode,
		Addrs:                  addrs,
		RetryQueueName:         "retry-q",
		ResultQueueName:        "result-q",
		PollIntervalMs:         25,
		BatchSize:              10,
		ClaimLeaseTTLSeconds:   10,
		ClaimReclaimIntervalMs: 50,
		Queues: []SortedSetQueueConfig{{
			ID:         "q1",
			QueueName:  queueName,
			IGWBaseURL: "http://igw:8000",
		}},
	}
	// In unit tests against standalone miniredis, construct directly when clusterKeys=true
	// so go-redis uses a standalone client while RedisSortedSetFlow exercises cluster key slots.
	if cluster {
		cfg.Mode = ""
		cfg.Addrs = nil
		cfg.URL = "redis://" + mr.Addr()
	}
	flow, err := NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	if err != nil {
		t.Fatalf("NewRedisSortedSetFlow: %v", err)
	}
	if cluster {
		flow.clusterKeys = true
	}
	t.Cleanup(func() {
		_ = flow.rdb.Close()
	})
	return flow
}

func seedAndClaim(t *testing.T, flow *RedisSortedSetFlow, queueName, reqID, reqToken string) (string, string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Minute).Unix()
	ir := api.NewInternalRequest(api.InternalRouting{
		RequestToken:     reqToken,
		QueueID:          "q1",
		RequestQueueName: queueName,
	}, &api.RequestMessage{
		ID:       reqID,
		Created:  time.Now().Unix(),
		Deadline: deadline,
		Payload:  json.RawMessage(`{"model":"m","prompt":"hello"}`),
	})
	memberBytes, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal InternalRequest: %v", err)
	}
	member := string(memberBytes)
	if err := flow.rdb.ZAdd(ctx, queueName, redis.Z{Score: ir.QueueScore(), Member: member}).Err(); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	token, claimed, err := flow.claimRequest(ctx, queueName, ir, member, float64(deadline))
	if err != nil || !claimed {
		t.Fatalf("claimRequest claimed=%v err=%v", claimed, err)
	}
	return member, token
}

func TestCheckpointStore_SaveLoadAndAtomicAckCleanup(t *testing.T) {
	mr := miniredis.RunT(t)
	flow := newCheckpointTestFlow(t, mr, false, "req-queue")
	ctx := context.Background()

	_, _ = seedAndClaim(t, flow, "req-queue", "req-1", "tok-1")
	store := flow.CheckpointStore()

	got, err := store.Load(ctx, "req-queue", "req-1", "tok-1")
	if err != nil {
		t.Fatalf("initial Load error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil checkpoint before Save, got %+v", got)
	}

	rec := pipeline.CheckpointRecord{
		Endpoint:               "/v1/completions",
		State:                  json.RawMessage(`{"endpoint":"/v1/completions","text":"partial "}`),
		ResumeCount:            1,
		GeneratedTokens:        12,
		RecomputedPromptTokens: 20,
		LastInterruptReason:    pipeline.ResumeReasonEviction,
		UpdatedAtUnixMs:        time.Now().UnixMilli(),
	}
	res, err := store.Save(ctx, "req-queue", "req-1", "tok-1", rec)
	if err != nil {
		t.Fatalf("Save error: %v", err)
	}
	if res != pipeline.CheckpointSaved {
		t.Fatalf("Save result = %v, want CheckpointSaved", res)
	}

	ckptKey := newClaimKeys("req-queue", false).checkpoint(claimKey("req-1", "tok-1"))
	if ttl := mr.TTL(ckptKey); ttl <= 0 {
		t.Fatalf("expected positive TTL on checkpoint key %q, got %v", ckptKey, ttl)
	}

	loaded, err := store.Load(ctx, "req-queue", "req-1", "tok-1")
	if err != nil || loaded == nil {
		t.Fatalf("Load after Save: loaded=%+v err=%v", loaded, err)
	}
	if loaded.GeneratedTokens != 12 || loaded.LastInterruptReason != pipeline.ResumeReasonEviction {
		t.Fatalf("unexpected loaded checkpoint: %+v", loaded)
	}

	pushed, err := flow.ackResult(ctx, "req-queue", "result-q", "req-1", "tok-1", "", `{"id":"req-1"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("ackResult pushed=%v err=%v", pushed, err)
	}

	afterAck, err := store.Load(ctx, "req-queue", "req-1", "tok-1")
	if err != nil {
		t.Fatalf("Load after ack error: %v", err)
	}
	if afterAck != nil {
		t.Fatalf("expected checkpoint deleted on ackResult, got %+v", afterAck)
	}
}

func TestCheckpointStore_FencingBlocksStaleWorkerAfterReclaim(t *testing.T) {
	mr := miniredis.RunT(t)
	flowA := newCheckpointTestFlow(t, mr, false, "req-queue")
	flowB := newCheckpointTestFlow(t, mr, false, "req-queue")
	ctx := context.Background()

	_, _ = seedAndClaim(t, flowA, "req-queue", "req-fence", "gen-1")
	resA1, err := flowA.CheckpointStore().Save(ctx, "req-queue", "req-fence", "gen-1", pipeline.CheckpointRecord{
		Endpoint:        "/v1/chat/completions",
		State:           json.RawMessage(`{"text":"worker-A-initial"}`),
		GeneratedTokens: 5,
	})
	if err != nil || resA1 != pipeline.CheckpointSaved {
		t.Fatalf("flowA initial Save = (%v, %v)", resA1, err)
	}

	// Expire lease in claim index and let flowB reclaim and re-claim the request.
	keys := newClaimKeys("req-queue", false)
	cid := claimKey("req-fence", "gen-1")
	if err := flowA.rdb.ZAdd(ctx, keys.idx, redis.Z{Score: float64(time.Now().Add(-time.Minute).Unix()), Member: cid}).Err(); err != nil {
		t.Fatalf("force expire lease: %v", err)
	}
	reclaimed, err := flowB.reclaimExpiredClaims(ctx)
	if err != nil || reclaimed != 1 {
		t.Fatalf("reclaimExpiredClaims = (%d, %v), want 1", reclaimed, err)
	}

	// Worker B claims the redelivered entry and reads Worker A's checkpoint.
	members, err := flowB.rdb.ZRange(ctx, "req-queue", 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("ZRange pending = %v, %v", members, err)
	}
	var ir api.InternalRequest
	if err := json.Unmarshal([]byte(members[0]), &ir); err != nil {
		t.Fatalf("unmarshal reclaimed member: %v", err)
	}
	if _, claimed, err := flowB.claimRequest(ctx, "req-queue", &ir, members[0], float64(ir.PublicRequest.ReqDeadline())); err != nil || !claimed {
		t.Fatalf("flowB claimRequest = (%v, %v)", claimed, err)
	}

	loadedByB, err := flowB.CheckpointStore().Load(ctx, "req-queue", "req-fence", "gen-1")
	if err != nil || loadedByB == nil || loadedByB.GeneratedTokens != 5 {
		t.Fatalf("flowB expected to load flowA checkpoint, got %+v (err=%v)", loadedByB, err)
	}

	// Worker B writes newer progress; stale Worker A (still holding its old in-memory
	// claimHandle) must be fenced and unable to clobber Worker B's checkpoint.
	if resB, err := flowB.CheckpointStore().Save(ctx, "req-queue", "req-fence", "gen-1", pipeline.CheckpointRecord{
		Endpoint:        "/v1/chat/completions",
		State:           json.RawMessage(`{"text":"worker-B-resumed"}`),
		ResumeCount:     1,
		GeneratedTokens: 25,
	}); err != nil || resB != pipeline.CheckpointSaved {
		t.Fatalf("flowB Save = (%v, %v)", resB, err)
	}

	resStale, err := flowA.CheckpointStore().Save(ctx, "req-queue", "req-fence", "gen-1", pipeline.CheckpointRecord{
		Endpoint:        "/v1/chat/completions",
		State:           json.RawMessage(`{"text":"worker-A-stale-overwrite"}`),
		GeneratedTokens: 6,
	})
	if err != nil {
		t.Fatalf("flowA stale Save unexpected error: %v", err)
	}
	if resStale != pipeline.CheckpointFenced {
		t.Fatalf("flowA stale Save = %v, want CheckpointFenced", resStale)
	}

	finalLoaded, err := flowB.CheckpointStore().Load(ctx, "req-queue", "req-fence", "gen-1")
	if err != nil || finalLoaded == nil || finalLoaded.GeneratedTokens != 25 {
		t.Fatalf("checkpoint clobbered by stale worker: %+v (err=%v)", finalLoaded, err)
	}
}

func TestCheckpointStore_ClusterCrossSlotDropClaimDeletesCheckpoint(t *testing.T) {
	mr := miniredis.RunT(t)
	flow := newCheckpointTestFlow(t, mr, true, "{pool-a}:requests")
	ctx := context.Background()

	_, _ = seedAndClaim(t, flow, "{pool-a}:requests", "req-cluster", "tok-cluster")
	keys := newClaimKeys("{pool-a}:requests", true)
	cid := claimKey("req-cluster", "tok-cluster")
	if !sameHashSlot(keys.owners, keys.checkpoint(cid)) {
		t.Fatalf("owners key %q and checkpoint key %q do not share cluster hash slot", keys.owners, keys.checkpoint(cid))
	}

	if res, err := flow.CheckpointStore().Save(ctx, "{pool-a}:requests", "req-cluster", "tok-cluster", pipeline.CheckpointRecord{
		Endpoint:        "/v1/responses",
		State:           json.RawMessage(`{"endpoint":"/v1/responses","text":"cluster"}`),
		GeneratedTokens: 9,
	}); err != nil || res != pipeline.CheckpointSaved {
		t.Fatalf("cluster Save = (%v, %v)", res, err)
	}

	// Cross-slot result list triggers ackResultCluster -> dropClaimScript.
	resultList := "{other-slot}:results"
	if sameHashSlot(keys.owners, resultList) {
		t.Fatalf("expected cross-slot test setup, got identical hash slot")
	}
	pushed, err := flow.ackResult(ctx, "{pool-a}:requests", resultList, "req-cluster", "tok-cluster", "", `{"id":"req-cluster"}`, 0)
	if err != nil || !pushed {
		t.Fatalf("cluster cross-slot ackResult = (%v, %v)", pushed, err)
	}

	after, err := flow.CheckpointStore().Load(ctx, "{pool-a}:requests", "req-cluster", "tok-cluster")
	if err != nil || after != nil {
		t.Fatalf("expected cluster checkpoint deleted by dropClaimScript, got %+v (err=%v)", after, err)
	}
}

func TestSortedSetConfig_ResumeMergeAndValidation(t *testing.T) {
	raw := []byte(`{
		"url": "redis://localhost:6379",
		"resume": {
			"enabled": true,
			"max_resumes": 4,
			"checkpoint_interval_tokens": 32
		},
		"queues": [
			{"id": "q-inherit", "queue_name": "q1", "igw_base_url": "http://igw"},
			{"id": "q-override", "queue_name": "q2", "igw_base_url": "http://igw", "resume": {"enabled": false, "max_resumes": 2}}
		]
	}`)
	cfg, err := LoadSortedSetConfig(raw)
	if err != nil {
		t.Fatalf("LoadSortedSetConfig: %v", err)
	}
	if !cfg.Queues[0].Resume.IsEnabled() || cfg.Queues[0].Resume.MaxResumes != 4 || cfg.Queues[0].Resume.CheckpointIntervalTokens != 32 || cfg.Queues[0].Resume.MaxCheckpointTokens != pipeline.DefaultMaxCheckpointTokens {
		t.Fatalf("unexpected inherited resume config: %+v", cfg.Queues[0].Resume)
	}
	if cfg.Queues[1].Resume.IsEnabled() || cfg.Queues[1].Resume.MaxResumes != 2 || cfg.Queues[1].Resume.CheckpointIntervalTokens != 32 {
		t.Fatalf("unexpected overridden resume config: %+v", cfg.Queues[1].Resume)
	}

	badRaw := []byte(`{
		"url": "redis://localhost:6379",
		"resume": {"max_resumes": -1},
		"queues": [{"queue_name": "q1", "igw_base_url": "http://igw"}]
	}`)
	if _, err := LoadSortedSetConfig(badRaw); err == nil {
		t.Fatal("expected negative resume.max_resumes to fail validation")
	}
}
