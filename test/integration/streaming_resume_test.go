//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	randomrobin "github.com/llm-d/llm-d-async/pkg/async/mergepolicy/randomrobin"
	"github.com/llm-d/llm-d-async/pkg/asyncworker"
	"github.com/llm-d/llm-d-async/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newResumeSortedSetCfg(addr, igwBaseURL, requestPath string, leaseTTLSeconds, reclaimIntervalMs int64) redis.SortedSetConfig {
	enabled := true
	cfg := redis.SortedSetConfig{
		URL:                    "redis://" + addr,
		RetryQueueName:         "retry-sortedset",
		ResultQueueName:        "result-list",
		PollIntervalMs:         25,
		BatchSize:              10,
		ClaimLeaseTTLSeconds:   leaseTTLSeconds,
		ClaimReclaimIntervalMs: reclaimIntervalMs,
		Resume: &pipeline.ResumeConfig{
			Enabled:                  &enabled,
			CheckpointIntervalTokens: 2,
			CheckpointIntervalMs:     100,
			MaxResumes:               3,
		},
		Queues: []redis.SortedSetQueueConfig{{
			QueueName:      "request-sortedset",
			WorkerPoolID:   "default",
			RequestPathURL: requestPath,
			IGWBaseURL:     igwBaseURL,
		}},
	}
	cfg.ApplyDefaults()
	return cfg
}

// TestStreamingResume_FullPipeline_MidStreamDropAndCleanAck verifies that a
// chat-completion request with reasoning + content tokens streams over HTTP SSE,
// checkpoints periodically to Redis, resumes with continue_final_message after
// an upstream connection drop, delivers the combined non-streaming response,
// and atomically removes the checkpoint key on ack.
func TestStreamingResume_FullPipeline_MidStreamDropAndCleanAck(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var req map[string]any
		_ = json.Unmarshal(body, &req)

		n := attempts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)

		if n == 1 {
			_, _ = w.Write([]byte("data: {\"id\":\"chat-1\",\"model\":\"Qwen/Qwen3-8B\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning\":\"step 1. \"}}]}\n\n"))
			flusher.Flush()
			_, _ = w.Write([]byte("data: {\"id\":\"chat-1\",\"model\":\"Qwen/Qwen3-8B\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Answer: \"}}]}\n\n"))
			flusher.Flush()
			// Return without [DONE] to simulate mid-stream SSE interruption/eviction.
			return
		}

		// Verify continuation fields on second attempt.
		if req["continue_final_message"] != true || req["add_generation_prompt"] != false {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"missing continuation flags"}`))
			return
		}
		_, _ = w.Write([]byte("data: {\"id\":\"chat-2\",\"model\":\"Qwen/Qwen3-8B\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"42\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":2,\"total_tokens\":22}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		flusher.Flush()
	}))
	defer server.Close()

	s := miniredis.RunT(t)
	cfg := newResumeSortedSetCfg(s.Addr(), server.URL, "/v1/chat/completions", 30, 500)
	flow, err := redis.NewRedisSortedSetFlow(cfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	require.NoError(t, err)

	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(asyncworker.WithCheckpointStore(context.Background(), flow.CheckpointStore()))
	var wg sync.WaitGroup
	t.Cleanup(func() {
		flow.StopConsuming()
		cancel()
		wg.Wait()
		flow.Shutdown()
	})

	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	dispatch := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flow.RequestChannels(), pools)

	client := asyncworker.NewHTTPInferenceClient(server.Client())
	wg.Add(1)
	go func() {
		defer wg.Done()
		asyncworker.WorkerWithGate(ctx, ctx, flow.Characteristics(),
			client, dispatch.Channels["default"], flow.RetryChannel(), flow.ResultChannel(),
			time.Minute, nil, nil)
	}()
	flow.Start(ctx)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "request-sortedset"},
		&asyncapi.RequestMessage{
			ID:       "chat-resume-e2e",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload: testPayload(map[string]any{
				"model":      "Qwen/Qwen3-8B",
				"max_tokens": 50,
				"messages":   []any{map[string]any{"role": "user", "content": "What is 6*7?"}},
			}),
		},
	)
	member, err := ir.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(context.Background(), "request-sortedset", goredis.Z{
		Score:  ir.QueueScore(),
		Member: string(member),
	}).Err())

	waitUntil(t, 10*time.Second, func() bool {
		n, err := rdb.LLen(context.Background(), "result-list").Result()
		return err == nil && n == 1
	})

	raw, err := rdb.LRange(context.Background(), "result-list", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, raw, 1)

	var res asyncapi.ResultMessage
	require.NoError(t, json.Unmarshal([]byte(raw[0]), &res))
	assert.Contains(t, res.Payload, `"reasoning":"step 1. "`)
	assert.Contains(t, res.Payload, `"content":"Answer: 42"`)
	assert.Equal(t, int32(2), attempts.Load())

	// Checkpoint key must be deleted once acked.
	keys := s.Keys()
	for _, k := range keys {
		assert.False(t, strings.Contains(k, ":checkpoint:"), "leftover checkpoint key %s", k)
	}
}

// TestStreamingResume_GracefulShutdownHandback verifies that cancelling drainCtx
// during an active stream flushes a "shutdown" checkpoint and immediately
// releases the claim back to originQueue so a sibling instance resumes without
// waiting for lease expiry.
func TestStreamingResume_GracefulShutdownHandback(t *testing.T) {
	firstChunkSent := make(chan struct{})
	blockStream := make(chan struct{})

	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"id\":\"c-1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"text\":\"Graceful \"}]}\n\n"))
		flusher.Flush()
		close(firstChunkSent)
		<-blockStream
	}))
	defer func() {
		select {
		case <-blockStream:
		default:
			close(blockStream)
		}
		serverA.Close()
	}()

	s := miniredis.RunT(t)
	// Long lease (120s) proves handback happens via ImmediateRequeue rather than lease expiry.
	// High checkpoint intervals ensure ONLY the graceful-shutdown flush writes the checkpoint.
	cfgA := newResumeSortedSetCfg(s.Addr(), serverA.URL, "/v1/completions", 120, 5000)
	cfgA.Resume.CheckpointIntervalTokens = 1000
	cfgA.Resume.CheckpointIntervalMs = 60000
	cfgA.Queues[0].Resume = nil
	cfgA.ApplyDefaults()
	flowA, err := redis.NewRedisSortedSetFlow(cfgA, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	require.NoError(t, err)

	rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	baseCtx := asyncworker.WithCheckpointStore(context.Background(), flowA.CheckpointStore())
	consumeCtxA, consumeCancelA := context.WithCancel(baseCtx)
	drainCtxA, drainCancelA := context.WithCancel(baseCtx)

	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	dispatchA := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flowA.RequestChannels(), pools)

	var wgA sync.WaitGroup
	wgA.Add(1)
	go func() {
		defer wgA.Done()
		asyncworker.WorkerWithGate(consumeCtxA, drainCtxA, flowA.Characteristics(),
			asyncworker.NewHTTPInferenceClient(serverA.Client()),
			dispatchA.Channels["default"], flowA.RetryChannel(), flowA.ResultChannel(),
			time.Minute, nil, nil)
	}()
	flowA.Start(consumeCtxA)

	ir := asyncapi.NewInternalRequest(
		asyncapi.InternalRouting{RequestQueueName: "request-sortedset"},
		&asyncapi.RequestMessage{
			ID:       "req-graceful-shutdown",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  testPayload(map[string]any{"model": "m", "prompt": "Say: "}),
		},
	)
	member, err := ir.MarshalJSON()
	require.NoError(t, err)
	require.NoError(t, rdb.ZAdd(context.Background(), "request-sortedset", goredis.Z{
		Score:  ir.QueueScore(),
		Member: string(member),
	}).Err())

	<-firstChunkSent
	// Allow the client's loopback reader to ingest the flushed SSE frame before cancelling drainCtx.
	time.Sleep(50 * time.Millisecond)

	// Simulate pod graceful shutdown sequence from pkg/server/runner.go:
	flowA.StopConsuming()
	consumeCancelA()
	drainCancelA()
	wgA.Wait()
	flowA.Shutdown()

	// Sibling pod B starts immediately and completes from the "shutdown" checkpoint.
	var gotPrompt atomic.Value
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		gotPrompt.Store(req["prompt"])
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"c-2\",\"model\":\"m\",\"choices\":[{\"index\":0,\"text\":\"handback!\",\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer serverB.Close()

	cfgB := newResumeSortedSetCfg(s.Addr(), serverB.URL, "/v1/completions", 120, 5000)
	flowB, err := redis.NewRedisSortedSetFlow(cfgB, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
	require.NoError(t, err)

	ctxB, cancelB := context.WithCancel(asyncworker.WithCheckpointStore(context.Background(), flowB.CheckpointStore()))
	var wgB sync.WaitGroup
	t.Cleanup(func() {
		flowB.StopConsuming()
		cancelB()
		wgB.Wait()
		flowB.Shutdown()
	})

	dispatchB := randomrobin.NewRandomRobinPolicy("test", randomrobin.Config{}).
		MergeRequestChannels(flowB.RequestChannels(), pools)
	wgB.Add(1)
	go func() {
		defer wgB.Done()
		asyncworker.WorkerWithGate(ctxB, ctxB, flowB.Characteristics(),
			asyncworker.NewHTTPInferenceClient(serverB.Client()),
			dispatchB.Channels["default"], flowB.RetryChannel(), flowB.ResultChannel(),
			time.Minute, nil, nil)
	}()
	flowB.Start(ctxB)

	waitUntil(t, 5*time.Second, func() bool {
		n, err := rdb.LLen(context.Background(), "result-list").Result()
		return err == nil && n == 1
	})

	assert.Equal(t, "Say: Graceful ", gotPrompt.Load())
	raw, err := rdb.LRange(context.Background(), "result-list", 0, -1).Result()
	require.NoError(t, err)
	var res asyncapi.ResultMessage
	require.NoError(t, json.Unmarshal([]byte(raw[0]), &res))
	assert.Contains(t, res.Payload, `"text":"Graceful handback!"`)
}
