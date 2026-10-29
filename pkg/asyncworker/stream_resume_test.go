package asyncworker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	"github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/prometheus/client_golang/prometheus/testutil"
	goredis "github.com/redis/go-redis/v9"
)

type memCheckpointStore struct {
	rec    *pipeline.CheckpointRecord
	fenced bool
	saves  int
}

func (m *memCheckpointStore) Load(_ context.Context, _, _, _ string) (*pipeline.CheckpointRecord, error) {
	if m.rec == nil {
		return nil, nil
	}
	cp := *m.rec
	cp.State = append(json.RawMessage(nil), m.rec.State...)
	return &cp, nil
}

func (m *memCheckpointStore) Save(_ context.Context, _, _, _ string, record pipeline.CheckpointRecord) (pipeline.CheckpointWriteResult, error) {
	m.saves++
	if m.fenced {
		return pipeline.CheckpointFenced, nil
	}
	cp := record
	cp.State = append(json.RawMessage(nil), record.State...)
	m.rec = &cp
	return pipeline.CheckpointSaved, nil
}

func sseBody(frames ...string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(strings.Join(frames, "")))
}

func resumeEmb(id, token, queueName, url string, payload map[string]any, maxResumes int) pipeline.EmbelishedRequestMessage {
	raw, _ := json.Marshal(payload)
	ir := asyncapi.NewInternalRequest(asyncapi.InternalRouting{
		RequestToken:     token,
		QueueID:          queueName,
		RequestQueueName: queueName,
	}, &asyncapi.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  raw,
	})
	return pipeline.EmbelishedRequestMessage{
		InternalRequest: ir,
		HttpHeaders:     map[string]string{"Content-Type": "application/json"},
		RequestURL:      url,
		WorkerPoolID:    "default",
		Resume: pipeline.ResumeConfig{
			Enabled:                  pipeline.BoolPtr(true),
			MaxResumes:               maxResumes,
			CheckpointIntervalTokens: 2,
			CheckpointIntervalMs:     5000,
			MaxCheckpointTokens:      4096,
		},
	}
}

func TestStreamingResume_CompletionsErrorAndContinuation(t *testing.T) {
	store := &memCheckpointStore{}
	var attempt atomic.Int32

	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		bodyBytes, _ := io.ReadAll(req.Body)
		var doc map[string]any
		_ = json.Unmarshal(bodyBytes, &doc)
		if doc["stream"] != true {
			t.Fatalf("expected stream=true, got %v", doc["stream"])
		}
		n := attempt.Add(1)
		if n == 1 {
			if doc["prompt"] != "Once upon a " {
				t.Fatalf("attempt 1 prompt = %q", doc["prompt"])
			}
			// Truncate mid-stream without [DONE] to simulate server drop!
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: sseBody(
					"data: {\"id\":\"cmpl-1\",\"model\":\"qwen\",\"created\":100,\"choices\":[{\"index\":0,\"text\":\"time \"}]}\n\n",
					"data: {\"id\":\"cmpl-1\",\"model\":\"qwen\",\"created\":100,\"choices\":[{\"index\":0,\"text\":\"in a \"}]}\n\n",
				),
			}, nil
		}
		if doc["prompt"] != "Once upon a time in a " {
			t.Fatalf("attempt 2 continuation prompt = %q, want concatenated prefix", doc["prompt"])
		}
		if int(doc["max_tokens"].(float64)) != 18 {
			t.Fatalf("attempt 2 max_tokens = %v, want 18 (20 - 2)", doc["max_tokens"])
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: sseBody(
				"data: {\"id\":\"cmpl-2\",\"model\":\"qwen\",\"created\":101,\"choices\":[{\"index\":0,\"text\":\"land far away.\",\"finish_reason\":\"stop\"}]}\n\n",
				"data: {\"id\":\"cmpl-2\",\"model\":\"qwen\",\"created\":101,\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":3,\"total_tokens\":12}}\n\n",
				"data: [DONE]\n\n",
			),
		}, nil
	})

	reqCh := make(chan pipeline.EmbelishedRequestMessage, 2)
	retryCh := make(chan pipeline.RetryMessage, 2)
	resCh := make(chan asyncapi.ResultMessage, 2)
	ctx, cancel := context.WithCancel(WithCheckpointStore(context.Background(), store))
	defer cancel()

	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), reqCh, retryCh, resCh, 10*time.Second, nil)

	queueID := "q-cmpl-err"
	beforeResumes := testutil.ToFloat64(metrics.Resumes.WithLabelValues(queueID, queueID, "default", pipeline.ResumeReasonError))
	beforeRecov := testutil.ToFloat64(metrics.ResumeRecoveredTokens.WithLabelValues(queueID, queueID, "default", pipeline.ResumeReasonError))
	beforeRecomp := testutil.ToFloat64(metrics.ResumeRecomputedPromptTokens.WithLabelValues(queueID, queueID, "default"))

	msg := resumeEmb("req-cmpl", "tok-1", queueID, "http://igw/v1/completions", map[string]any{
		"model":      "qwen",
		"prompt":     "Once upon a ",
		"max_tokens": 20,
	}, 3)
	reqCh <- msg

	retry := <-retryCh
	if retry.RetryCount != 1 {
		t.Fatalf("expected RetryCount=1 after server stream drop, got %d", retry.RetryCount)
	}
	if store.rec == nil || store.rec.LastInterruptReason != pipeline.ResumeReasonError {
		t.Fatalf("expected error checkpoint saved, got %+v", store.rec)
	}

	reqCh <- retry.EmbelishedRequestMessage
	res := <-resCh
	if res.StatusCode != http.StatusOK {
		t.Fatalf("final status = %d, payload = %s", res.StatusCode, res.Payload)
	}

	var parsed struct {
		ID      string `json:"id"`
		Choices []struct {
			Text         string `json:"text"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(res.Payload), &parsed); err != nil {
		t.Fatalf("unmarshal assembled completion: %v", err)
	}
	if parsed.ID != "cmpl-1" {
		t.Errorf("ResponseID = %q, want cmpl-1 from attempt 1", parsed.ID)
	}
	if got := parsed.Choices[0].Text; got != "time in a land far away." {
		t.Errorf("assembled text = %q, want %q", got, "time in a land far away.")
	}
	if parsed.Usage.CompletionTokens != 5 {
		t.Errorf("delivered completion_tokens = %d, want 5 (2 + 3)", parsed.Usage.CompletionTokens)
	}

	if got := testutil.ToFloat64(metrics.Resumes.WithLabelValues(queueID, queueID, "default", pipeline.ResumeReasonError)) - beforeResumes; got != 1 {
		t.Errorf("resumes_total{reason=error} delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ResumeRecoveredTokens.WithLabelValues(queueID, queueID, "default", pipeline.ResumeReasonError)) - beforeRecov; got != 2 {
		t.Errorf("resume_recovered_tokens_total delta = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.ResumeRecomputedPromptTokens.WithLabelValues(queueID, queueID, "default")) - beforeRecomp; got != 9 {
		t.Errorf("resume_recomputed_prompt_tokens_total delta = %v, want 9", got)
	}
}

func TestStreamingResume_ChatReasoningAndEvictionDoesNotIncrementRetryCount(t *testing.T) {
	store := &memCheckpointStore{}
	var attempt atomic.Int32

	httpClient := NewTestClient(func(req *http.Request) (*http.Response, error) {
		bodyBytes, _ := io.ReadAll(req.Body)
		var doc map[string]any
		_ = json.Unmarshal(bodyBytes, &doc)
		n := attempt.Add(1)

		switch n {
		case 1:
			// Stream partial reasoning block, then drop mid-reasoning.
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: sseBody(
					"data: {\"id\":\"chat-1\",\"model\":\"Qwen/Qwen3-8B\",\"created\":200,\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning\":\"Let me think step 1. \"}}]}\n\n",
					"data: {\"id\":\"chat-1\",\"model\":\"Qwen/Qwen3-8B\",\"created\":200,\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"Step 2: 6*7=42.\"}}]}\n\n",
				),
			}, nil
		case 2:
			// Simulate pre-TTFT EPP flow-control eviction (HTTP 429 + x-llm-d-request-dropped-reason: evicted).
			if doc["continue_final_message"] != true || doc["add_generation_prompt"] != false {
				t.Fatalf("attempt 2 expected continue_final_message=true & add_generation_prompt=false, got %v", doc)
			}
			h := make(http.Header)
			h.Set(asyncapi.DroppedReasonHeader, asyncapi.DroppedReasonEvicted)
			h.Set("Retry-After", "0")
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     h,
				Body:       io.NopCloser(bytes.NewBufferString("inference error: ResourceExhausted - evicted")),
			}, nil
		default:
			// Attempt 3: finish reasoning with </think> split in content delta + final answer!
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: sseBody(
					"data: {\"id\":\"chat-3\",\"model\":\"Qwen/Qwen3-8B\",\"created\":202,\"choices\":[{\"index\":0,\"delta\":{\"content\":\" Done thinking.</think>The answer is 42.\"},\"finish_reason\":\"stop\"}]}\n\n",
					"data: {\"id\":\"chat-3\",\"model\":\"Qwen/Qwen3-8B\",\"created\":202,\"choices\":[],\"usage\":{\"prompt_tokens\":30,\"completion_tokens\":8,\"total_tokens\":38}}\n\n",
					"data: [DONE]\n\n",
				),
			}, nil
		}
	})

	reqCh := make(chan pipeline.EmbelishedRequestMessage, 4)
	retryCh := make(chan pipeline.RetryMessage, 4)
	resCh := make(chan asyncapi.ResultMessage, 2)
	ctx, cancel := context.WithCancel(WithCheckpointStore(context.Background(), store))
	defer cancel()

	go Worker(ctx, ctx, pipeline.Characteristics{}, NewHTTPInferenceClient(httpClient), reqCh, retryCh, resCh, 10*time.Second, nil)

	queueID := "q-chat-evict"
	beforeEvictResumes := testutil.ToFloat64(metrics.Resumes.WithLabelValues(queueID, queueID, "default", pipeline.ResumeReasonEviction))

	msg := resumeEmb("req-chat", "tok-chat", queueID, "http://igw/v1/chat/completions", map[string]any{
		"model":    "Qwen/Qwen3-8B",
		"messages": []map[string]any{{"role": "user", "content": "What is 6*7?"}},
	}, 4)

	// Attempt 1 -> mid-stream drop -> RetryCount becomes 1
	reqCh <- msg
	r1 := <-retryCh
	if r1.RetryCount != 1 {
		t.Fatalf("after attempt 1 drop: RetryCount = %d, want 1", r1.RetryCount)
	}

	// Attempt 2 -> HTTP 429 evicted -> RetryCount MUST remain 1 (no penalty)!
	reqCh <- r1.EmbelishedRequestMessage
	r2 := <-retryCh
	if r2.RetryCount != 1 {
		t.Fatalf("after HTTP 429 eviction: RetryCount = %d, want unchanged 1", r2.RetryCount)
	}
	if store.rec == nil || store.rec.LastInterruptReason != pipeline.ResumeReasonEviction {
		t.Fatalf("expected checkpoint reason=eviction, got %+v", store.rec)
	}

	// Attempt 3 -> completes mid-think split cleanly!
	reqCh <- r2.EmbelishedRequestMessage
	res := <-resCh
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.StatusCode, res.Payload)
	}

	var chatResp struct {
		ID      string `json:"id"`
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				Reasoning string `json:"reasoning"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(res.Payload), &chatResp); err != nil {
		t.Fatalf("unmarshal chat response: %v", err)
	}
	if chatResp.ID != "chat-1" {
		t.Errorf("ID = %q, want chat-1", chatResp.ID)
	}
	if got := chatResp.Choices[0].Message.Reasoning; got != "Let me think step 1. Step 2: 6*7=42. Done thinking." {
		t.Errorf("reasoning = %q", got)
	}
	if got := chatResp.Choices[0].Message.Content; got != "The answer is 42." {
		t.Errorf("content = %q, want %q", got, "The answer is 42.")
	}
	if got := testutil.ToFloat64(metrics.Resumes.WithLabelValues(queueID, queueID, "default", pipeline.ResumeReasonEviction)) - beforeEvictResumes; got != 1 {
		t.Errorf("resumes_total{reason=eviction} delta = %v, want 1", got)
	}
}

func TestStreamingResume_ResponsesShutdownCrashRedeliveryMaxResumesAndCancel(t *testing.T) {
	t.Run("responses graceful shutdown immediate requeue and resume", func(t *testing.T) {
		mr := miniredis.RunT(t)
		cfgJSON, _ := json.Marshal(redis.SortedSetConfig{
			URL:                    "redis://" + mr.Addr(),
			PollIntervalMs:         20,
			BatchSize:              5,
			ClaimLeaseTTLSeconds:   30,
			ClaimReclaimIntervalMs: 50,
			Resume: &pipeline.ResumeConfig{
				Enabled:                  pipeline.BoolPtr(true),
				MaxResumes:               3,
				CheckpointIntervalTokens: 1,
			},
			Queues: []redis.SortedSetQueueConfig{{
				ID:              "q-resp",
				QueueName:       "q-resp",
				ResultQueueName: "res-resp",
				RequestPathURL:  "/v1/responses",
				IGWBaseURL:      "http://igw",
			}},
		})
		ssCfg, err := redis.LoadSortedSetConfig(cfgJSON)
		if err != nil {
			t.Fatalf("LoadSortedSetConfig: %v", err)
		}
		flowA, err := redis.NewRedisSortedSetFlow(*ssCfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
		if err != nil {
			t.Fatalf("NewRedisSortedSetFlow A: %v", err)
		}

		consumeCtxA, stopConsumeA := context.WithCancel(context.Background())
		drainCtxA, cancelDrainA := context.WithCancel(WithCheckpointStore(context.Background(), flowA.CheckpointStore()))

		chunkSent := make(chan struct{})
		clientA := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
			pr, pw := io.Pipe()
			go func() {
				_, _ = pw.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"qwen\",\"created_at\":300}}\n\n"))
				_, _ = pw.Write([]byte("event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n"))
				_, _ = pw.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"msg_1\",\"delta\":\"Hello from \"}\n\n"))
				close(chunkSent)
				<-req.Context().Done()
				_ = pw.CloseWithError(req.Context().Err())
			}()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       pr,
			}, nil
		}))

		reqChA := make(chan pipeline.EmbelishedRequestMessage, 1)
		go func() {
			for ir := range flowA.RequestChannels()[0].Channel {
				reqChA <- pipeline.EmbelishedRequestMessage{
					InternalRequest: ir,
					HttpHeaders:     map[string]string{"Content-Type": "application/json"},
					RequestURL:      "http://igw/v1/responses",
					WorkerPoolID:    "default",
					Resume:          flowA.RequestChannels()[0].Resume,
				}
			}
		}()

		workerADone := make(chan struct{})
		go func() {
			defer close(workerADone)
			Worker(consumeCtxA, drainCtxA, flowA.Characteristics(), clientA, reqChA, flowA.RetryChannel(), flowA.ResultChannel(), 30*time.Second, nil)
		}()
		flowA.Start(consumeCtxA)

		// Enqueue /v1/responses request into Redis.
		rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
		defer func() { _ = rdb.Close() }()
		ir := asyncapi.NewInternalRequest(asyncapi.InternalRouting{
			RequestToken:     "tok-resp-1",
			QueueID:          "q-resp",
			RequestQueueName: "q-resp",
			ResultQueueName:  "res-resp",
		}, &asyncapi.RequestMessage{
			ID:       "req-resp-1",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(5 * time.Minute).Unix(),
			Payload:  json.RawMessage(`{"model":"qwen","input":"Say hello"}`),
		})
		memberBytes, _ := json.Marshal(ir)
		if err := rdb.ZAdd(context.Background(), "q-resp", goredis.Z{Score: ir.QueueScore(), Member: string(memberBytes)}).Err(); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}

		// Wait until Worker A has streamed the first chunk, then trigger graceful shutdown!
		<-chunkSent
		stopConsumeA()
		flowA.StopConsuming()
		cancelDrainA()
		<-workerADone
		flowA.Shutdown()

		// Now start Worker B on a fresh flow instance: the request must already be back in q-resp
		// (released immediately without waiting 30s lease expiry!) with LastInterruptReason="shutdown".
		flowB, err := redis.NewRedisSortedSetFlow(*ssCfg, []pipeline.WorkerPoolConfig{{ID: "default", Workers: 1}}, nil)
		if err != nil {
			t.Fatalf("NewRedisSortedSetFlow B: %v", err)
		}
		ctxB, cancelB := context.WithCancel(WithCheckpointStore(context.Background(), flowB.CheckpointStore()))
		defer cancelB()
		defer flowB.Shutdown()

		clientB := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: sseBody(
					"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_2\",\"model\":\"qwen\",\"created_at\":301}}\n\n",
					"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_2\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
					"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"msg_2\",\"delta\":\"Worker B!\"}\n\n",
					"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_2\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"Worker B!\"}]}}\n\n",
					"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_2\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n",
				),
			}, nil
		}))

		reqChB := make(chan pipeline.EmbelishedRequestMessage, 1)
		go func() {
			for next := range flowB.RequestChannels()[0].Channel {
				reqChB <- pipeline.EmbelishedRequestMessage{
					InternalRequest: next,
					HttpHeaders:     map[string]string{"Content-Type": "application/json"},
					RequestURL:      "http://igw/v1/responses",
					WorkerPoolID:    "default",
					Resume:          flowB.RequestChannels()[0].Resume,
				}
			}
		}()
		beforeShutdownResume := testutil.ToFloat64(metrics.Resumes.WithLabelValues("q-resp", "q-resp", "default", pipeline.ResumeReasonShutdown))
		go Worker(ctxB, ctxB, flowB.Characteristics(), clientB, reqChB, flowB.RetryChannel(), flowB.ResultChannel(), 10*time.Second, nil)
		flowB.Start(ctxB)

		// Poll result list in Redis!
		deadline := time.Now().Add(3 * time.Second)
		var rawResult string
		for time.Now().Before(deadline) {
			vals, _ := rdb.LRange(context.Background(), "res-resp", 0, -1).Result()
			if len(vals) > 0 {
				rawResult = vals[0]
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
		if rawResult == "" {
			t.Fatal("timed out waiting for Worker B to complete graceful-shutdown resumed /v1/responses request")
		}
		if !strings.Contains(rawResult, "Hello from Worker B!") {
			t.Fatalf("expected concatenated /v1/responses text 'Hello from Worker B!', got: %s", rawResult)
		}
		if got := testutil.ToFloat64(metrics.Resumes.WithLabelValues("q-resp", "q-resp", "default", pipeline.ResumeReasonShutdown)) - beforeShutdownResume; got != 1 {
			t.Errorf("resumes_total{reason=shutdown} delta = %v, want 1", got)
		}
	})

	t.Run("max_resumes cap and n>1 fallback and in-flight cancel cleanup", func(t *testing.T) {
		// 1. max_resumes=1 terminates on second mid-stream failure.
		store := &memCheckpointStore{}
		dropClient := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       sseBody("data: {\"id\":\"c1\",\"model\":\"m\",\"choices\":[{\"index\":0,\"text\":\"chunk \"}]}\n\n"),
			}, nil
		}))
		reqCh := make(chan pipeline.EmbelishedRequestMessage, 2)
		retryCh := make(chan pipeline.RetryMessage, 2)
		resCh := make(chan asyncapi.ResultMessage, 2)
		ctx, cancel := context.WithCancel(WithCheckpointStore(context.Background(), store))
		defer cancel()
		go Worker(ctx, ctx, pipeline.Characteristics{}, dropClient, reqCh, retryCh, resCh, 5*time.Second, nil)

		msg := resumeEmb("req-cap", "tok-cap", "q-cap", "http://igw/v1/completions", map[string]any{"model": "m", "prompt": "p"}, 1)
		reqCh <- msg
		r1 := <-retryCh // attempt 1 -> checkpoint saved (resumeCount=0)
		reqCh <- r1.EmbelishedRequestMessage
		term := <-resCh // attempt 2 (resumeCount=1 == max_resumes) -> fails terminally!
		if !strings.Contains(term.Payload, "max_resumes (1) exhausted") {
			t.Fatalf("expected max_resumes exhausted error, got: %s", term.Payload)
		}

		// 2. n > 1 falls back to non-streaming SendRequest without injecting stream=true.
		var sawStreamField bool
		nonStreamClient := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(req.Body)
			var doc map[string]any
			_ = json.Unmarshal(b, &doc)
			_, sawStreamField = doc["stream"]
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewBufferString(`{"id":"c-multi","choices":[{"text":"a"},{"text":"b"}]}`)),
			}, nil
		}))
		reqCh2 := make(chan pipeline.EmbelishedRequestMessage, 1)
		resCh2 := make(chan asyncapi.ResultMessage, 1)
		go Worker(ctx, ctx, pipeline.Characteristics{}, nonStreamClient, reqCh2, retryCh, resCh2, 5*time.Second, nil)
		reqCh2 <- resumeEmb("req-n2", "tok-n2", "q-cap", "http://igw/v1/completions", map[string]any{"model": "m", "prompt": "p", "n": 2}, 3)
		res2 := <-resCh2
		if res2.StatusCode != http.StatusOK || sawStreamField {
			t.Fatalf("expected n=2 non-streaming fallback (sawStream=%v, status=%d)", sawStreamField, res2.StatusCode)
		}

		// 3. Crash redelivery (empty LastInterruptReason) records reason="redelivery".
		crashStore := &memCheckpointStore{}
		stateBytes, _ := json.Marshal(map[string]any{
			"endpoint": "/v1/completions",
			"attempt":  1,
			"text":     "recovered ",
			"usage":    map[string]int{"generated_tokens": 3},
		})
		crashStore.rec = &pipeline.CheckpointRecord{
			Endpoint:            "/v1/completions",
			State:               stateBytes,
			ResumeCount:         0,
			GeneratedTokens:     3,
			LastInterruptReason: "", // periodic checkpoint before SIGKILL!
		}
		okClient := NewHTTPInferenceClient(NewTestClient(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body: sseBody(
					"data: {\"id\":\"c-rec\",\"model\":\"m\",\"choices\":[{\"index\":0,\"text\":\"tail\",\"finish_reason\":\"stop\"}]}\n\n",
					"data: [DONE]\n\n",
				),
			}, nil
		}))
		ctx3, cancel3 := context.WithCancel(WithCheckpointStore(context.Background(), crashStore))
		defer cancel3()
		reqCh3 := make(chan pipeline.EmbelishedRequestMessage, 1)
		resCh3 := make(chan asyncapi.ResultMessage, 1)
		beforeRedelivery := testutil.ToFloat64(metrics.Resumes.WithLabelValues("q-redeliv", "q-redeliv", "default", pipeline.ResumeReasonRedelivery))
		go Worker(ctx3, ctx3, pipeline.Characteristics{}, okClient, reqCh3, retryCh, resCh3, 5*time.Second, nil)
		reqCh3 <- resumeEmb("req-redeliv", "tok-r", "q-redeliv", "http://igw/v1/completions", map[string]any{"model": "m", "prompt": "start "}, 3)
		res3 := <-resCh3
		if !strings.Contains(res3.Payload, "recovered tail") {
			t.Fatalf("expected crash-redelivery continuation, got: %s", res3.Payload)
		}
		if got := testutil.ToFloat64(metrics.Resumes.WithLabelValues("q-redeliv", "q-redeliv", "default", pipeline.ResumeReasonRedelivery)) - beforeRedelivery; got != 1 {
			t.Errorf("resumes_total{reason=redelivery} delta = %v, want 1", got)
		}
	})
}
