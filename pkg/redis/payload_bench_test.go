package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

// benchPromptSizes are prompt lengths in whitespace-separated tokens.
var benchPromptSizes = []int{1_000, 16_000, 128_000}

// benchBatchSize matches the SortedSetQueueConfig.BatchSize default.
const benchBatchSize = 10

// benchRedis returns a client against BENCH_REDIS_ADDR, or miniredis when it is unset.
func benchRedis(b *testing.B) *redis.Client {
	b.Helper()
	addr := os.Getenv("BENCH_REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(b).Addr()
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("connect to %s: %v", addr, err)
	}
	b.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// benchPayload builds an inference payload whose prompt is promptTokens words.
func benchPayload(b *testing.B, promptTokens int) json.RawMessage {
	b.Helper()
	payload, err := json.Marshal(map[string]any{
		"model":      "bench-model",
		"max_tokens": 500,
		"prompt":     strings.Repeat("word ", promptTokens),
	})
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

// seedQueue fills queue with benchBatchSize requests and returns the payload
// size in bytes. byRef stores each payload under its own key and leaves the
// sorted-set member holding only the envelope; otherwise the member carries the
// payload inline, as producers before #458 wrote it.
func seedQueue(b *testing.B, rdb *redis.Client, queue string, promptTokens int, byRef bool) int {
	b.Helper()
	ctx := context.Background()
	payload := benchPayload(b, promptTokens)
	deadline := time.Now().Add(time.Hour).Unix()
	written := []string{queue}
	for i := range benchBatchSize {
		id := fmt.Sprintf("bench-%d", i)
		token := fmt.Sprintf("gen-%d", i)
		ir := api.NewInternalRequest(api.InternalRouting{RequestToken: token}, &api.RequestMessage{
			ID:       id,
			Created:  time.Now().Unix(),
			Deadline: deadline,
			Payload:  payload,
		})
		var member []byte
		var err error
		if byRef {
			ir.PayloadRef = api.RequestPayloadKey(id, token)
			var stored json.RawMessage
			member, stored, err = api.SplitPayload(ir)
			if err != nil {
				b.Fatal(err)
			}
			if err := rdb.Set(ctx, ir.PayloadRef, []byte(stored), time.Hour).Err(); err != nil {
				b.Fatal(err)
			}
			written = append(written, ir.PayloadRef)
		} else if member, err = json.Marshal(ir); err != nil {
			b.Fatal(err)
		}
		if err := rdb.ZAdd(ctx, queue, redis.Z{Score: float64(deadline), Member: string(member)}).Err(); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { _ = rdb.Del(context.Background(), written...).Err() })
	return len(payload)
}

// BenchmarkPeekAndLoad measures one dispatch poll: peek a batch of queued
// requests and make their payloads available for dispatch. The inline arm reads
// each payload as part of its sorted-set member; the payload_ref arm reads the
// envelopes and fetches the payloads with one MGET.
func BenchmarkPeekAndLoad(b *testing.B) {
	for _, byRef := range []bool{false, true} {
		storage := "inline"
		if byRef {
			storage = "payload_ref"
		}
		for _, tokens := range benchPromptSizes {
			b.Run(fmt.Sprintf("%s/%dk_tokens", storage, tokens/1000), func(b *testing.B) {
				rdb := benchRedis(b)
				flow := &RedisSortedSetFlow{rdb: rdb}
				queue := "bench-queue:" + b.Name()
				payloadBytes := seedQueue(b, rdb, queue, tokens, byRef)
				ctx := context.Background()
				logger := logr.Discard()
				now := float64(time.Now().Unix())

				b.SetBytes(int64(payloadBytes * benchBatchSize))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					zs, err := rdb.ZRangeByScoreWithScores(ctx, queue, &redis.ZRangeBy{
						Min:   "-inf",
						Max:   "+inf",
						Count: benchBatchSize,
					}).Result()
					if err != nil {
						b.Fatal(err)
					}
					peeked, err := flow.loadRequests(ctx, zs, now, logger)
					if err != nil {
						b.Fatal(err)
					}
					for _, p := range peeked {
						if !p.ok || p.payloadMissing || len(p.ir.PublicRequest.ReqPayload()) != payloadBytes {
							b.Fatal("payload did not survive the load")
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(benchBatchSize)*float64(b.N)/b.Elapsed().Seconds(), "req/s")
			})
		}
	}
}

// legacyRequestMessage is api.RequestMessage as it stood before #458, when the
// payload decoded into a map and was re-encoded for dispatch.
type legacyRequestMessage struct {
	ID       string            `json:"id"`
	Created  int64             `json:"created"`
	Deadline int64             `json:"deadline"`
	Payload  map[string]any    `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
}

// legacyWire is the tagged envelope api.InternalRequest.UnmarshalJSON reads.
type legacyWire struct {
	Internal    api.InternalRouting `json:"internal"`
	RequestKind string              `json:"request_kind"`
	Data        json.RawMessage     `json:"data"`
}

// BenchmarkPayloadDecode prices the payload representation itself, with no Redis
// in the way. raw_message hands the stored bytes through untouched. map_payload
// decodes both envelope levels the way InternalRequest.UnmarshalJSON does, lands
// the payload in a map[string]any and re-encodes it for dispatch, which is the
// work the read path did before #458.
func BenchmarkPayloadDecode(b *testing.B) {
	for _, tokens := range benchPromptSizes {
		payload := benchPayload(b, tokens)
		member, err := json.Marshal(api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{
			ID:       "bench",
			Created:  time.Now().Unix(),
			Deadline: time.Now().Add(time.Hour).Unix(),
			Payload:  payload,
		}))
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("raw_message/%dk_tokens", tokens/1000), func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for range b.N {
				var ir api.InternalRequest
				if err := json.Unmarshal(member, &ir); err != nil {
					b.Fatal(err)
				}
				if len(ir.PublicRequest.ReqPayload()) != len(payload) {
					b.Fatal("payload did not survive the decode")
				}
			}
		})

		b.Run(fmt.Sprintf("map_payload/%dk_tokens", tokens/1000), func(b *testing.B) {
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for range b.N {
				var w legacyWire
				if err := json.Unmarshal(member, &w); err != nil {
					b.Fatal(err)
				}
				var msg legacyRequestMessage
				if err := json.Unmarshal(w.Data, &msg); err != nil {
					b.Fatal(err)
				}
				dispatched, err := json.Marshal(msg.Payload)
				if err != nil {
					b.Fatal(err)
				}
				if len(dispatched) == 0 {
					b.Fatal("payload did not survive the decode")
				}
			}
		})
	}
}
