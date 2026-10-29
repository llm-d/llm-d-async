package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

func BenchmarkCheckpointWrite(b *testing.B) {
	sizes := []int{256, 1024, 4096}
	concurrencies := []int{1, 16, 64}

	for _, tokCount := range sizes {
		stateJSON, err := json.Marshal(map[string]any{
			"endpoint": "/v1/chat/completions",
			"attempt":  1,
			"text":     strings.Repeat("tok ", tokCount),
			"usage": map[string]int{
				"original_prompt_tokens": 512,
				"generated_tokens":       tokCount,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		record := pipeline.CheckpointRecord{
			Endpoint:        "/v1/chat/completions",
			State:           stateJSON,
			ResumeCount:     0,
			GeneratedTokens: tokCount,
			UpdatedAtUnixMs: time.Now().UnixMilli(),
		}
		rawRec, _ := json.Marshal(record)

		for _, workers := range concurrencies {
			name := fmt.Sprintf("tokens=%d/workers=%d", tokCount, workers)
			b.Run(name, func(b *testing.B) {
				rdb := benchRedis(b)
				ctx := context.Background()
				queueName := fmt.Sprintf("bench-ckpt-%d-%d", tokCount, workers)
				flow := &RedisSortedSetFlow{
					rdb:           rdb,
					claimLeaseTTL: 5 * time.Minute,
				}
				store := flow.CheckpointStore()

				deadline := time.Now().Add(time.Hour).Unix()
				writtenKeys := []string{
					queueName,
					newClaimKeys(queueName, false).claimed,
					newClaimKeys(queueName, false).owners,
					newClaimKeys(queueName, false).idx,
				}

				for i := range workers {
					reqID := fmt.Sprintf("req-%d", i)
					reqTok := fmt.Sprintf("gen-%d", i)
					ir := api.NewInternalRequest(api.InternalRouting{
						RequestToken:     reqTok,
						RequestQueueName: queueName,
					}, &api.RequestMessage{
						ID:       reqID,
						Created:  time.Now().Unix(),
						Deadline: deadline,
						Payload:  json.RawMessage(`{"model":"bench","prompt":"hi"}`),
					})
					memberBytes, err := json.Marshal(ir)
					if err != nil {
						b.Fatal(err)
					}
					if err := rdb.ZAdd(ctx, queueName, redis.Z{Score: float64(deadline), Member: string(memberBytes)}).Err(); err != nil {
						b.Fatal(err)
					}
					if _, claimed, err := flow.claimRequest(ctx, queueName, ir, string(memberBytes), float64(deadline)); err != nil || !claimed {
						b.Fatalf("claimRequest(%s): claimed=%v err=%v", reqID, claimed, err)
					}
					writtenKeys = append(writtenKeys, newClaimKeys(queueName, false).checkpoint(claimKey(reqID, reqTok)))
				}
				b.Cleanup(func() {
					_ = rdb.Del(context.Background(), writtenKeys...).Err()
				})

				b.SetBytes(int64(len(rawRec)))
				b.ReportAllocs()
				b.ResetTimer()

				if workers == 1 {
					for range b.N {
						res, err := store.Save(ctx, queueName, "req-0", "gen-0", record)
						if err != nil || res != pipeline.CheckpointSaved {
							b.Fatalf("Save = (%v, %v)", res, err)
						}
					}
					return
				}

				b.SetParallelism(workers)
				var seq atomic.Uint64
				b.RunParallel(func(pb *testing.PB) {
					idx := int(seq.Add(1)-1) % workers
					reqID := fmt.Sprintf("req-%d", idx)
					reqTok := fmt.Sprintf("gen-%d", idx)
					for pb.Next() {
						res, err := store.Save(ctx, queueName, reqID, reqTok, record)
						if err != nil || res != pipeline.CheckpointSaved {
							b.Fatalf("Save(%s) = (%v, %v)", reqID, res, err)
						}
					}
				})
			})
		}
	}
}
