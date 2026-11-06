package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

type redisCheckpointStore struct {
	flow *RedisSortedSetFlow
}

// CheckpointStore returns a lease-fenced CheckpointStore backed by this flow's
// Redis connection and active claim-token map.
func (r *RedisSortedSetFlow) CheckpointStore() pipeline.CheckpointStore {
	return &redisCheckpointStore{flow: r}
}

func (s *redisCheckpointStore) Load(ctx context.Context, queueName, requestID, requestToken string) (*pipeline.CheckpointRecord, error) {
	if requestID == "" {
		return nil, nil
	}
	claimID := claimKey(requestID, requestToken)
	resolvedQueue := queueName
	if resolvedQueue == "" {
		if v, ok := s.flow.claimTokens.Load(claimID); ok {
			if h, ok := v.(*claimHandle); ok && h != nil {
				resolvedQueue = h.queue
			}
		}
	}
	if resolvedQueue == "" {
		resolvedQueue = s.flow.fallbackRequestQueue()
	}
	if resolvedQueue == "" {
		return nil, nil
	}

	keys := newClaimKeys(resolvedQueue, s.flow.clusterKeys)
	raw, err := s.flow.rdb.Get(ctx, keys.checkpoint(claimID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint for %q: %w", requestID, err)
	}

	var rec pipeline.CheckpointRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint for %q: %w", requestID, err)
	}
	return &rec, nil
}

func (s *redisCheckpointStore) Save(ctx context.Context, queueName, requestID, requestToken string, record pipeline.CheckpointRecord) (pipeline.CheckpointWriteResult, error) {
	if requestID == "" {
		return pipeline.CheckpointFenced, nil
	}
	claimID := claimKey(requestID, requestToken)
	v, ok := s.flow.claimTokens.Load(claimID)
	if !ok {
		return pipeline.CheckpointFenced, nil
	}
	handle, ok := v.(*claimHandle)
	if !ok || handle == nil || handle.token == "" {
		return pipeline.CheckpointFenced, nil
	}

	resolvedQueue := queueName
	if resolvedQueue == "" {
		resolvedQueue = handle.queue
	}
	if resolvedQueue == "" {
		resolvedQueue = s.flow.fallbackRequestQueue()
	}
	if resolvedQueue == "" {
		return pipeline.CheckpointFenced, nil
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("marshal checkpoint for %q: %w", requestID, err)
	}

	var pxMs int64
	if handle.deadline > 0 {
		expiryMs := int64((handle.deadline + reclaimGraceAfterDeadline.Seconds()) * 1000)
		pxMs = expiryMs - time.Now().UnixMilli()
	} else {
		pxMs = (s.flow.claimLeaseTTL + reclaimGraceAfterDeadline).Milliseconds()
	}
	if pxMs < 1000 {
		pxMs = 1000
	}

	keys := newClaimKeys(resolvedQueue, s.flow.clusterKeys)
	res, err := saveCheckpointScript.Run(
		ctx,
		s.flow.rdb,
		[]string{keys.owners, keys.checkpoint(claimID)},
		claimID,
		handle.token,
		payload,
		pxMs,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("save checkpoint for %q: %w", requestID, err)
	}
	if res == 0 {
		return pipeline.CheckpointFenced, nil
	}
	return pipeline.CheckpointSaved, nil
}
