package sqlflow

import (
	"context"
	"database/sql"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGateReleasesAreScopedToRequestAttempt(t *testing.T) {
	var first, second, redelivery atomic.Int32
	f := &Flow{}
	key1 := sqlqueue.Stamp{Key: sqlqueue.Key{ID: "same-id", Token: "generation-1"}, Attempt: 1}
	key2 := sqlqueue.Stamp{Key: sqlqueue.Key{ID: "same-id", Token: "generation-2"}, Attempt: 1}
	key3 := sqlqueue.Stamp{Key: key1.Key, Attempt: 2}

	f.trackGateReleases(key1, []pipeline.GateReleaseFunc{func() { first.Add(1) }})
	f.trackGateReleases(key2, []pipeline.GateReleaseFunc{func() { second.Add(1) }})
	f.trackGateReleases(key3, []pipeline.GateReleaseFunc{func() { redelivery.Add(1) }})
	assert.Zero(t, first.Load(), "tracking another generation must not release the first")
	assert.Zero(t, second.Load())
	assert.Zero(t, redelivery.Load(), "tracking a redelivery must not release its earlier attempt")

	f.releaseGateReleases(key1)
	assert.EqualValues(t, 1, first.Load())
	assert.Zero(t, second.Load(), "completing the first generation must not release the second")

	f.releaseGateReleases(key2)
	assert.EqualValues(t, 1, second.Load())

	f.releaseGateReleases(key3)
	assert.EqualValues(t, 1, redelivery.Load())
}

func TestRebalanceRunsQueuesConcurrently(t *testing.T) {
	store := cancelTestStore(t)
	ctx := context.Background()
	cfg := Config{LeaseTTLSeconds: 3}
	for _, name := range []string{"q1", "q2", "q3"} {
		cfg.Queues = append(cfg.Queues, QueueConfig{ID: name, QueueName: name, WorkerPoolID: "default"})
	}
	f, err := NewWithStore(context.Background(), store, cfg, []pipeline.WorkerPoolConfig{{ID: "default"}}, nil)
	require.NoError(t, err)
	t.Cleanup(f.cancelChecks.stop)
	t.Cleanup(f.quota.Close)
	require.Equal(t, time.Second, f.storeTimeout())

	db, err := sql.Open("pgx", os.Getenv("TEST_POSTGRES_URL"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(ctx, "LOCK TABLE async_partitions IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	start := time.Now()
	f.rebalance(ctx)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, f.storeTimeout(), "every queue waits on the lock until its store timeout")
	assert.Less(t, elapsed, 2*f.storeTimeout(), "queues share one heartbeat pass instead of queuing behind each other")
}
