package sqlqueue

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const drainTimeout = 5 * time.Minute

func newConsumer(s *Store, owner string) *Consumer {
	return NewConsumer(s, "q", owner, leaseTTL, drainTimeout)
}

func rebalance(t *testing.T, c *Consumer, now time.Time) {
	t.Helper()
	require.NoError(t, c.Rebalance(context.Background(), now))
}

func owned(c *Consumer) (active, draining []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.partitionsLocked()
}

func inflightOf(c *Consumer) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.inflight)
}

func TestDoneStampDoesNotForgetANewerAttempt(t *testing.T) {
	key := Key{ID: "same", Token: "generation"}
	c := &Consumer{
		leases:   map[int]*partitionState{3: {epoch: 2, inflight: 1}},
		inflight: map[Key]tracked{key: {partition: 3, epoch: 2}},
	}

	c.doneStamps([]Stamp{{Key: key, Epoch: 1}})
	assert.Equal(t, 1, inflightOf(c), "a stale outcome must not forget the current attempt")
	assert.Equal(t, 1, c.leases[3].inflight)

	c.doneStamps([]Stamp{{Key: key, Epoch: 2}})
	assert.Zero(t, inflightOf(c))
	assert.Zero(t, c.leases[3].inflight)
}

func (c *Consumer) stamps(keys []Key) []Stamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Stamp, 0, len(keys))
	for _, k := range keys {
		if t, ok := c.inflight[k]; ok {
			out = append(out, Stamp{Key: k, Epoch: t.epoch})
		}
	}
	return out
}

func poll(t *testing.T, c *Consumer, now time.Time) []string {
	t.Helper()
	rows, err := c.Poll(context.Background(), now, 10000)
	require.NoError(t, err)
	got := ids(rows)
	sort.Strings(got)
	return got
}

func ack(t *testing.T, c *Consumer, id string) bool {
	t.Helper()
	key := Key{ID: id, Token: "t"}
	stamps := c.stamps([]Key{key})
	require.Len(t, stamps, 1)
	comp := completion(id, "route")
	comp.Epoch = stamps[0].Epoch
	acked, err := c.Ack(context.Background(), []Completion{comp})
	require.NoError(t, err)
	return acked[0]
}

func enqueueOnePerPartition(t *testing.T, s *Store, prefix string, now time.Time) []string {
	t.Helper()
	out := make([]string, Partitions)
	var batch []Request
	for p := range Partitions {
		out[p] = idInPartition(t, prefix, p)
		batch = append(batch, req(out[p], now.Unix()+3600))
	}
	require.NoError(t, s.Enqueue(context.Background(), batch...))
	return out
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func TestConsumerAloneOwnsEveryPartition(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		active, draining := owned(a)
		assert.Len(t, active, Partitions)
		assert.Empty(t, draining)

		idsByPartition := enqueueOnePerPartition(t, s, "solo", now)
		assert.Equal(t, sorted(idsByPartition), poll(t, a, now))
		assert.Equal(t, Partitions, inflightOf(a))
		for _, id := range idsByPartition {
			assert.True(t, ack(t, a, id))
		}
		assert.Zero(t, inflightOf(a))

		rebalance(t, a, now.Add(10*time.Second))
		active, _ = owned(a)
		assert.Len(t, active, Partitions, "a steady rebalance keeps everything")
	})
}

func TestConsumersConvergeOnAnEvenSplit(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		consumers := []*Consumer{newConsumer(s, "a"), newConsumer(s, "b"), newConsumer(s, "c")}
		for round := 0; round < 4; round++ {
			for _, c := range consumers {
				rebalance(t, c, now.Add(time.Duration(round)*time.Second))
			}
		}
		seen := map[int]string{}
		for _, c := range consumers {
			active, draining := owned(c)
			assert.Empty(t, draining, "%s still draining", c.owner)
			assert.GreaterOrEqual(t, len(active), 20, c.owner)
			assert.LessOrEqual(t, len(active), 22, c.owner)
			for _, p := range active {
				prev, dup := seen[p]
				assert.False(t, dup, "partition %d owned by %s and %s", p, prev, c.owner)
				seen[p] = c.owner
			}
		}
		assert.Len(t, seen, Partitions)

		idsByPartition := enqueueOnePerPartition(t, s, "split", now)
		var all []string
		for _, c := range consumers {
			all = append(all, poll(t, c, now.Add(4*time.Second))...)
		}
		assert.Equal(t, sorted(idsByPartition), sorted(all), "every request dispatched exactly once across the group")
	})
}

func TestConsumerHandoffWaitsForInFlightWork(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		a, b := newConsumer(s, "a"), newConsumer(s, "b")
		rebalance(t, a, now)
		first := enqueueOnePerPartition(t, s, "first", now)
		require.Len(t, poll(t, a, now), Partitions)

		rebalance(t, b, now)
		active, _ := owned(b)
		assert.Empty(t, active, "nothing is free while a holds everything")

		rebalance(t, a, now.Add(time.Second))
		active, draining := owned(a)
		assert.Len(t, active, Partitions/2)
		require.Len(t, draining, Partitions/2)
		assert.Equal(t, Partitions/2, draining[0], "ties drain the highest partitions")

		for p := 32; p < 48; p++ {
			assert.True(t, ack(t, a, first[p]))
		}
		rebalance(t, a, now.Add(2*time.Second))
		_, draining = owned(a)
		assert.Len(t, draining, 16, "finished partitions are released, busy ones wait")

		rebalance(t, b, now.Add(2*time.Second))
		active, _ = owned(b)
		assert.Len(t, active, 16)
		for _, p := range active {
			assert.GreaterOrEqual(t, p, 32)
			assert.Less(t, p, 48)
		}

		second := enqueueOnePerPartition(t, s, "second", now)
		assert.Equal(t, sorted(second[32:48]), poll(t, b, now.Add(2*time.Second)),
			"b serves only the partitions handed to it and redelivers nothing")
		assert.Equal(t, sorted(second[0:32]), poll(t, a, now.Add(2*time.Second)),
			"a serves its active partitions but nothing new from draining ones")

		later := now.Add(drainTimeout + 3*time.Second)
		for at := now.Add(10 * time.Second); at.Before(later); at = at.Add(10 * time.Second) {
			rebalance(t, a, at)
			rebalance(t, b, at)
			_, draining = owned(a)
			require.Len(t, draining, 16, "busy partitions keep draining until the timeout")
		}
		rebalance(t, a, later)
		_, draining = owned(a)
		assert.Empty(t, draining, "drain timeout releases partitions that never finished")
		rebalance(t, b, later)
		active, _ = owned(b)
		assert.Len(t, active, Partitions/2)
		redelivered := append(append([]string(nil), first[48:]...), second[48:]...)
		assert.Equal(t, sorted(redelivered), poll(t, b, later),
			"after a timed-out handoff the new owner redelivers the stuck work")
		assert.False(t, ack(t, a, first[50]), "the old owner is fenced once the partition moved")
	})
}

func TestConsumerTakesOverFromDeadPeer(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		a, b := newConsumer(s, "a"), newConsumer(s, "b")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(context.Background(), req("orphan", now.Unix()+3600)))
		require.Equal(t, []string{"orphan"}, poll(t, a, now))

		rebalance(t, b, now.Add(time.Second))
		active, _ := owned(b)
		assert.Empty(t, active, "a's leases are still live")

		afterLapse := now.Add(leaseTTL + time.Second)
		rebalance(t, b, afterLapse)
		active, _ = owned(b)
		assert.Len(t, active, Partitions, "a's membership lapsed with its leases, so b takes everything")
		assert.Equal(t, []string{"orphan"}, poll(t, b, afterLapse))

		assert.False(t, ack(t, a, "orphan"), "the dead owner's late result is fenced")
		assert.True(t, ack(t, b, "orphan"))

		rebalance(t, a, afterLapse.Add(time.Second))
		active, _ = owned(a)
		assert.Empty(t, active, "a zombie comes back with nothing and must wait for a share")
	})
}

func TestConsumerStopDrainsThenReleasesEverything(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		a, b := newConsumer(s, "a"), newConsumer(s, "b")
		rebalance(t, a, now)
		busy := idInPartition(t, "busy", 7)
		require.NoError(t, s.Enqueue(context.Background(), req(busy, now.Unix()+3600)))
		require.Equal(t, []string{busy}, poll(t, a, now))

		a.Stop()
		rebalance(t, a, now)
		active, draining := owned(a)
		assert.Empty(t, active)
		assert.Equal(t, []int{7}, draining, "idle partitions are released at once, the busy one drains")

		rebalance(t, b, now)
		active, _ = owned(b)
		assert.Len(t, active, Partitions-1)
		assert.Empty(t, poll(t, b, now))

		assert.True(t, ack(t, a, busy), "a draining owner still acks")
		rebalance(t, a, now)
		_, draining = owned(a)
		assert.Empty(t, draining)
		rebalance(t, b, now)
		active, _ = owned(b)
		assert.Len(t, active, Partitions)
	})
}

func TestConsumerUndrainsWhenItsShareGrowsBack(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		a, b := newConsumer(s, "a"), newConsumer(s, "b")
		rebalance(t, a, now)
		enqueueOnePerPartition(t, s, "busy", now)
		require.Len(t, poll(t, a, now), Partitions)
		rebalance(t, b, now)
		rebalance(t, a, now)
		_, draining := owned(a)
		require.Len(t, draining, Partitions/2)

		require.NoError(t, b.Close(context.Background()))
		rebalance(t, a, now)
		active, draining := owned(a)
		assert.Len(t, active, Partitions, "draining partitions are taken back instead of released")
		assert.Empty(t, draining)
		fresh := idInPartition(t, "fresh", 60)
		require.NoError(t, s.Enqueue(context.Background(), req(fresh, now.Unix())))
		assert.Equal(t, []string{fresh}, poll(t, a, now))
	})
}

func TestConsumerRetriesAbandonedRequestsOnRebalance(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("lost", now.Unix()+60), req("parked", now.Unix()+60), req("back", now.Unix()+60)))
		require.Len(t, poll(t, a, now), 3)

		a.Abandon(a.stamps([]Key{{ID: "lost", Token: "t"}})...)
		assert.Equal(t, 2, inflightOf(a))
		assert.Empty(t, poll(t, a, now), "an abandoned request stays in flight until the next rebalance")
		rebalance(t, a, now)
		assert.Equal(t, []string{"lost"}, poll(t, a, now))

		key := Key{ID: "parked", Token: "t"}
		stamps := a.stamps([]Key{key})
		require.Len(t, stamps, 1)
		ok, err := a.Retry(ctx, stamps[0], now.Unix()+30, `{"retry":1}`)
		require.NoError(t, err)
		assert.True(t, ok)
		require.NoError(t, a.Undispatch(ctx, a.stamps([]Key{{ID: "back", Token: "t"}})))
		assert.Equal(t, 1, inflightOf(a), "retry and undispatch stop tracking their requests")
		assert.Equal(t, []string{"back"}, poll(t, a, now))
		assert.Equal(t, []string{"parked"}, poll(t, a, now.Add(30*time.Second)))
	})
}

func TestConsumerRepolledOrphanIsNotReset(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("flaky", now.Unix()+60)))
		require.Equal(t, []string{"flaky"}, poll(t, a, now))

		require.NoError(t, s.Undispatch(ctx, "a", a.stamps([]Key{{ID: "flaky", Token: "t"}})))
		a.Abandon(a.stamps([]Key{{ID: "flaky", Token: "t"}})...)
		require.Equal(t, []string{"flaky"}, poll(t, a, now), "the request is dispatched again")

		rebalance(t, a, now)
		assert.Empty(t, poll(t, a, now), "the orphan entry must not reset the live dispatch")
		assert.True(t, ack(t, a, "flaky"))
	})
}

func TestConsumerPollHandsOutARedispatchedRequest(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("twice", now.Unix()+60)))
		require.Equal(t, []string{"twice"}, poll(t, a, now))

		stamp := a.stamps([]Key{{ID: "twice", Token: "t"}})[0]
		ok, err := s.Retry(ctx, "a", stamp, 0, `{"id":"twice"}`)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, []string{"twice"}, poll(t, a, now), "a request dispatched again is handed out again")
		assert.Equal(t, 1, inflightOf(a))
		active, _ := owned(a)
		inflight := 0
		for _, p := range active {
			inflight += a.leases[p].inflight
		}
		assert.Equal(t, 1, inflight, "the partition counts the request once")
		assert.True(t, ack(t, a, "twice"))
	})
}

func TestConsumerRetryBookkeepingKeepsTheAttemptPollHandedOut(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("raced", now.Unix()+3600)))
		require.Equal(t, []string{"raced"}, poll(t, a, now))
		stamp := a.stamps([]Key{{ID: "raced", Token: "t"}})[0]

		ok, err := s.Retry(ctx, "a", stamp, now.Unix(), `{"id":"raced"}`)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, []string{"raced"}, poll(t, a, now))
		a.doneStamps([]Stamp{stamp})
		assert.Equal(t, 1, inflightOf(a), "the retry's bookkeeping must not forget the attempt Poll handed out")

		a.Abandon(stamp)
		assert.Equal(t, []string{"raced"}, redelivered(t, a, now), "an attempt whose outcome could not be written is handed out again")
	})
}

func TestConsumerReconcileIgnoresTheBookkeepingOfAFinishedRetry(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("raced", now.Unix()+3600)))
		require.Equal(t, []string{"raced"}, poll(t, a, now))
		stamp := a.stamps([]Key{{ID: "raced", Token: "t"}})[0]

		ok, err := s.Retry(ctx, "a", stamp, now.Unix(), `{"id":"raced"}`)
		require.NoError(t, err)
		require.True(t, ok)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = a.Poll(cancelled, now, 10)
		require.Error(t, err)
		redispatched, err := s.Dispatch(ctx, "q", "a", now, 10)
		require.NoError(t, err)
		require.Len(t, redispatched, 1)
		require.Equal(t, stamp, redispatched[0].Stamp(), "the lost dispatch reuses the retried stamp")
		rebalance(t, a, now)
		a.doneStamps([]Stamp{stamp})

		assert.Equal(t, []string{"raced"}, redelivered(t, a, now), "a dispatch whose reply was lost is handed out again")
	})
}

func redelivered(t *testing.T, c *Consumer, now time.Time) []string {
	t.Helper()
	var got []string
	for range fullResetEvery {
		rebalance(t, c, now)
		got = append(got, poll(t, c, now)...)
	}
	return got
}

func TestConsumerRedeliversAfterReacquiringAPartition(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("slow", now.Unix()+3600)))
		require.Equal(t, []string{"slow"}, poll(t, a, now))
		old := a.stamps([]Key{{ID: "slow", Token: "t"}})[0]

		require.NoError(t, s.ReleasePartitions(ctx, "q", "a", allPartitionIDs()))
		rebalance(t, a, now)
		active, _ := owned(a)
		require.Len(t, active, Partitions)

		assert.Equal(t, []string{"slow"}, poll(t, a, now), "the reset request is handed out under the new epoch")
		stale := completion("slow", "route")
		stale.Epoch = old.Epoch
		acked, err := a.Ack(ctx, []Completion{stale})
		require.NoError(t, err)
		assert.False(t, acked[0], "the attempt from the old epoch is fenced")
		assert.True(t, ack(t, a, "slow"), "the attempt from the new epoch completes")
	})
}

func TestConsumerReconcilesAfterPollWithUnknownOutcome(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("tracked", now.Unix()+60), req("lost-reply", now.Unix()+60)))
		require.Equal(t, []string{"tracked"}, ids(mustPoll(t, a, now, 1)))

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := a.Poll(cancelled, now, 10)
		require.Error(t, err)
		_, err = s.Dispatch(ctx, "q", "a", now, 10)
		require.NoError(t, err)
		assert.Empty(t, poll(t, a, now))

		rebalance(t, a, now)
		assert.Equal(t, []string{"lost-reply"}, poll(t, a, now), "the untracked dispatch is returned to pending")
		assert.Equal(t, 2, inflightOf(a))
		rebalance(t, a, now)
		assert.Empty(t, poll(t, a, now), "tracked requests are left alone")
	})
}

func TestConsumerRetryAfterLeaseBounceIsFenced(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("bounced", now.Unix()+60)))
		require.Equal(t, []string{"bounced"}, poll(t, a, now))
		old := a.stamps([]Key{{ID: "bounced", Token: "t"}})[0]

		require.NoError(t, s.ReleasePartitions(ctx, "q", "a", allPartitionIDs()))
		_, err := s.AcquirePartitions(ctx, "q", "a", now, leaseTTL, Partitions)
		require.NoError(t, err)
		require.NoError(t, s.ResetStale(ctx, "q", "a", nil))
		redispatched, err := s.Dispatch(ctx, "q", "a", now, 10)
		require.NoError(t, err)
		require.Len(t, redispatched, 1)

		ok, err := s.Retry(ctx, "a", old, now.Unix()+30, `{}`)
		require.NoError(t, err)
		assert.False(t, ok, "the retry from the first dispatch cannot reset the second")
		assert.Empty(t, dispatchIDs(t, s, "a", now.Add(time.Minute), 10))
	})
}

func TestConsumerFullResetRecoversOldPartitions(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		_, err := s.db.ExecContext(ctx, `UPDATE async_partitions SET epoch = epoch + 1 WHERE queue = 'q' AND owner = 'a'`)
		require.NoError(t, err)
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("zombie-stamp", now.Unix()+3600)))
		_, err = s.db.ExecContext(ctx, `UPDATE async_requests SET dispatch_epoch = 1 WHERE id = 'zombie-stamp'`)
		require.NoError(t, err)

		later := now.Add(3 * leaseTTL)
		for a.beats < fullResetEvery-1 {
			rebalance(t, a, later)
		}
		assert.Empty(t, poll(t, a, later), "partitions acquired long ago are not swept on ordinary heartbeats")
		rebalance(t, a, later)
		assert.Equal(t, []string{"zombie-stamp"}, poll(t, a, later), "the periodic full sweep recovers the stale stamp")
	})
}

func mustPoll(t *testing.T, c *Consumer, now time.Time, limit int) []Request {
	t.Helper()
	rows, err := c.Poll(context.Background(), now, limit)
	require.NoError(t, err)
	return rows
}

func TestConsumerCloseHandsOverImmediately(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a, b := newConsumer(s, "a"), newConsumer(s, "b")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("unfinished", now.Unix()+60), req("abandoned", now.Unix()+60)))
		require.Len(t, poll(t, a, now), 2)
		a.Abandon(a.stamps([]Key{{ID: "abandoned", Token: "t"}})...)
		require.NoError(t, a.Close(ctx))
		active, _ := owned(a)
		assert.Empty(t, active)

		rebalance(t, b, now)
		active, _ = owned(b)
		assert.Len(t, active, Partitions, "no lease wait after a clean close")
		assert.Equal(t, []string{"abandoned", "unfinished"}, poll(t, b, now))
	})
}

func TestConsumerWithMorePeersThanPartitionsIdlesExtras(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		now := time.Now()
		var consumers []*Consumer
		for i := 0; i < Partitions+6; i++ {
			c := newConsumer(s, fmt.Sprintf("c%03d", i))
			consumers = append(consumers, c)
			require.NoError(t, s.EnsurePartitions(context.Background(), "q"))
			_, _, err := s.Heartbeat(context.Background(), "q", c.owner, now, leaseTTL, true)
			require.NoError(t, err)
		}
		for _, c := range consumers {
			rebalance(t, c, now)
		}
		idle, total := 0, 0
		for _, c := range consumers {
			active, _ := owned(c)
			assert.LessOrEqual(t, len(active), 1)
			if len(active) == 0 {
				idle++
			}
			total += len(active)
		}
		assert.Equal(t, Partitions, total)
		assert.Equal(t, 6, idle)
	})
}

func TestConsumerStaleOutcomeKeepsTheNewEpochsCount(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		old := idInPartition(t, "old", 7)
		require.NoError(t, s.Enqueue(ctx, req(old, now.Unix()+3600)))
		require.Equal(t, []string{old}, poll(t, a, now))
		stale := a.stamps([]Key{{ID: old, Token: "t"}})[0]

		require.NoError(t, s.ReleasePartitions(ctx, "q", "a", allPartitionIDs()))
		_, err := s.db.ExecContext(ctx, `DELETE FROM async_requests WHERE id = $1`, old)
		require.NoError(t, err, "a peer completed the old attempt in the meantime")
		rebalance(t, a, now)
		fresh := idInPartition(t, "fresh", 7)
		require.NoError(t, s.Enqueue(ctx, req(fresh, now.Unix()+3600)))
		require.Equal(t, []string{fresh}, poll(t, a, now))

		comp := completion(old, "route")
		comp.Epoch = stale.Epoch
		acked, err := a.Ack(ctx, []Completion{comp})
		require.NoError(t, err)
		assert.False(t, acked[0])
		a.mu.Lock()
		count := a.leases[7].inflight
		a.mu.Unlock()
		assert.Equal(t, 1, count, "the fenced outcome must not uncount the request dispatched under the new epoch")

		a.Stop()
		rebalance(t, a, now)
		active, draining := owned(a)
		assert.Empty(t, active)
		assert.Equal(t, []int{7}, draining, "only the partition with work in flight is kept")
		assert.True(t, ack(t, a, fresh))
	})
}

func TestConsumerStaleAbandonLeavesTheNewAttempt(t *testing.T) {
	withStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		now := time.Now()
		a := newConsumer(s, "a")
		rebalance(t, a, now)
		require.NoError(t, s.Enqueue(ctx, req("bounced", now.Unix()+3600)))
		require.Equal(t, []string{"bounced"}, poll(t, a, now))
		stale := a.stamps([]Key{{ID: "bounced", Token: "t"}})[0]

		require.NoError(t, s.ReleasePartitions(ctx, "q", "a", allPartitionIDs()))
		rebalance(t, a, now)
		require.Equal(t, []string{"bounced"}, poll(t, a, now))

		a.Abandon(stale)
		require.NoError(t, a.Undispatch(ctx, []Stamp{stale}))
		rebalance(t, a, now)
		assert.Empty(t, poll(t, a, now), "the stale attempt's failure must not return the live attempt to pending")
		assert.Equal(t, 1, inflightOf(a))
		assert.True(t, ack(t, a, "bounced"))
	})
}
