package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRedisClient_RejectsInconsistentTopology(t *testing.T) {
	cases := map[string]struct {
		cfg  RedisSortedSetConfig
		want string
	}{
		"missing url":             {RedisSortedSetConfig{}, "RedisURL is required"},
		"unknown mode":            {RedisSortedSetConfig{RedisURL: "redis://h:1", RedisMode: "ring"}, "unknown RedisMode"},
		"addrs without mode":      {RedisSortedSetConfig{RedisURL: "redis://h:1", RedisAddrs: []string{"h2:1"}}, "RedisAddrs requires"},
		"master without sentinel": {RedisSortedSetConfig{RedisURL: "redis://h:1", RedisMasterName: "m"}, "RedisMasterName requires"},
		"master with cluster":     {RedisSortedSetConfig{RedisURL: "redis://h:1", RedisMode: RedisModeCluster, RedisMasterName: "m"}, "RedisMasterName requires"},
		"sentinel without master": {RedisSortedSetConfig{RedisURL: "redis://h:1", RedisMode: RedisModeSentinel}, "RedisMasterName is required"},
		"cluster with database":   {RedisSortedSetConfig{RedisURL: "redis://h:1/2", RedisMode: RedisModeCluster}, "database 2"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newRedisClient(tc.cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestNewRedisClient_SelectsClientPerMode(t *testing.T) {
	cases := map[string]struct {
		cfg     RedisSortedSetConfig
		cluster bool
	}{
		"standalone": {RedisSortedSetConfig{RedisURL: "redis://h:6379"}, false},
		"cluster":    {RedisSortedSetConfig{RedisURL: "redis://h:7000", RedisMode: RedisModeCluster, RedisAddrs: []string{"h2:7000"}}, true},
		"sentinel":   {RedisSortedSetConfig{RedisURL: "redis://h:26379", RedisMode: RedisModeSentinel, RedisMasterName: "m"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := newRedisClient(tc.cfg)
			require.NoError(t, err)
			defer c.Close() //nolint:errcheck
			assert.Equal(t, tc.cluster, isClusterClient(c), "client type %T", c)
		})
	}
}

func TestNewRedisSortedSetProducer_ClusterModeUsesClusterClientAndLayout(t *testing.T) {
	// miniredis answers CLUSTER SLOTS as a single-node cluster, so a genuine
	// go-redis cluster client bootstraps against it and routes every command
	// through its slot map.
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	p, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:         "redis://" + mr.Addr(),
		RedisMode:        RedisModeCluster,
		RequestQueueName: "requests",
		ResultQueueName:  "results",
	})
	require.NoError(t, err)
	defer p.Close() //nolint:errcheck
	assert.True(t, isClusterClient(p.client))
	assert.True(t, p.clusterKeys, "cluster client selects the tagged key layout")

	ctx := context.Background()
	deadline := time.Now().Add(time.Hour).Unix()
	require.NoError(t, p.SubmitRequest(ctx, &api.RequestMessage{
		ID: "r1", Created: time.Now().Unix(), Deadline: deadline, Payload: json.RawMessage(`{"prompt":"hi"}`),
	}))
	assert.True(t, mr.Exists("request-seq:{requests}:"+fmt.Sprint(deadline)))
	require.NoError(t, p.CancelRequests(ctx, []string{"r1"}))
	assert.True(t, mr.Exists(api.RequestCancellationKey("r1")))
	depth, err := p.RequestQueueDepth(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), depth)
}

func TestSlotKey_ProducerMirrorsDispatcherLayout(t *testing.T) {
	assert.Equal(t, "results", slotKey("results", false))
	assert.Equal(t, "{results}", slotKey("results", true))
	assert.Equal(t, "{pool-a}:results", slotKey("{pool-a}:results", true), "an existing tag is kept")
	assert.Equal(t, "{a{b}", slotKey("a{b", true), "an unterminated brace is not a tag")

	standalone := newResultClaimKeys("results", false)
	assert.Equal(t, "results:result-claimed", standalone.claimed)
	cluster := newResultClaimKeys("results", true)
	assert.Equal(t, "results", cluster.pending, "the shared pending list keeps its name")
	assert.Equal(t, "{results}:result-claimed", cluster.claimed)
	assert.Equal(t, "{results}:result-ack-tombstones", cluster.tombstones)

	assert.Equal(t, "request-seq:q:5", enqueueSeqKey("q", 5, false))
	assert.Equal(t, "request-seq:{q}:5", enqueueSeqKey("q", 5, true))
}

// newClusterLayoutProducer runs the producer against miniredis with the
// cluster key layout forced on, which exercises the cluster write paths
// (split marker writes, tagged sequence and result-claim keys) without a
// multi-node server. The real topology is covered by the gated integration
// test in test/integration/redis_cluster_test.go.
func newClusterLayoutProducer(t *testing.T) (*RedisSortedSetProducer, *miniredis.Miniredis) {
	t.Helper()
	p, mr := setupTestProducer(t)
	p.clusterKeys = true
	return p, mr
}

func TestClusterLayout_SubmitStampsSequenceInQueueSlot(t *testing.T) {
	p, mr := newClusterLayoutProducer(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Hour).Unix()

	for i := 1; i <= 3; i++ {
		require.NoError(t, p.SubmitRequest(ctx, &api.RequestMessage{
			ID: fmt.Sprintf("r%d", i), Created: time.Now().Unix(), Deadline: deadline,
			Payload: json.RawMessage(`{"prompt":"hi"}`),
		}))
	}

	assert.True(t, mr.Exists(enqueueSeqKey("test-request-queue", deadline, true)), "sequence counter lives under the tagged key")
	assert.False(t, mr.Exists(enqueueSeqKey("test-request-queue", deadline, false)), "standalone counter name must not be used")
	members, err := mr.ZMembers("test-request-queue")
	require.NoError(t, err)
	require.Len(t, members, 3)
	for i, member := range members {
		var ir api.InternalRequest
		require.NoError(t, json.Unmarshal([]byte(member), &ir))
		assert.Equal(t, int64(i+1), ir.EnqueueSeq, "members sort by submission order")
		score, err := mr.ZScore("test-request-queue", member)
		require.NoError(t, err)
		assert.Equal(t, ir.QueueScore(), score)
		token, err := mr.Get(api.RequestActiveTokenKey(ir.PublicRequest.ReqID()))
		require.NoError(t, err)
		assert.Equal(t, ir.RequestToken, token, "active marker holds this generation's token")
	}
}

func TestClusterLayout_CancelAndResubmitUseSingleKeyWrites(t *testing.T) {
	p, mr := newClusterLayoutProducer(t)
	ctx := context.Background()
	deadline := time.Now().Add(time.Hour).Unix()
	submit := func() string {
		t.Helper()
		require.NoError(t, p.SubmitRequest(ctx, &api.RequestMessage{
			ID: "r", Created: time.Now().Unix(), Deadline: deadline, Payload: json.RawMessage(`{"prompt":"hi"}`),
		}))
		token, err := mr.Get(api.RequestActiveTokenKey("r"))
		require.NoError(t, err)
		return token
	}

	first := submit()
	require.NoError(t, p.CancelRequests(ctx, []string{"r", "unknown"}))
	marker, err := mr.Get(api.RequestCancellationKey("r"))
	require.NoError(t, err)
	assert.Equal(t, first, marker, "marker copies the active generation token")
	assert.InDelta(t, cancellationMarkerTTL.Seconds(), mr.TTL(api.RequestCancellationKey("r")).Seconds(), 2)
	assert.False(t, mr.Exists(api.RequestCancellationKey("unknown")), "no marker without an active generation")

	// Reusing the ID clears the stale marker before the new generation enqueues.
	second := submit()
	assert.NotEqual(t, first, second)
	assert.False(t, mr.Exists(api.RequestCancellationKey("r")), "resubmission must clear the old cancellation marker")
}

func TestClusterLayout_DurableResultRoundTrip(t *testing.T) {
	p, mr := newClusterLayoutProducer(t)
	ctx := context.Background()
	payload, err := json.Marshal(api.InternalResult{
		ResultMessage: api.ResultMessage{ID: "r1", Payload: `{"ok":true}`},
		RequestToken:  "gen-1",
	})
	require.NoError(t, err)
	_, err = mr.Lpush("test-result-queue", string(payload))
	require.NoError(t, err)

	delivery, err := p.ReceiveResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, "r1", delivery.Result.ID)
	keys := newResultClaimKeys("test-result-queue", true)
	assert.True(t, mr.Exists(keys.claimed), "claim state under the tagged key")
	assert.False(t, mr.Exists("test-result-queue:result-claimed"), "standalone claim key must not be used")

	require.NoError(t, p.RenewResult(ctx, delivery))
	require.NoError(t, p.AckResult(ctx, delivery))
	assert.False(t, mr.Exists(keys.claimed))
	assert.True(t, mr.Exists(keys.tombstones), "ack leaves a tombstone under the tagged key")
	require.NoError(t, p.AckResult(ctx, delivery), "repeated ack is idempotent")

	// A duplicate record for the acked generation is suppressed by the tombstone.
	_, err = mr.Lpush("test-result-queue", string(payload))
	require.NoError(t, err)
	noResultCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	dup, err := p.ReceiveResult(noResultCtx)
	assert.Nil(t, dup)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, p.ClearResultQueue(ctx))
	for _, key := range []string{keys.pending, keys.claimed, keys.owners, keys.idx, keys.tombstones} {
		assert.False(t, mr.Exists(key), "ClearResultQueue left %s", key)
	}
}

func TestWithRedisClient_AcceptsAnyUniversalClient(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	var client redis.UniversalClient = redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close() //nolint:errcheck

	p, err := NewRedisSortedSetProducer(RedisSortedSetConfig{ResultQueueName: "results"}, WithRedisClient(client))
	require.NoError(t, err)
	assert.False(t, p.clusterKeys, "a standalone client keeps the standalone layout")

	cluster := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"h:7000"}})
	defer cluster.Close() //nolint:errcheck
	_, err = NewRedisSortedSetProducer(RedisSortedSetConfig{ResultQueueName: "results"}, WithRedisClient(cluster))
	require.Error(t, err, "unreachable cluster fails the connection check")
	assert.False(t, strings.Contains(err.Error(), "RedisURL"), "injected client must not demand a URL")
}

func TestClusterLayout_PayloadKeyIsWrittenBeforeTheEnqueue(t *testing.T) {
	p, mr := setupTestProducer(t, WithPayloadKeys())
	p.clusterKeys = true
	ctx := context.Background()
	deadline := time.Now().Add(time.Hour)
	require.NoError(t, p.SubmitRequest(ctx, &api.RequestMessage{
		ID: "r1", Created: time.Now().Unix(), Deadline: deadline.Unix(), Payload: json.RawMessage(`{"prompt":"long"}`),
	}))

	members, err := mr.ZMembers("test-request-queue")
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.NotContains(t, members[0], "long", "the queued envelope does not carry the payload")
	var ir api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(members[0]), &ir))
	assert.Equal(t, api.RequestPayloadKey("r1", ir.RequestToken), ir.PayloadRef)
	stored, err := mr.Get(ir.PayloadRef)
	require.NoError(t, err)
	assert.JSONEq(t, `{"prompt":"long"}`, stored)
	ttl := mr.TTL(ir.PayloadRef)
	assert.Greater(t, ttl, time.Until(deadline))
	assert.LessOrEqual(t, ttl, time.Until(deadline)+payloadTTLGrace+time.Second)
	assert.True(t, mr.Exists(enqueueSeqKey("test-request-queue", deadline.Unix(), true)))
}
