package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDurableResultProducer(t *testing.T, mr *miniredis.Miniredis, route string) *RedisSortedSetProducer {
	t.Helper()
	p, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: route,
	}, WithResultClaimLeaseTTL(time.Minute), WithResultClaimReclaimInterval(time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	return p
}

func pushDurableResult(t *testing.T, mr *miniredis.Miniredis, route, id, requestToken string) {
	t.Helper()
	wire := api.InternalResult{
		ResultMessage: api.ResultMessage{ID: id, Payload: `{"ok":true}`},
		RequestToken:  requestToken,
	}
	payload, err := json.Marshal(wire)
	require.NoError(t, err)
	_, err = mr.Lpush(route, string(payload))
	require.NoError(t, err)
}

func receiveWithTimeout(t *testing.T, p *RedisSortedSetProducer) *ResultDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	delivery, err := p.ReceiveResult(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	require.NotNil(t, delivery.Result)
	return delivery
}

func TestReceiveResultIsNonDestructiveUntilAck(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "result-1", "generation-1")

	delivery := receiveWithTimeout(t, p)
	assert.Equal(t, "result-1", delivery.Result.ID)
	assert.Equal(t, "generation-1", delivery.Result.Routing.RequestToken)

	keys := newResultClaimKeys("results", false)
	assert.EqualValues(t, 0, p.client.LLen(context.Background(), keys.pending).Val())
	assert.EqualValues(t, 1, p.client.HLen(context.Background(), keys.claimed).Val())
	assert.EqualValues(t, 1, p.client.HLen(context.Background(), keys.owners).Val())
	assert.EqualValues(t, 1, p.client.ZCard(context.Background(), keys.idx).Val())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	redelivery, err := p.ReceiveResult(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, redelivery)

	require.NoError(t, p.AckResult(context.Background(), delivery))
	assert.EqualValues(t, 0, p.client.HLen(context.Background(), keys.claimed).Val())
	assert.EqualValues(t, 0, p.client.HLen(context.Background(), keys.owners).Val())
	assert.EqualValues(t, 0, p.client.ZCard(context.Background(), keys.idx).Val())
}

func TestReceiveResultExpiredLeaseRedeliversAndFencesStaleOwner(t *testing.T) {
	mr := miniredis.RunT(t)
	first := setupDurableResultProducer(t, mr, "results")
	second := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "result-1", "generation-1")

	stale := receiveWithTimeout(t, first)
	keys := newResultClaimKeys("results", false)
	mr.ZAdd(keys.idx, -1, stale.claimID)

	err := first.RenewResult(context.Background(), stale)
	assert.ErrorIs(t, err, ErrResultDeliveryOwnershipLost)
	err = first.AckResult(context.Background(), stale)
	assert.ErrorIs(t, err, ErrResultDeliveryOwnershipLost)
	assert.EqualValues(t, 1, first.client.HLen(context.Background(), keys.claimed).Val())

	redelivered := receiveWithTimeout(t, second)
	assert.Equal(t, stale.Result.ID, redelivered.Result.ID)
	assert.Equal(t, stale.Result.Routing.RequestToken, redelivered.Result.Routing.RequestToken)
	assert.NotEqual(t, stale.ownerToken, redelivered.ownerToken)

	err = first.RenewResult(context.Background(), stale)
	assert.ErrorIs(t, err, ErrResultDeliveryOwnershipLost)
	err = first.AckResult(context.Background(), stale)
	assert.ErrorIs(t, err, ErrResultDeliveryOwnershipLost)
	assert.EqualValues(t, 1, first.client.HLen(context.Background(), keys.claimed).Val())

	require.NoError(t, second.AckResult(context.Background(), redelivered))
}

func TestAckResultIsIdempotentAndPreventsRedelivery(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "result-1", "generation-1")

	delivery := receiveWithTimeout(t, p)
	// This assignment stands in for the consumer's durable checkpoint before ACK.
	checkpoint := delivery.Result.ID + "\x00" + delivery.Result.Routing.RequestToken
	require.Equal(t, "result-1\x00generation-1", checkpoint)
	require.NoError(t, p.AckResult(context.Background(), delivery))
	require.NoError(t, p.AckResult(context.Background(), delivery))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	redelivery, err := p.ReceiveResult(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, redelivery)
}

func TestRenewResultExtendsLease(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "result-1", "generation-1")
	delivery := receiveWithTimeout(t, p)

	keys := newResultClaimKeys("results", false)
	oldExpiry := float64(time.Now().Add(time.Second).UnixMilli())
	mr.ZAdd(keys.idx, oldExpiry, delivery.claimID)
	require.NoError(t, p.RenewResult(context.Background(), delivery))
	score, err := mr.ZScore(keys.idx, delivery.claimID)
	require.NoError(t, err)
	assert.Greater(t, score, oldExpiry)
}

func TestReceiveResultRouteIsolation(t *testing.T) {
	mr := miniredis.RunT(t)
	alpha := setupDurableResultProducer(t, mr, "results:$alpha")
	beta := setupDurableResultProducer(t, mr, "results:$beta")
	pushDurableResult(t, mr, "results:$alpha", "alpha", "token-alpha")
	pushDurableResult(t, mr, "results:$beta", "beta", "token-beta")

	alphaDelivery := receiveWithTimeout(t, alpha)
	betaDelivery := receiveWithTimeout(t, beta)
	assert.Equal(t, "alpha", alphaDelivery.Result.ID)
	assert.Equal(t, "beta", betaDelivery.Result.ID)
	require.NoError(t, alpha.AckResult(context.Background(), alphaDelivery))
	require.NoError(t, beta.AckResult(context.Background(), betaDelivery))
}

func TestReceiveResultSameIDDifferentRequestTokensAreIsolated(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "shared-id", "generation-1")
	pushDurableResult(t, mr, "results", "shared-id", "generation-2")

	first := receiveWithTimeout(t, p)
	second := receiveWithTimeout(t, p)
	assert.Equal(t, "generation-1", first.Result.Routing.RequestToken)
	assert.Equal(t, "generation-2", second.Result.Routing.RequestToken)
	assert.NotEqual(t, first.claimID, second.claimID)

	require.NoError(t, p.AckResult(context.Background(), first))
	keys := newResultClaimKeys("results", false)
	assert.EqualValues(t, 1, p.client.HLen(context.Background(), keys.claimed).Val())
	require.NoError(t, p.AckResult(context.Background(), second))
}

func TestReceiveResultLegacyPayloadHasDegradedIDOnlyIsolation(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	payload, err := json.Marshal(api.ResultMessage{ID: "legacy", Payload: "done"})
	require.NoError(t, err)
	_, err = mr.Lpush("results", string(payload))
	require.NoError(t, err)

	delivery := receiveWithTimeout(t, p)
	assert.Equal(t, "legacy", delivery.Result.ID)
	assert.Empty(t, delivery.Result.Routing.RequestToken)
	assert.Equal(t, "legacy", delivery.claimID)
	require.NoError(t, p.AckResult(context.Background(), delivery))
}

func TestGetResultParsesEnrichedWireWithoutChangingLegacyAPI(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "result-1", "generation-1")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := p.GetResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, "result-1", result.ID)
	assert.Equal(t, "generation-1", result.Routing.RequestToken)
}

func TestAckResultTombstonesAreTimeBoundAndCleaned(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "result-1", "generation-1")
	delivery := receiveWithTimeout(t, p)
	require.NoError(t, p.AckResult(context.Background(), delivery))

	keys := newResultClaimKeys("results", false)
	assert.EqualValues(t, 1, p.client.ZCard(context.Background(), keys.tombstones).Val())
	assert.Positive(t, mr.TTL(keys.tombstones))

	// Expired tombstones are removed on the next receive, allowing the same
	// generation identity to be claimed again after the bounded retention.
	mr.ZAdd(keys.tombstones, -1, delivery.claimID)
	pushDurableResult(t, mr, "results", "result-1", "generation-1")
	again := receiveWithTimeout(t, p)
	assert.Equal(t, delivery.claimID, again.claimID)
	require.NoError(t, p.AckResult(context.Background(), again))
}

func TestReceiveResultBoundsTombstoneCleanup(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	keys := newResultClaimKeys("results", false)
	for i := 0; i < resultTombstoneCleanupBatchSize+1; i++ {
		err := p.client.ZAdd(context.Background(), keys.tombstones, redis.Z{
			Score:  -1,
			Member: fmt.Sprintf("expired-%d", i),
		}).Err()
		require.NoError(t, err)
	}
	pushDurableResult(t, mr, "results", "result-1", "generation-1")

	delivery := receiveWithTimeout(t, p)
	assert.EqualValues(t, 1, p.client.ZCard(context.Background(), keys.tombstones).Val())
	require.NoError(t, p.AckResult(context.Background(), delivery))
	// ACK removes the remaining expired entry and adds only its own live tombstone.
	assert.EqualValues(t, 1, p.client.ZCard(context.Background(), keys.tombstones).Val())
}

func TestReceiveResultValidationAndCancellation(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	delivery, err := p.ReceiveResult(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, delivery)

	assert.Error(t, p.RenewResult(context.Background(), nil))
	assert.Error(t, p.AckResult(context.Background(), nil))
	assert.False(t, errors.Is(p.AckResult(context.Background(), &ResultDelivery{}), ErrResultDeliveryOwnershipLost))
}

func TestReceiveResultUnparsableRecordsRemainDurableAndDoNotBlockRoute(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "valid", "generation-1")

	goRejected := `{"id":"schema-skew","status_code":"not-a-number","payload":"invalid"}`
	luaRejected := `{"id":123,"payload":"invalid"}`
	_, err := mr.RPush("results", goRejected, luaRejected)
	require.NoError(t, err)

	for range 2 {
		delivery, receiveErr := p.ReceiveResult(context.Background())
		assert.ErrorIs(t, receiveErr, ErrUnparsableResult)
		assert.Nil(t, delivery)
	}

	keys := newResultClaimKeys("results", false)
	assert.EqualValues(t, 2, p.client.HLen(context.Background(), keys.claimed).Val())
	claimedPayloads, err := p.client.HVals(context.Background(), keys.claimed).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{goRejected, luaRejected}, claimedPayloads)

	delivery := receiveWithTimeout(t, p)
	assert.Equal(t, "valid", delivery.Result.ID)
	require.NoError(t, p.AckResult(context.Background(), delivery))

	claimIDs, err := p.client.ZRange(context.Background(), keys.idx, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, claimIDs, 2)
	for _, claimID := range claimIDs {
		mr.ZAdd(keys.idx, -1, claimID)
	}
	for range 2 {
		redelivery, receiveErr := p.ReceiveResult(context.Background())
		assert.ErrorIs(t, receiveErr, ErrUnparsableResult)
		assert.Nil(t, redelivery)
	}
	assert.EqualValues(t, 2, p.client.HLen(context.Background(), keys.claimed).Val())
}

func TestClearResultQueueClearsDurableState(t *testing.T) {
	mr := miniredis.RunT(t)
	p := setupDurableResultProducer(t, mr, "results")
	pushDurableResult(t, mr, "results", "claimed", "generation-1")
	claimed := receiveWithTimeout(t, p)
	pushDurableResult(t, mr, "results", "pending", "generation-2")

	setupDurableResultProducer(t, mr, "other")
	pushDurableResult(t, mr, "other", "other", "generation-3")
	require.NoError(t, p.ClearResultQueue(context.Background()))

	keys := newResultClaimKeys("results", false)
	assert.False(t, mr.Exists(keys.pending))
	assert.False(t, mr.Exists(keys.claimed))
	assert.False(t, mr.Exists(keys.owners))
	assert.False(t, mr.Exists(keys.idx))
	assert.False(t, mr.Exists(keys.tombstones))
	assert.True(t, mr.Exists("other"))
	assert.ErrorIs(t, p.AckResult(context.Background(), claimed), ErrResultDeliveryOwnershipLost)
}

func TestDurableResultOptionsRejectNonPositiveDurations(t *testing.T) {
	mr := miniredis.RunT(t)
	_, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: "results",
	}, WithResultClaimLeaseTTL(0))
	assert.ErrorContains(t, err, "WithResultClaimLeaseTTL")

	_, err = NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: "results",
	}, WithResultClaimReclaimInterval(-time.Second))
	assert.ErrorContains(t, err, "WithResultClaimReclaimInterval")
}

func TestResultDeliveryConfigReportsEffectiveSettings(t *testing.T) {
	mr := miniredis.RunT(t)
	p, err := NewRedisSortedSetProducer(RedisSortedSetConfig{
		RedisURL:        "redis://" + mr.Addr(),
		ResultQueueName: "results",
	}, WithResultClaimLeaseTTL(30*time.Second), WithResultClaimReclaimInterval(250*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })

	assert.Equal(t, ResultDeliveryConfig{
		LeaseTTL:        30 * time.Second,
		ReclaimInterval: 250 * time.Millisecond,
	}, p.ResultDeliveryConfig())
}
