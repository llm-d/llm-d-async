package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

// Claim-based durable dequeue for the sorted-set transport.
//
// Requests leave the pending zset only when claimed under a lease; the claim
// is dropped exactly once per terminal outcome (ack, release, or expiry
// redelivery). Delivery is at-least-once across crashes; only the current
// lease owner may ack, so stale completions are fenced. Requires Redis
// persistence (AOF/replication). See docs/guides/durable-dequeue.md.

const (
	// reclaimGraceAfterDeadline extends leases past the request deadline
	// so a claim never expires into an already-expired request; it must
	// exceed the worker's request timeout to avoid racing real results.
	reclaimGraceAfterDeadline = 5 * time.Minute

	// reclaimBatchSize bounds how many expired claims one reclaimer pass
	// releases, keeping each tick's Redis work bounded under backlog.
	reclaimBatchSize = 100

	// claimTokenBytes is the entropy of the per-claim ownership token.
	claimTokenBytes = 8
)

// claimKeys bundles the Redis keys implementing claims for one queue.
type claimKeys struct {
	pending string // zset: reqID-scored members awaiting dispatch
	claimed string // hash: claimKey -> original member JSON
	owners  string // hash: claimKey -> ownership token
	idx     string // zset: claimKey -> lease expiry unix seconds
}

func newClaimKeys(queueName string) claimKeys {
	return claimKeys{
		pending: queueName,
		claimed: queueName + ":claimed",
		owners:  queueName + ":claim-owners",
		idx:     queueName + ":claims-idx",
	}
}

// CLAIM moves one member from pending to claimed. Returns 0 when another
// consumer won. Overwrites an existing self-claim (retried requests re-enter
// pending while still owned).
var claimScript = redis.NewScript(`
if redis.call('ZREM', KEYS[1], ARGV[2]) == 0 then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
redis.call('ZADD', KEYS[4], ARGV[4], ARGV[1])
return 1
`)

// RELEASE hands a claimed request back to pending at its queue score.
// Token-guarded: a stale owner must not drop the new owner's claim.
var releaseClaimScript = redis.NewScript(`
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[4] then
  return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
`)

// ACKRESULT records a terminal result and drops the claim atomically.
// Only the current owner may publish; stale owners are fenced. Missing
// owners or token mismatches return 0, preventing duplicate pushes.
//
// KEYS: claimed, owners, idx, resultList
// ARGV: id, resultJSON, token, listTTLSeconds
// Returns 1 when the result was recorded, 0 when fenced as stale.
var ackResultScript = redis.NewScript(`
local owner = redis.call('HGET', KEYS[2], ARGV[1])
if not owner or owner ~= ARGV[3] or ARGV[3] == "" then
  return 0
end
redis.call('LPUSH', KEYS[4], ARGV[2])
local listTTL = tonumber(ARGV[4])
if listTTL > 0 then
  redis.call('EXPIRE', KEYS[4], listTTL)
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

// RECLAIMIFEXPIRED redelivers an expired claim back to pending at its queue
// score; renewed claims and ghosts are left alone.
var reclaimExpiredScript = redis.NewScript(`
local exp = redis.call('ZSCORE', KEYS[4], ARGV[1])
if not exp then
  return 0
end
if tonumber(exp) > tonumber(ARGV[3]) then
  return 0
end
local payload = redis.call('HGET', KEYS[2], ARGV[1])
if not payload then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), payload)
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
`)

// RENEWCLAIM extends a live claim's lease. Token-guarded: stale owners
// cannot extend a claim they no longer own. Returns 1 on success, -1 on
// token mismatch, 0 if no claim exists. Verified via HGET owners vs token
// — old replica heartbeat after takeover gets -1 and deletes local handle.
var renewClaimScript = redis.NewScript(`
if redis.call('HGET', KEYS[3], ARGV[1]) ~= ARGV[3] then
  if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
    return -1
  end
  return 0
end
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
return 1
`)

// newClaimToken generates the per-claim ownership token that lets ack/release
// distinguish "my claim" from "a claim redelivered to another instance".
func newClaimToken() (string, error) {
	buf := make([]byte, claimTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// claimKey returns the generation-scoped map key. RequestToken is the
// per-enqueue generation UUID; ReqID alone may be reused across
// submissions (see redis_sortedset_producer.go:206-208).
func claimKey(requestID, requestToken string) string {
	if requestToken == "" {
		return requestID
	}
	return requestID + "\x00" + requestToken
}

// claimHandle is what this instance tracks per in-flight request: the
// ownership proof plus everything the heartbeater needs to renew the claim
// without re-reading Redis state.
type claimHandle struct {
	token        string
	queue        string
	deadline     float64
	requestID    string
	requestToken string
}

// claimExpiry computes the lease deadline for a request claimed now: the
// lease TTL, capped so it never outlives the request deadline plus grace —
// an expired request must terminate through the deadline-exceeded path, not
// linger claimed.
func (r *RedisSortedSetFlow) claimExpiry(deadline float64) float64 {
	now := float64(time.Now().Unix())
	expiry := now + r.claimLeaseTTL.Seconds()
	if grace := float64(reclaimGraceAfterDeadline.Seconds()); deadline > 0 && deadline+grace < expiry {
		expiry = deadline + grace
	}
	return expiry
}

// claimRequest claims one peeked request. On success the caller owns the
// request until it acks a terminal result, releases it, or dies.
func (r *RedisSortedSetFlow) claimRequest(ctx context.Context, queueName string, ir *api.InternalRequest, member string, deadline float64) (token string, ok bool, err error) {
	token, err = newClaimToken()
	if err != nil {
		return "", false, err
	}
	keys := newClaimKeys(queueName)
	// Key Redis claim state by generation (ID + RequestToken) so concurrent
	// submissions with the same ReqID cannot overwrite each other's claim.
	reqID := ir.PublicRequest.ReqID()
	reqToken := ir.RequestToken
	claimID := claimKey(reqID, reqToken)
	res, err := claimScript.Run(ctx, r.rdb, []string{
		keys.pending, keys.claimed, keys.owners, keys.idx,
	}, claimID, member, token, r.claimExpiry(deadline)).Int()
	if err != nil {
		return "", false, fmt.Errorf("claim request %q on queue %q: %w", reqID, queueName, err)
	}
	if res == 0 {
		return "", false, nil
	}
	r.claimTokens.Store(claimID, &claimHandle{
		token:        token,
		queue:        queueName,
		deadline:     deadline,
		requestID:    reqID,
		requestToken: reqToken,
	})
	return token, true, nil
}

// releaseClaim returns a claimed request to pending during graceful shutdown.
func (r *RedisSortedSetFlow) releaseClaim(ctx context.Context, queueName string, requestID string, requestToken string, member string, score float64, token string) error {
	keys := newClaimKeys(queueName)
	claimID := claimKey(requestID, requestToken)
	err := releaseClaimScript.Run(ctx, r.rdb, []string{
		keys.pending, keys.claimed, keys.owners, keys.idx,
	}, claimID, member, score, token).Err()
	if err != nil {
		return fmt.Errorf("release claim for %q on queue %q: %w", requestID, queueName, err)
	}
	r.claimTokens.Delete(claimID)
	return nil
}

// ackResult records a terminal result (idempotently) and drops this flow's
// claim. claimQueueName hosts the claim bookkeeping; resultList is the
// resolved destination. pushed=false means a stale owner was fenced.
func (r *RedisSortedSetFlow) ackResult(ctx context.Context, claimQueueName string, resultList string, requestID string, requestToken string, resultJSON string, listTTL time.Duration) (pushed bool, err error) {
	// Peek the token rather than consuming it: if the script errors the
	// caller may retry this ack, and the ownership proof must survive.
	claimID := claimKey(requestID, requestToken)
	var token string
	if v, ok := r.claimTokens.Load(claimID); ok {
		if h, ok := v.(*claimHandle); ok {
			token = h.token
		}
	}
	keys := newClaimKeys(claimQueueName)
	listTTLSec := int64(0)
	if listTTL > 0 {
		listTTLSec = int64(listTTL.Seconds())
	}
	res, err := ackResultScript.Run(ctx, r.rdb, []string{
		keys.claimed, keys.owners, keys.idx, resultList,
	}, claimID, resultJSON, token, listTTLSec).Int()
	if err != nil {
		return false, fmt.Errorf("ack result for %q: %w", requestID, err)
	}
	r.claimTokens.Delete(claimID)
	if res == 0 {
		return false, nil
	}
	return true, nil
}

// renewClaim extends the lease of a request being sent to retry. Ownership is
// retained across the backoff; the eventual terminal result acks and releases.
func (r *RedisSortedSetFlow) renewClaim(ctx context.Context, queueName string, requestID string, requestToken string, deadline float64, token string) (int, error) {
	keys := newClaimKeys(queueName)
	claimID := claimKey(requestID, requestToken)
	res, err := renewClaimScript.Run(ctx, r.rdb, []string{keys.claimed, keys.idx, keys.owners},
		claimID, r.claimExpiry(deadline), token).Int()
	if err != nil {
		return 0, fmt.Errorf("renew claim for %q on queue %q: %w", requestID, queueName, err)
	}
	return res, nil
}

// reclaimExpiredClaims releases every claim whose lease has lapsed, returning
// each request to its queue's pending zset for redelivery. Runs on whichever
// instance holds the transport open; instances that died cannot run it, which
// is precisely the point — survivors take over their in-flight work.
func (r *RedisSortedSetFlow) reclaimExpiredClaims(ctx context.Context) (released int, err error) {
	logger := log.FromContext(ctx)
	now := float64(time.Now().Unix())

	for _, ch := range r.queueSnapshot() {
		queueName := ch.queueName
		keys := newClaimKeys(queueName)
		expiredIDs, err := r.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key: keys.idx, ByScore: true,
			Start: "-inf", Stop: fmt.Sprintf("%f", now),
			Count: reclaimBatchSize, Offset: 0,
		}).Result()
		if err != nil {
			return released, fmt.Errorf("read expired claims on queue %q: %w", queueName, err)
		}
		for _, id := range expiredIDs {
			payload, err := r.rdb.HGet(ctx, keys.claimed, id).Result()
			if err != nil && err != redis.Nil {
				return released, fmt.Errorf("read claim payload for %q: %w", id, err)
			}
			score := float64(0)
			if err == nil {
				var ir api.InternalRequest
				if jsonErr := json.Unmarshal([]byte(payload), &ir); jsonErr == nil && ir.PublicRequest != nil {
					score = ir.QueueScore()
				}
			}
			res, err := reclaimExpiredScript.Run(ctx, r.rdb, []string{
				keys.pending, keys.claimed, keys.owners, keys.idx,
			}, id, score, now).Int()
			if err != nil {
				return released, fmt.Errorf("reclaim claim for %q on queue %q: %w", id, queueName, err)
			}
			if res == 1 {
				released++
				logger.V(logutil.DEBUG).Info("Reclaimed expired claim, redelivering", "id", id, "queue", queueName)
			}
		}
	}
	return released, nil
}

// startReclaimer launches the background loop that redelivers lapsed claims.
// It runs on the drain context so it keeps working through the graceful
// shutdown window, catching claims abandoned by workers that hit DrainTimeout.
func (r *RedisSortedSetFlow) startReclaimer(ctx context.Context) {
	logger := log.FromContext(ctx)
	interval := r.claimReclaimInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.reclaimExpiredClaims(ctx); err != nil {
				logger.V(logutil.DEFAULT).Error(err, "Failed to reclaim expired claims")
			}
		}
	}
}

// heartbeatInterval is how often live claims are renewed: a third of the
// lease TTL, clamped so ticks are neither sub-second spam nor multi-minute
// gaps.
func (r *RedisSortedSetFlow) heartbeatInterval() time.Duration {
	hb := r.claimLeaseTTL / 3
	if hb < time.Second {
		hb = time.Second
	}
	if hb > 30*time.Second {
		hb = 30 * time.Second
	}
	return hb
}

// heartbeatClaims renews every held claim so slow-but-alive work is not
// treated as dead. Acked/released ids leave the map and are skipped.
// Any result other than 1 (0 = missing, -1 = stolen) means the handle is
// stale and is deleted to prevent leaks and wasted renewals.
func (r *RedisSortedSetFlow) heartbeatClaims(ctx context.Context) {
	logger := log.FromContext(ctx)
	r.claimTokens.Range(func(key, value any) bool {
		id, _ := key.(string)
		h, ok := value.(*claimHandle)
		if !ok || h == nil || id == "" {
			return true
		}
		reqID := h.requestID
		if reqID == "" {
			reqID = id
		}
		res, err := r.renewClaim(ctx, h.queue, reqID, h.requestToken, h.deadline, h.token)
		if err != nil {
			logger.V(logutil.DEBUG).Error(err, "Failed to renew claim lease", "id", id)
			return true
		}
		if res != 1 {
			r.claimTokens.Delete(id)
		}
		return true
	})
}

// startHeartbeat renews held claims until the flow's drain context closes.
// It runs alongside the reclaimer: the reclaimer catches owners that died,
// the heartbeater proves this owner is not one of them.
func (r *RedisSortedSetFlow) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(r.heartbeatInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.heartbeatClaims(ctx)
		}
	}
}
