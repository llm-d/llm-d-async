package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"
)

const (
	defaultResultClaimLeaseTTL        = 5 * time.Minute
	defaultResultClaimReclaimInterval = time.Second
	resultAckTombstoneTTL             = 7 * 24 * time.Hour
	resultReclaimBatchSize            = 100
	resultReceiveScanBatchSize        = 100
	resultTombstoneCleanupBatchSize   = 100
)

// resultClaimKeys bundles the keys implementing durable delivery for one
// result route. The pending list is the shared contract with the dispatcher
// and keeps its name; the rest are private to the producer and, in cluster
// mode, hash-tagged into the pending key's slot (see slotKey).
type resultClaimKeys struct {
	pending    string
	claimed    string
	owners     string
	idx        string
	tombstones string
}

func newResultClaimKeys(route string, cluster bool) resultClaimKeys {
	base := slotKey(route, cluster)
	return resultClaimKeys{
		pending:    route,
		claimed:    base + ":result-claimed",
		owners:     base + ":result-claim-owners",
		idx:        base + ":result-claims-idx",
		tombstones: base + ":result-ack-tombstones",
	}
}

// receiveResultScript requeues expired claims, skips acknowledged duplicates,
// and atomically moves the oldest pending result into route-local claim state.
// The legacy LPUSH/RPOP FIFO direction is preserved.
var receiveResultScript = redis.NewScript(`
local serverTime = redis.call('TIME')
local now = (tonumber(serverTime[1]) * 1000) + math.floor(tonumber(serverTime[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, ARGV[3])
for i = #expired, 1, -1 do
  local claimID = expired[i]
  local payload = redis.call('HGET', KEYS[2], claimID)
  if payload then
    redis.call('RPUSH', KEYS[1], payload)
  end
  redis.call('HDEL', KEYS[2], claimID)
  redis.call('HDEL', KEYS[3], claimID)
  redis.call('ZREM', KEYS[4], claimID)
end

local expiredTombstones = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', now, 'LIMIT', 0, ARGV[5])
if #expiredTombstones > 0 then
  redis.call('ZREM', KEYS[5], unpack(expiredTombstones))
end

local pending = math.min(redis.call('LLEN', KEYS[1]), tonumber(ARGV[4]))
if pending == 0 then
  return {}
end
local function claim(payload, claimID)
  redis.call('RPOP', KEYS[1])
  redis.call('HSET', KEYS[2], claimID, payload)
  redis.call('HSET', KEYS[3], claimID, ARGV[1])
  redis.call('ZADD', KEYS[4], now + tonumber(ARGV[2]), claimID)
end
for _ = 1, pending do
  local payload = redis.call('LINDEX', KEYS[1], -1)
  if not payload then
    return {}
  end

  local decodedOK, result = pcall(cjson.decode, payload)
  if not decodedOK or type(result) ~= 'table' or type(result['id']) ~= 'string' or result['id'] == '' then
    local claimID = string.char(0) .. 'unparsable-result' .. string.char(0) .. ARGV[1]
    claim(payload, claimID)
    return {-1, payload, claimID}
  end

  local requestToken = result['request_token']
  if requestToken == nil then
    requestToken = ''
  elseif type(requestToken) ~= 'string' then
    local claimID = string.char(0) .. 'unparsable-result' .. string.char(0) .. ARGV[1]
    claim(payload, claimID)
    return {-1, payload, claimID}
  end

  local claimID = result['id']
  if requestToken ~= '' then
    claimID = claimID .. string.char(0) .. requestToken
  end

  local tombstoneExpiry = redis.call('ZSCORE', KEYS[5], claimID)
  if tombstoneExpiry and tonumber(tombstoneExpiry) <= now then
    redis.call('ZREM', KEYS[5], claimID)
    tombstoneExpiry = false
  end
  if tombstoneExpiry then
    redis.call('RPOP', KEYS[1])
  elseif redis.call('HEXISTS', KEYS[3], claimID) == 1 then
    redis.call('RPOP', KEYS[1])
  else
    claim(payload, claimID)
    return {1, payload, claimID}
  end
end

return {0}
`)

var renewResultScript = redis.NewScript(`
local serverTime = redis.call('TIME')
local now = (tonumber(serverTime[1]) * 1000) + math.floor(tonumber(serverTime[2]) / 1000)
local owner = redis.call('HGET', KEYS[1], ARGV[1])
local expiry = redis.call('ZSCORE', KEYS[2], ARGV[1])
if not owner or owner ~= ARGV[2] or ARGV[2] == '' or not expiry or tonumber(expiry) <= now then
  return 0
end
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[3]), ARGV[1])
return 1
`)

// ackResultDeliveryScript is owner-fenced and idempotent. Successful ACKs
// leave a bounded tombstone so a retried ACK succeeds and duplicate records
// for the same request generation are suppressed during the retention window.
var ackResultDeliveryScript = redis.NewScript(`
local serverTime = redis.call('TIME')
local now = (tonumber(serverTime[1]) * 1000) + math.floor(tonumber(serverTime[2]) / 1000)
local expiredTombstones = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, ARGV[4])
if #expiredTombstones > 0 then
  redis.call('ZREM', KEYS[4], unpack(expiredTombstones))
end
local owner = redis.call('HGET', KEYS[2], ARGV[1])
local expiry = redis.call('ZSCORE', KEYS[3], ARGV[1])
if owner and owner == ARGV[2] and ARGV[2] ~= '' and expiry and tonumber(expiry) > now then
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('HDEL', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[3], ARGV[1])
  redis.call('ZADD', KEYS[4], now + tonumber(ARGV[3]), ARGV[1])
  redis.call('PEXPIRE', KEYS[4], ARGV[3])
  return 1
end
local tombstoneExpiry = redis.call('ZSCORE', KEYS[4], ARGV[1])
if tombstoneExpiry and tonumber(tombstoneExpiry) > now then
  return 2
end
if tombstoneExpiry then
  redis.call('ZREM', KEYS[4], ARGV[1])
end
return 0
`)

// ResultDeliveryConfig returns the effective lease and reclaim settings so a
// deployment can report its result recovery-time contract.
func (p *RedisSortedSetProducer) ResultDeliveryConfig() ResultDeliveryConfig {
	return ResultDeliveryConfig{
		LeaseTTL:        p.resultClaimLeaseTTL,
		ReclaimInterval: p.resultClaimReclaimInterval,
	}
}

// ReceiveResult claims the oldest result from this producer's configured
// result route. The result remains durable in Redis until AckResult succeeds;
// an expired lease is requeued for another receiver.
func (p *RedisSortedSetProducer) ReceiveResult(ctx context.Context) (*ResultDelivery, error) {
	keys := newResultClaimKeys(p.resultQueueName, p.clusterKeys)
	ownerToken, err := newRequestToken()
	if err != nil {
		return nil, fmt.Errorf("failed to create result claim owner: %w", err)
	}
	for {
		raw, err := receiveResultScript.Run(ctx, p.client, []string{
			keys.pending, keys.claimed, keys.owners, keys.idx, keys.tombstones,
		}, ownerToken, max(int64(1), p.resultClaimLeaseTTL.Milliseconds()), resultReclaimBatchSize,
			resultReceiveScanBatchSize, resultTombstoneCleanupBatchSize).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			return nil, fmt.Errorf("failed to receive durable result: %w", err)
		}

		values, ok := raw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected durable result response type %T", raw)
		}
		if len(values) > 0 {
			status, ok := values[0].(int64)
			if !ok {
				return nil, fmt.Errorf("unexpected durable result status type %T", values[0])
			}
			if status == 0 {
				continue
			}
			if len(values) < 2 {
				return nil, errors.New("unexpected durable result response")
			}
			payload, ok := values[1].(string)
			if !ok {
				return nil, fmt.Errorf("unexpected durable result payload type %T", values[1])
			}
			if status == -1 {
				if len(values) != 3 {
					return nil, errors.New("unexpected durable result response")
				}
				return nil, fmt.Errorf("%w: missing valid 'id' or 'request_token' field", ErrUnparsableResult)
			}
			if status != 1 || len(values) != 3 {
				return nil, errors.New("unexpected durable result response")
			}
			claimID, ok := values[2].(string)
			if !ok {
				return nil, fmt.Errorf("unexpected durable result claim ID type %T", values[2])
			}
			result, err := p.parseResult(payload)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrUnparsableResult, err)
			}
			return &ResultDelivery{Result: result, claimID: claimID, ownerToken: ownerToken}, nil
		}

		timer := time.NewTimer(p.resultClaimReclaimInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// RenewResult extends a delivery's lease when checkpointing takes longer than
// the configured lease TTL. A stale owner is fenced.
func (p *RedisSortedSetProducer) RenewResult(ctx context.Context, delivery *ResultDelivery) error {
	if delivery == nil || delivery.claimID == "" || delivery.ownerToken == "" {
		return errors.New("result delivery is required")
	}
	keys := newResultClaimKeys(p.resultQueueName, p.clusterKeys)
	res, err := renewResultScript.Run(ctx, p.client, []string{keys.owners, keys.idx},
		delivery.claimID, delivery.ownerToken, max(int64(1), p.resultClaimLeaseTTL.Milliseconds())).Int()
	if err != nil {
		return fmt.Errorf("failed to renew result delivery: %w", err)
	}
	if res != 1 {
		return ErrResultDeliveryOwnershipLost
	}
	return nil
}

// AckResult acknowledges that the consumer durably accepted a delivery.
// Repeating a successful acknowledgement is safe; stale owners are fenced.
func (p *RedisSortedSetProducer) AckResult(ctx context.Context, delivery *ResultDelivery) error {
	if delivery == nil || delivery.claimID == "" || delivery.ownerToken == "" {
		return errors.New("result delivery is required")
	}
	keys := newResultClaimKeys(p.resultQueueName, p.clusterKeys)
	res, err := ackResultDeliveryScript.Run(ctx, p.client, []string{
		keys.claimed, keys.owners, keys.idx, keys.tombstones,
	}, delivery.claimID, delivery.ownerToken, resultAckTombstoneTTL.Milliseconds(), resultTombstoneCleanupBatchSize).Int()
	if err != nil {
		return fmt.Errorf("failed to acknowledge result delivery: %w", err)
	}
	if res == 0 {
		return ErrResultDeliveryOwnershipLost
	}
	return nil
}

func parseInternalResult(data string) (*api.ResultMessage, error) {
	var result api.InternalResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}
	if result.ID == "" {
		return nil, errors.New("result missing 'id' field")
	}
	result.Routing.RequestToken = result.RequestToken
	return &result.ResultMessage, nil
}
