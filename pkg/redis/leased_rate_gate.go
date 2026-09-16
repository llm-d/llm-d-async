package redis

import (
	"context"
	"fmt"
	"math"
	"strconv"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/metrics"
	goredis "github.com/redis/go-redis/v9"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

const (
	dispatchRateAPIVersionField = "api_version"
	dispatchRatePoolIDField     = "pool_id"
	dispatchRateRPSField        = "max_admission_rps"
	dispatchRateValidUntilField = "valid_until_unix_ms"
	dispatchRateDecisionIDField = "decision_id"
)

var _ pipeline.Gate = (*RedisLeasedRateGate)(nil)

// RedisLeasedRateGate enforces a dynamically controlled, pool-wide token
// bucket. The command is a leased Redis hash; a missing, malformed, mismatched,
// expired, or unreadable command fails closed. Token state is also stored in
// Redis, so multiple llm-d Async replicas share one aggregate rate ceiling.
//
// This gate is normally configured as a pool-level gate inside
// wait-on-refuse. Budget reports only whether a positive lease is active; the
// exact request rate is enforced by Apply.
type RedisLeasedRateGate struct {
	rdb          *goredis.Client
	controlKey   string
	stateKey     string
	poolID       string
	burstSeconds float64
}

// NewRedisLeasedRateGate constructs a leased rate gate. A one-second token
// bucket is the conventional default; callers may choose a smaller burst.
func NewRedisLeasedRateGate(client *goredis.Client, controlKey, stateKey, poolID string, burstSeconds float64) *RedisLeasedRateGate {
	if stateKey == "" {
		stateKey = controlKey + ":state"
	}
	gate := &RedisLeasedRateGate{
		rdb:          client,
		controlKey:   controlKey,
		stateKey:     stateKey,
		poolID:       poolID,
		burstSeconds: burstSeconds,
	}
	// Pool-level worker gating calls Apply directly, so initialize the labelset
	// here rather than waiting for the first request. This gives controllers a
	// safe bootstrap observation (no valid applied lease) before they seed one.
	metrics.SetDrainLimit(poolID, 0, 0, false)
	return gate
}

// Budget reports whether a positive, valid lease exists. It intentionally does
// not convert an absolute RPS command into a normalized fraction: Apply owns the
// exact token-bucket enforcement and avoids coupling the API to queue poll size.
func (g *RedisLeasedRateGate) Budget(ctx context.Context) float64 {
	limit, err := g.readLimit(ctx)
	if err != nil {
		metrics.SetDrainLimit(g.poolID, 0, 0, false)
		return 0
	}
	metrics.SetDrainLimit(g.poolID, limit.MaxAdmissionRPS, limit.ValidUntilUnixMillis, true)
	if limit.MaxAdmissionRPS == 0 {
		return 0
	}
	return 1
}

// Apply atomically consumes one token from the pool-wide Redis token bucket.
// Any control-plane or Redis failure returns Refuse with no error so a
// wait-on-refuse wrapper parks the worker instead of turning a controller outage
// into a terminal inference result.
func (g *RedisLeasedRateGate) Apply(ctx context.Context, _ *api.InternalRequest, _ *[]pipeline.GateReleaseFunc) (pipeline.Verdict, error) {
	result, err := leasedRateAcquireScript.Run(ctx, g.rdb, []string{g.controlKey, g.stateKey},
		api.DispatchRateLimitAPIVersion,
		g.poolID,
		g.burstSeconds,
	).Result()
	if err != nil {
		log.FromContext(ctx).V(logutil.DEFAULT).Error(err, "Failed to evaluate leased dispatch-rate limit", "pool", g.poolID)
		metrics.SetDrainLimit(g.poolID, 0, 0, false)
		return pipeline.Refuse(), nil
	}

	allowed, leaseValid, rate, validUntil, err := parseLeasedRateResult(result)
	if err != nil {
		log.FromContext(ctx).V(logutil.DEFAULT).Error(err, "Invalid leased dispatch-rate result", "pool", g.poolID)
		metrics.SetDrainLimit(g.poolID, 0, 0, false)
		return pipeline.Refuse(), nil
	}
	metrics.SetDrainLimit(g.poolID, rate, validUntil, leaseValid)
	if !allowed {
		return pipeline.Refuse(), nil
	}
	return pipeline.Continue(), nil
}

func (g *RedisLeasedRateGate) readLimit(ctx context.Context) (api.DispatchRateLimit, error) {
	var valuesCmd *goredis.SliceCmd
	var timeCmd *goredis.TimeCmd
	// Read the command and Redis's authoritative clock in one MULTI/EXEC
	// snapshot. No control-plane writer can interleave between the two reads,
	// and Budget evaluates expiry against the same clock used by Apply's Lua
	// script on every Async replica.
	_, err := g.rdb.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
		valuesCmd = pipe.HMGet(ctx, g.controlKey,
			dispatchRateAPIVersionField,
			dispatchRatePoolIDField,
			dispatchRateRPSField,
			dispatchRateValidUntilField,
			dispatchRateDecisionIDField,
		)
		timeCmd = pipe.Time(ctx)
		return nil
	})
	if err != nil {
		return api.DispatchRateLimit{}, fmt.Errorf("read dispatch-rate command and Redis time: %w", err)
	}
	values, err := valuesCmd.Result()
	if err != nil {
		return api.DispatchRateLimit{}, fmt.Errorf("read dispatch-rate command: %w", err)
	}
	redisNow, err := timeCmd.Result()
	if err != nil {
		return api.DispatchRateLimit{}, fmt.Errorf("read Redis time: %w", err)
	}
	if len(values) != 5 {
		return api.DispatchRateLimit{}, fmt.Errorf("dispatch-rate command has %d fields, want 5", len(values))
	}
	for i, value := range values {
		if value == nil {
			return api.DispatchRateLimit{}, fmt.Errorf("dispatch-rate command field %d is missing", i)
		}
	}
	rate, err := strconv.ParseFloat(fmt.Sprint(values[2]), 64)
	if err != nil {
		return api.DispatchRateLimit{}, fmt.Errorf("invalid max_admission_rps: %w", err)
	}
	validUntil, err := strconv.ParseInt(fmt.Sprint(values[3]), 10, 64)
	if err != nil {
		return api.DispatchRateLimit{}, fmt.Errorf("invalid valid_until_unix_ms: %w", err)
	}
	limit := api.DispatchRateLimit{
		APIVersion:           fmt.Sprint(values[0]),
		PoolID:               fmt.Sprint(values[1]),
		MaxAdmissionRPS:      rate,
		ValidUntilUnixMillis: validUntil,
		DecisionID:           fmt.Sprint(values[4]),
	}
	if err := limit.ValidateAt(redisNow); err != nil {
		return api.DispatchRateLimit{}, fmt.Errorf("validate dispatch-rate command: %w", err)
	}
	if limit.PoolID != g.poolID {
		return api.DispatchRateLimit{}, fmt.Errorf("dispatch-rate command is for pool %q, want %q", limit.PoolID, g.poolID)
	}
	return limit, nil
}

func parseLeasedRateResult(raw any) (allowed, leaseValid bool, rate float64, validUntil int64, err error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 4 {
		return false, false, 0, 0, fmt.Errorf("unexpected script response %T", raw)
	}
	allowedInt, err := resultInt64(values[0])
	if err != nil {
		return false, false, 0, 0, err
	}
	validInt, err := resultInt64(values[1])
	if err != nil {
		return false, false, 0, 0, err
	}
	rate, err = strconv.ParseFloat(fmt.Sprint(values[2]), 64)
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return false, false, 0, 0, fmt.Errorf("invalid rate in script response")
	}
	validUntil, err = resultInt64(values[3])
	if err != nil {
		return false, false, 0, 0, err
	}
	return allowedInt == 1, validInt == 1, rate, validUntil, nil
}

func resultInt64(value any) (int64, error) {
	var text string
	switch value := value.(type) {
	case int64:
		return value, nil
	case string:
		text = value
	case []byte:
		text = string(value)
	default:
		text = fmt.Sprint(value)
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse integer script result %q: %w", text, err)
	}
	return parsed, nil
}

//nolint:gochecknoglobals // The parsed Redis script is immutable and shared by every gate instance.
var leasedRateAcquireScript = goredis.NewScript(`
local command = redis.call("HMGET", KEYS[1],
  "api_version", "pool_id", "max_admission_rps", "valid_until_unix_ms", "decision_id")

for i = 1, 5 do
  if not command[i] or command[i] == false or command[i] == "" then
    return {0, 0, "0", 0}
  end
end

if command[1] ~= ARGV[1] or command[2] ~= ARGV[2] then
  return {0, 0, "0", 0}
end

local rate = tonumber(command[3])
local valid_until = tonumber(command[4])
local redis_time = redis.call("TIME")
local now_seconds = tonumber(redis_time[1])
local now_microseconds = tonumber(redis_time[2])
local burst_seconds = tonumber(ARGV[3])
if not now_seconds or not now_microseconds then
  return {0, 0, "0", 0}
end
local now = (now_seconds * 1000) + math.floor(now_microseconds / 1000)
local rate_invalid = not rate or rate ~= rate or rate == math.huge or rate == -math.huge or rate < 0
local valid_until_invalid = not valid_until or valid_until ~= valid_until or
  valid_until == math.huge or valid_until == -math.huge or valid_until % 1 ~= 0 or valid_until <= now
local burst_invalid = not burst_seconds or burst_seconds ~= burst_seconds or
  burst_seconds == math.huge or burst_seconds == -math.huge or burst_seconds <= 0
if rate_invalid or valid_until_invalid or burst_invalid then
  return {0, 0, "0", 0}
end
if rate == 0 then
  return {0, 1, "0", valid_until}
end

local requested_capacity = rate * burst_seconds
if requested_capacity ~= requested_capacity or requested_capacity == math.huge or requested_capacity == -math.huge then
  return {0, 0, "0", 0}
end
local capacity = math.max(1, requested_capacity)
local tokens = tonumber(redis.call("HGET", KEYS[2], "tokens"))
local last_ms = tonumber(redis.call("HGET", KEYS[2], "last_ms"))
if not tokens or not last_ms then
  tokens = capacity
  last_ms = now
else
  -- Redis TIME is shared by every Async replica. If the Redis host clock ever
  -- moves backwards, preserve the last bucket timestamp so the later clock
  -- correction cannot refill the same interval twice.
  if now < last_ms then
    now = last_ms
  end
  tokens = math.min(capacity, tokens + ((now - last_ms) / 1000.0) * rate)
end

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call("HSET", KEYS[2], "tokens", tostring(tokens), "last_ms", tostring(now))
local wall_now = (now_seconds * 1000) + math.floor(now_microseconds / 1000)
local state_ttl_ms = math.max(1000, valid_until - wall_now + 1000)
redis.call("PEXPIRE", KEYS[2], state_ttl_ms)
return {allowed, 1, tostring(rate), valid_until}
`)
