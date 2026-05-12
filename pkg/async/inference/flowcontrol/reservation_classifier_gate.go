/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package flowcontrol

import (
	"context"
	"fmt"
	"strings"

	"github.com/llm-d-incubation/llm-d-async/pipeline"
	"github.com/redis/go-redis/v9"
)

// ReservationClassifierGate stamps an outgoing classification label
// (default key "class", values "reserved" / "overflow") on each
// message based on whether the bucket the message falls into is
// within its operator-configured in-flight reservation cap.
//
// The bucket is the concatenation of the label values named by
// BucketKeys, in order. With BucketKeys=[team,tier,model] and a
// message labeled {team=foo, tier=async, model=k2-6}, the bucket
// key suffix is "foo:async:k2-6"; in-flight is tracked at
// "rmp:inflight:foo:async:k2-6", cap is read from
// "rmp:cap:foo:async:k2-6".
//
// Atomicity is via a single Lua script: GET cap, INCR in-flight,
// EXPIRE in-flight (leak ceiling), and on over-cap DECR back. One
// round-trip per classify. The returned slot-held flag tells the gate
// whether to attach a release that DECRs on terminal — overflow
// messages don't hold the slot, so they need no release.
//
// On redis errors, the gate fails open: classifies as overflow and
// returns Continue with an attached error. The dispatcher's
// strict-priority + round-robin ordering still works; degradation is
// graceful (everyone competes equally for pool saturation without
// reservation isolation).
type ReservationClassifierGate struct {
	rdb           *redis.Client
	bucketKeys    []string
	classKey      string
	reservedValue string
	overflowValue string
	keyPrefix     string
	ttlSeconds    int
	fallbackCap   int

	script *redis.Script
}

var _ pipeline.Gate = (*ReservationClassifierGate)(nil)

// Default values.
const (
	defaultClassifierClassKey      = "class"
	defaultClassifierReservedValue = "reserved"
	defaultClassifierOverflowValue = "overflow"
	defaultClassifierKeyPrefix     = "rmp"
	defaultClassifierTTLSeconds    = 24 * 60 * 60 // 24h, well above any reasonable request deadline
)

// classifyScript runs in redis atomically.
//
// KEYS[1] = inflight key   (e.g. rmp:inflight:teamA:async:k2-6)
// KEYS[2] = cap key        (e.g. rmp:cap:teamA:async:k2-6)
// ARGV[1] = TTL seconds    (string int)
// ARGV[2] = fallback cap   (string int; used when cap key is unset)
//
// Returns 1 if the bucket was within cap (slot held; caller attaches
// release), 0 if over cap (slot returned; no release).
const classifyScript = `
local cap = tonumber(redis.call("GET", KEYS[2])) or tonumber(ARGV[2])
local cur = redis.call("INCR", KEYS[1])
redis.call("EXPIRE", KEYS[1], ARGV[1])
if cur > cap then
  redis.call("DECR", KEYS[1])
  return 0
end
return 1
`

// ReservationClassifierConfig is the parsed gate config.
type ReservationClassifierConfig struct {
	BucketKeys    []string // ordered label keys composing the bucket key
	ClassKey      string   // output label key (default "class")
	ReservedValue string   // value stamped when reserved (default "reserved")
	OverflowValue string   // value stamped when overflow (default "overflow")
	KeyPrefix     string   // redis key prefix (default "rmp")
	TTLSeconds    int      // in-flight key TTL in seconds (default 86400)
	FallbackCap   int      // cap used when no rmp:cap:<bucket> key set (default 0 => all overflow)
}

// NewReservationClassifierGate constructs a gate that classifies via redis.
// cfg.BucketKeys must be non-empty. rdb must be non-nil.
func NewReservationClassifierGate(rdb *redis.Client, cfg ReservationClassifierConfig) (*ReservationClassifierGate, error) {
	if rdb == nil {
		return nil, fmt.Errorf("reservation-classifier: redis client is required")
	}
	if len(cfg.BucketKeys) == 0 {
		return nil, fmt.Errorf("reservation-classifier: bucket_keys must be non-empty")
	}
	g := &ReservationClassifierGate{
		rdb:           rdb,
		bucketKeys:    cfg.BucketKeys,
		classKey:      cfg.ClassKey,
		reservedValue: cfg.ReservedValue,
		overflowValue: cfg.OverflowValue,
		keyPrefix:     cfg.KeyPrefix,
		ttlSeconds:    cfg.TTLSeconds,
		fallbackCap:   cfg.FallbackCap,
		script:        redis.NewScript(classifyScript),
	}
	if g.classKey == "" {
		g.classKey = defaultClassifierClassKey
	}
	if g.reservedValue == "" {
		g.reservedValue = defaultClassifierReservedValue
	}
	if g.overflowValue == "" {
		g.overflowValue = defaultClassifierOverflowValue
	}
	if g.keyPrefix == "" {
		g.keyPrefix = defaultClassifierKeyPrefix
	}
	if g.ttlSeconds <= 0 {
		g.ttlSeconds = defaultClassifierTTLSeconds
	}
	if g.fallbackCap < 0 {
		g.fallbackCap = 0
	}
	return g, nil
}

// bucketSuffix joins the configured bucket-key label values with ":".
// Missing labels become empty segments, which is a stable bucket the
// operator can address with redis-cli if they care to.
func (g *ReservationClassifierGate) bucketSuffix(labels pipeline.Labels) string {
	parts := make([]string, len(g.bucketKeys))
	for i, k := range g.bucketKeys {
		parts[i] = labels.Get(k)
	}
	return strings.Join(parts, ":")
}

// Apply implements pipeline.Gate.
//
// Always returns Continue. The verdict signal is in the message Labels
// (class=reserved|overflow). Always-Continue keeps classification
// orthogonal from admission — pool gates downstream consult labels for
// fail-fast / blocking decisions.
//
// On redis error, classifies as overflow (fail-safe — see package doc).
// The error is returned alongside Continue so the Flow's gate-eval site
// can log it; the message still flows.
func (g *ReservationClassifierGate) Apply(ctx context.Context, msg *pipeline.EmbelishedRequestMessage) (pipeline.Verdict, error) {
	if msg == nil {
		return pipeline.Continue, nil
	}
	bucket := g.bucketSuffix(msg.Labels)
	inflightKey := g.keyPrefix + ":inflight:" + bucket
	capKey := g.keyPrefix + ":cap:" + bucket

	res, err := g.script.Run(
		ctx,
		g.rdb,
		[]string{inflightKey, capKey},
		g.ttlSeconds,
		g.fallbackCap,
	).Int()
	if err != nil {
		// Fail-safe: classify as overflow, keep the message flowing.
		// Caller logs the err.
		if msg.Labels == nil {
			msg.Labels = pipeline.Labels{}
		}
		msg.Labels.Set(g.classKey, g.overflowValue)
		return pipeline.Continue, err
	}

	if msg.Labels == nil {
		msg.Labels = pipeline.Labels{}
	}
	if res == 1 {
		msg.Labels.Set(g.classKey, g.reservedValue)
		// Slot held — DECR on terminal.
		rdb := g.rdb
		msg.AttachRelease(func() {
			// Use a fresh background context: terminal release may
			// fire after the original request ctx is canceled.
			rdb.Decr(context.Background(), inflightKey)
		})
	} else {
		msg.Labels.Set(g.classKey, g.overflowValue)
		// Lua already DECR'd; no release.
	}
	return pipeline.Continue, nil
}
