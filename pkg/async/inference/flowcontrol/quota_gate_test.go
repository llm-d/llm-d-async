package flowcontrol_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/async/inference/flowcontrol"
	redisgate "github.com/llm-d/llm-d-async/pkg/redis"
	"github.com/llm-d/llm-d-async/pkg/sqlflow"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var quotaBackends = []struct {
	name string
	open func(t *testing.T, slotTTL time.Duration) flowcontrol.QuotaStore
}{
	{"redis", func(t *testing.T, slotTTL time.Duration) flowcontrol.QuotaStore {
		s := miniredis.RunT(t)
		rdb := goredis.NewClient(&goredis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return redisgate.NewQuotaStore(rdb, slotTTL)
	}},
	{"sql", func(t *testing.T, _ time.Duration) flowcontrol.QuotaStore {
		store := openSQLStore(t)
		q := sqlflow.NewQuotaStore(store, 30*time.Second, time.Second, logr.Discard())
		t.Cleanup(q.Close)
		return q
	}},
}

// openSQLStore opens a store in a schema of its own, dropped when the test ends.
func openSQLStore(t *testing.T) *sqlqueue.Store {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	admin, err := sql.Open("pgx", pgURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	schema := fmt.Sprintf("quota_gate_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA " + schema + " CASCADE") })

	u, err := url.Parse(pgURL)
	require.NoError(t, err)
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	store, err := sqlqueue.Open(context.Background(), u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type applied struct {
	verdict  pipeline.Verdict
	class    api.QuotaClassification
	releases []pipeline.GateReleaseFunc
}

func apply(t *testing.T, gate pipeline.Gate, metadata map[string]string) applied {
	t.Helper()
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{ID: "req", Metadata: metadata})
	var releases []pipeline.GateReleaseFunc
	verdict, err := gate.Apply(context.Background(), msg, &releases)
	require.NoError(t, err)
	return applied{verdict: verdict, class: msg.GetClassification(), releases: releases}
}

func (a applied) release() { pipeline.ReleaseGateReleases(a.releases) }

func TestQuotaGateConcurrencyBlocking(t *testing.T) {
	for _, b := range quotaBackends {
		t.Run(b.name, func(t *testing.T) {
			gate := flowcontrol.NewQuotaGate(b.open(t, 10*time.Second), "userid", flowcontrol.QuotaModeConcurrency, 2, 10*time.Second, "quota:")
			user1 := map[string]string{"userid": "user1"}

			first := apply(t, gate, user1)
			second := apply(t, gate, user1)
			for _, a := range []applied{first, second} {
				assert.Equal(t, pipeline.ActionContinue, a.verdict.Action)
				assert.Equal(t, api.ClassificationReserved, a.class)
				assert.Len(t, a.releases, 1)
			}

			third := apply(t, gate, user1)
			assert.Equal(t, pipeline.ActionRefuse, third.verdict.Action)
			assert.Equal(t, api.ClassificationOverflow, third.class)
			assert.Empty(t, third.releases, "a refused request holds nothing to release")

			other := apply(t, gate, map[string]string{"userid": "user2"})
			assert.Equal(t, pipeline.ActionContinue, other.verdict.Action, "each attribute value has its own quota")

			first.release()
			assert.Eventually(t, func() bool {
				a := apply(t, gate, user1)
				if a.verdict.Action == pipeline.ActionContinue {
					a.release()
					return true
				}
				return false
			}, 5*time.Second, 20*time.Millisecond, "a released slot is available again")
			second.release()
			other.release()
		})
	}
}

func TestQuotaGateRateLimitBlocking(t *testing.T) {
	for _, b := range quotaBackends {
		t.Run(b.name, func(t *testing.T) {
			gate := flowcontrol.NewQuotaGate(b.open(t, 0), "userid", flowcontrol.QuotaModeRateLimit, 2, time.Second, "quota:")
			user1 := map[string]string{"userid": "user1"}
			for range 2 {
				a := apply(t, gate, user1)
				assert.Equal(t, pipeline.ActionContinue, a.verdict.Action)
				assert.Equal(t, api.ClassificationReserved, a.class)
				assert.Empty(t, a.releases, "a rate admission has nothing to give back")
			}
			a := apply(t, gate, user1)
			assert.Equal(t, pipeline.ActionRefuse, a.verdict.Action)
			assert.Equal(t, api.ClassificationOverflow, a.class)

			time.Sleep(1100 * time.Millisecond)
			a = apply(t, gate, user1)
			assert.Equal(t, pipeline.ActionContinue, a.verdict.Action, "the window has passed")
		})
	}
}

func TestQuotaGateClassifyingLetsOverflowThrough(t *testing.T) {
	for _, b := range quotaBackends {
		t.Run(b.name, func(t *testing.T) {
			store := b.open(t, 10*time.Second)
			rate := flowcontrol.NewQuotaGate(store, "userid", flowcontrol.QuotaModeRateLimit, 1, 10*time.Second, "rate:").
				WithGatingMode(flowcontrol.GatingModeClassifying)
			user1 := map[string]string{"userid": "user1"}
			assert.Equal(t, api.ClassificationReserved, apply(t, rate, user1).class)
			a := apply(t, rate, user1)
			assert.Equal(t, pipeline.ActionContinue, a.verdict.Action)
			assert.Equal(t, api.ClassificationOverflow, a.class)

			slots := flowcontrol.NewQuotaGate(store, "userid", flowcontrol.QuotaModeConcurrency, 1, 10*time.Second, "slots:").
				WithGatingMode(flowcontrol.GatingModeClassifying)
			held := apply(t, slots, user1)
			assert.Equal(t, api.ClassificationReserved, held.class)
			a = apply(t, slots, user1)
			assert.Equal(t, pipeline.ActionContinue, a.verdict.Action)
			assert.Equal(t, api.ClassificationOverflow, a.class)
			assert.Empty(t, a.releases, "an overflow request took no slot")
			held.release()
		})
	}
}

func TestQuotaGateIgnoresRequestsWithoutTheAttribute(t *testing.T) {
	for _, b := range quotaBackends {
		t.Run(b.name, func(t *testing.T) {
			gate := flowcontrol.NewQuotaGate(b.open(t, 0), "userid", flowcontrol.QuotaModeRateLimit, 1, time.Second, "quota:")
			for range 3 {
				a := apply(t, gate, map[string]string{"teamid": "team1"})
				assert.Equal(t, pipeline.ActionContinue, a.verdict.Action)
				assert.Equal(t, api.ClassificationNone, a.class)
			}
		})
	}
}

func TestQuotaGatePrefixesSeparateCounters(t *testing.T) {
	for _, b := range quotaBackends {
		t.Run(b.name, func(t *testing.T) {
			store := b.open(t, 0)
			a := flowcontrol.NewQuotaGate(store, "userid", flowcontrol.QuotaModeRateLimit, 1, time.Minute, "a:")
			bGate := flowcontrol.NewQuotaGate(store, "userid", flowcontrol.QuotaModeRateLimit, 1, time.Minute, "b:")
			user1 := map[string]string{"userid": "user1"}
			assert.Equal(t, pipeline.ActionContinue, apply(t, a, user1).verdict.Action)
			assert.Equal(t, pipeline.ActionContinue, apply(t, bGate, user1).verdict.Action)
			assert.Equal(t, pipeline.ActionRefuse, apply(t, a, user1).verdict.Action)
		})
	}
}

func TestQuotaGateReportsStoreErrors(t *testing.T) {
	store := openSQLStore(t)
	q := sqlflow.NewQuotaStore(store, 30*time.Second, time.Second, logr.Discard())
	t.Cleanup(q.Close)
	require.NoError(t, store.Close())
	gate := flowcontrol.NewQuotaGate(q, "userid", flowcontrol.QuotaModeRateLimit, 1, time.Second, "quota:")
	msg := api.NewInternalRequest(api.InternalRouting{}, &api.RequestMessage{Metadata: map[string]string{"userid": "u"}})
	_, err := gate.Apply(context.Background(), msg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quota:userid:u")
}

func TestGateFactoryQuotaParams(t *testing.T) {
	s := miniredis.RunT(t)
	for _, tc := range []struct {
		name   string
		params map[string]any
		errMsg string
	}{
		{"missing limit", map[string]any{}, "positive 'limit'"},
		{"zero limit", map[string]any{"limit": 0}, "positive 'limit'"},
		{"unknown mode", map[string]any{"limit": 1, "mode": "burst"}, "mode must be"},
		{"unknown gating mode", map[string]any{"limit": 1, "gating_mode": "maybe"}, "gating_mode must be"},
		{"zero rate window", map[string]any{"limit": 1, "window": "0s"}, "positive 'window'"},
		{"bad window", map[string]any{"limit": 1, "window": "soon"}, "valid 'window'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]any{"address": s.Addr()}
			for k, v := range tc.params {
				params[k] = v
			}
			_, err := flowcontrol.NewGateFactory("").CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: params})
			require.ErrorContains(t, err, tc.errMsg)
		})
	}

	_, err := flowcontrol.NewGateFactory("").CreateGate(pipeline.GateConfig{GateType: "redis-quota", GateParams: map[string]any{
		"address": s.Addr(), "limit": 1, "mode": "concurrency", "window": "0s",
	}})
	require.NoError(t, err, "concurrency mode does not need a window")
}

func TestGateFactorySQLQuota(t *testing.T) {
	_, err := flowcontrol.NewGateFactory("").CreateGate(pipeline.GateConfig{GateType: "sql-quota", GateParams: map[string]any{"limit": 1}})
	require.ErrorContains(t, err, "requires the sql transport")

	store := openSQLStore(t)
	q := sqlflow.NewQuotaStore(store, 30*time.Second, time.Second, logr.Discard())
	t.Cleanup(q.Close)
	factory := flowcontrol.NewGateFactory("").WithSQLQuota(q)
	_, err = factory.CreateGate(pipeline.GateConfig{GateType: "sql-quota", GateParams: map[string]any{"mode": "burst", "limit": 1}})
	require.ErrorContains(t, err, "sql-quota gate: mode must be")

	gate, err := factory.CreateGate(pipeline.GateConfig{GateType: "sql-quota", GateParams: map[string]any{
		"limit": 1, "mode": "concurrency", "gating_mode": "classifying",
	}})
	require.NoError(t, err)
	user := map[string]string{"userid": "u"}
	held := apply(t, gate, user)
	assert.Equal(t, api.ClassificationReserved, held.class)
	a := apply(t, gate, user)
	assert.Equal(t, pipeline.ActionContinue, a.verdict.Action, "classifying mode from params")
	assert.Equal(t, api.ClassificationOverflow, a.class)
	held.release()
}
