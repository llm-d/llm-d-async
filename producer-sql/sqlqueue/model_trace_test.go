package sqlqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The trace is replayed against the DispatchToken model in llm-d-async-formal:
// every step must be enabled there and every recorded state must match.
var traceDDL = []string{
	`CREATE TABLE trace_events (
	seq          BIGSERIAL PRIMARY KEY,
	tbl          TEXT NOT NULL,
	op           TEXT NOT NULL,
	id           TEXT,
	old_epoch    BIGINT,
	old_attempt  BIGINT,
	new_epoch    BIGINT,
	new_attempt  BIGINT,
	partition_id INTEGER,
	old_owner    TEXT,
	new_owner    TEXT,
	new_pepoch   BIGINT,
	old_draining SMALLINT,
	new_draining SMALLINT
)`,
	`CREATE FUNCTION trace_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF TG_OP = 'DELETE' THEN
		INSERT INTO trace_events (tbl, op, id, old_epoch, old_attempt)
		VALUES ('r', 'delete', OLD.id, OLD.dispatch_epoch, OLD.dispatch_attempt);
		RETURN OLD;
	END IF;
	IF OLD.dispatch_epoch IS DISTINCT FROM NEW.dispatch_epoch OR OLD.dispatch_attempt IS DISTINCT FROM NEW.dispatch_attempt THEN
		INSERT INTO trace_events (tbl, op, id, old_epoch, old_attempt, new_epoch, new_attempt)
		VALUES ('r', 'update', NEW.id, OLD.dispatch_epoch, OLD.dispatch_attempt, NEW.dispatch_epoch, NEW.dispatch_attempt);
	END IF;
	RETURN NEW;
END $$`,
	`CREATE FUNCTION trace_partition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF OLD.owner IS DISTINCT FROM NEW.owner OR OLD.epoch IS DISTINCT FROM NEW.epoch OR OLD.draining IS DISTINCT FROM NEW.draining THEN
		INSERT INTO trace_events (tbl, op, partition_id, old_owner, new_owner, new_pepoch, old_draining, new_draining)
		VALUES ('p', 'update', NEW.partition_id, OLD.owner, NEW.owner, NEW.epoch, OLD.draining, NEW.draining);
	END IF;
	RETURN NEW;
END $$`,
	`CREATE TRIGGER trace_request AFTER UPDATE OR DELETE ON async_requests FOR EACH ROW EXECUTE FUNCTION trace_request()`,
	`CREATE TRIGGER trace_partition AFTER UPDATE ON async_partitions FOR EACH ROW EXECUTE FUNCTION trace_partition()`,
}

var traceTeardown = []string{
	`DROP TRIGGER IF EXISTS trace_request ON async_requests`,
	`DROP TRIGGER IF EXISTS trace_partition ON async_partitions`,
	`DROP FUNCTION IF EXISTS trace_request()`,
	`DROP FUNCTION IF EXISTS trace_partition()`,
	`DROP TABLE IF EXISTS trace_events`,
}

const (
	traceNodes    = 3
	traceKeys     = 8
	traceMaxEpoch = 127
	traceMaxAtt   = 255
)

type traceEvent struct {
	tbl, op, id              string
	oldEpoch, oldAttempt     int64
	newEpoch, newAttempt     int64
	partition                int
	oldOwner, newOwner       string
	newPEpoch                int64
	oldDraining, newDraining int
}

type traceStep struct {
	Act  string  `json:"act"`
	Args []int64 `json:"args"`
}

type traceState struct {
	Pending   []int      `json:"pending"`
	Stamped   [][3]int64 `json:"stamped"`
	Result    []int      `json:"result"`
	Owner     [][2]int   `json:"owner"`
	PEpoch    [][2]int64 `json:"pEpoch"`
	Draining  []int      `json:"draining"`
	Alive     []int      `json:"alive"`
	Inflight  [][3]int64 `json:"inflight"`
	Orphan    [][3]int64 `json:"orphan"`
	Reconcile []int      `json:"reconcile"`
	Att       [][4]int64 `json:"att"`
	Working   []int64    `json:"working"`
	Written   []int64    `json:"written"`
	Failed    []int64    `json:"failed"`
}

type traceLine struct {
	Op    string      `json:"op"`
	Steps []traceStep `json:"steps"`
	State traceState  `json:"state"`
}

type attemptInfo struct {
	node      int
	key       int
	epoch     int64
	partition int
}

type traceWorker struct {
	node    int
	attempt int64
}

// traceSettle is an outcome whose store call ran but whose Consumer bookkeeping has not.
type traceSettle struct {
	traceWorker
	failed bool
}

type tracer struct {
	t        *testing.T
	s        *Store
	rng      *rand.Rand
	now      time.Time
	keys     []string
	keyIndex map[string]int
	owners   []string
	nodes    []*Consumer
	alive    []bool
	attempts map[int64]int64
	info     []attemptInfo
	working  []traceWorker
	settling []traceSettle
	lastSeq  int64
	out      *json.Encoder
}

func TestModelTrace(t *testing.T) {
	dir := os.Getenv("SQLQUEUE_TRACE_DIR")
	if dir == "" {
		t.Skip("SQLQUEUE_TRACE_DIR not set")
	}
	seeds, err := strconv.Atoi(os.Getenv("SQLQUEUE_TRACE_SEEDS"))
	if err != nil || seeds <= 0 {
		seeds = 20
	}
	for seed := range seeds {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			withStore(t, func(t *testing.T, s *Store) {
				recordTrace(t, s, uint64(seed), filepath.Join(dir, fmt.Sprintf("seed%03d.jsonl", seed)))
			})
		})
	}
}

func recordTrace(t *testing.T, s *Store, seed uint64, path string) {
	ctx := context.Background()
	execAll(t, s, traceTeardown)
	execAll(t, s, traceDDL)
	t.Cleanup(func() { execAll(t, s, traceTeardown) })

	f, err := os.Create(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	tr := &tracer{
		t: t, s: s, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), now: time.Now(),
		keyIndex: map[string]int{}, attempts: map[int64]int64{}, out: json.NewEncoder(f),
	}
	for i := range traceKeys {
		id := idInPartition(t, fmt.Sprintf("s%d-k%d-", seed, i), []int{3, 7}[i%2])
		tr.keys = append(tr.keys, id)
		tr.keyIndex[id] = i
	}
	for n := range traceNodes {
		tr.owners = append(tr.owners, fmt.Sprintf("n%d-%d", n, seed))
		tr.alive = append(tr.alive, true)
	}
	tr.nodes = []*Consumer{newConsumer(s, tr.owners[0]), newConsumer(s, tr.owners[1]), nil}

	var reqs []Request
	partOf := make([]int, traceKeys)
	for i, id := range tr.keys {
		reqs = append(reqs, req(id, tr.now.Unix()+3600))
		partOf[i] = partitionOf(id)
	}
	require.NoError(t, s.Enqueue(ctx, reqs...))
	require.NoError(t, s.EnsurePartitions(ctx, "q"))
	require.NoError(t, tr.out.Encode(map[string]any{"nodes": traceNodes, "keys": traceKeys, "partOf": partOf}))
	tr.record("init", nil)

	for range 80 {
		tr.step()
		if len(tr.attempts) >= traceMaxAtt-8 {
			break
		}
	}
}

func execAll(t *testing.T, s *Store, stmts []string) {
	t.Helper()
	for _, stmt := range stmts {
		_, err := s.db.ExecContext(context.Background(), stmt)
		require.NoError(t, err, stmt)
	}
}

func (tr *tracer) liveNodes() []int {
	var out []int
	for n, c := range tr.nodes {
		if c != nil && tr.alive[n] {
			out = append(out, n)
		}
	}
	return out
}

func (tr *tracer) step() {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	live := tr.liveNodes()
	n := live[tr.rng.IntN(len(live))]
	c := tr.nodes[n]
	var mine []int
	for i, w := range tr.working {
		if w.node == n {
			mine = append(mine, i)
		}
	}
	roll := tr.rng.IntN(100)
	switch {
	case roll < 25:
		tr.rebalance(n)
	case roll < 45:
		rows, err := c.Poll(ctx, tr.now, 1+tr.rng.IntN(4))
		require.NoError(tr.t, err)
		tr.poll(n, rows)
	case roll < 48:
		_, err := c.Poll(cancelled, tr.now, 4)
		require.Error(tr.t, err)
		tr.quiet()
		tr.record(fmt.Sprintf("pollFailed n%d", n), []traceStep{{"pollFailed", []int64{int64(n)}}})
	case roll < 51:
		rows, err := c.store.Dispatch(ctx, "q", c.owner, tr.now, 1+tr.rng.IntN(4))
		require.NoError(tr.t, err)
		_, err = c.Poll(cancelled, tr.now, 4)
		require.Error(tr.t, err)
		var steps []traceStep
		for _, ev := range tr.events() {
			require.Equal(tr.t, "r", ev.tbl, "Dispatch changed a partition")
			steps = append(steps, traceStep{"dispatchLost", []int64{int64(n), int64(tr.keyIndex[ev.id]), tr.newAttempt(ev, n)}})
		}
		require.Len(tr.t, steps, len(rows))
		if len(steps) == 0 {
			steps = []traceStep{{"pollFailed", []int64{int64(n)}}}
		}
		tr.record(fmt.Sprintf("pollLost n%d", n), steps)
	case roll < 54:
		_, err := c.store.AcquirePartitions(ctx, "q", c.owner, tr.now, leaseTTL, 1+tr.rng.IntN(Partitions))
		require.NoError(tr.t, err)
		var steps []traceStep
		for _, ev := range tr.events() {
			require.Equal(tr.t, "p", ev.tbl, "AcquirePartitions changed a request")
			steps = append(steps, traceStep{"acquire", []int64{int64(n), int64(ev.partition), ev.newPEpoch}})
		}
		tr.record(fmt.Sprintf("acquireOnly n%d", n), steps)
	case roll < 60:
		tr.now = tr.now.Add([]time.Duration{time.Second, leaseTTL + time.Second, drainTimeout + time.Second}[tr.rng.IntN(3)])
		tr.quiet()
		tr.record("tick", nil)
	case roll < 62 && tr.nodes[2] == nil:
		tr.alive[n] = false
		tr.nodes[n] = nil
		tr.nodes[2] = newConsumer(tr.s, tr.owners[2])
		tr.quiet()
		tr.record(fmt.Sprintf("crash n%d", n), []traceStep{{Act: "crash", Args: []int64{int64(n)}}})
	case roll < 64:
		c.Stop()
		tr.quiet()
		tr.record(fmt.Sprintf("stop n%d", n), nil)
	case roll < 72 && len(tr.settleable()) > 0:
		ready := tr.settleable()
		i := ready[tr.rng.IntN(len(ready))]
		pending := tr.settling[i]
		tr.settling = slices.Delete(tr.settling, i, i+1)
		a := tr.attempts[pending.attempt]
		st := tr.stamp(pending.attempt)
		if pending.failed {
			tr.nodes[pending.node].Abandon(st)
			tr.quiet()
			tr.record(fmt.Sprintf("abandon n%d a%d", pending.node, a), []traceStep{{"abandon", []int64{a}}})
		} else {
			tr.nodes[pending.node].doneStamps([]Stamp{st})
			tr.quiet()
			tr.record(fmt.Sprintf("done n%d a%d", pending.node, a), []traceStep{{"done", []int64{a}}})
		}
	case len(mine) > 0:
		i := mine[tr.rng.IntN(len(mine))]
		w := tr.working[i]
		tr.working = slices.Delete(tr.working, i, i+1)
		a := tr.attempts[w.attempt]
		st := tr.stamp(w.attempt)
		comp := []Completion{{Key: st.Key, Attempt: st.Attempt, Route: "r", Payload: "{}"}}
		fail := tr.rng.IntN(5) == 0
		defer tr.workerEvents()
		op := []string{"ack", "retry", "undispatch"}[tr.rng.IntN(3)]
		commit, lost := map[string]string{"ack": "ackCommit", "retry": "requeueCommit", "undispatch": "requeueCommit"}[op],
			map[string]string{"ack": "ackError", "retry": "requeueError", "undispatch": "requeueError"}[op]
		if tr.rng.IntN(2) == 0 {
			var err error
			use := ctx
			if fail {
				use = cancelled
			}
			switch op {
			case "ack":
				_, err = c.store.Ack(use, c.owner, comp)
			case "retry":
				_, err = c.store.Retry(use, c.owner, st, tr.now.Unix(), `{}`)
			default:
				err = c.store.Undispatch(use, c.owner, []Stamp{st})
			}
			tr.settling = append(tr.settling, traceSettle{traceWorker: w, failed: fail})
			if fail {
				require.Error(tr.t, err)
				tr.record(fmt.Sprintf("%sStoreFailed n%d a%d", op, n, a), []traceStep{{lost, []int64{a, 0}}})
			} else {
				require.NoError(tr.t, err)
				tr.record(fmt.Sprintf("%sStore n%d a%d", op, n, a), []traceStep{{commit, []int64{a}}})
			}
			return
		}
		switch {
		case op == "ack" && !fail:
			_, err := c.Ack(ctx, comp)
			require.NoError(tr.t, err)
		case op == "ack":
			_, err := c.Ack(cancelled, comp)
			require.Error(tr.t, err)
			c.Abandon(st)
		case op == "retry" && !fail:
			_, err := c.Retry(ctx, st, tr.now.Unix(), `{}`)
			require.NoError(tr.t, err)
		case op == "retry":
			_, err := c.Retry(cancelled, st, tr.now.Unix(), `{}`)
			require.Error(tr.t, err)
			c.Abandon(st)
		case !fail:
			require.NoError(tr.t, c.Undispatch(ctx, []Stamp{st}))
		default:
			require.Error(tr.t, c.Undispatch(cancelled, []Stamp{st}))
		}
		if fail {
			tr.record(fmt.Sprintf("%sFailed n%d a%d", op, n, a), []traceStep{{lost, []int64{a, 0}}, {"abandon", []int64{a}}})
		} else {
			tr.record(fmt.Sprintf("%s n%d a%d", op, n, a), []traceStep{{commit, []int64{a}}, {"done", []int64{a}}})
		}
	default:
		tr.rebalance(n)
	}
}

func (tr *tracer) stamp(attempt int64) Stamp {
	return Stamp{Key: Key{ID: tr.keys[tr.info[tr.attempts[attempt]].key], Token: "t"}, Attempt: attempt}
}

func (tr *tracer) settleable() []int {
	var out []int
	for i, pending := range tr.settling {
		if tr.alive[pending.node] && tr.nodes[pending.node] != nil {
			out = append(out, i)
		}
	}
	return out
}

func (tr *tracer) poll(n int, rows []Request) {
	var steps []traceStep
	for _, ev := range tr.events() {
		require.Equal(tr.t, "r", ev.tbl, "Poll changed a partition")
		require.Zero(tr.t, ev.oldEpoch, "Poll restamped a dispatched row")
		a := tr.newAttempt(ev, n)
		steps = append(steps, traceStep{"dispatch", []int64{int64(n), int64(tr.keyIndex[ev.id]), a}})
	}
	for _, r := range rows {
		steps = append(steps, traceStep{"book", []int64{tr.attempts[r.Attempt]}})
		tr.working = append(tr.working, traceWorker{node: n, attempt: r.Attempt})
	}
	tr.record(fmt.Sprintf("poll n%d", n), steps)
}

func (tr *tracer) rebalance(n int) {
	c := tr.nodes[n]
	c.mu.Lock()
	orphans := make(map[Key]int64, len(c.orphans))
	for k, a := range c.orphans {
		orphans[k] = a
	}
	reconcile := c.reconcile
	tracked := make(map[Key]int64, len(c.inflight))
	for k, t := range c.inflight {
		tracked[k] = t.attempt
	}
	c.mu.Unlock()
	pEpoch, owner := tr.partitions()

	require.NoError(tr.t, c.Rebalance(context.Background(), tr.now))

	var steps []traceStep
	var orphanKeys []Key
	for k := range orphans {
		orphanKeys = append(orphanKeys, k)
	}
	slices.SortFunc(orphanKeys, func(a, b Key) int { return tr.keyIndex[a.ID] - tr.keyIndex[b.ID] })
	for _, k := range orphanKeys {
		steps = append(steps, traceStep{"undispatchOrphan", []int64{int64(n), int64(tr.keyIndex[k.ID]), tr.attempts[orphans[k]]}})
	}
	if reconcile {
		steps = append(steps, traceStep{"reconcileInFlight", []int64{int64(n)}})
	}
	for _, ev := range tr.events() {
		switch ev.tbl {
		case "r":
			k := Key{ID: ev.id, Token: "t"}
			require.Equal(tr.t, "update", ev.op, "Rebalance deleted a row")
			require.Zero(tr.t, ev.newEpoch, "Rebalance dispatched a row")
			part := partitionOf(ev.id)
			if a, ok := orphans[k]; ok && a == ev.oldAttempt {
				continue
			}
			if reconcile && owner[part] == tr.owners[n] && ev.oldEpoch == pEpoch[part] && tracked[k] != ev.oldAttempt {
				continue
			}
			steps = append(steps, traceStep{"resetStale", []int64{int64(n), int64(tr.keyIndex[ev.id]), ev.oldEpoch, tr.attempts[ev.oldAttempt]}})
		case "p":
			switch {
			case ev.newOwner == tr.owners[n] && ev.newPEpoch != pEpoch[ev.partition]:
				steps = append(steps, traceStep{"acquire", []int64{int64(n), int64(ev.partition), ev.newPEpoch}})
			case ev.oldOwner == tr.owners[n] && ev.newOwner == "":
				steps = append(steps, traceStep{"release", []int64{int64(n), int64(ev.partition)}})
			case ev.oldOwner == tr.owners[n] && ev.newOwner == tr.owners[n] && ev.oldDraining != ev.newDraining:
				steps = append(steps, traceStep{"setDraining", []int64{int64(n), int64(ev.partition), int64(ev.newDraining)}})
			default:
				tr.t.Fatalf("unclassified partition event %+v by %s", ev, tr.owners[n])
			}
			pEpoch[ev.partition], owner[ev.partition] = ev.newPEpoch, ev.newOwner
		}
	}
	tr.record(fmt.Sprintf("rebalance n%d", n), steps)
}

func (tr *tracer) newAttempt(ev traceEvent, n int) int64 {
	a := int64(len(tr.info))
	require.Less(tr.t, a, int64(traceMaxAtt), "too many attempts for the model's attempt sort")
	require.LessOrEqual(tr.t, ev.newEpoch, int64(traceMaxEpoch), "epoch beyond the model's epoch sort")
	tr.attempts[ev.newAttempt] = a
	tr.info = append(tr.info, attemptInfo{node: n, key: tr.keyIndex[ev.id], epoch: ev.newEpoch, partition: partitionOf(ev.id)})
	return a
}

func (tr *tracer) events() []traceEvent {
	rows, err := tr.s.db.QueryContext(context.Background(), `
		SELECT seq, tbl, op, COALESCE(id, ''), COALESCE(old_epoch, 0), COALESCE(old_attempt, 0),
			COALESCE(new_epoch, 0), COALESCE(new_attempt, 0), COALESCE(partition_id, 0),
			COALESCE(old_owner, ''), COALESCE(new_owner, ''), COALESCE(new_pepoch, 0),
			COALESCE(old_draining, 0), COALESCE(new_draining, 0)
		FROM trace_events WHERE seq > $1 ORDER BY seq`, tr.lastSeq)
	require.NoError(tr.t, err)
	defer func() { _ = rows.Close() }()
	var out []traceEvent
	for rows.Next() {
		var ev traceEvent
		require.NoError(tr.t, rows.Scan(&tr.lastSeq, &ev.tbl, &ev.op, &ev.id, &ev.oldEpoch, &ev.oldAttempt,
			&ev.newEpoch, &ev.newAttempt, &ev.partition, &ev.oldOwner, &ev.newOwner, &ev.newPEpoch,
			&ev.oldDraining, &ev.newDraining))
		out = append(out, ev)
	}
	require.NoError(tr.t, rows.Err())
	return out
}

func (tr *tracer) partitions() (map[int]int64, map[int]string) {
	rows, err := tr.s.db.QueryContext(context.Background(), `SELECT partition_id, epoch, owner FROM async_partitions WHERE queue = 'q'`)
	require.NoError(tr.t, err)
	defer func() { _ = rows.Close() }()
	epochs, owners := map[int]int64{}, map[int]string{}
	for rows.Next() {
		var p int
		var e int64
		var o string
		require.NoError(tr.t, rows.Scan(&p, &e, &o))
		epochs[p], owners[p] = e, o
	}
	require.NoError(tr.t, rows.Err())
	return epochs, owners
}

func (tr *tracer) quiet() {
	require.Empty(tr.t, tr.events(), "an operation with no store effect wrote to the store")
}

// workerEvents checks that an outcome write only deleted its row or returned it to pending.
func (tr *tracer) workerEvents() {
	for _, ev := range tr.events() {
		require.Equal(tr.t, "r", ev.tbl, "an outcome write changed a partition")
		require.True(tr.t, ev.op == "delete" || ev.newEpoch == 0, "an outcome write dispatched a row")
	}
}

func (tr *tracer) record(op string, steps []traceStep) {
	ctx := context.Background()
	var st traceState
	rows, err := tr.s.db.QueryContext(ctx, `SELECT id, dispatch_epoch, dispatch_attempt FROM async_requests`)
	require.NoError(tr.t, err)
	for rows.Next() {
		var id string
		var e, a int64
		require.NoError(tr.t, rows.Scan(&id, &e, &a))
		if e == 0 {
			st.Pending = append(st.Pending, tr.keyIndex[id])
		} else {
			st.Stamped = append(st.Stamped, [3]int64{int64(tr.keyIndex[id]), e, tr.attempts[a]})
		}
	}
	require.NoError(tr.t, rows.Err())
	require.NoError(tr.t, rows.Close())
	rows, err = tr.s.db.QueryContext(ctx, `SELECT DISTINCT id FROM async_results`)
	require.NoError(tr.t, err)
	for rows.Next() {
		var id string
		require.NoError(tr.t, rows.Scan(&id))
		st.Result = append(st.Result, tr.keyIndex[id])
	}
	require.NoError(tr.t, rows.Err())
	require.NoError(tr.t, rows.Close())
	rows, err = tr.s.db.QueryContext(ctx, `SELECT partition_id, owner, epoch, draining FROM async_partitions WHERE queue = 'q'`)
	require.NoError(tr.t, err)
	for rows.Next() {
		var p, d int
		var o string
		var e int64
		require.NoError(tr.t, rows.Scan(&p, &o, &e, &d))
		if n := slices.Index(tr.owners, o); n >= 0 {
			st.Owner = append(st.Owner, [2]int{p, n})
		} else {
			require.Empty(tr.t, o, "partition owned by an unknown dispatcher")
		}
		require.LessOrEqual(tr.t, e, int64(traceMaxEpoch), "epoch beyond the model's epoch sort")
		st.PEpoch = append(st.PEpoch, [2]int64{int64(p), e})
		if d == 1 {
			st.Draining = append(st.Draining, p)
		}
	}
	require.NoError(tr.t, rows.Err())
	require.NoError(tr.t, rows.Close())
	for n, c := range tr.nodes {
		if !tr.alive[n] {
			continue
		}
		st.Alive = append(st.Alive, n)
		if c == nil {
			continue
		}
		c.mu.Lock()
		for k, t := range c.inflight {
			st.Inflight = append(st.Inflight, [3]int64{int64(n), int64(tr.keyIndex[k.ID]), tr.attempts[t.attempt]})
		}
		for k, a := range c.orphans {
			st.Orphan = append(st.Orphan, [3]int64{int64(n), int64(tr.keyIndex[k.ID]), tr.attempts[a]})
		}
		if c.reconcile {
			st.Reconcile = append(st.Reconcile, n)
		}
		c.mu.Unlock()
	}
	for a, info := range tr.info {
		st.Att = append(st.Att, [4]int64{int64(a), int64(info.node), int64(info.key), info.epoch})
	}
	for _, w := range tr.working {
		st.Working = append(st.Working, tr.attempts[w.attempt])
	}
	for _, pending := range tr.settling {
		if pending.failed {
			st.Failed = append(st.Failed, tr.attempts[pending.attempt])
		} else {
			st.Written = append(st.Written, tr.attempts[pending.attempt])
		}
	}
	require.NoError(tr.t, tr.out.Encode(traceLine{Op: op, Steps: steps, State: st}))
}
