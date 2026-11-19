package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/pkg/redis"
)

type reconfigStub struct {
	mu       sync.Mutex
	attempts int
	calls    [][]redis.SortedSetQueueConfig
	result   redis.QueueReconfigureResult
	err      error
}

func (s *reconfigStub) ReconfigureQueues(queues []redis.SortedSetQueueConfig, beforeCommit func([]pipeline.RequestChannel) error) (redis.QueueReconfigureResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.err != nil {
		return s.result, s.err
	}
	if beforeCommit != nil {
		if err := beforeCommit(s.result.Added); err != nil {
			return redis.QueueReconfigureResult{}, err
		}
	}
	s.calls = append(s.calls, queues)
	return s.result, s.err
}

func (s *reconfigStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *reconfigStub) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *reconfigStub) call(index int) []redis.SortedSetQueueConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]redis.SortedSetQueueConfig(nil), s.calls[index]...)
}

type dynPolicyStub struct {
	pipeline.RequestMergePolicy
	mu    sync.Mutex
	added [][]pipeline.RequestChannel
	err   error
}

func (s *dynPolicyStub) AddRequestChannels(channels []pipeline.RequestChannel, pools map[string]pipeline.WorkerPoolConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.added = append(s.added, channels)
	return s.err
}

func (s *dynPolicyStub) addCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.added)
}

func (s *dynPolicyStub) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func writeQueuesFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write queues file: %v", err)
	}
}

func sortedSetFile(queues string) string {
	return `{"url":"redis://localhost:6379","queues":` + queues + `}`
}

func loadTestConfig(t *testing.T, path string) *redis.SortedSetConfig {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read test config: %v", err)
	}
	cfg, err := redis.LoadSortedSetConfig(data)
	if err != nil {
		t.Fatalf("load test config: %v", err)
	}
	return cfg
}

func TestQueuesConfigReload_AppliesChanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queues.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))

	flow := &reconfigStub{}
	policy := &dynPolicyStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, policy, pools, logr.Discard())

	// A changed file is applied again.
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"},{"id":"q2","queue_name":"q2","igw_base_url":"http://gw"}]`))
	waitFor(t, "second apply", func() bool { return flow.callCount() >= 1 })

	if got := len(flow.call(0)); got != 2 {
		t.Fatalf("second apply carried %d queues, want 2", got)
	}
}

func TestQueuesConfigReload_UnchangedFileIsNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queues.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))

	flow := &reconfigStub{}
	policy := &dynPolicyStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, policy, pools, logr.Discard())
	time.Sleep(100 * time.Millisecond)
	if got := flow.callCount(); got != 0 {
		t.Fatalf("unchanged file re-applied %d times, want 1", got)
	}
}

func TestQueuesConfigReload_BadContentKeepsLastGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queues.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))

	flow := &reconfigStub{}
	policy := &dynPolicyStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, policy, pools, logr.Discard())

	writeQueuesFile(t, path, `{not json`)
	time.Sleep(150 * time.Millisecond)
	if got := flow.callCount(); got != 0 {
		t.Fatalf("broken file applied, callCount=%d", got)
	}

	// Recovery: valid new content applies again.
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q3","queue_name":"q3","igw_base_url":"http://gw"}]`))
	waitFor(t, "recovery apply", func() bool { return flow.callCount() >= 1 })
	if got := flow.call(0)[0].ID; got != "q3" {
		t.Fatalf("recovery carried %q, want q3", got)
	}
}

func TestQueuesConfigReload_ReconfigureErrorKeepsFingerprint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queues.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))

	flow := &reconfigStub{err: errors.New("boom")}
	policy := &dynPolicyStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, policy, pools, logr.Discard())

	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q2","queue_name":"q2","igw_base_url":"http://gw"}]`))
	waitFor(t, "first failed apply", func() bool { return flow.attemptCount() >= 1 })
	waitFor(t, "retries because last good is unchanged", func() bool { return flow.attemptCount() >= 3 })
	if got := policy.addCount(); got != 0 {
		t.Fatalf("policy must not be extended on a failed apply, got %d calls", got)
	}
}

func TestQueuesConfigReload_PolicyErrorDoesNotCommitAndRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transport.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))
	flow := &reconfigStub{result: redis.QueueReconfigureResult{Added: []pipeline.RequestChannel{{WorkerPoolID: "default"}}}}
	policy := &dynPolicyStub{err: errors.New("policy unavailable")}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, policy, pools, logr.Discard())

	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q2","queue_name":"q2","igw_base_url":"http://gw"}]`))
	waitFor(t, "policy rejection", func() bool { return policy.addCount() >= 1 })
	if got := flow.callCount(); got != 0 {
		t.Fatalf("flow committed despite policy error: %d calls", got)
	}

	policy.setError(nil)
	waitFor(t, "recovery with unchanged file", func() bool { return flow.callCount() == 1 })
	if got := flow.call(0)[0].ID; got != "q2" {
		t.Fatalf("recovery carried %q, want q2", got)
	}
}

func TestQueuesConfigReload_RejectsNonQueueChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transport.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))
	flow := &reconfigStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, &dynPolicyStub{}, pools, logr.Discard())
	writeQueuesFile(t, path, `{"url":"redis://other","queues":[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]}`)
	time.Sleep(100 * time.Millisecond)
	if got := flow.callCount(); got != 0 {
		t.Fatalf("non-queue change was applied %d times", got)
	}
}

func TestQueuesConfigReload_UnknownPoolThenCorrection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transport.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))
	flow := &reconfigStub{}
	policy := &dynPolicyStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, policy, pools, logr.Discard())
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q2","queue_name":"q2","worker_pool_id":"ghost","igw_base_url":"http://gw"}]`))
	time.Sleep(100 * time.Millisecond)
	if got := flow.callCount(); got != 0 {
		t.Fatalf("unknown pool was applied %d times", got)
	}
	if got := policy.addCount(); got != 0 {
		t.Fatalf("policy was called before unknown-pool rejection: %d", got)
	}
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q2","queue_name":"q2","worker_pool_id":"default","igw_base_url":"http://gw"}]`))
	waitFor(t, "corrected config", func() bool { return flow.callCount() == 1 })
}

func TestQueuesConfigReload_ClearsAndReaddsQueues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transport.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))
	flow := &reconfigStub{}
	pools := map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startQueuesConfigReload(ctx, path, 10*time.Millisecond, loadTestConfig(t, path), flow, &dynPolicyStub{}, pools, logr.Discard())
	writeQueuesFile(t, path, sortedSetFile(`[]`))
	waitFor(t, "cleared queues", func() bool { return flow.callCount() == 1 })
	if got := len(flow.call(0)); got != 0 {
		t.Fatalf("clear carried %d queues, want 0", got)
	}
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q2","queue_name":"q2","igw_base_url":"http://gw"}]`))
	waitFor(t, "re-added queue", func() bool { return flow.callCount() == 2 })
	if got := flow.call(1)[0].ID; got != "q2" {
		t.Fatalf("re-add carried %q, want q2", got)
	}
}

func TestQueuesConfigReload_StopsWithContext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transport.json")
	writeQueuesFile(t, path, sortedSetFile(`[{"id":"q1","queue_name":"q1","igw_base_url":"http://gw"}]`))

	ctx, cancel := context.WithCancel(context.Background())
	done := startQueuesConfigReload(ctx, path, time.Hour, loadTestConfig(t, path), &reconfigStub{}, &dynPolicyStub{},
		map[string]pipeline.WorkerPoolConfig{"default": {ID: "default", Workers: 1}}, logr.Discard())
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("reload goroutine did not stop with its context")
	}
}
