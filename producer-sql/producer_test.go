package producersql

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newProducer(t *testing.T) (*Producer, *sqlqueue.Store) {
	t.Helper()
	pgURL := os.Getenv("TEST_POSTGRES_URL")
	if pgURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	store, err := sqlqueue.Open(ctx, pgURL, sqlqueue.WithMigrate())
	require.NoError(t, err)
	require.NoError(t, store.TruncateForTest(ctx))
	t.Cleanup(func() { _ = store.Close() })
	p, err := New(ctx, Config{RequestQueueName: "requests", ResultQueueName: "results", PollInterval: 10 * time.Millisecond}, WithStore(store))
	require.NoError(t, err)
	return p, store
}

func message(id string) *api.RequestMessage {
	return &api.RequestMessage{ID: id, Created: time.Now().Unix(), Deadline: time.Now().Add(time.Hour).Unix(), Payload: testPayload(map[string]any{"prompt": id})}
}

func decodeRow(t *testing.T, row sqlqueue.Request) *api.InternalRequest {
	t.Helper()
	var ir api.InternalRequest
	require.NoError(t, json.Unmarshal([]byte(row.Envelope), &ir))
	require.NoError(t, api.AttachPayload(&ir, row.Payload))
	return &ir
}

func dispatchAll(t *testing.T, store *sqlqueue.Store, queue string) (*sqlqueue.Consumer, []sqlqueue.Request) {
	t.Helper()
	ctx := context.Background()
	c := sqlqueue.NewConsumer(store, queue, "dispatcher", time.Minute, time.Minute)
	require.NoError(t, c.Rebalance(ctx, time.Now()))
	rows, err := c.Poll(ctx, time.Now(), 1000)
	require.NoError(t, err)
	return c, rows
}

func TestSubmitRequestsIsAllOrNothing(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	expired := message("expired")
	expired.Deadline = time.Now().Add(-time.Minute).Unix()
	notJSON := message("not-json")
	notJSON.Payload = json.RawMessage("not json")
	truncated := message("truncated")
	truncated.Payload = json.RawMessage(`{"model":"m"`)
	emptyBytes := message("empty-bytes")
	emptyBytes.Payload = json.RawMessage{}
	for name, bad := range map[string]api.Request{
		"nil":                 nil,
		"no id":               message(""),
		"expired":             expired,
		"payload not json":    notJSON,
		"truncated payload":   truncated,
		"empty payload bytes": emptyBytes,
	} {
		err := p.SubmitRequests(ctx, []api.Request{message("ok-1"), bad, message("ok-2")})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "request 1", name)
	}
	has, err := store.HasRequests(ctx, "requests")
	require.NoError(t, err)
	assert.False(t, has, "a batch with an invalid request enqueues nothing")

	require.NoError(t, p.SubmitRequests(ctx, []api.Request{message("dup"), message("dup")}),
		"the same ID twice gets two generations")
	require.NoError(t, p.SubmitRequests(ctx, nil))
}

func TestSubmitRequestsStampsRoutingAndTokens(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	override := &api.RedisRequest{RequestMessage: *message("override"), RequestQueueName: "other", ResultQueueName: "elsewhere"}
	require.NoError(t, p.SubmitRequests(ctx, []api.Request{message("a"), message("b"), override}))

	_, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 2)
	tokens := map[string]bool{}
	for _, row := range rows {
		assert.NotContains(t, row.Envelope, `"prompt"`, "the envelope does not carry the payload")
		assert.JSONEq(t, `{"prompt":"`+row.ID+`"}`, string(row.Payload))
		ir := decodeRow(t, row)
		assert.Equal(t, row.ID, ir.PublicRequest.ReqID())
		assert.JSONEq(t, `{"prompt":"`+row.ID+`"}`, string(ir.PublicRequest.ReqPayload()))
		assert.Equal(t, "requests", ir.RequestQueueName)
		assert.Equal(t, "results", ir.ResultQueueName)
		assert.Equal(t, row.Token, ir.RequestToken)
		assert.Len(t, row.Token, 32)
		tokens[row.Token] = true
	}
	assert.Len(t, tokens, 2, "every request gets its own token")

	_, rows = dispatchAll(t, store, "other")
	require.Len(t, rows, 1)
	ir := decodeRow(t, rows[0])
	assert.Equal(t, "elsewhere", ir.ResultQueueName, "per-message queue overrides are kept")
}

func TestSubmitRequestsKeepsTheModel(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	plain := message("plain")
	plain.Model = "m-plain"
	other := &api.PubSubRequest{RequestMessage: *message("other")}
	other.Model = "m-other"
	require.NoError(t, p.SubmitRequests(ctx, []api.Request{plain, other}))

	_, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 2)
	models := map[string]string{}
	for _, row := range rows {
		ir := decodeRow(t, row)
		models[row.ID] = ir.PublicRequest.ReqModel()
	}
	assert.Equal(t, map[string]string{"plain": "m-plain", "other": "m-other"}, models)
}

func TestSubmitRequestsStoresAnEmptyPayload(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	empty := message("empty")
	empty.Payload = nil
	require.NoError(t, p.SubmitRequests(ctx, []api.Request{empty}))

	_, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 1)
	assert.Empty(t, rows[0].Payload)
	ir := decodeRow(t, rows[0])
	assert.Empty(t, ir.PublicRequest.ReqPayload())
}

func TestGetResultsReturnsBatchesInOrder(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	var reqs []api.Request
	for i := range 5 {
		reqs = append(reqs, message(fmt.Sprintf("r%d", i)))
	}
	require.NoError(t, p.SubmitRequests(ctx, reqs))
	c, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 5)
	var completions []sqlqueue.Completion
	for _, row := range rows {
		payload, err := json.Marshal(resultEnvelope{ResultMessage: api.ResultMessage{ID: row.ID, StatusCode: 200, Payload: `{}`}, RequestToken: row.Token})
		require.NoError(t, err)
		completions = append(completions, sqlqueue.Completion{Key: row.Key(), Attempt: row.Attempt, Route: "results", Payload: string(payload)})
	}
	acked, err := c.Ack(ctx, completions)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, true, true, true, true}, acked)

	first, err := p.GetResults(ctx, 3)
	require.NoError(t, err)
	require.Len(t, first, 3)
	rest, err := p.GetResults(ctx, 3)
	require.NoError(t, err)
	require.Len(t, rest, 2)
	got := append(first, rest...)
	for i, res := range got {
		assert.Equal(t, rows[i].ID, res.ID, "results come back in write order")
		assert.Equal(t, rows[i].Token, res.Routing.RequestToken)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = p.GetResult(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "an empty route polls until ctx ends")

	_, err = p.GetResults(ctx, 0)
	require.Error(t, err)
}

func TestGetResultsTurnsUnparsableResultsIntoErrorResults(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	require.NoError(t, p.SubmitRequests(ctx, []api.Request{message("good"), message("bad")}))
	c, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 2)
	var completions []sqlqueue.Completion
	for _, row := range rows {
		payload := `{"id":"good"}`
		if row.ID == "bad" {
			payload = `not json`
		}
		completions = append(completions, sqlqueue.Completion{Key: row.Key(), Attempt: row.Attempt, Route: "results", Payload: payload})
	}
	_, err := c.Ack(ctx, completions)
	require.NoError(t, err)

	got, err := p.GetResults(ctx, 10)
	require.NoError(t, err)
	require.Len(t, got, 2)
	byID := map[string]*api.ResultMessage{}
	for _, r := range got {
		byID[r.ID] = r
	}
	assert.Empty(t, byID["good"].ErrorCode)
	require.Contains(t, byID, "bad")
	assert.Equal(t, api.ErrCodeInferenceError, byID["bad"].ErrorCode)
	assert.Contains(t, byID["bad"].ErrorMessage, "unparsable result payload")
	assert.NotEmpty(t, byID["bad"].Routing.RequestToken)
}

func publish(t *testing.T, p *Producer, store *sqlqueue.Store, ids ...string) []sqlqueue.Request {
	t.Helper()
	ctx := context.Background()
	var reqs []api.Request
	for _, id := range ids {
		reqs = append(reqs, message(id))
	}
	require.NoError(t, p.SubmitRequests(ctx, reqs))
	c, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, len(ids))
	var completions []sqlqueue.Completion
	for _, row := range rows {
		payload, err := json.Marshal(resultEnvelope{ResultMessage: api.ResultMessage{ID: row.ID, StatusCode: 200, Payload: `{}`}, RequestToken: row.Token})
		require.NoError(t, err)
		completions = append(completions, sqlqueue.Completion{Key: row.Key(), Attempt: row.Attempt, Route: "results", Payload: string(payload)})
	}
	_, err := c.Ack(ctx, completions)
	require.NoError(t, err)
	return rows
}

func withLease(t *testing.T, store *sqlqueue.Store, lease time.Duration) *Producer {
	t.Helper()
	p, err := New(context.Background(), Config{RequestQueueName: "requests", ResultQueueName: "results", PollInterval: 10 * time.Millisecond, ResultLeaseTTL: lease}, WithStore(store))
	require.NoError(t, err)
	return p
}

func TestReceiveResultsKeepResultsUntilAcknowledged(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	rows := publish(t, p, store, "r0", "r1", "r2")

	got, err := p.ReceiveResults(ctx, 10)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for i, d := range got {
		assert.Equal(t, rows[i].ID, d.Result.ID, "deliveries come back in write order")
		assert.Equal(t, rows[i].Token, d.Result.Routing.RequestToken)
	}
	depth, err := store.ResultDepth(ctx, "results")
	require.NoError(t, err)
	assert.EqualValues(t, 3, depth, "receiving deletes nothing")

	require.NoError(t, p.AckResult(ctx, got[0]))
	require.NoError(t, p.AckResult(ctx, got[0]), "a retried acknowledgement succeeds")
	depth, err = store.ResultDepth(ctx, "results")
	require.NoError(t, err)
	assert.EqualValues(t, 2, depth)

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = p.GetResult(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a destructive read cannot take leased results")
	waitCtx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = p.ReceiveResult(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "leased results are not delivered twice while the lease holds")

	for _, d := range got[1:] {
		require.NoError(t, p.AckResult(ctx, d))
	}
	depth, err = store.ResultDepth(ctx, "results")
	require.NoError(t, err)
	assert.Zero(t, depth)

	_, err = p.ReceiveResults(ctx, 0)
	require.Error(t, err)
	require.Error(t, p.AckResult(ctx, nil))
	require.Error(t, p.AckResult(ctx, &ResultDelivery{}))
}

// A receiver that dies between receiving a result and checkpointing it leaves the result
// leased; once the lease lapses a replacement receiver gets the same result, and the dead
// receiver's late acknowledgement is fenced.
func TestReceiverFailureBeforeAckRedeliversTheResult(t *testing.T) {
	_, store := newProducer(t)
	ctx := context.Background()
	crashed := withLease(t, store, 100*time.Millisecond)
	rows := publish(t, crashed, store, "r0")

	lost, err := crashed.ReceiveResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, "r0", lost.Result.ID)

	replacement := withLease(t, store, time.Minute)
	recovered, err := replacement.ReceiveResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, lost.Result, recovered.Result, "the replacement receives the result inference already produced")
	assert.Equal(t, rows[0].Token, recovered.Result.Routing.RequestToken, "the deduplication key survives redelivery")

	require.ErrorIs(t, crashed.AckResult(ctx, lost), ErrResultDeliveryOwnershipLost)
	depth, err := store.ResultDepth(ctx, "results")
	require.NoError(t, err)
	assert.EqualValues(t, 1, depth, "the fenced acknowledgement left the result for its new owner")

	require.NoError(t, replacement.AckResult(ctx, recovered))
	waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, err = replacement.ReceiveResult(waitCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "an acknowledged result is never delivered again")
	require.NoError(t, crashed.AckResult(ctx, lost), "after the new owner acknowledges, the result is accounted for")
}

func TestReceiveResultsTurnsUnparsableResultsIntoErrorResults(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	require.NoError(t, p.SubmitRequests(ctx, []api.Request{message("bad")}))
	c, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 1)
	_, err := c.Ack(ctx, []sqlqueue.Completion{{Key: rows[0].Key(), Attempt: rows[0].Attempt, Route: "results", Payload: `not json`}})
	require.NoError(t, err)

	d, err := p.ReceiveResult(ctx)
	require.NoError(t, err)
	assert.Equal(t, "bad", d.Result.ID)
	assert.Equal(t, rows[0].Token, d.Result.Routing.RequestToken)
	assert.Equal(t, api.ErrCodeInferenceError, d.Result.ErrorCode)
	require.NoError(t, p.AckResult(ctx, d), "an unparsable result is acknowledged like any other")
}

func TestResultLeaseTTLDefaultsToFiveMinutes(t *testing.T) {
	p, store := newProducer(t)
	assert.Equal(t, 5*time.Minute, p.resultLeaseTTL)
	assert.Equal(t, time.Second, withLease(t, store, time.Second).resultLeaseTTL)
}

func TestCancelRequestsFlagsQueuedRequests(t *testing.T) {
	p, store := newProducer(t)
	ctx := context.Background()
	require.NoError(t, p.SubmitRequests(ctx, []api.Request{message("keep"), message("drop")}))
	require.NoError(t, p.CancelRequests(ctx, []string{"drop", "never-submitted"}))
	require.NoError(t, p.CancelRequests(ctx, nil))
	_, rows := dispatchAll(t, store, "requests")
	require.Len(t, rows, 2)
	for _, row := range rows {
		assert.Equal(t, row.ID == "drop", row.Cancelled, row.ID)
	}
}

func testPayload(m map[string]any) json.RawMessage {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return b
}
