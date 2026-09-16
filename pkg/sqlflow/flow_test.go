package sqlflow

import (
	"sync/atomic"
	"testing"

	"github.com/llm-d/llm-d-async/pipeline"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/stretchr/testify/assert"
)

func TestGateReleasesAreScopedToRequestAttempt(t *testing.T) {
	var first, second, redelivery atomic.Int32
	f := &Flow{}
	key1 := sqlqueue.Stamp{Key: sqlqueue.Key{ID: "same-id", Token: "generation-1"}, Epoch: 1}
	key2 := sqlqueue.Stamp{Key: sqlqueue.Key{ID: "same-id", Token: "generation-2"}, Epoch: 1}
	key3 := sqlqueue.Stamp{Key: key1.Key, Epoch: 2}

	f.trackGateReleases(key1, []pipeline.GateReleaseFunc{func() { first.Add(1) }})
	f.trackGateReleases(key2, []pipeline.GateReleaseFunc{func() { second.Add(1) }})
	f.trackGateReleases(key3, []pipeline.GateReleaseFunc{func() { redelivery.Add(1) }})
	assert.Zero(t, first.Load(), "tracking another generation must not release the first")
	assert.Zero(t, second.Load())
	assert.Zero(t, redelivery.Load(), "tracking a redelivery must not release its earlier attempt")

	f.releaseGateReleases(key1)
	assert.EqualValues(t, 1, first.Load())
	assert.Zero(t, second.Load(), "completing the first generation must not release the second")

	f.releaseGateReleases(key2)
	assert.EqualValues(t, 1, second.Load())

	f.releaseGateReleases(key3)
	assert.EqualValues(t, 1, redelivery.Load())
}
