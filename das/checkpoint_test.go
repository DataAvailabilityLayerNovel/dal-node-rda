package das

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func uint32PtrCheckpoint(v uint32) *uint32 {
	return &v
}

func TestCheckpointStore(t *testing.T) {
	ds := newCheckpointStore(sync.MutexWrap(datastore.NewMapDatastore()))
	now := time.Now().UTC().Round(time.Second)
	failed := make(map[uint64]int)
	failed[2] = 1
	failed[3] = 2
	cp := checkpoint{
		Version:     currentCheckpointVersion,
		SampleFrom:  1,
		NetworkHead: 6,
		Cursor: checkpointCursor{
			CatchupHead: 0,
		},
		Failed: failed,
		RetryState: map[uint64]retryCheckpoint{
			2: {
				Count:       1,
				After:       now,
				Reason:      "rda: query timeout",
				Retryable:   true,
				SampleIndex: uint32PtrCheckpoint(2),
			},
		},
		Workers: []workerCheckpoint{
			{
				From:    1,
				To:      2,
				JobType: retryJob,
			},
			{
				From:    5,
				To:      10,
				JobType: recentJob,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer t.Cleanup(cancel)
	assert.NoError(t, ds.store(ctx, cp))
	got, err := ds.load(ctx)
	require.NoError(t, err)
	assert.Equal(t, cp, got)
}

func TestCheckpointStore_LoadLegacyCheckpointWithoutVersion(t *testing.T) {
	ds := newCheckpointStore(sync.MutexWrap(datastore.NewMapDatastore()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer t.Cleanup(cancel)

	legacyPayload := map[string]any{
		"sample_from":  uint64(10),
		"network_head": uint64(20),
		"failed": map[string]int{
			"11": 2,
		},
		"workers": []map[string]any{
			{
				"from":     uint64(12),
				"to":       uint64(14),
				"job_type": string(catchupJob),
			},
		},
	}

	raw, err := json.Marshal(legacyPayload)
	require.NoError(t, err)
	require.NoError(t, ds.Put(ctx, checkpointKey, raw))

	cp, err := ds.load(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, currentCheckpointVersion, cp.Version)
	assert.EqualValues(t, 10, cp.SampleFrom)
	assert.EqualValues(t, 20, cp.NetworkHead)
	assert.EqualValues(t, 9, cp.Cursor.CatchupHead)
	assert.Equal(t, map[uint64]int{11: 2}, cp.Failed)
	assert.Empty(t, cp.RetryState)
	require.Len(t, cp.Workers, 1)
	assert.Equal(t, catchupJob, cp.Workers[0].JobType)
	assert.EqualValues(t, 12, cp.Workers[0].From)
	assert.EqualValues(t, 14, cp.Workers[0].To)
}

func TestCheckpointStore_LoadRetryStateWithoutFailedCount(t *testing.T) {
	ds := newCheckpointStore(sync.MutexWrap(datastore.NewMapDatastore()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer t.Cleanup(cancel)

	now := time.Now().UTC().Round(time.Second)
	rawPayload := map[string]any{
		"version":      currentCheckpointVersion,
		"sample_from":  uint64(10),
		"network_head": uint64(20),
		"retry_state": map[string]any{
			"11": map[string]any{
				"count":        3,
				"after":        now.Format(time.RFC3339Nano),
				"reason":       "rda: peer unavailable",
				"retryable":    true,
				"sample_index": 9,
			},
		},
	}

	raw, err := json.Marshal(rawPayload)
	require.NoError(t, err)
	require.NoError(t, ds.Put(ctx, checkpointKey, raw))

	cp, err := ds.load(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, currentCheckpointVersion, cp.Version)
	assert.EqualValues(t, 9, cp.Cursor.CatchupHead)
	require.Len(t, cp.RetryState, 1)
	assert.Equal(t, 3, cp.RetryState[11].Count)
	assert.WithinDuration(t, now, cp.RetryState[11].After, time.Second)
	assert.Equal(t, "rda: peer unavailable", cp.RetryState[11].Reason)
	assert.True(t, cp.RetryState[11].Retryable)
	assert.NotNil(t, cp.RetryState[11].SampleIndex)
}

func TestCheckpointStore_LoadUnsupportedVersion(t *testing.T) {
	ds := newCheckpointStore(sync.MutexWrap(datastore.NewMapDatastore()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer t.Cleanup(cancel)

	rawPayload := map[string]any{
		"version":      currentCheckpointVersion + 1,
		"sample_from":  uint64(10),
		"network_head": uint64(20),
	}

	raw, err := json.Marshal(rawPayload)
	require.NoError(t, err)
	require.NoError(t, ds.Put(ctx, checkpointKey, raw))

	_, err = ds.load(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnsupportedCheckpointVersion)
}
