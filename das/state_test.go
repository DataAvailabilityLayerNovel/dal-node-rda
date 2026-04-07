package das

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func uint32Ptr(v uint32) *uint32 {
	return &v
}

func TestCoordinatorStateNextJob_FairnessAvoidsCatchupStarvation(t *testing.T) {
	s := &coordinatorState{
		samplingRange:           1,
		failed:                  map[uint64]retryAttempt{100: {count: 1, after: time.Now().Add(-time.Second)}, 101: {count: 1, after: time.Now().Add(-time.Second)}, 102: {count: 1, after: time.Now().Add(-time.Second)}},
		inRetry:                 make(map[uint64]retryAttempt),
		inProgress:              make(map[int]func() workerState),
		next:                    1,
		networkHead:             5,
		maxConsecutiveRetryJobs: 2,
		catchUpDoneCh:           make(chan struct{}),
	}

	j1, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j1.jobType)

	j2, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j2.jobType)

	j3, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, catchupJob, j3.jobType)

	j4, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j4.jobType)
}

func TestCoordinatorStateNextJob_RetryNotBlockedWithoutCatchupWork(t *testing.T) {
	s := &coordinatorState{
		samplingRange:           1,
		failed:                  map[uint64]retryAttempt{50: {count: 1, after: time.Now().Add(-time.Second)}, 51: {count: 1, after: time.Now().Add(-time.Second)}},
		inRetry:                 make(map[uint64]retryAttempt),
		inProgress:              make(map[int]func() workerState),
		next:                    10,
		networkHead:             9,
		maxConsecutiveRetryJobs: 1,
		catchUpDoneCh:           make(chan struct{}),
	}

	j1, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j1.jobType)

	j2, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j2.jobType)
}

func TestCoordinatorStateNextJob_RetryWindowCapAvoidsStorm(t *testing.T) {
	s := &coordinatorState{
		samplingRange:           1,
		failed:                  map[uint64]retryAttempt{10: {count: 1, after: time.Now().Add(-time.Second), sampleIndex: uint32Ptr(1)}, 11: {count: 1, after: time.Now().Add(-time.Second), sampleIndex: uint32Ptr(2)}, 20: {count: 1, after: time.Now().Add(-time.Second), sampleIndex: uint32Ptr(40)}},
		inRetry:                 make(map[uint64]retryAttempt),
		inProgress:              make(map[int]func() workerState),
		next:                    100,
		networkHead:             99,
		maxConsecutiveRetryJobs: 10,
		retryWindowSize:         16,
		maxRetryPerWindow:       1,
		retryWindowUsage:        make(map[uint32]int),
		catchUpDoneCh:           make(chan struct{}),
	}

	j1, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j1.jobType)
	assert.EqualValues(t, 10, j1.from)

	j2, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j2.jobType)
	assert.EqualValues(t, 20, j2.from)
}

func TestCoordinatorStateNextJob_RetryWindowCapResetsWhenOnlyOneWindowRemains(t *testing.T) {
	s := &coordinatorState{
		samplingRange:           1,
		failed:                  map[uint64]retryAttempt{10: {count: 1, after: time.Now().Add(-time.Second), sampleIndex: uint32Ptr(1)}, 11: {count: 1, after: time.Now().Add(-time.Second), sampleIndex: uint32Ptr(2)}},
		inRetry:                 make(map[uint64]retryAttempt),
		inProgress:              make(map[int]func() workerState),
		next:                    100,
		networkHead:             99,
		maxConsecutiveRetryJobs: 10,
		retryWindowSize:         16,
		maxRetryPerWindow:       1,
		retryWindowUsage:        make(map[uint32]int),
		catchUpDoneCh:           make(chan struct{}),
	}

	j1, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j1.jobType)
	assert.EqualValues(t, 10, j1.from)

	j2, ok := s.nextJob()
	assert.True(t, ok)
	assert.Equal(t, retryJob, j2.jobType)
	assert.EqualValues(t, 11, j2.from)
}

func TestCoordinatorStateResumeFromCheckpoint_UsesRetryStateMetadata(t *testing.T) {
	params := DefaultParameters()
	s := newCoordinatorState(params)

	after := time.Now().Add(3 * time.Minute).UTC().Round(time.Second)
	cp := checkpoint{
		Version:     currentCheckpointVersion,
		SampleFrom:  10,
		NetworkHead: 20,
		Failed:      map[uint64]int{11: 1, 12: 9},
		RetryState: map[uint64]retryCheckpoint{
			11: {
				Count:       3,
				After:       after,
				Reason:      "rda query share_index=11: rda: query timeout",
				Retryable:   true,
				SampleIndex: uint32Ptr(11),
			},
		},
	}

	s.resumeFromCheckpoint(cp)

	assert.EqualValues(t, 10, s.next)
	assert.EqualValues(t, 20, s.networkHead)

	retry11, ok := s.failed[11]
	require.True(t, ok)
	assert.Equal(t, 3, retry11.count)
	assert.WithinDuration(t, after, retry11.after, time.Second)
	assert.Equal(t, "rda query share_index=11: rda: query timeout", retry11.reason)
	assert.True(t, retry11.retryable)
	require.NotNil(t, retry11.sampleIndex)
	assert.EqualValues(t, 11, *retry11.sampleIndex)

	// Height 12 has no retry_state entry and should fall back to legacy Failed semantics.
	retry12, ok := s.failed[12]
	require.True(t, ok)
	assert.Equal(t, 9, retry12.count)
	assert.False(t, retry12.after.IsZero())
}

func Test_coordinatorStats(t *testing.T) {
	tests := []struct {
		name  string
		state *coordinatorState
		want  SamplingStats
	}{
		{
			"basic",
			&coordinatorState{
				inProgress: map[int]func() workerState{
					1: func() workerState {
						return workerState{
							result: result{
								job: job{
									jobType: recentJob,
									from:    21,
									to:      30,
								},
								failed: map[uint64]int{22: 1},
								failedDetails: map[uint64]failedUnitInfo{
									22: {
										reason:      "rda query share_index=2: rda: query timeout",
										retryable:   true,
										sampleIndex: uint32Ptr(2),
									},
								},
								err: errors.New("22: failed"),
							},
							curr: 25,
						}
					},
					2: func() workerState {
						return workerState{
							result: result{
								job: job{
									jobType: catchupJob,
									from:    11,
									to:      20,
								},
								failed: map[uint64]int{12: 1, 13: 1},
								failedDetails: map[uint64]failedUnitInfo{
									12: {
										reason:      "rda query share_index=12: rda: proof invalid",
										retryable:   false,
										sampleIndex: uint32Ptr(12),
									},
									13: {
										reason:    "rda: peer unavailable",
										retryable: true,
									},
								},
								err: errors.Join(errors.New("12: failed"), errors.New("13: failed")),
							},
							curr: 15,
						}
					},
				},
				failed: map[uint64]retryAttempt{
					22: {count: 1},
					23: {count: 1},
					24: {count: 2},
				},
				nextJobID:   0,
				next:        31,
				networkHead: 100,
			},
			SamplingStats{
				SampledChainHead: 11,
				CatchupHead:      30,
				NetworkHead:      100,
				Failed:           map[uint64]int{22: 2, 23: 1, 24: 2, 12: 1, 13: 1},
				FailedDetails: map[uint64]FailedUnitStats{
					22: {
						Header:      22,
						Attempts:    2,
						Retryable:   true,
						Reason:      "rda query share_index=2: rda: query timeout",
						SampleIndex: uint32Ptr(2),
					},
					12: {
						Header:      12,
						Attempts:    1,
						Retryable:   false,
						Reason:      "rda query share_index=12: rda: proof invalid",
						SampleIndex: uint32Ptr(12),
					},
					13: {
						Header:    13,
						Attempts:  1,
						Retryable: true,
						Reason:    "rda: peer unavailable",
					},
				},
				Workers: []WorkerStats{
					{
						JobType: recentJob,
						Curr:    25,
						From:    21,
						To:      30,
						ErrMsg:  "22: failed",
					},
					{
						JobType: catchupJob,
						Curr:    15,
						From:    11,
						To:      20,
						ErrMsg:  "12: failed\n13: failed",
					},
				},
				Concurrency: 2,
				CatchUpDone: false,
				IsRunning:   true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := tt.state.unsafeStats()
			sort.Slice(stats.Workers, func(i, j int) bool {
				return stats.Workers[i].From > stats.Workers[j].Curr
			})
			assert.Equal(t, tt.want, stats, "stats are not equal")
		})
	}
}
