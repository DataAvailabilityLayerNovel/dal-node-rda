package das

import (
	"errors"
	"fmt"
	"time"
)

const currentCheckpointVersion uint32 = 2

var ErrUnsupportedCheckpointVersion = errors.New("unsupported checkpoint version")

type checkpoint struct {
	Version     uint32           `json:"version,omitempty"`
	SampleFrom  uint64           `json:"sample_from"`
	NetworkHead uint64           `json:"network_head"`
	Cursor      checkpointCursor `json:"cursor,omitempty"`
	// Failed heights will be retried
	Failed map[uint64]int `json:"failed,omitempty"`
	// RetryState stores richer retry metadata for pending heights.
	RetryState map[uint64]retryCheckpoint `json:"retry_state,omitempty"`
	// Workers will resume on restart from previous state
	Workers []workerCheckpoint `json:"workers,omitempty"`
}

type retryCheckpoint struct {
	Count       int       `json:"count"`
	After       time.Time `json:"after"`
	Reason      string    `json:"reason,omitempty"`
	Retryable   bool      `json:"retryable"`
	SampleIndex *uint32   `json:"sample_index,omitempty"`
}

type checkpointCursor struct {
	// CatchupHead mirrors the highest height already assigned to catchup workers.
	CatchupHead uint64 `json:"catchup_head"`
	// SampledChainHead mirrors the highest fully sampled contiguous chain head snapshot.
	SampledChainHead uint64 `json:"sampled_chain_head"`
}

// workerCheckpoint will be used to resume worker on restart
type workerCheckpoint struct {
	From    uint64  `json:"from"`
	To      uint64  `json:"to"`
	JobType jobType `json:"job_type"`
}

func newCheckpoint(stats SamplingStats) checkpoint {
	workers := make([]workerCheckpoint, 0, len(stats.Workers))
	for _, w := range stats.Workers {
		// no need to resume recent jobs after restart. On the other hand, retry jobs will resume from
		// failed heights map. it leaves only catchup jobs to be stored and resumed
		if w.JobType == catchupJob {
			workers = append(workers, workerCheckpoint{
				From:    w.Curr,
				To:      w.To,
				JobType: w.JobType,
			})
		}
	}
	return checkpoint{
		Version:     currentCheckpointVersion,
		SampleFrom:  stats.CatchupHead + 1,
		NetworkHead: stats.NetworkHead,
		Cursor: checkpointCursor{
			CatchupHead:      stats.CatchupHead,
			SampledChainHead: stats.SampledChainHead,
		},
		Failed:  stats.Failed,
		Workers: workers,
	}
}

func newRetryCheckpoint(attempt retryAttempt) retryCheckpoint {
	return retryCheckpoint{
		Count:       attempt.count,
		After:       attempt.after,
		Reason:      attempt.reason,
		Retryable:   attempt.retryable,
		SampleIndex: cloneSampleIndex(attempt.sampleIndex),
	}
}

func (r retryCheckpoint) toRetryAttempt(now time.Time) retryAttempt {
	after := r.After
	if after.IsZero() {
		after = now
	}
	return retryAttempt{
		count:       r.Count,
		after:       after,
		reason:      r.Reason,
		retryable:   r.Retryable,
		sampleIndex: cloneSampleIndex(r.SampleIndex),
	}
}

func (c *checkpoint) normalizeVersion() {
	if c.Version == 0 {
		c.Version = currentCheckpointVersion
	}
}

func (c *checkpoint) normalizeCursor() {
	if c.SampleFrom == 0 {
		if c.Cursor.CatchupHead > 0 {
			c.SampleFrom = c.Cursor.CatchupHead + 1
		}
	} else {
		expectedCatchup := c.SampleFrom - 1
		if c.Cursor.CatchupHead == 0 || c.Cursor.CatchupHead != expectedCatchup {
			c.Cursor.CatchupHead = expectedCatchup
		}
	}
}

func (c checkpoint) validateVersion() error {
	if c.Version > currentCheckpointVersion {
		return fmt.Errorf("%w: got=%d supported=%d", ErrUnsupportedCheckpointVersion, c.Version, currentCheckpointVersion)
	}
	return nil
}

func (c checkpoint) String() string {
	str := fmt.Sprintf("Version: %v, SampleFrom: %v, NetworkHead: %v", c.Version, c.SampleFrom, c.NetworkHead)
	str += fmt.Sprintf(", Cursor: {catchup=%v sampled=%v}", c.Cursor.CatchupHead, c.Cursor.SampledChainHead)

	if len(c.Workers) > 0 {
		str += fmt.Sprintf(", Workers: %v", len(c.Workers))
	}

	if len(c.Failed) > 0 {
		str += fmt.Sprintf("\nFailed: %v", c.Failed)
	}

	if len(c.RetryState) > 0 {
		str += fmt.Sprintf("\nRetryState: %v", len(c.RetryState))
	}

	return str
}
