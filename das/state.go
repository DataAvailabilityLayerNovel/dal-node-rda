package das

import (
	"context"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/celestiaorg/celestia-node/header"
)

// coordinatorState represents the current state of sampling process
type coordinatorState struct {
	// samplingRange is the maximum amount of headers processed in one job.
	samplingRange uint64

	// keeps track of running workers
	inProgress map[int]func() workerState

	// retryStrategy implements retry backoff
	retryStrategy retryStrategy
	// stores heights of failed headers with amount of retry attempt as value
	failed map[uint64]retryAttempt
	// inRetry stores (height -> attempt count) of failed headers that are currently being retried by
	// workers
	inRetry map[uint64]retryAttempt
	// consecutiveRetryJobs tracks how many retry jobs were scheduled in a row.
	consecutiveRetryJobs int
	// maxConsecutiveRetryJobs caps consecutive retry scheduling when catchup work exists.
	maxConsecutiveRetryJobs int
	// retryWindowSize groups sample indexes into windows to avoid retry storms on one subnet.
	retryWindowSize uint32
	// maxRetryPerWindow limits how many retries can be scheduled from same window before switching.
	maxRetryPerWindow int
	// retryWindowUsage tracks scheduled retries per window in current balancing cycle.
	retryWindowUsage map[uint32]int

	// nextJobID is a unique identifier that will be used for creation of next job
	nextJobID int
	// all headers before next were sent to workers
	next uint64
	// networkHead is the height of the latest known network head
	networkHead uint64

	// catchUpDone indicates if all headers are sampled
	catchUpDone atomic.Bool
	// catchUpDoneCh blocks until all headers are sampled
	catchUpDoneCh chan struct{}
}

// retryAttempt represents a retry attempt with a backoff delay.
type retryAttempt struct {
	// count specifies the number of retry attempts made so far.
	count int
	// after specifies the time for the next retry attempt.
	after time.Time

	// reason is the most recent failure reason observed for the header.
	reason string
	// retryable indicates whether the most recent failure is retryable.
	retryable bool
	// sampleIndex points to failing sample index when available.
	sampleIndex *uint32
}

// newCoordinatorState initiates state for samplingCoordinator
func newCoordinatorState(params Parameters) coordinatorState {
	return coordinatorState{
		samplingRange: params.SamplingRange,
		inProgress:    make(map[int]func() workerState),
		retryStrategy: newRetryStrategy(exponentialBackoff(
			defaultBackoffInitialInterval,
			defaultBackoffMultiplier,
			defaultBackoffMaxRetryCount,
		)),
		failed:                  make(map[uint64]retryAttempt),
		inRetry:                 make(map[uint64]retryAttempt),
		maxConsecutiveRetryJobs: 3,
		retryWindowSize:         16,
		maxRetryPerWindow:       2,
		retryWindowUsage:        make(map[uint32]int),
		nextJobID:               0,
		catchUpDoneCh:           make(chan struct{}),
	}
}

func (s *coordinatorState) resumeFromCheckpoint(c checkpoint) {
	s.next = c.SampleFrom
	s.networkHead = c.NetworkHead
	now := time.Now()

	for h, persisted := range c.RetryState {
		attempt := persisted.toRetryAttempt(now)
		if attempt.count <= 0 {
			if count, ok := c.Failed[h]; ok {
				attempt.count = count
			}
		}
		s.failed[h] = attempt
	}

	for h, count := range c.Failed {
		if _, ok := s.failed[h]; ok {
			continue
		}
		// resumed retries should start without backoff delay
		s.failed[h] = retryAttempt{
			count: count,
			after: now,
		}
	}
}

func (s *coordinatorState) handleResult(res result) {
	delete(s.inProgress, res.id)

	switch res.jobType {
	case recentJob, catchupJob:
		s.handleRecentOrCatchupResult(res)
	case retryJob:
		s.handleRetryResult(res)
	}

	s.checkDone()
}

func (s *coordinatorState) handleRecentOrCatchupResult(res result) {
	// check if the worker retried any of the previously failed heights
	for h := range s.failed {
		if h < res.from || h > res.to {
			continue
		}

		if res.failed[h] == 0 {
			delete(s.failed, h)
		}
	}

	// update failed heights
	for h := range res.failed {
		nextRetry, _ := s.retryStrategy.nextRetry(retryAttempt{}, time.Now())
		if info, ok := res.failedDetails[h]; ok {
			nextRetry.reason = info.reason
			nextRetry.retryable = info.retryable
			nextRetry.sampleIndex = cloneSampleIndex(info.sampleIndex)
		}
		s.failed[h] = nextRetry
	}
}

func (s *coordinatorState) handleRetryResult(res result) {
	// move heights that has failed again to failed with keeping retry count, they will be picked up by
	// retry workers later
	for h := range res.failed {
		lastRetry := s.inRetry[h]
		// height will be retried after backoff
		nextRetry, retryExceeded := s.retryStrategy.nextRetry(lastRetry, time.Now())
		if info, ok := res.failedDetails[h]; ok {
			nextRetry.reason = info.reason
			nextRetry.retryable = info.retryable
			nextRetry.sampleIndex = cloneSampleIndex(info.sampleIndex)
		} else {
			nextRetry.reason = lastRetry.reason
			nextRetry.retryable = lastRetry.retryable
			nextRetry.sampleIndex = cloneSampleIndex(lastRetry.sampleIndex)
		}
		if retryExceeded {
			log.Warnw(
				"header exceeded maximum amount of sampling attempts",
				"height", h,
				"attempts", nextRetry.count,
			)
		}
		s.failed[h] = nextRetry
	}

	// processed heights are either already moved to failed map or succeeded, cleanup inRetry
	for h := res.from; h <= res.to; h++ {
		delete(s.inRetry, h)
	}
}

func (s *coordinatorState) isNewHead(newHead uint64) bool {
	// seen this header before
	if newHead <= s.networkHead {
		log.Warnf(
			"received head height: %v, which is lower or the same as previously known: %v",
			newHead,
			s.networkHead,
		)
		return false
	}
	return true
}

func (s *coordinatorState) updateHead(newHead uint64) {
	oldHead := s.networkHead
	s.networkHead = newHead

	log.Debugw("updated head", "from_height", oldHead, "to_height", newHead)
	s.checkDone()
}

// recentJob creates a job to process a recent header.
func (s *coordinatorState) recentJob(header *header.ExtendedHeader) job {
	// move next, to prevent catchup job from processing same height
	if s.next == header.Height() {
		s.next++
	}
	s.nextJobID++
	return job{
		id:      s.nextJobID,
		jobType: recentJob,
		header:  header,
		from:    header.Height(),
		to:      header.Height(),
	}
}

// nextJob will return next catchup or retry job according to priority (retry -> catchup)
func (s *coordinatorState) nextJob() (next job, found bool) {
	if job, retryFound := s.retryJob(); retryFound {
		// If catchup work exists, limit how many retries can run back-to-back to avoid starvation.
		if s.hasCatchupWork() && s.consecutiveRetryJobs >= s.effectiveMaxConsecutiveRetryJobs() {
			// return job back to failed queue and force a catchup pick first
			attempt := s.inRetry[job.from]
			delete(s.inRetry, job.from)
			s.failed[job.from] = retryAttempt{
				count:       attempt.count,
				after:       attempt.after,
				reason:      attempt.reason,
				retryable:   attempt.retryable,
				sampleIndex: cloneSampleIndex(attempt.sampleIndex),
			}
		} else {
			s.consecutiveRetryJobs++
			return job, true
		}
	}

	job, found := s.catchupJob()
	if found {
		s.consecutiveRetryJobs = 0
		s.retryWindowUsage = make(map[uint32]int)
	}
	return job, found
}

func (s *coordinatorState) hasCatchupWork() bool {
	return s.next <= s.networkHead
}

func (s *coordinatorState) effectiveMaxConsecutiveRetryJobs() int {
	if s.maxConsecutiveRetryJobs <= 0 {
		return 1
	}
	return s.maxConsecutiveRetryJobs
}

// catchupJob creates a catchup job if catchup is not finished
func (s *coordinatorState) catchupJob() (next job, found bool) {
	if s.next > s.networkHead {
		return job{}, false
	}

	to := min(s.next+s.samplingRange-1, s.networkHead)
	j := s.newJob(catchupJob, s.next, to)
	s.next = to + 1
	return j, true
}

// retryJob creates a job to retry previously failed header
func (s *coordinatorState) retryJob() (next job, found bool) {
	eligible := s.collectRetryCandidates()
	if len(eligible) == 0 {
		return job{}, false
	}

	selectedHeight, selectedAttempt, ok := s.selectRetryCandidate(eligible)
	if !ok {
		return job{}, false
	}

	delete(s.failed, selectedHeight)
	s.inRetry[selectedHeight] = selectedAttempt

	if s.retryWindowUsage == nil {
		s.retryWindowUsage = make(map[uint32]int)
	}
	window := s.retryWindow(selectedHeight, selectedAttempt)
	s.retryWindowUsage[window]++

	j := s.newJob(retryJob, selectedHeight, selectedHeight)
	return j, true
}

func (s *coordinatorState) collectRetryCandidates() map[uint64]retryAttempt {
	eligible := make(map[uint64]retryAttempt)
	for h, attempt := range s.failed {
		if !attempt.canRetry() {
			continue
		}
		eligible[h] = attempt
	}
	return eligible
}

func (s *coordinatorState) selectRetryCandidate(
	eligible map[uint64]retryAttempt,
) (height uint64, attempt retryAttempt, found bool) {
	if len(eligible) == 0 {
		return 0, retryAttempt{}, false
	}

	heights := make([]uint64, 0, len(eligible))
	for h := range eligible {
		heights = append(heights, h)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	bestIdx := -1
	bestUsage := 0
	bestWindow := uint32(0)
	for i, h := range heights {
		a := eligible[h]
		window := s.retryWindow(h, a)
		usage := s.retryWindowUsage[window]
		if usage >= s.effectiveMaxRetryPerWindow() {
			continue
		}

		if bestIdx == -1 || usage < bestUsage || (usage == bestUsage && window < bestWindow) {
			bestIdx = i
			bestUsage = usage
			bestWindow = window
		}
	}

	if bestIdx == -1 {
		s.retryWindowUsage = make(map[uint32]int)
		bestIdx = 0
	}

	height = heights[bestIdx]
	return height, eligible[height], true
}

func (s *coordinatorState) retryWindow(height uint64, attempt retryAttempt) uint32 {
	if attempt.sampleIndex == nil {
		return uint32(height)
	}

	windowSize := s.effectiveRetryWindowSize()
	return *attempt.sampleIndex / windowSize
}

func (s *coordinatorState) effectiveMaxRetryPerWindow() int {
	if s.maxRetryPerWindow <= 0 {
		return 1
	}
	return s.maxRetryPerWindow
}

func (s *coordinatorState) effectiveRetryWindowSize() uint32 {
	if s.retryWindowSize == 0 {
		return 1
	}
	return s.retryWindowSize
}

func (s *coordinatorState) putInProgress(jobID int, getState func() workerState) {
	s.inProgress[jobID] = getState
}

func (s *coordinatorState) newJob(jobType jobType, from, to uint64) job {
	s.nextJobID++
	return job{
		id:      s.nextJobID,
		jobType: jobType,
		from:    from,
		to:      to,
	}
}

// unsafeStats collects coordinator stats without thread-safety
func (s *coordinatorState) unsafeStats() SamplingStats {
	workers := make([]WorkerStats, 0, len(s.inProgress))
	lowestFailedOrInProgress := s.next
	failed := make(map[uint64]int)
	rawFailedDetails := make(map[uint64]failedUnitInfo)

	// gather worker stats
	for _, getStats := range s.inProgress {
		wstats := getStats()
		var errMsg string
		if wstats.err != nil {
			errMsg = wstats.err.Error()
		}
		workers = append(workers, WorkerStats{
			JobType: wstats.jobType,
			Curr:    wstats.curr,
			From:    wstats.from,
			To:      wstats.to,
			ErrMsg:  errMsg,
		})

		for h := range wstats.failed {
			failed[h]++
			rawFailedDetails[h] = mergeFailureInfo(rawFailedDetails[h], wstats.failedDetails[h])
			if h < lowestFailedOrInProgress {
				lowestFailedOrInProgress = h
			}
		}

		if wstats.curr < lowestFailedOrInProgress {
			lowestFailedOrInProgress = wstats.curr
		}
	}

	// set lowestFailedOrInProgress to minimum failed - 1
	for h, retry := range s.failed {
		failed[h] += retry.count
		rawFailedDetails[h] = mergeFailureInfo(rawFailedDetails[h], failedUnitInfo{
			reason:      retry.reason,
			retryable:   retry.retryable,
			sampleIndex: retry.sampleIndex,
		})
		if h < lowestFailedOrInProgress {
			lowestFailedOrInProgress = h
		}
	}

	for h, retry := range s.inRetry {
		failed[h] += retry.count
		rawFailedDetails[h] = mergeFailureInfo(rawFailedDetails[h], failedUnitInfo{
			reason:      retry.reason,
			retryable:   retry.retryable,
			sampleIndex: retry.sampleIndex,
		})
	}

	failedDetails := make(map[uint64]FailedUnitStats)
	for h, attempts := range failed {
		info := rawFailedDetails[h]
		if !shouldExposeFailureInfo(info) {
			continue
		}

		failedDetails[h] = FailedUnitStats{
			Header:      h,
			Attempts:    attempts,
			Retryable:   info.retryable,
			Reason:      info.reason,
			SampleIndex: cloneSampleIndex(info.sampleIndex),
		}
	}

	if len(failedDetails) == 0 {
		failedDetails = nil
	}

	return SamplingStats{
		SampledChainHead: lowestFailedOrInProgress - 1,
		CatchupHead:      s.next - 1,
		NetworkHead:      s.networkHead,
		Failed:           failed,
		FailedDetails:    failedDetails,
		Workers:          workers,
		Concurrency:      len(workers),
		CatchUpDone:      s.catchUpDone.Load(),
		IsRunning:        len(workers) > 0 || s.catchUpDone.Load(),
	}
}

func (s *coordinatorState) checkDone() {
	if len(s.inProgress) == 0 && len(s.failed) == 0 && s.next > s.networkHead {
		if s.catchUpDone.CompareAndSwap(false, true) {
			close(s.catchUpDoneCh)
		}
		return
	}

	if s.catchUpDone.Load() {
		// overwrite channel before storing done flag
		s.catchUpDoneCh = make(chan struct{})
		s.catchUpDone.Store(false)
	}
}

// waitCatchUp waits for sampling process to indicate catchup is done
func (s *coordinatorState) waitCatchUp(ctx context.Context) error {
	if s.catchUpDone.Load() {
		return nil
	}
	select {
	case <-s.catchUpDoneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *coordinatorState) snapshotCheckpoint() checkpoint {
	cp := newCheckpoint(s.unsafeStats())
	if len(s.failed) == 0 && len(s.inRetry) == 0 {
		return cp
	}

	cp.RetryState = make(map[uint64]retryCheckpoint, len(s.failed)+len(s.inRetry))
	for h, attempt := range s.failed {
		cp.RetryState[h] = newRetryCheckpoint(attempt)
	}
	for h, attempt := range s.inRetry {
		if _, exists := cp.RetryState[h]; exists {
			continue
		}
		cp.RetryState[h] = newRetryCheckpoint(attempt)
	}

	return cp
}

// nextRetryDelay returns the time until the nearest retry attempt becomes eligible.
// If there are no failed headers waiting for retry, ok will be false.
func (s *coordinatorState) nextRetryDelay() (delay time.Duration, ok bool) {
	if len(s.failed) == 0 {
		return 0, false
	}

	now := time.Now()
	minDelay := time.Duration(math.MaxInt64)
	for _, attempt := range s.failed {
		if attempt.after.Before(now) {
			return 0, true
		}

		wait := time.Until(attempt.after)
		if wait < minDelay {
			minDelay = wait
		}
	}

	if minDelay < 0 {
		return 0, true
	}
	return minDelay, true
}

// canRetry returns true if the time stored in the "after" has passed.
func (r retryAttempt) canRetry() bool {
	return r.after.Before(time.Now())
}
