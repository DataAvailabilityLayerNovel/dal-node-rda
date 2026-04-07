package das

import (
	"context"
	"sync"
	"time"

	libhead "github.com/celestiaorg/go-header"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/shrexsub"
)

// samplingCoordinator runs and coordinates sampling workers and updates current sampling state
type samplingCoordinator struct {
	concurrencyLimit int
	samplingTimeout  time.Duration

	getter      libhead.Getter[*header.ExtendedHeader]
	sampleFn    sampleFn
	broadcastFn shrexsub.BroadcastFn

	state coordinatorState

	// resultCh fans-in sampling results from worker to coordinator
	resultCh chan result
	// updHeadCh signals to update network head header height
	updHeadCh chan *header.ExtendedHeader
	// waitCh signals to block coordinator for external access to state
	waitCh chan *sync.WaitGroup

	workersWg sync.WaitGroup
	metrics   *metrics
	done
}

// result will carry errors to coordinator after worker finishes the job
type result struct {
	job
	failed        map[uint64]int
	failedDetails map[uint64]failedUnitInfo
	err           error
}

func newSamplingCoordinator(
	params Parameters,
	getter libhead.Getter[*header.ExtendedHeader],
	sample sampleFn,
	broadcast shrexsub.BroadcastFn,
) *samplingCoordinator {
	return &samplingCoordinator{
		concurrencyLimit: params.ConcurrencyLimit,
		samplingTimeout:  params.SampleTimeout,
		getter:           getter,
		sampleFn:         sample,
		broadcastFn:      broadcast,
		state:            newCoordinatorState(params),
		resultCh:         make(chan result),
		updHeadCh:        make(chan *header.ExtendedHeader),
		waitCh:           make(chan *sync.WaitGroup),
		done:             newDone("sampling coordinator"),
	}
}

func (sc *samplingCoordinator) run(ctx context.Context, cp checkpoint) {
	sc.state.resumeFromCheckpoint(cp)

	// resume workers
	for _, wk := range cp.Workers {
		sc.runWorker(ctx, sc.state.newJob(wk.JobType, wk.From, wk.To))
	}

	for {
		for !sc.nonRecentConcurrencyLimitReached() {
			next, found := sc.state.nextJob()
			if !found {
				break
			}
			sc.runWorker(ctx, next)
		}

		var retryWakeUp <-chan time.Time
		if wait, ok := sc.state.nextRetryDelay(); ok {
			if wait <= 0 {
				retryWakeUp = time.After(time.Millisecond)
			} else {
				retryWakeUp = time.After(wait)
			}
		}

		select {
		case head := <-sc.updHeadCh:
			if sc.state.isNewHead(head.Height()) {
				if !sc.recentJobsLimitReached() {
					sc.runWorker(ctx, sc.state.recentJob(head))
				}
				sc.state.updateHead(head.Height())
				// run worker without concurrency limit restrictions to reduced delay
				sc.metrics.observeNewHead(ctx)
			}
		case res := <-sc.resultCh:
			sc.state.handleResult(res)
		case <-retryWakeUp:
			// retry backoff elapsed; continue loop to schedule eligible retry jobs
		case wg := <-sc.waitCh:
			wg.Wait()
		case <-ctx.Done():
			sc.workersWg.Wait()
			sc.indicateDone()
			return
		}
	}
}

// nonRecentConcurrencyLimitReached indicates whether catchup/retry workers filled the budget,
// leaving a slot for recent jobs when concurrency is larger than 1.
func (sc *samplingCoordinator) nonRecentConcurrencyLimitReached() bool {
	if sc.concurrencyLimit <= 1 {
		return len(sc.state.inProgress) >= sc.concurrencyLimit
	}

	nonRecent := 0
	for _, getState := range sc.state.inProgress {
		if getState().jobType != recentJob {
			nonRecent++
		}
	}

	return nonRecent >= sc.concurrencyLimit-1
}

// runWorker runs job in separate worker go-routine
func (sc *samplingCoordinator) runWorker(ctx context.Context, j job) {
	w := newWorker(j, sc.getter, sc.sampleFn, sc.broadcastFn, sc.metrics)
	sc.state.putInProgress(j.id, w.getState)

	// launch worker go-routine
	sc.workersWg.Add(1)
	go func() {
		defer sc.workersWg.Done()
		w.run(ctx, sc.samplingTimeout, sc.resultCh)
	}()
}

// listen notifies the coordinator about a new network head received via subscription.
func (sc *samplingCoordinator) listen(ctx context.Context, h *header.ExtendedHeader) {
	select {
	case sc.updHeadCh <- h:
	case <-ctx.Done():
	}
}

// stats pauses the coordinator to get stats in a concurrently safe manner
func (sc *samplingCoordinator) stats(ctx context.Context) (SamplingStats, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	select {
	case sc.waitCh <- &wg:
	case <-ctx.Done():
		return SamplingStats{}, ctx.Err()
	}

	return sc.state.unsafeStats(), nil
}

func (sc *samplingCoordinator) getCheckpoint(ctx context.Context) (checkpoint, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	select {
	case sc.waitCh <- &wg:
	case <-ctx.Done():
		return checkpoint{}, ctx.Err()
	}

	return sc.state.snapshotCheckpoint(), nil
}

// concurrencyLimitReached indicates whether concurrencyLimit has been reached
func (sc *samplingCoordinator) concurrencyLimitReached() bool {
	return len(sc.state.inProgress) >= sc.concurrencyLimit
}

// recentJobsLimitReached indicates whether concurrency limit for recent jobs has been reached
func (sc *samplingCoordinator) recentJobsLimitReached() bool {
	return len(sc.state.inProgress) >= 2*sc.concurrencyLimit
}
