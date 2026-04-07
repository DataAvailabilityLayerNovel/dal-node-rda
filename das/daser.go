package das

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	"github.com/celestiaorg/go-fraud"
	libhead "github.com/celestiaorg/go-header"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/eds/byzantine"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/shrexsub"
)

var log = logging.Logger("das")

// DASer continuously validates availability of data committed to headers.
type DASer struct {
	params Parameters

	da     share.Availability
	rda    RDAAdapter
	mode   Mode
	strat  samplingStrategy
	bcast  fraud.Broadcaster[*header.ExtendedHeader]
	hsub   libhead.Subscriber[*header.ExtendedHeader] // listens for new headers in the network
	getter libhead.Store[*header.ExtendedHeader]      // retrieves past headers

	sampler    *samplingCoordinator
	store      checkpointStore
	subscriber subscriber

	cancel  context.CancelFunc
	running atomic.Bool

	rdaDirectHits   atomic.Uint64
	rdaFallbackHits atomic.Uint64
	rdaGetRequests  atomic.Uint64
	rdaGetSuccesses atomic.Uint64
	rdaGetTimeouts  atomic.Uint64
	rdaSyncRequests atomic.Uint64
	rdaSyncSymbols  atomic.Uint64
}

type (
	listenFn func(context.Context, *header.ExtendedHeader)
	sampleFn func(context.Context, *header.ExtendedHeader) error
)

// NewDASer creates a new DASer.
func NewDASer(
	da share.Availability,
	hsub libhead.Subscriber[*header.ExtendedHeader],
	getter libhead.Store[*header.ExtendedHeader],
	dstore datastore.Datastore,
	bcast fraud.Broadcaster[*header.ExtendedHeader],
	shrexBroadcast shrexsub.BroadcastFn,
	options ...Option,
) (*DASer, error) {
	d := &DASer{
		params:     DefaultParameters(),
		da:         da,
		bcast:      bcast,
		hsub:       hsub,
		getter:     getter,
		store:      newCheckpointStore(dstore),
		subscriber: newSubscriber(),
	}

	for _, applyOpt := range options {
		applyOpt(d)
	}

	err := d.params.Validate()
	if err != nil {
		return nil, err
	}

	mode, err := parseMode(string(d.params.Mode))
	if err != nil {
		return nil, err
	}
	d.mode = mode
	d.strat = newSamplingStrategy(d)

	d.sampler = newSamplingCoordinator(d.params, getter, d.sample, shrexBroadcast)
	return d, nil
}

// Start initiates subscription for new ExtendedHeaders and spawns a sampling routine.
func (d *DASer) Start(ctx context.Context) error {
	if !d.running.CompareAndSwap(false, true) {
		return errors.New("da: DASer already started")
	}

	cp, err := d.checkpoint(ctx)
	if err != nil {
		return err
	}

	sub, err := d.hsub.Subscribe()
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go d.sampler.run(runCtx, cp)
	go d.subscriber.run(runCtx, sub, d.sampler.listen)
	go d.store.runBackgroundStore(runCtx, d.params.BackgroundStoreInterval, d.sampler.getCheckpoint)

	return nil
}

func (d *DASer) checkpoint(ctx context.Context) (checkpoint, error) {
	tail, err := d.getter.Tail(ctx)
	if err != nil {
		return checkpoint{}, err
	}

	head, err := d.getter.Head(ctx)
	if err != nil {
		return checkpoint{}, err
	}

	// load latest DASed checkpoint
	cp, err := d.store.load(ctx)
	switch {
	case errors.Is(err, datastore.ErrNotFound):
		log.Warnf("checkpoint not found, initializing with Tail (%d) and Head (%d)", tail.Height(), head.Height())

		cp = checkpoint{
			Version:     currentCheckpointVersion,
			SampleFrom:  tail.Height(),
			NetworkHead: head.Height(),
		}
	case errors.Is(err, ErrUnsupportedCheckpointVersion):
		log.Warnw("checkpoint version is not supported; resetting checkpoint",
			"err", err,
			"tail", tail.Height(),
			"head", head.Height(),
		)

		cp = checkpoint{
			Version:     currentCheckpointVersion,
			SampleFrom:  tail.Height(),
			NetworkHead: head.Height(),
		}
	case err != nil:
		return checkpoint{}, err
	default:
		if cp.SampleFrom < tail.Height() {
			cp.SampleFrom = tail.Height()
		}
		if cp.NetworkHead < head.Height() {
			cp.NetworkHead = head.Height()
		}

		for height := range cp.Failed {
			if height < tail.Height() {
				// means the sample status is outdated and we don't need to sample it again
				delete(cp.Failed, height)
			}
		}

		if len(cp.Workers) > 0 {
			wrkrs := make([]workerCheckpoint, 0, len(cp.Workers))
			for _, wrk := range cp.Workers {
				if wrk.To >= tail.Height() {
					if wrk.From < tail.Height() {
						wrk.From = tail.Height()
					}

					wrkrs = append(wrkrs, wrk)
				}
			}

			cp.Workers = wrkrs
		}
	}
	cp.normalizeCursor()

	log.Info("starting DASer from checkpoint: ", cp.String())
	return cp, nil
}

// Stop stops sampling.
func (d *DASer) Stop(ctx context.Context) error {
	if !d.running.CompareAndSwap(true, false) {
		return nil
	}

	// try to store checkpoint without waiting for coordinator and workers to stop
	cp, err := d.sampler.getCheckpoint(ctx)
	if err != nil {
		log.Error("DASer coordinator checkpoint is unavailable")
	}

	if err = d.store.store(ctx, cp); err != nil {
		log.Errorw("storing checkpoint to disk", "err", err)
	}

	d.cancel()

	if err := d.sampler.metrics.close(); err != nil {
		log.Warnw("closing metrics", "err", err)
	}

	if err = d.sampler.wait(ctx); err != nil {
		return fmt.Errorf("DASer force quit: %w", err)
	}

	// save updated checkpoint after sampler and all workers are shut down
	if err = d.store.store(ctx, d.sampler.state.snapshotCheckpoint()); err != nil {
		log.Errorw("storing checkpoint to disk", "err", err)
	}

	if err = d.store.wait(ctx); err != nil {
		return fmt.Errorf("DASer force quit with err: %w", err)
	}
	return d.subscriber.wait(ctx)
}

func (d *DASer) sample(ctx context.Context, h *header.ExtendedHeader) error {
	if d.strat == nil {
		d.strat = newSamplingStrategy(d)
	}

	result := d.strat.Execute(StrategyInput{
		Context:        ctx,
		Header:         h,
		SamplingBudget: d.params.RDAMaxRetries,
	})
	return result.Err
}

func (d *DASer) sampleClassic(ctx context.Context, h *header.ExtendedHeader) error {
	err := d.da.SharesAvailable(ctx, h)
	if err != nil {
		var byzantineErr *byzantine.ErrByzantine
		if errors.As(err, &byzantineErr) {
			log.Warn("Propagating proof...")
			sendErr := d.bcast.Broadcast(ctx, byzantine.CreateBadEncodingProof(h.Hash(), h.Height(), byzantineErr))
			if sendErr != nil {
				log.Errorw("fraud proof propagating failed", "err", sendErr)
			}
		}
		return err
	}
	return nil
}

func (d *DASer) sampleRDA(ctx context.Context, h *header.ExtendedHeader) error {
	if d.rda == nil {
		return share.ErrRDANotStarted
	}
	if h == nil || h.DAH == nil {
		return share.ErrRDAProtocolDecode
	}

	width := len(h.DAH.RowRoots)
	if width == 0 {
		return share.ErrRDAProtocolDecode
	}

	handle := ""
	handle = fmt.Sprintf("%X", h.DAH.Hash())

	queryCount := d.params.RDAParallelQueries
	if queryCount <= 0 {
		queryCount = 1
	}
	if queryCount > width {
		queryCount = width
	}

	startCol := int(h.Height() % uint64(width))

	var (
		firstRetryableErr error
		firstTerminalErr  error
		retryableFailures int
		terminalFailures  int
		success           int
	)

	for offset := 0; offset < queryCount; offset++ {
		col := uint32((startCol + offset) % width)
		queryStart := time.Now()
		d.observeRDAGetRequest(ctx)
		symbol, err := d.rda.QuerySymbol(ctx, handle, col)
		if d.sampler != nil && d.sampler.metrics != nil {
			outcome := "success"
			if err != nil {
				outcome = "error"
			}
			d.sampler.metrics.observeRDAMatrixDirectFetch(ctx, float64(time.Since(queryStart).Milliseconds()), d.mode, outcome)
			d.sampler.metrics.recordRDASamplingOperation(ctx, d.mode, outcome)
		}
		if err == nil {
			d.observeRDAGetSuccess(ctx)
		} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, share.ErrRDAQueryTimeout) {
			d.observeRDAGetTimeout(ctx)
		}
		if err != nil {
			wrapped := fmt.Errorf("rda query share_index=%d: %w", col, err)
			if d.params.RDAModeStrictPredicate {
				return wrapped
			}
			if classifyRetryable(err) {
				retryableFailures++
				if firstRetryableErr == nil {
					firstRetryableErr = wrapped
				}
				continue
			}

			terminalFailures++
			if firstTerminalErr == nil {
				firstTerminalErr = wrapped
			}
			continue
		}

		if symbol == nil {
			err = fmt.Errorf("rda query share_index=%d: %w", col, share.ErrRDAProtocolDecode)
			if d.params.RDAModeStrictPredicate {
				return err
			}
			retryableFailures++
			if firstRetryableErr == nil {
				firstRetryableErr = err
			}
			continue
		}

		if symbol.ShareIndex != col || (symbol.Handle != "" && symbol.Handle != handle) {
			d.observeRDAPredValidationFail(ctx)
			err = fmt.Errorf("rda query share_index=%d: %w", col, share.ErrRDAProofInvalid)
			if d.params.RDAModeStrictPredicate {
				return err
			}
			terminalFailures++
			if firstTerminalErr == nil {
				firstTerminalErr = err
			}
			continue
		}

		success++
	}

	if success > 0 {
		return nil
	}

	// Prefer terminal failures over retryable ones when all samples fail.
	if terminalFailures > 0 && firstTerminalErr != nil {
		return firstTerminalErr
	}

	if retryableFailures > 0 && firstRetryableErr != nil {
		return firstRetryableErr
	}

	return share.ErrRDASymbolNotFound
}

func (d *DASer) observeRDADirectHit(ctx context.Context) {
	d.rdaDirectHits.Add(1)
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDADirectHit(ctx, d.mode)
		d.sampler.metrics.updateRDABandwidthSavings(ctx, 100)
	}
}

func (d *DASer) observeRDAFallbackHit(ctx context.Context) {
	d.rdaFallbackHits.Add(1)
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDAFallbackHit(ctx, d.mode)
		d.sampler.metrics.updateRDABandwidthSavings(ctx, 0)
	}
}

func (d *DASer) observeRDAGetRequest(ctx context.Context) {
	d.rdaGetRequests.Add(1)
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDAGetRequest(ctx, d.mode)
	}
}

func (d *DASer) observeRDAGetSuccess(ctx context.Context) {
	d.rdaGetSuccesses.Add(1)
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDAGetSuccess(ctx, d.mode)
	}
}

func (d *DASer) observeRDAGetTimeout(ctx context.Context) {
	d.rdaGetTimeouts.Add(1)
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDAGetTimeout(ctx, d.mode)
	}
}

func (d *DASer) observeRDAPredValidationFail(ctx context.Context) {
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDAPredValidationFail(ctx, d.mode)
	}
}

func (d *DASer) observeRDASyncRequest(ctx context.Context) {
	d.rdaSyncRequests.Add(1)
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDASyncRequest(ctx, d.mode)
	}
}

func (d *DASer) observeRDASyncSymbolsReceived(ctx context.Context, symbols int64) {
	if symbols <= 0 {
		return
	}
	d.rdaSyncSymbols.Add(uint64(symbols))
	if d.sampler != nil && d.sampler.metrics != nil {
		d.sampler.metrics.recordRDASyncSymbolsReceived(ctx, d.mode, symbols)
	}
}

// SamplingStats returns the current statistics over the DA sampling process.
func (d *DASer) SamplingStats(ctx context.Context) (SamplingStats, error) {
	stats, err := d.sampler.stats(ctx)
	if err != nil {
		return SamplingStats{}, err
	}

	mode, err := d.RuntimeMode(ctx)
	if err != nil {
		return SamplingStats{}, err
	}
	stats.RDADefinition = mode.RDADefinition
	stats.Mode = mode.Mode
	stats.FallbackActive = mode.FallbackActive

	return stats, nil
}

// RuntimeMode returns runtime mode and fallback diagnostics for operations.
func (d *DASer) RuntimeMode(context.Context) (RuntimeModeStatus, error) {
	return RuntimeModeStatus{
		RDADefinition:  "Robust Distributed Array",
		Mode:           d.mode,
		FallbackActive: d.rdaFallbackHits.Load() > 0,
	}, nil
}

// RDADiagnostics returns consolidated RDA runtime diagnostics for operators.
func (d *DASer) RDADiagnostics(ctx context.Context) (RDADiagnosticsStatus, error) {
	runtime, err := d.RuntimeMode(ctx)
	if err != nil {
		return RDADiagnosticsStatus{}, err
	}

	requests := d.rdaGetRequests.Load()
	successes := d.rdaGetSuccesses.Load()
	timeouts := d.rdaGetTimeouts.Load()
	directHits := d.rdaDirectHits.Load()
	fallbackHits := d.rdaFallbackHits.Load()
	syncRequests := d.rdaSyncRequests.Load()
	syncSymbols := d.rdaSyncSymbols.Load()

	stats := RDADiagnosticsStatus{
		RuntimeModeStatus:        runtime,
		GetRequestTotal:          requests,
		GetSuccessTotal:          successes,
		GetTimeoutTotal:          timeouts,
		SyncRequestTotal:         syncRequests,
		SyncSymbolsReceivedTotal: syncSymbols,
		MinPeersPerSubnet:        d.params.RDAMinPeersPerSubnet,
	}

	if requests > 0 {
		stats.QuerySuccessRatio = float64(successes) / float64(requests)
	}

	totalOutcomes := directHits + fallbackHits
	if totalOutcomes > 0 {
		stats.FallbackRatio = float64(fallbackHits) / float64(totalOutcomes)
	}

	if d.rda == nil {
		stats.TopologyError = "rda adapter not configured"
		stats.HealthError = "rda adapter not configured"
		return stats, nil
	}

	topo, topoErr := d.rda.GetTopologySnapshot(ctx)
	if topoErr != nil {
		stats.TopologyError = topoErr.Error()
	} else {
		stats.Topology = topo
		stats.SubnetReady = topo.TotalSubnetPeers >= d.params.RDAMinPeersPerSubnet
	}

	health, healthErr := d.rda.GetHealthSnapshot(ctx)
	if healthErr != nil {
		stats.HealthError = healthErr.Error()
	} else {
		stats.Health = health
	}

	return stats, nil
}

// WaitCatchUp waits for DASer to indicate catchup is done
func (d *DASer) WaitCatchUp(ctx context.Context) error {
	return d.sampler.state.waitCatchUp(ctx)
}
