package das

import (
	"context"
	"errors"
	"time"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
)

// samplingStrategy defines the execution contract for DAS sample operations.
type samplingStrategy interface {
	Mode() Mode
	Execute(StrategyInput) StrategyResult
}

// StrategyInput defines the normalized strategy input contract.
type StrategyInput struct {
	Context        context.Context
	Header         *header.ExtendedHeader
	SamplingBudget int
}

// StrategyTelemetry provides strategy execution diagnostics in a common shape.
type StrategyTelemetry struct {
	Mode      Mode
	Retryable bool
	Error     string
	Fallback  bool
}

// StrategyResult defines the normalized strategy output contract.
type StrategyResult struct {
	Success   bool
	Retryable bool
	Err       error
	Telemetry StrategyTelemetry
}

func classifyRetryable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	if errors.Is(err, share.ErrRDAQueryTimeout) {
		return true
	}

	if errors.Is(err, share.ErrRDAPeerUnavailable) {
		return true
	}

	if errors.Is(err, share.ErrRDAProofInvalid) {
		return false
	}

	if errors.Is(err, share.ErrRDASymbolNotFound) {
		return false
	}

	if errors.Is(err, share.ErrRDAProtocolDecode) {
		return true
	}

	if errors.Is(err, share.ErrRDANotStarted) {
		return false
	}

	if errors.Is(err, share.ErrSubnetNotInitialized) {
		return false
	}

	// Default behavior remains conservative for unknown errors.
	return true
}

func shouldFallback(params Parameters, err error, retries int) bool {
	if err == nil || !params.RDAFallbackEnabled {
		return false
	}

	if !classifyRetryable(err) {
		return true
	}

	return retries >= params.RDAFallbackAfterRetries
}

func executeRDAFirstWithFallback(d *DASer, mode Mode, input StrategyInput) StrategyResult {
	var (
		err         error
		retriesUsed int
	)

	for attempt := 0; attempt <= d.params.RDAMaxRetries; attempt++ {
		attemptCtx := input.Context
		cancel := func() {}
		if d.params.RDAQueryTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(input.Context, d.params.RDAQueryTimeout)
		}

		err = d.sampleRDA(attemptCtx, input.Header)
		cancel()
		if err == nil {
			d.observeRDADirectHit(input.Context)
			return makeResult(mode, nil)
		}

		if !classifyRetryable(err) {
			break
		}

		if attempt < d.params.RDAMaxRetries {
			retriesUsed++
		}
	}

	if !shouldFallback(d.params, err, retriesUsed) {
		return makeResult(mode, err)
	}

	if d.params.RDASyncOnCatchup && d.rda != nil && input.Header != nil {
		d.observeRDASyncRequest(input.Context)
		if syncErr := d.rda.SyncColumn(input.Context, input.Header.Height()); syncErr == nil {
			// SYNC adapter currently does not expose precise symbol count; use configured batch size as
			// conservative per-request accounting unit for observability.
			d.observeRDASyncSymbolsReceived(input.Context, int64(d.params.RDASyncBatchSize))
		}
	}

	fallbackStart := time.Now()
	fallbackErr := d.sampleClassic(input.Context, input.Header)
	res := makeResult(mode, fallbackErr)
	res.Telemetry.Fallback = true
	if fallbackErr == nil {
		d.observeRDAFallbackHit(input.Context)
		if d.sampler != nil && d.sampler.metrics != nil {
			d.sampler.metrics.recordRDAErasureRecovery(input.Context)
			d.sampler.metrics.observeRDARecoveryLatency(input.Context, float64(time.Since(fallbackStart).Milliseconds()))
		}
		res.Telemetry.Error = err.Error()
	}
	return res
}

func makeResult(mode Mode, err error) StrategyResult {
	retryable := classifyRetryable(err)
	telemetry := StrategyTelemetry{
		Mode:      mode,
		Retryable: retryable,
	}
	if err != nil {
		telemetry.Error = err.Error()
	}

	return StrategyResult{
		Success:   err == nil,
		Retryable: retryable,
		Err:       err,
		Telemetry: telemetry,
	}
}

type classicStrategy struct {
	d *DASer
}

func (s *classicStrategy) Mode() Mode {
	return ModeClassic
}

func (s *classicStrategy) Execute(input StrategyInput) StrategyResult {
	err := s.d.sampleClassic(input.Context, input.Header)
	return makeResult(s.Mode(), err)
}

type rdaStrategy struct {
	d *DASer
}

func (s *rdaStrategy) Mode() Mode {
	return ModeRDA
}

func (s *rdaStrategy) Execute(input StrategyInput) StrategyResult {
	return executeRDAFirstWithFallback(s.d, s.Mode(), input)
}

type hybridStrategy struct {
	d *DASer
}

func (s *hybridStrategy) Mode() Mode {
	return ModeHybrid
}

func (s *hybridStrategy) Execute(input StrategyInput) StrategyResult {
	return executeRDAFirstWithFallback(s.d, s.Mode(), input)
}

func newSamplingStrategy(d *DASer) samplingStrategy {
	switch d.mode {
	case ModeRDA:
		return &rdaStrategy{d: d}
	case ModeHybrid:
		return &hybridStrategy{d: d}
	default:
		return &classicStrategy{d: d}
	}
}
