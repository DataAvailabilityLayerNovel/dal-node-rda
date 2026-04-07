package das

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/header/headertest"
	"github.com/celestiaorg/celestia-node/share"
)

type fallbackTestAvailability struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (a *fallbackTestAvailability) SharesAvailable(context.Context, *header.ExtendedHeader) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return a.err
}

func (a *fallbackTestAvailability) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

type fallbackTestRDAAdapter struct {
	err error
}

func (a fallbackTestRDAAdapter) QuerySymbol(_ context.Context, handle string, shareIndex uint32) (*RDASymbol, error) {
	if a.err != nil {
		return nil, a.err
	}
	return &RDASymbol{Handle: handle, ShareIndex: shareIndex}, nil
}

func (fallbackTestRDAAdapter) SyncColumn(context.Context, uint64) error { return nil }

func (fallbackTestRDAAdapter) GetTopologySnapshot(context.Context) (RDATopologySnapshot, error) {
	return RDATopologySnapshot{}, nil
}

func (fallbackTestRDAAdapter) GetHealthSnapshot(context.Context) (RDAHealthSnapshot, error) {
	return RDAHealthSnapshot{Synced: true}, nil
}

type sequenceRDAAdapter struct {
	mu    sync.Mutex
	errs  []error
	calls int
}

func (a *sequenceRDAAdapter) QuerySymbol(_ context.Context, handle string, shareIndex uint32) (*RDASymbol, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.calls++
	idx := a.calls - 1
	if idx < len(a.errs) && a.errs[idx] != nil {
		return nil, a.errs[idx]
	}
	return &RDASymbol{Handle: handle, ShareIndex: shareIndex}, nil
}

func (*sequenceRDAAdapter) SyncColumn(context.Context, uint64) error { return nil }

func (*sequenceRDAAdapter) GetTopologySnapshot(context.Context) (RDATopologySnapshot, error) {
	return RDATopologySnapshot{}, nil
}

func (*sequenceRDAAdapter) GetHealthSnapshot(context.Context) (RDAHealthSnapshot, error) {
	return RDAHealthSnapshot{Synced: true}, nil
}

func (a *sequenceRDAAdapter) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

type syncTrackingRDAAdapter struct {
	queryErr  error
	syncErr   error
	syncCalls int
}

func (a *syncTrackingRDAAdapter) QuerySymbol(_ context.Context, handle string, shareIndex uint32) (*RDASymbol, error) {
	if a.queryErr != nil {
		return nil, a.queryErr
	}
	return &RDASymbol{Handle: handle, ShareIndex: shareIndex}, nil
}

func (a *syncTrackingRDAAdapter) SyncColumn(context.Context, uint64) error {
	a.syncCalls++
	return a.syncErr
}

func (*syncTrackingRDAAdapter) GetTopologySnapshot(context.Context) (RDATopologySnapshot, error) {
	return RDATopologySnapshot{}, nil
}

func (*syncTrackingRDAAdapter) GetHealthSnapshot(context.Context) (RDAHealthSnapshot, error) {
	return RDAHealthSnapshot{Synced: true}, nil
}

func TestHybridStrategy_FallbackOnNonRetryableRDAError(t *testing.T) {
	avail := &fallbackTestAvailability{}

	d := &DASer{
		da:     avail,
		rda:    fallbackTestRDAAdapter{err: share.ErrRDANotStarted},
		mode:   ModeHybrid,
		params: DefaultParameters(),
	}
	strat := &hybridStrategy{d: d}
	h := headertest.RandExtendedHeader(t)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.NoError(t, res.Err)
	require.True(t, res.Success)
	require.True(t, res.Telemetry.Fallback)
	require.Equal(t, 1, avail.Calls())
	require.Equal(t, uint64(0), d.rdaDirectHits.Load())
	require.Equal(t, uint64(1), d.rdaFallbackHits.Load())
}

func TestHybridStrategy_NoFallbackForRetryableBeforeThreshold(t *testing.T) {
	avail := &fallbackTestAvailability{}
	adapter := &sequenceRDAAdapter{errs: []error{share.ErrRDAQueryTimeout, share.ErrRDAQueryTimeout}}

	params := DefaultParameters()
	params.RDAFallbackEnabled = true
	params.RDAParallelQueries = 2
	params.RDAMaxRetries = 1
	params.RDAFallbackAfterRetries = 1000

	d := &DASer{
		da:     avail,
		rda:    adapter,
		mode:   ModeHybrid,
		params: params,
	}
	strat := &hybridStrategy{d: d}
	h := makeHeaderWithWidth(4, 10)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.ErrorIs(t, res.Err, share.ErrRDAQueryTimeout)
	require.False(t, res.Telemetry.Fallback)
	require.Equal(t, 0, avail.Calls())
	require.Equal(t, 2, adapter.Calls())
	require.Equal(t, uint64(0), d.rdaDirectHits.Load())
	require.Equal(t, uint64(0), d.rdaFallbackHits.Load())
}

func TestHybridStrategy_FallbackForRetryableAtZeroThreshold(t *testing.T) {
	avail := &fallbackTestAvailability{}

	params := DefaultParameters()
	params.RDAFallbackEnabled = true
	params.RDAFallbackAfterRetries = 0

	d := &DASer{
		da:     avail,
		rda:    fallbackTestRDAAdapter{err: share.ErrRDAQueryTimeout},
		mode:   ModeHybrid,
		params: params,
	}
	strat := &hybridStrategy{d: d}
	h := headertest.RandExtendedHeader(t)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.NoError(t, res.Err)
	require.True(t, res.Telemetry.Fallback)
	require.Equal(t, 1, avail.Calls())
	require.Equal(t, uint64(0), d.rdaDirectHits.Load())
	require.Equal(t, uint64(1), d.rdaFallbackHits.Load())
}

func TestHybridStrategy_RetryTimeoutThenSuccess(t *testing.T) {
	avail := &fallbackTestAvailability{}

	adapter := &sequenceRDAAdapter{errs: []error{share.ErrRDAQueryTimeout, nil}}
	params := DefaultParameters()
	params.RDAMaxRetries = 1
	params.RDAFallbackEnabled = true
	params.RDAFallbackAfterRetries = 1

	d := &DASer{
		da:     avail,
		rda:    adapter,
		mode:   ModeHybrid,
		params: params,
	}
	strat := &hybridStrategy{d: d}
	h := headertest.RandExtendedHeader(t)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.NoError(t, res.Err)
	require.True(t, res.Success)
	require.False(t, res.Telemetry.Fallback)
	require.Equal(t, 3, adapter.Calls())
	require.Equal(t, uint64(1), d.rdaDirectHits.Load())
	require.Equal(t, uint64(0), d.rdaFallbackHits.Load())
}

func TestHybridStrategy_RetryBoundedByMaxRetries(t *testing.T) {
	avail := &fallbackTestAvailability{}

	adapter := &sequenceRDAAdapter{errs: []error{share.ErrRDAQueryTimeout, share.ErrRDAQueryTimeout, share.ErrRDAQueryTimeout}}
	params := DefaultParameters()
	params.RDAMaxRetries = 1
	params.RDAFallbackEnabled = false

	d := &DASer{
		da:     avail,
		rda:    adapter,
		mode:   ModeHybrid,
		params: params,
	}
	strat := &hybridStrategy{d: d}
	h := headertest.RandExtendedHeader(t)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.ErrorIs(t, res.Err, share.ErrRDAQueryTimeout)
	require.False(t, res.Telemetry.Fallback)
	require.Equal(t, 2, adapter.Calls())
	require.Equal(t, uint64(0), d.rdaDirectHits.Load())
	require.Equal(t, uint64(0), d.rdaFallbackHits.Load())
}

func TestHybridStrategy_SyncOnCatchupBeforeFallback(t *testing.T) {
	avail := &fallbackTestAvailability{}
	adapter := &syncTrackingRDAAdapter{queryErr: share.ErrRDAQueryTimeout}

	params := DefaultParameters()
	params.RDAFallbackEnabled = true
	params.RDAFallbackAfterRetries = 0
	params.RDASyncOnCatchup = true
	params.RDASyncBatchSize = 32

	d := &DASer{
		da:     avail,
		rda:    adapter,
		mode:   ModeHybrid,
		params: params,
	}
	strat := &hybridStrategy{d: d}
	h := headertest.RandExtendedHeader(t)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.NoError(t, res.Err)
	require.True(t, res.Telemetry.Fallback)
	require.Equal(t, 1, adapter.syncCalls)
	require.Equal(t, uint64(1), d.rdaSyncRequests.Load())
	require.Equal(t, uint64(32), d.rdaSyncSymbols.Load())
}

func TestHybridStrategy_RDAMetricsNonZeroDuringSamplingRuns(t *testing.T) {
	avail := &fallbackTestAvailability{}

	params := DefaultParameters()
	params.RDAParallelQueries = 1
	params.RDAMaxRetries = 0
	params.RDAFallbackEnabled = true

	d := &DASer{
		da:     avail,
		rda:    fallbackTestRDAAdapter{},
		mode:   ModeHybrid,
		params: params,
	}
	strat := &hybridStrategy{d: d}
	h := makeHeaderWithWidth(4, 10)

	res := strat.Execute(StrategyInput{Context: context.Background(), Header: h})
	require.NoError(t, res.Err)
	require.True(t, res.Success)

	diag, err := d.RDADiagnostics(context.Background())
	require.NoError(t, err)
	require.Greater(t, diag.GetRequestTotal, uint64(0))
	require.Greater(t, diag.GetSuccessTotal, uint64(0))
	require.Greater(t, diag.QuerySuccessRatio, 0.0)
}
