package das

import (
	"context"
	"errors"
	"testing"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/stretchr/testify/require"
)

type sequenceQueryAdapter struct {
	errs  []error
	calls int
	seen  []uint32
}

func (a *sequenceQueryAdapter) QuerySymbol(ctx context.Context, handle string, shareIndex uint32) (*RDASymbol, error) {
	_ = ctx
	a.seen = append(a.seen, shareIndex)
	idx := a.calls
	a.calls++
	if idx < len(a.errs) && a.errs[idx] != nil {
		return nil, a.errs[idx]
	}
	return &RDASymbol{Handle: handle, ShareIndex: shareIndex}, nil
}

func (*sequenceQueryAdapter) SyncColumn(context.Context, uint64) error { return nil }

func (*sequenceQueryAdapter) GetTopologySnapshot(context.Context) (RDATopologySnapshot, error) {
	return RDATopologySnapshot{}, nil
}

func (*sequenceQueryAdapter) GetHealthSnapshot(context.Context) (RDAHealthSnapshot, error) {
	return RDAHealthSnapshot{Synced: true}, nil
}

func makeHeaderWithWidth(width int, height int64) *header.ExtendedHeader {
	return &header.ExtendedHeader{
		RawHeader: header.RawHeader{Height: height},
		DAH:       &share.AxisRoots{RowRoots: make([][]byte, width)},
	}
}

func TestSampleRDA_PerSymbolQueriesBoundedByParallelism(t *testing.T) {
	adapter := &sequenceQueryAdapter{}
	d := &DASer{
		rda:    adapter,
		params: DefaultParameters(),
	}
	d.params.RDAParallelQueries = 3
	d.params.RDAModeStrictPredicate = true

	err := d.sampleRDA(context.Background(), makeHeaderWithWidth(6, 10))
	require.NoError(t, err)
	require.Equal(t, 3, adapter.calls)
	require.Len(t, adapter.seen, 3)
}

func TestSampleRDA_StrictModeFailsFast(t *testing.T) {
	adapter := &sequenceQueryAdapter{errs: []error{share.ErrRDAQueryTimeout}}
	d := &DASer{
		rda:    adapter,
		params: DefaultParameters(),
	}
	d.params.RDAParallelQueries = 4
	d.params.RDAModeStrictPredicate = true

	err := d.sampleRDA(context.Background(), makeHeaderWithWidth(8, 1))
	require.Error(t, err)
	require.True(t, errors.Is(err, share.ErrRDAQueryTimeout))
	require.Equal(t, 1, adapter.calls)
}

func TestSampleRDA_TolerantModeAllowsPartialSuccess(t *testing.T) {
	adapter := &sequenceQueryAdapter{errs: []error{share.ErrRDAQueryTimeout, nil, nil}}
	d := &DASer{
		rda:    adapter,
		params: DefaultParameters(),
	}
	d.params.RDAParallelQueries = 3
	d.params.RDAModeStrictPredicate = false

	err := d.sampleRDA(context.Background(), makeHeaderWithWidth(8, 2))
	require.NoError(t, err)
	require.Equal(t, 3, adapter.calls)
}

func TestSampleRDA_TolerantModeFailsWhenAllQueriesFail(t *testing.T) {
	adapter := &sequenceQueryAdapter{errs: []error{share.ErrRDAQueryTimeout, share.ErrRDAPeerUnavailable}}
	d := &DASer{
		rda:    adapter,
		params: DefaultParameters(),
	}
	d.params.RDAParallelQueries = 2
	d.params.RDAModeStrictPredicate = false

	err := d.sampleRDA(context.Background(), makeHeaderWithWidth(4, 3))
	require.Error(t, err)
	require.True(t, errors.Is(err, share.ErrRDAQueryTimeout) || errors.Is(err, share.ErrRDAPeerUnavailable))
	require.Equal(t, 2, adapter.calls)
}

type mixedFailureAdapter struct {
	calls int
}

func (a *mixedFailureAdapter) QuerySymbol(_ context.Context, handle string, shareIndex uint32) (*RDASymbol, error) {
	defer func() { a.calls++ }()
	switch a.calls {
	case 0:
		// Terminal failure: wrong symbol index returned.
		return &RDASymbol{Handle: handle, ShareIndex: shareIndex + 1}, nil
	default:
		// Retryable failure.
		return nil, share.ErrRDAQueryTimeout
	}
}

func (*mixedFailureAdapter) SyncColumn(context.Context, uint64) error { return nil }

func (*mixedFailureAdapter) GetTopologySnapshot(context.Context) (RDATopologySnapshot, error) {
	return RDATopologySnapshot{}, nil
}

func (*mixedFailureAdapter) GetHealthSnapshot(context.Context) (RDAHealthSnapshot, error) {
	return RDAHealthSnapshot{Synced: true}, nil
}

func TestSampleRDA_TolerantModePrefersTerminalFailureClassification(t *testing.T) {
	adapter := &mixedFailureAdapter{}
	d := &DASer{
		rda:    adapter,
		params: DefaultParameters(),
	}
	d.params.RDAParallelQueries = 2
	d.params.RDAModeStrictPredicate = false

	err := d.sampleRDA(context.Background(), makeHeaderWithWidth(4, 5))
	require.Error(t, err)
	require.ErrorIs(t, err, share.ErrRDAProofInvalid)
	require.Equal(t, 2, adapter.calls)
}
