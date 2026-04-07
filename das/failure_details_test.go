package das

import (
	"errors"
	"testing"

	"github.com/celestiaorg/celestia-node/share"
	"github.com/stretchr/testify/require"
)

func TestBuildFailedUnitInfo_ExtractsSampleIndexAndRetryability(t *testing.T) {
	err := errors.New("rda query share_index=7: rda: query timeout")

	info := buildFailedUnitInfo(err)
	require.Equal(t, err.Error(), info.reason)
	require.True(t, info.retryable)
	require.NotNil(t, info.sampleIndex)
	require.Equal(t, uint32(7), *info.sampleIndex)
}

func TestBuildFailedUnitInfo_HandlesTerminalWithoutSampleIndex(t *testing.T) {
	err := share.ErrRDAProofInvalid

	info := buildFailedUnitInfo(err)
	require.Equal(t, err.Error(), info.reason)
	require.False(t, info.retryable)
	require.Nil(t, info.sampleIndex)
}
