package das

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	sharecore "github.com/celestiaorg/celestia-node/share"
)

func TestRDAServiceAdapter_RuntimeCallPath(t *testing.T) {
	adapter := &rdaServiceAdapter{service: &sharecore.RDANodeService{}}

	_, err := adapter.QuerySymbol(context.Background(), "handle", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get protocol requester not available")

	err = adapter.SyncColumn(context.Background(), 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sync protocol requester not available")
}

func TestRDAServiceAdapter_SnapshotCalls(t *testing.T) {
	adapter := &rdaServiceAdapter{service: &sharecore.RDANodeService{}}

	topo, err := adapter.GetTopologySnapshot(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, topo.Rows)
	require.Equal(t, 0, topo.Cols)

	health, err := adapter.GetHealthSnapshot(context.Background())
	require.NoError(t, err)
	require.False(t, health.Synced)
}
