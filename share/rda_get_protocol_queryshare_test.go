package share

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func makeTestRequesterForQueryShare() *RDAGetProtocolRequester {
	selfID := peer.ID("self-peer")
	peerA := peer.ID("peer-a")
	peerB := peer.ID("peer-b")

	gm := NewRDAGridManager(GridDimensions{Rows: 8, Cols: 8})
	gm.peerGrid[selfID.String()] = GridPosition{Row: 1, Col: 2}

	pm := &RDAPeerManager{
		gridManager: gm,
		myPosition:  GridPosition{Row: 1, Col: 2},
		rowPeers: map[int]map[peer.ID]peerInfo{
			1: {
				peerA: {id: peerA, position: GridPosition{Row: 1, Col: 2}},
				peerB: {id: peerB, position: GridPosition{Row: 1, Col: 2}},
			},
		},
		colPeers: map[int]map[peer.ID]peerInfo{
			2: {
				peerA: {id: peerA, position: GridPosition{Row: 1, Col: 2}},
				peerB: {id: peerB, position: GridPosition{Row: 1, Col: 2}},
			},
		},
	}

	r := &RDAGetProtocolRequester{
		selfPeerID:       selfID,
		gridManager:      gm,
		peerManager:      pm,
		retryAttempts:    2,
		retryBackoffBase: 0,
		retryBackoffMax:  0,
		predicateChk:     NewRDAPredicateChecker(8),
	}
	return r
}

func TestQueryShare_SucceedsOnValidPeer(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	r.sendGetRequestFn = func(context.Context, peer.ID, string, uint32) (*RDASymbol, error) {
		return &RDASymbol{
			Handle:     "handle-12345678",
			ShareIndex: 10,
			Row:        1,
			Col:        2,
			ShareData:  []byte("payload"),
			NMTProof: NMTProofData{
				RootHash: []byte("root"),
			},
		}, nil
	}

	symbol, err := r.QueryShare(context.Background(), "handle-12345678", 10)
	require.NoError(t, err)
	require.NotNil(t, symbol)
	require.Equal(t, "handle-12345678", symbol.Handle)
	require.Equal(t, uint32(10), symbol.ShareIndex)
	require.Equal(t, int64(1), r.successfulGets)
}

func TestQueryShare_FailurePathDeterministicByRetryBudget(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	calls := 0
	r.sendGetRequestFn = func(context.Context, peer.ID, string, uint32) (*RDASymbol, error) {
		calls++
		return nil, ErrRDAPeerUnavailable
	}

	_, err := r.QueryShare(context.Background(), "handle-12345678", 10)
	require.ErrorIs(t, err, ErrRDAPeerUnavailable)
	// 2 attempts * 2 deterministic peers from same subnet
	require.Equal(t, 4, calls)
	require.Equal(t, int64(1), r.failedRetries)
}

func TestQueryShare_DoesNotDialSelfWhenOnlyCandidate(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	selfID := r.selfPeerID
	r.peerManager.rowPeers[r.peerManager.myPosition.Row] = map[peer.ID]peerInfo{
		selfID: {id: selfID, position: r.peerManager.myPosition},
	}
	r.peerManager.colPeers[r.peerManager.myPosition.Col] = map[peer.ID]peerInfo{
		selfID: {id: selfID, position: r.peerManager.myPosition},
	}

	calls := 0
	r.sendGetRequestFn = func(context.Context, peer.ID, string, uint32) (*RDASymbol, error) {
		calls++
		return nil, ErrRDAPeerUnavailable
	}

	_, err := r.QueryShare(context.Background(), "handle-12345678", 10)
	require.ErrorIs(t, err, ErrRDAPeerUnavailable)
	require.Equal(t, 0, calls)
}

func TestQueryShare_FallbackWhenIntersectionEmpty(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	targetCol := int(Cell(10, uint32(r.gridManager.GetGridDimensions().Cols)))
	delete(r.peerManager.colPeers, targetCol)
	r.gridManager.peerGrid = map[string]GridPosition{
		r.selfPeerID.String(): {Row: r.peerManager.myPosition.Row, Col: r.peerManager.myPosition.Col},
	}

	fallbackPeer := peer.ID("peer-fallback")
	r.peerManager.rowPeers[r.peerManager.myPosition.Row][fallbackPeer] = peerInfo{id: fallbackPeer, position: r.peerManager.myPosition}

	calls := 0
	r.sendGetRequestFn = func(context.Context, peer.ID, string, uint32) (*RDASymbol, error) {
		calls++
		return &RDASymbol{
			Handle:     "handle-12345678",
			ShareIndex: 10,
			Row:        1,
			Col:        uint32(targetCol),
			ShareData:  []byte("payload"),
			NMTProof: NMTProofData{
				RootHash: []byte("root"),
			},
		}, nil
	}

	symbol, err := r.QueryShare(context.Background(), "handle-12345678", 10)
	require.NoError(t, err)
	require.NotNil(t, symbol)
	require.Equal(t, 1, calls)
}

func TestQueryShare_ExpandPeersOnSymbolNotFound(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	peerA := peer.ID("peer-a")
	peerB := peer.ID("peer-b")
	peerC := peer.ID("peer-c")

	targetCol := int(Cell(10, uint32(r.gridManager.GetGridDimensions().Cols)))
	r.gridManager.peerGrid = map[string]GridPosition{
		r.selfPeerID.String(): {Row: r.peerManager.myPosition.Row, Col: r.peerManager.myPosition.Col},
		peerA.String():        {Row: r.peerManager.myPosition.Row, Col: targetCol},
		peerB.String():        {Row: r.peerManager.myPosition.Row, Col: targetCol},
	}
	r.peerManager.rowPeers[r.peerManager.myPosition.Row][peerC] = peerInfo{id: peerC, position: r.peerManager.myPosition}

	calls := 0
	r.sendGetRequestFn = func(context.Context, peer.ID, string, uint32) (*RDASymbol, error) {
		calls++
		if calls == 1 {
			return nil, ErrRDASymbolNotFound
		}
		return &RDASymbol{
			Handle:     "handle-12345678",
			ShareIndex: 10,
			Row:        1,
			Col:        uint32(targetCol),
			ShareData:  []byte("payload"),
			NMTProof: NMTProofData{
				RootHash: []byte("root"),
			},
		}, nil
	}

	symbol, err := r.QueryShare(context.Background(), "handle-12345678", 10)
	require.NoError(t, err)
	require.NotNil(t, symbol)
	require.GreaterOrEqual(t, calls, 2)
}

func TestQueryShare_FallbackPrefersTargetColumnPeers(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	targetCol := int(Cell(10, uint32(r.gridManager.GetGridDimensions().Cols)))
	wrongColPeer := peer.ID("peer-wrong-col")
	rightColPeer := peer.ID("peer-right-col")

	r.gridManager.peerGrid[wrongColPeer.String()] = GridPosition{Row: r.peerManager.myPosition.Row, Col: 0}
	r.gridManager.peerGrid[rightColPeer.String()] = GridPosition{Row: r.peerManager.myPosition.Row, Col: targetCol}

	r.peerManager.rowPeers[r.peerManager.myPosition.Row][wrongColPeer] = peerInfo{id: wrongColPeer, position: GridPosition{Row: r.peerManager.myPosition.Row, Col: 0}}
	r.peerManager.rowPeers[r.peerManager.myPosition.Row][rightColPeer] = peerInfo{id: rightColPeer, position: GridPosition{Row: r.peerManager.myPosition.Row, Col: targetCol}}

	got := r.findFallbackPeers(uint32(r.peerManager.myPosition.Row), uint32(targetCol), nil)
	require.NotEmpty(t, got)
	require.Equal(t, rightColPeer, got[0])
}

func TestShortPeerID_UsesPrefixAndSuffix(t *testing.T) {
	id := peer.ID("12D3KooWABCDEFGHIJKLMN")
	short := shortPeerID(id)
	full := id.String()
	require.Greater(t, len(full), 12)
	require.Equal(t, full[:8]+".."+full[len(full)-4:], short)
}

func TestQueryShare_FallbackExcludesKnownWrongColumnPeers(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	targetCol := int(Cell(10, uint32(r.gridManager.GetGridDimensions().Cols)))
	wrongColPeerA := peer.ID("peer-wrong-col-a")
	wrongColPeerB := peer.ID("peer-wrong-col-b")

	r.gridManager.peerGrid[wrongColPeerA.String()] = GridPosition{Row: r.peerManager.myPosition.Row, Col: 0}
	r.gridManager.peerGrid[wrongColPeerB.String()] = GridPosition{Row: r.peerManager.myPosition.Row, Col: 1}
	r.peerManager.rowPeers[r.peerManager.myPosition.Row][wrongColPeerA] = peerInfo{id: wrongColPeerA, position: GridPosition{Row: r.peerManager.myPosition.Row, Col: 0}}
	r.peerManager.rowPeers[r.peerManager.myPosition.Row][wrongColPeerB] = peerInfo{id: wrongColPeerB, position: GridPosition{Row: r.peerManager.myPosition.Row, Col: 1}}

	got := r.findFallbackPeers(uint32(r.peerManager.myPosition.Row), uint32(targetCol), nil)
	require.NotContains(t, got, wrongColPeerA)
	require.NotContains(t, got, wrongColPeerB)
}

func TestQueryShare_UsesLocalStorageFastPath(t *testing.T) {
	r := makeTestRequesterForQueryShare()

	storage := NewRDAStorage(RDAStorageConfig{
		MyRow:    uint32(r.peerManager.myPosition.Row),
		MyCol:    uint32(r.peerManager.myPosition.Col),
		GridSize: uint32(r.gridManager.GetGridDimensions().Cols),
	})
	r.localStorage = storage

	ctx := context.Background()
	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "handle-12345678",
		Row:      1,
		Col:      2,
		SymbolID: 10,
		Data:     []byte("payload-local"),
		Height:   99,
	}))

	calls := 0
	r.sendGetRequestFn = func(context.Context, peer.ID, string, uint32) (*RDASymbol, error) {
		calls++
		return nil, ErrRDAPeerUnavailable
	}

	symbol, err := r.QueryShare(ctx, "handle-12345678", 10)
	require.NoError(t, err)
	require.NotNil(t, symbol)
	require.Equal(t, []byte("payload-local"), symbol.ShareData)
	require.Equal(t, 0, calls)
}
