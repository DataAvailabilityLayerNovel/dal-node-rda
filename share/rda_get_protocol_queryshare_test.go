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
