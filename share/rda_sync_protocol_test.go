package share

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	libhost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func setupConnectedHosts(t *testing.T) (libhost.Host, libhost.Host) {
	t.Helper()

	ctx := context.Background()
	h1, err := libp2p.New()
	require.NoError(t, err)

	h2, err := libp2p.New()
	require.NoError(t, err)

	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	require.NoError(t, err)

	err = h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, h1.Close())
		require.NoError(t, h2.Close())
	})

	return h1, h2
}

func TestSyncScanColumnShares_UsesSinceHeight(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	ctx := context.Background()

	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "h-90",
		Row:      0,
		Col:      2,
		SymbolID: 2,
		Data:     []byte("older"),
		Height:   90,
	}))
	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "h-100",
		Row:      1,
		Col:      2,
		SymbolID: 10,
		Data:     []byte("newer"),
		Height:   100,
	}))

	h := &RDASyncProtocolHandler{storage: storage}
	shares, latest, hasMore, nextOffset, err := h.scanColumnShares(ctx, 2, 95, 0, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(100), latest)
	require.Len(t, shares, 1)
	require.False(t, hasMore)
	require.Equal(t, uint32(1), nextOffset)
	require.Equal(t, "h-100", shares[0].Handle)
	require.Equal(t, uint32(10), shares[0].ShareIndex)
	require.Equal(t, uint64(100), shares[0].Height)
	require.Equal(t, []byte("newer"), shares[0].Data)
}

func TestSyncScanColumnShares_AppliesDefaultWindowWhenSinceHeightZero(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	ctx := context.Background()

	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "h-100",
		Row:      0,
		Col:      2,
		SymbolID: 2,
		Data:     []byte("outside-window"),
		Height:   100,
	}))
	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "h-200",
		Row:      1,
		Col:      2,
		SymbolID: 10,
		Data:     []byte("inside-window"),
		Height:   200,
	}))

	h := &RDASyncProtocolHandler{storage: storage}
	shares, latest, hasMore, nextOffset, err := h.scanColumnShares(ctx, 2, 0, 0, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(200), latest)
	require.Len(t, shares, 1)
	require.False(t, hasMore)
	require.Equal(t, uint32(1), nextOffset)
	require.Equal(t, "h-200", shares[0].Handle)
	require.Equal(t, uint64(200), shares[0].Height)
}

func TestSyncScanColumnShares_WrongColumnFails(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	h := &RDASyncProtocolHandler{storage: storage}

	_, _, _, _, err := h.scanColumnShares(context.Background(), 3, 0, 0, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot scan column")
}

func TestSyncScanColumnShares_Pagination(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		height := uint64(100 + i)
		row := uint32(i)
		symbolID := row*8 + 2
		require.NoError(t, storage.StoreShare(ctx, &RDAShare{
			Handle:   "h-page",
			Row:      row,
			Col:      2,
			SymbolID: symbolID,
			Data:     []byte{byte(i + 1)},
			Height:   height,
		}))
	}

	h := &RDASyncProtocolHandler{storage: storage}

	page1, latest, hasMore, nextOffset, err := h.scanColumnShares(ctx, 2, 0, 0, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(104), latest)
	require.Len(t, page1, 2)
	require.True(t, hasMore)
	require.Equal(t, uint32(2), nextOffset)

	page2, _, hasMore, nextOffset, err := h.scanColumnShares(ctx, 2, 0, nextOffset, 2)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	require.True(t, hasMore)
	require.Equal(t, uint32(4), nextOffset)

	page3, _, hasMore, nextOffset, err := h.scanColumnShares(ctx, 2, 0, nextOffset, 2)
	require.NoError(t, err)
	require.Len(t, page3, 1)
	require.False(t, hasMore)
	require.Equal(t, uint32(5), nextOffset)
}

func TestProcessSyncedShares_ValidatesPredicateBeforeCommit(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		storage:      storage,
		predicateChk: NewRDAPredicateChecker(8),
	}

	validShare := SyncShareData{
		Handle:     "h-valid",
		ShareIndex: 10,
		Row:        1,
		Col:        2,
		Data:       []byte("ok"),
		Height:     123,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	invalidPredShare := validShare
	invalidPredShare.Handle = "h-invalid"
	invalidPredShare.ShareIndex = 18
	invalidPredShare.Col = 2
	invalidPredShare.NMTProof = NMTProofData{}

	count := r.processSyncedShares(context.Background(), []SyncShareData{validShare, invalidPredShare})
	require.Equal(t, 1, count)

	stored, err := storage.GetShare(context.Background(), 123, 1, 2, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), stored)

	_, err = storage.GetShare(context.Background(), 123, 1, 2, 18)
	require.Error(t, err)
}

func TestProcessSyncedShares_DeduplicatesIncomingSymbols(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		storage:      storage,
		predicateChk: NewRDAPredicateChecker(8),
	}

	share := SyncShareData{
		Handle:     "h-dup",
		ShareIndex: 10,
		Row:        1,
		Col:        2,
		Data:       []byte("dup"),
		Height:     999,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	count := r.processSyncedShares(context.Background(), []SyncShareData{share, share})
	require.Equal(t, 1, count)
	require.Equal(t, int64(1), r.totalSharesReceived)
}

func TestProcessSyncedShares_IntegrityCheckBeforeStore(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		storage:      storage,
		predicateChk: NewRDAPredicateChecker(8),
	}

	badColShare := SyncShareData{
		Handle:     "h-bad-col",
		ShareIndex: 10,
		Row:        1,
		Col:        3,
		Data:       []byte("bad-col"),
		Height:     321,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	count := r.processSyncedShares(context.Background(), []SyncShareData{badColShare})
	require.Equal(t, 0, count)

	_, err := storage.GetShare(context.Background(), 321, 1, 2, 10)
	require.Error(t, err)
}

func TestValidateSyncShareIntegrity_ErrorCases(t *testing.T) {
	r := &RDASyncProtocolRequester{predicateChk: NewRDAPredicateChecker(8)}

	err := r.validateSyncShareIntegrity(SyncShareData{ShareIndex: 10, Col: 2, Data: []byte("x")})
	require.Error(t, err)

	err = r.validateSyncShareIntegrity(SyncShareData{Handle: "h", ShareIndex: 10, Col: 2})
	require.Error(t, err)

	err = r.validateSyncShareIntegrity(SyncShareData{Handle: "h", ShareIndex: 10, Col: 3, Data: []byte("x")})
	require.Error(t, err)
}

func TestProcessSyncedShares_DoesNotOverwriteNewerWithOlder(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		storage:      storage,
		predicateChk: NewRDAPredicateChecker(8),
	}

	ctx := context.Background()
	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "h-versioned",
		Row:      1,
		Col:      2,
		SymbolID: 10,
		Data:     []byte("newer"),
		Height:   200,
	}))

	olderIncoming := SyncShareData{
		Handle:     "h-versioned",
		ShareIndex: 10,
		Row:        1,
		Col:        2,
		Data:       []byte("older"),
		Height:     150,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	count := r.processSyncedShares(ctx, []SyncShareData{olderIncoming})
	require.Equal(t, 0, count)

	data, height, _, _, err := storage.GetShareByHandleAndSymbol(ctx, "h-versioned", 10)
	require.NoError(t, err)
	require.Equal(t, uint64(200), height)
	require.Equal(t, []byte("newer"), data)
	require.Equal(t, int64(1), r.auditStaleSkipped)
}

func TestProcessSyncedShares_AuditCounters(t *testing.T) {
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		storage:      storage,
		predicateChk: NewRDAPredicateChecker(8),
	}

	ctx := context.Background()
	require.NoError(t, storage.StoreShare(ctx, &RDAShare{
		Handle:   "h-stale",
		Row:      1,
		Col:      2,
		SymbolID: 10,
		Data:     []byte("latest"),
		Height:   500,
	}))

	validShare := SyncShareData{
		Handle:     "h-ok",
		ShareIndex: 18,
		Row:        2,
		Col:        2,
		Data:       []byte("ok"),
		Height:     101,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	dedupShare := validShare
	dedupShare.Height = 102

	integrityFailShare := validShare
	integrityFailShare.Handle = ""

	predicateFailShare := validShare
	predicateFailShare.Handle = "h-pred"
	predicateFailShare.ShareIndex = 26
	predicateFailShare.Col = 2
	predicateFailShare.NMTProof = NMTProofData{}

	staleShare := SyncShareData{
		Handle:     "h-stale",
		ShareIndex: 10,
		Row:        1,
		Col:        2,
		Data:       []byte("older"),
		Height:     300,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	count := r.processSyncedShares(ctx, []SyncShareData{
		validShare,
		dedupShare,
		dedupShare,
		integrityFailShare,
		predicateFailShare,
		staleShare,
	})

	require.Equal(t, 2, count)
	require.Equal(t, int64(2), r.auditAccepted)
	require.Equal(t, int64(1), r.auditDedupSkipped)
	require.Equal(t, int64(1), r.auditIntegrityFail)
	require.Equal(t, int64(1), r.auditPredicateFail)
	require.Equal(t, int64(1), r.auditStaleSkipped)
}

func TestProcessSyncedShares_UsesBlockstoreBridgeWhenConfigured(t *testing.T) {
	ctx := context.Background()

	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bridge := NewRDAStorageWithBlockstore(storage, bs)

	r := &RDASyncProtocolRequester{
		storage:      storage,
		predicateChk: NewRDAPredicateChecker(8),
	}
	r.SetBlockstoreBridge(bridge)

	share := SyncShareData{
		Handle:     "h-bridge",
		ShareIndex: 10,
		Row:        1,
		Col:        2,
		Data:       []byte("bridge-data"),
		Height:     700,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	count := r.processSyncedShares(ctx, []SyncShareData{share})
	require.Equal(t, 1, count)

	stored, err := storage.GetShare(ctx, 700, 1, 2, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("bridge-data"), stored)

	block := bridge.createIPLDBlock(&RDAShare{Data: []byte("bridge-data")})
	_, err = bs.Get(ctx, block.Cid())
	require.NoError(t, err)

	metrics := bridge.GetMetrics()
	require.Equal(t, int64(1), metrics["shares_stored_to_blockstore"])
}

func TestSyncFromPeer_PaginatesAcrossResponses(t *testing.T) {
	requesterHost, responderHost := setupConnectedHosts(t)

	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		host:           requesterHost,
		storage:        storage,
		predicateChk:   NewRDAPredicateChecker(8),
		requestTimeout: 2 * time.Second,
	}

	makeShare := func(handle string, row uint32, height uint64) SyncShareData {
		return SyncShareData{
			Handle:     handle,
			ShareIndex: row*8 + 2,
			Row:        row,
			Col:        2,
			Data:       []byte(fmt.Sprintf("data-%d", row)),
			Height:     height,
			NMTProof: NMTProofData{
				Nodes:       [][]byte{[]byte("n1")},
				RootHash:    []byte("root"),
				NamespaceID: "ns",
			},
		}
	}

	responderHost.SetStreamHandler(RDASyncProtocolID, func(stream network.Stream) {
		defer stream.Close()

		var req SyncMessage
		require.NoError(t, json.NewDecoder(stream).Decode(&req))

		resp := SyncMessage{
			Type:      SyncMessageTypeResponse,
			RequestID: req.RequestID,
			SenderID:  responderHost.ID().String(),
		}

		switch req.PageOffset {
		case 0:
			resp.Shares = []SyncShareData{makeShare("h-page-stream", 1, 101)}
			resp.HasMore = true
			resp.NextOffset = 1
		case 1:
			resp.Shares = []SyncShareData{makeShare("h-page-stream", 2, 102)}
			resp.HasMore = false
			resp.NextOffset = 2
		default:
			resp.Shares = []SyncShareData{}
			resp.HasMore = false
			resp.NextOffset = req.PageOffset
		}

		require.NoError(t, json.NewEncoder(stream).Encode(resp))
	})

	err := r.syncFromPeer(context.Background(), responderHost.ID(), 2, 0)
	require.NoError(t, err)

	_, err = storage.GetShare(context.Background(), 101, 1, 2, 10)
	require.NoError(t, err)
	_, err = storage.GetShare(context.Background(), 102, 2, 2, 18)
	require.NoError(t, err)
	require.Equal(t, int64(2), r.totalSharesReceived)
	require.Equal(t, int64(2), r.auditAccepted)
}

func TestSyncFromPeer_InvalidPaginationContractFails(t *testing.T) {
	requesterHost, responderHost := setupConnectedHosts(t)

	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		host:           requesterHost,
		storage:        storage,
		predicateChk:   NewRDAPredicateChecker(8),
		requestTimeout: 2 * time.Second,
	}

	responderHost.SetStreamHandler(RDASyncProtocolID, func(stream network.Stream) {
		defer stream.Close()

		var req SyncMessage
		require.NoError(t, json.NewDecoder(stream).Decode(&req))

		resp := SyncMessage{
			Type:      SyncMessageTypeResponse,
			RequestID: req.RequestID,
			SenderID:  responderHost.ID().String(),
			Shares: []SyncShareData{{
				Handle:     "h-invalid-page",
				ShareIndex: 10,
				Row:        1,
				Col:        2,
				Data:       []byte("x"),
				Height:     111,
				NMTProof: NMTProofData{
					Nodes:       [][]byte{[]byte("n1")},
					RootHash:    []byte("root"),
					NamespaceID: "ns",
				},
			}},
			HasMore: true,
			// Invalid contract: next offset must progress when has_more=true.
			NextOffset: req.PageOffset,
		}

		require.NoError(t, json.NewEncoder(stream).Encode(resp))
	})

	err := r.syncFromPeer(context.Background(), responderHost.ID(), 2, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid pagination response")
}

func TestRequestSyncPage_RequestIDMismatch(t *testing.T) {
	requesterHost, responderHost := setupConnectedHosts(t)

	r := &RDASyncProtocolRequester{
		host:           requesterHost,
		requestTimeout: 2 * time.Second,
	}

	responderHost.SetStreamHandler(RDASyncProtocolID, func(stream network.Stream) {
		defer stream.Close()

		var req SyncMessage
		require.NoError(t, json.NewDecoder(stream).Decode(&req))

		resp := SyncMessage{
			Type:       SyncMessageTypeResponse,
			RequestID:  req.RequestID + "-mismatch",
			SenderID:   responderHost.ID().String(),
			Shares:     []SyncShareData{},
			HasMore:    false,
			NextOffset: req.PageOffset,
		}

		require.NoError(t, json.NewEncoder(stream).Encode(resp))
	})

	_, err := r.requestSyncPage(context.Background(), responderHost.ID(), 2, 0, "req-fixed", 0, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "request ID mismatch")
}

func TestSyncFromPeer_NewNodeCanSyncRecentColumnDataAfterJoin(t *testing.T) {
	requesterHost, responderHost := setupConnectedHosts(t)

	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	r := &RDASyncProtocolRequester{
		host:           requesterHost,
		storage:        storage,
		predicateChk:   NewRDAPredicateChecker(8),
		requestTimeout: 2 * time.Second,
	}

	makeShare := func(row uint32, height uint64) SyncShareData {
		return SyncShareData{
			Handle:     fmt.Sprintf("h-recent-%d", row),
			ShareIndex: row*8 + 2,
			Row:        row,
			Col:        2,
			Data:       []byte(fmt.Sprintf("recent-%d", row)),
			Height:     height,
			NMTProof: NMTProofData{
				Nodes:       [][]byte{[]byte("n1")},
				RootHash:    []byte("root"),
				NamespaceID: "ns",
			},
		}
	}

	responderHost.SetStreamHandler(RDASyncProtocolID, func(stream network.Stream) {
		defer stream.Close()

		var req SyncMessage
		require.NoError(t, json.NewDecoder(stream).Decode(&req))

		resp := SyncMessage{
			Type:      SyncMessageTypeResponse,
			RequestID: req.RequestID,
			SenderID:  responderHost.ID().String(),
		}

		if req.PageOffset == 0 {
			resp.Shares = []SyncShareData{makeShare(1, 990), makeShare(2, 991)}
			resp.HasMore = true
			resp.NextOffset = 2
		} else {
			resp.Shares = []SyncShareData{makeShare(3, 992)}
			resp.HasMore = false
			resp.NextOffset = 3
		}

		require.NoError(t, json.NewEncoder(stream).Encode(resp))
	})

	err := r.syncFromPeer(context.Background(), responderHost.ID(), 2, 0)
	require.NoError(t, err)

	_, err = storage.GetShare(context.Background(), 990, 1, 2, 10)
	require.NoError(t, err)
	_, err = storage.GetShare(context.Background(), 991, 2, 2, 18)
	require.NoError(t, err)
	_, err = storage.GetShare(context.Background(), 992, 3, 2, 26)
	require.NoError(t, err)

	require.Equal(t, int64(3), r.totalSharesReceived)
	require.Equal(t, int64(3), r.auditAccepted)
}

func TestSyncFromPeer_SyncedDataAvailableForReconstructionPath(t *testing.T) {
	requesterHost, responderHost := setupConnectedHosts(t)

	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 2, GridSize: 8})
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bridge := NewRDAStorageWithBlockstore(storage, bs)

	r := &RDASyncProtocolRequester{
		host:           requesterHost,
		storage:        storage,
		predicateChk:   NewRDAPredicateChecker(8),
		requestTimeout: 2 * time.Second,
	}
	r.SetBlockstoreBridge(bridge)

	share := SyncShareData{
		Handle:     "h-reconstruct",
		ShareIndex: 10,
		Row:        1,
		Col:        2,
		Data:       []byte("reconstruct-ready"),
		Height:     1000,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}

	responderHost.SetStreamHandler(RDASyncProtocolID, func(stream network.Stream) {
		defer stream.Close()

		var req SyncMessage
		require.NoError(t, json.NewDecoder(stream).Decode(&req))

		resp := SyncMessage{
			Type:       SyncMessageTypeResponse,
			RequestID:  req.RequestID,
			SenderID:   responderHost.ID().String(),
			Shares:     []SyncShareData{share},
			HasMore:    false,
			NextOffset: 1,
		}

		require.NoError(t, json.NewEncoder(stream).Encode(resp))
	})

	err := r.syncFromPeer(context.Background(), responderHost.ID(), 2, 0)
	require.NoError(t, err)

	// Confirm local RDA storage has synced data.
	stored, err := storage.GetShare(context.Background(), 1000, 1, 2, 10)
	require.NoError(t, err)
	require.Equal(t, []byte("reconstruct-ready"), stored)

	// Confirm downstream reconstruction path can load same data from blockstore.
	block := bridge.createIPLDBlock(&RDAShare{Data: []byte("reconstruct-ready")})
	fromBS, err := bs.Get(context.Background(), block.Cid())
	require.NoError(t, err)
	require.Equal(t, []byte("reconstruct-ready"), fromBS.RawData())
}
