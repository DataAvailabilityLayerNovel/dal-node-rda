package share

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockColumnPeerResolver implements ColumnPeerResolver for testing
type MockColumnPeerResolver struct {
	columnPeers map[uint32][]string
	storage     map[string]*RDAStorage // Simulate remote node storage
	failPeer    string                  // Peer that always fails
	delayMs     int64
}

func NewMockColumnPeerResolver() *MockColumnPeerResolver {
	return &MockColumnPeerResolver{
		columnPeers: make(map[uint32][]string),
		storage:     make(map[string]*RDAStorage),
	}
}

func (m *MockColumnPeerResolver) GetColumnPeers(ctx context.Context, targetCol uint32) ([]string, error) {
	peers, exists := m.columnPeers[targetCol]
	if !exists {
		return nil, fmt.Errorf("no peers configured for column %d", targetCol)
	}
	return peers, nil
}

func (m *MockColumnPeerResolver) SendRetrievalRequest(ctx context.Context, peerID string, req *RetrievalRequest) (*RetrievalResponse, error) {
	// Simulate failure for failPeer
	if peerID == m.failPeer {
		return nil, fmt.Errorf("simulated peer failure")
	}

	// Get peer's storage
	peerStorage, exists := m.storage[peerID]
	if !exists {
		return nil, fmt.Errorf("peer %s not found", peerID)
	}

	// Retrieve from peer's storage
	data, err := peerStorage.GetShare(ctx, req.Height, req.Row, req.Col, req.SymbolID)
	if err != nil {
		return &RetrievalResponse{
			Success:   false,
			Error:     err.Error(),
			PeerID:    peerID,
			Timestamp: 0,
		}, nil
	}

	return &RetrievalResponse{
		Data:      data,
		Success:   true,
		PeerID:    peerID,
		Timestamp: 0,
	}, nil
}

// addPeerStorage adds a peer's storage to the resolver
func (m *MockColumnPeerResolver) addPeerStorage(peerID string, storage *RDAStorage) {
	m.storage[peerID] = storage
}

// addColumnPeer maps column to peer IDs
func (m *MockColumnPeerResolver) addColumnPeer(col uint32, peerID string) {
	if _, exists := m.columnPeers[col]; !exists {
		m.columnPeers[col] = []string{}
	}
	m.columnPeers[col] = append(m.columnPeers[col], peerID)
}

// ---------- TESTS ----------

func TestRDARetrieval_LocalRetrievalSuccessful(t *testing.T) {
	t.Log("========== TEST: Local Retrieval Successful ==========")

	// Setup: Local node with column 5
	localConfig := RDAStorageConfig{
		MyRow:    2,
		MyCol:    5,
		GridSize: 256,
	}
	localStorage := NewRDAStorage(localConfig)
	ctx := context.Background()

	// Store a share locally
	share := &RDAShare{
		Row:      10,
		Col:      5,
		SymbolID: 5, // 5 % 256 = 5
		Data:     []byte("test_data_local"),
		Height:   100,
	}
	err := localStorage.StoreShare(ctx, share)
	require.NoError(t, err)
	t.Log("✓ Share stored locally")

	// Setup retrieval service
	retrievalConfig := RDARetrievalConfig{
		MyRow:    2,
		MyCol:    5,
		GridSize: 256,
	}
	resolver := NewMockColumnPeerResolver()
	retrieval := NewRDARetrieval(retrievalConfig, localStorage, resolver)
	t.Log("✓ Retrieval service initialized")

	// Create retrieval request
	req := &RetrievalRequest{
		Height:   100,
		Row:      10,
		Col:      5,
		SymbolID: 5,
	}

	// Retrieve the share
	t.Log("TEST: Retrieving share from local storage")
	data, peerID, err := retrieval.GetShare(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, []byte("test_data_local"), data)
	assert.Equal(t, "local", peerID)
	t.Logf("✓ Local retrieval successful: peerID=%s, data_size=%d bytes\n", peerID, len(data))
}

func TestRDARetrieval_RemoteRetrievalSinglePeer(t *testing.T) {
	t.Log("========== TEST: Remote Retrieval (Single Peer) ==========")

	// Setup: Remote node (column 42) with storage
	remoteConfig := RDAStorageConfig{
		MyRow:    5,
		MyCol:    42,
		GridSize: 256,
	}
	remoteStorage := NewRDAStorage(remoteConfig)
	ctx := context.Background()

	// Store share in remote node
	remoteShare := &RDAShare{
		Row:      15,
		Col:      42,
		SymbolID: 42, // 42 % 256 = 42
		Data:     []byte("remote_share_data"),
		Height:   101,
	}
	err := remoteStorage.StoreShare(ctx, remoteShare)
	require.NoError(t, err)
	t.Log("✓ Share stored in remote node")

	// Setup: Local node (different column)
	localConfig := RDAStorageConfig{
		MyRow:    3,
		MyCol:    10,
		GridSize: 256,
	}
	localStorage := NewRDAStorage(localConfig)

	// Setup peer resolver
	resolver := NewMockColumnPeerResolver()
	resolver.addPeerStorage("peer_col42", remoteStorage)
	resolver.addColumnPeer(42, "peer_col42")
	t.Log("✓ Peer resolver setup: peer_col42 in column 42")

	// Setup retrieval service on local node
	retrievalConfig := RDARetrievalConfig{
		MyRow:    3,
		MyCol:    10,
		GridSize: 256,
	}
	retrieval := NewRDARetrieval(retrievalConfig, localStorage, resolver)

	// Create retrieval request for remote data
	req := &RetrievalRequest{
		Height:   101,
		Row:      15,
		Col:      42,
		SymbolID: 42,
	}

	t.Log("TEST: Attempting remote retrieval from column 42")
	data, peerID, err := retrieval.GetShare(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, []byte("remote_share_data"), data)
	assert.Equal(t, "peer_col42", peerID)
	t.Logf("✓ Remote retrieval successful: peerID=%s, hop=1, data_size=%d bytes\n", peerID, len(data))
}

func TestRDARetrieval_RemoteRetrievalMultiplePeers(t *testing.T) {
	t.Log("========== TEST: Remote Retrieval (Multiple Peers - Fallback) ==========")

	// Setup: Multiple remote nodes in column 7
	remoteConfig := RDAStorageConfig{
		MyRow:    1,
		MyCol:    7,
		GridSize: 256,
	}
	remoteStorage := NewRDAStorage(remoteConfig)
	ctx := context.Background()

	// Store share in remote
	remoteShare := &RDAShare{
		Row:      20,
		Col:      7,
		SymbolID: 7, // 7 % 256 = 7
		Data:     []byte("data_from_peer2"),
		Height:   102,
	}
	err := remoteStorage.StoreShare(ctx, remoteShare)
	require.NoError(t, err)
	t.Log("✓ Share stored in remote storage")

	// Setup: Local node
	localConfig := RDAStorageConfig{
		MyRow:    4,
		MyCol:    20,
		GridSize: 256,
	}
	localStorage := NewRDAStorage(localConfig)

	// Setup peer resolver with multiple peers in column 7
	resolver := NewMockColumnPeerResolver()
	resolver.addPeerStorage("peer2_col7", remoteStorage)
	resolver.addColumnPeer(7, "peer1_col7") // This one will fail
	resolver.addColumnPeer(7, "peer2_col7") // This one succeeds
	resolver.failPeer = "peer1_col7"        // Make first peer fail
	t.Log("✓ Peer resolver setup: multiple peers in column 7 (peer1 fails, peer2 succeeds)")

	// Setup retrieval
	retrievalConfig := RDARetrievalConfig{
		MyRow:    4,
		MyCol:    20,
		GridSize: 256,
	}
	retrieval := NewRDARetrieval(retrievalConfig, localStorage, resolver)

	// Retrieve
	req := &RetrievalRequest{
		Height:   102,
		Row:      20,
		Col:      7,
		SymbolID: 7,
	}

	t.Log("TEST: Attempting retrieval with fallback (peer1 fails, peer2 succeeds)")
	data, peerID, err := retrieval.GetShare(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, []byte("data_from_peer2"), data)
	assert.Equal(t, "peer2_col7", peerID)
	t.Log("✓ Successfully retrieved from peer2 after peer1 failed")
	t.Log("✓ Test PASSED: Fallback mechanism works\n")
}

func TestRDARetrieval_VerifyRetrievalRequest(t *testing.T) {
	t.Log("========== TEST: Retrieval Request Verification ==========")

	config := RDARetrievalConfig{
		MyRow:    5,
		MyCol:    15,
		GridSize: 256,
	}
	resolver := NewMockColumnPeerResolver()
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 5, MyCol: 15, GridSize: 256})
	retrieval := NewRDARetrieval(config, storage, resolver)

	// Test 1: Valid request
	t.Log("TEST: Valid request (symbolID=15 → col=15)")
	req := &RetrievalRequest{
		Height:   100,
		Row:      10,
		Col:      15,
		SymbolID: 15,
	}
	err := retrieval.VerifyRetrievalRequest(req)
	assert.NoError(t, err)
	t.Log("  ✓ ACCEPTED")

	// Test 2: Invalid grid position (row too large)
	t.Log("TEST: Invalid row (row=256 ≥ grid_size=256)")
	req = &RetrievalRequest{
		Height:   100,
		Row:      256,
		Col:      15,
		SymbolID: 15,
	}
	err = retrieval.VerifyRetrievalRequest(req)
	assert.Error(t, err)
	t.Logf("  ✓ REJECTED: %v", err)

	// Test 3: Symbol→column mapping violation
	t.Log("TEST: Symbol mapping mismatch (symbolID=20 → col=20, but request col=15)")
	req = &RetrievalRequest{
		Height:   100,
		Row:      10,
		Col:      15, // Node owns column 15
		SymbolID: 20, // But symbol 20 maps to column 20
	}
	err = retrieval.VerifyRetrievalRequest(req)
	assert.Error(t, err)
	t.Logf("  ✓ REJECTED: %v", err)

	// Test 4: Column not owned by this node
	t.Log("TEST: Requested column not owned (symbolID=50 → col=50, but node is col=15)")
	req = &RetrievalRequest{
		Height:   100,
		Row:      10,
		Col:      50, // A different column
		SymbolID: 50,
	}
	err = retrieval.VerifyRetrievalRequest(req)
	assert.Error(t, err)
	t.Logf("  ✓ REJECTED: %v\n", err)
}

func TestRDARetrieval_HandleRetrievalRequest(t *testing.T) {
	t.Log("========== TEST: Handle Incoming Retrieval Request ==========")

	// Setup server node (column 8) with data
	serverConfig := RDAStorageConfig{
		MyRow:    3,
		MyCol:    8,
		GridSize: 256,
	}
	serverStorage := NewRDAStorage(serverConfig)
	ctx := context.Background()

	// Store share
	share := &RDAShare{
		Row:      25,
		Col:      8,
		SymbolID: 8 + 256, // 264 % 256 = 8
		Data:     []byte("handler_test_data"),
		Height:   105,
	}
	err := serverStorage.StoreShare(ctx, share)
	require.NoError(t, err)
	t.Log("✓ Share stored for incoming requests")

	// Setup retrieval server
	retrievalConfig := RDARetrievalConfig{
		MyRow:    3,
		MyCol:    8,
		GridSize: 256,
	}
	resolver := NewMockColumnPeerResolver()
	retrieval := NewRDARetrieval(retrievalConfig, serverStorage, resolver)

	// Test 1: Valid request
	t.Log("TEST: Valid incoming request")
	req := &RetrievalRequest{
		Height:   105,
		Row:      25,
		Col:      8,
		SymbolID: 264,
	}
	resp := retrieval.HandleRetrievalRequest(ctx, "client_peer_1", req)
	assert.True(t, resp.Success)
	assert.Equal(t, []byte("handler_test_data"), resp.Data)
	t.Logf("  ✓ ACCEPTED: data_size=%d bytes", len(resp.Data))

	// Test 2: Invalid request (wrong column)
	t.Log("TEST: Invalid incoming request (wrong column)")
	req = &RetrievalRequest{
		Height:   105,
		Row:      25,
		Col:      10, // Node owns column 8, not 10
		SymbolID: 10,
	}
	resp = retrieval.HandleRetrievalRequest(ctx, "client_peer_2", req)
	assert.False(t, resp.Success)
	t.Logf("  ✓ REJECTED: %s", resp.Error)

	// Test 3: Missing data
	t.Log("TEST: Incoming request for non-existent share")
	req = &RetrievalRequest{
		Height:   999, // Non-existent height
		Row:      25,
		Col:      8,
		SymbolID: 8,
	}
	resp = retrieval.HandleRetrievalRequest(ctx, "client_peer_3", req)
	assert.False(t, resp.Success)
	t.Logf("  ✓ REJECTED: %s\n", resp.Error)
}

func TestRDARetrieval_CalculateTargetColumn(t *testing.T) {
	t.Log("========== TEST: Calculate Target Column ==========")

	config := RDARetrievalConfig{
		MyRow:    0,
		MyCol:    0,
		GridSize: 256,
	}
	resolver := NewMockColumnPeerResolver()
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 0, GridSize: 256})
	retrieval := NewRDARetrieval(config, storage, resolver)

	tests := []struct {
		symbolID   uint32
		expected   uint32
		description string
	}{
		{0, 0, "symbolID=0 → col=0"},
		{7, 7, "symbolID=7 → col=7"},
		{256, 0, "symbolID=256 → col=0 (wrap around)"},
		{257, 1, "symbolID=257 → col=1"},
		{512, 0, "symbolID=512 → col=0 (2*K)"},
		{255, 255, "symbolID=255 → col=255 (max single row)"},
	}

	for _, tc := range tests {
		t.Logf("TEST: %s", tc.description)
		col := retrieval.CalculateTargetColumn(tc.symbolID)
		assert.Equal(t, tc.expected, col)
		t.Log("  ✓ Correct mapping")
	}
	t.Log()
}

func TestRDARetrieval_Stats(t *testing.T) {
	t.Log("========== TEST: Retrieval Statistics ==========")

	config := RDARetrievalConfig{
		MyRow:    2,
		MyCol:    10,
		GridSize: 256,
	}
	resolver := NewMockColumnPeerResolver()
	storage := NewRDAStorage(RDAStorageConfig{MyRow: 2, MyCol: 10, GridSize: 256})
	retrieval := NewRDARetrieval(config, storage, resolver)

	// Store a share for testing
	ctx := context.Background()
	share := &RDAShare{
		Row:      5,
		Col:      10,
		SymbolID: 10,
		Data:     []byte("test"),
		Height:   1,
	}
	_ = storage.StoreShare(ctx, share)

	// Do some retrievals
	req := &RetrievalRequest{
		Height:   1,
		Row:      5,
		Col:      10,
		SymbolID: 10,
	}
	retrieval.GetShare(ctx, req)

	// Get stats
	t.Log("TEST: Checking retrieval statistics")
	stats := retrieval.GetStats()

	nodePos := stats["node_position"].(map[string]uint32)
	assert.Equal(t, uint32(2), nodePos["row"])
	assert.Equal(t, uint32(10), nodePos["col"])

	retrievalStats := stats["retrieval_stats"].(map[string]interface{})
	assert.Equal(t, int64(1), retrievalStats["total_attempts"])
	assert.Equal(t, int64(1), retrievalStats["total_successes"])
	assert.Equal(t, int64(1), retrievalStats["local_retrievals"])

	t.Logf("✓ Stats correct: attempts=%d, successes=%d, local=%d",
		retrievalStats["total_attempts"],
		retrievalStats["total_successes"],
		retrievalStats["local_retrievals"],
	)
	t.Log("✓ Test PASSED: Statistics collection works\n")
}

func TestRDARetrieval_1HopProtocol(t *testing.T) {
	t.Log("========== TEST: 1-Hop Retrieval Protocol ==========")
	t.Log("Simulating RDA 1-hop retrieval as per paper requirement")

	// Setup: 3-node network
	// Node1: (row=0, col=0) - Light client
	// Node2: (row=1, col=5) - Column 5 owner
	// Node3: (row=2, col=5) - Column 5 owner (backup)

	ctx := context.Background()

	// Setup Node2 (column 5 with data)
	node2Config := RDAStorageConfig{MyRow: 1, MyCol: 5, GridSize: 256}
	node2Storage := NewRDAStorage(node2Config)
	node2Share := &RDAShare{
		Row:      10,
		Col:      5,
		SymbolID: 5,
		Data:     []byte("symbol_5_data"),
		Height:   50,
	}
	_ = node2Storage.StoreShare(ctx, node2Share)
	t.Log("✓ Node2 (row=1, col=5): stored symbol 5")

	// Setup Node3 (column 5 backup)
	node3Config := RDAStorageConfig{MyRow: 2, MyCol: 5, GridSize: 256}
	node3Storage := NewRDAStorage(node3Config)
	node3Share := &RDAShare{
		Row:      10,
		Col:      5,
		SymbolID: 5 + 256, // 261 % 256 = 5
		Data:     []byte("symbol_261_data"),
		Height:   50,
	}
	_ = node3Storage.StoreShare(ctx, node3Share)
	t.Log("✓ Node3 (row=2, col=5): stored symbol 261")

	// Setup Node1 (light client, column 0)
	node1Config := RDARetrievalConfig{MyRow: 0, MyCol: 0, GridSize: 256}
	node1Storage := NewRDAStorage(RDAStorageConfig{MyRow: 0, MyCol: 0, GridSize: 256})
	resolver := NewMockColumnPeerResolver()
	resolver.addPeerStorage("node2", node2Storage)
	resolver.addPeerStorage("node3", node3Storage)
	resolver.addColumnPeer(5, "node2")
	resolver.addColumnPeer(5, "node3")

	node1Retrieval := NewRDARetrieval(node1Config, node1Storage, resolver)
	t.Log("✓ Node1 (light client): setup complete")

	// Simulate light client requesting symbols
	t.Log("\n--- Light Client Sampling Phase ---")

	// Request 1: Get symbol 5 from column 5
	t.Log("REQUEST 1: Light client requests symbol 5 (maps to column 5)")
	req1 := &RetrievalRequest{Height: 50, Row: 10, Col: 5, SymbolID: 5}
	data1, peer1, err1 := node1Retrieval.GetShare(ctx, req1)
	require.NoError(t, err1)
	assert.Equal(t, []byte("symbol_5_data"), data1)
	assert.Equal(t, "node2", peer1)
	t.Logf("  ✓ Retrieved in 1 hop from peer: %s", peer1)

	// Request 2: Get symbol 261 from column 5
	t.Log("REQUEST 2: Light client requests symbol 261 (maps to column 5)")
	req2 := &RetrievalRequest{Height: 50, Row: 10, Col: 5, SymbolID: 261}
	data2, peer2, err2 := node1Retrieval.GetShare(ctx, req2)
	require.NoError(t, err2)
	assert.Equal(t, []byte("symbol_261_data"), data2)
	assert.Equal(t, "node3", peer2)
	t.Logf("  ✓ Retrieved in 1 hop from peer: %s", peer2)

	t.Log("\n✓ Test PASSED: 1-hop retrieval protocol working as per paper\n")
}
