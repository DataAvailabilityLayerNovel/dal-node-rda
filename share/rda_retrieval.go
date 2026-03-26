package share

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var rdaRetrievalLog = logging.Logger("rda.retrieval")

// requestIDCounter is used to generate unique request IDs
var requestIDCounter int64 = 0

// generateRequestID creates a unique request ID for tracing
func generateRequestID() string {
	id := atomic.AddInt64(&requestIDCounter, 1)
	return fmt.Sprintf("req-%d-%d", time.Now().UnixMilli(), id)
}

// RDARetrievalConfig holds RDA retrieval configuration
type RDARetrievalConfig struct {
	MyRow    uint32 // This node's row position
	MyCol    uint32 // This node's column position
	GridSize uint32 // Grid size K (e.g., 256)
}

// ColumnPeerResolver resolves which peers are in a target column
// This interface allows plugging in different peer discovery mechanisms
type ColumnPeerResolver interface {
	// GetColumnPeers returns list of peer IDs in the target column
	GetColumnPeers(ctx context.Context, targetCol uint32) ([]string, error)
	// SendRetrievalRequest sends a request to retrieve a share from a remote peer
	SendRetrievalRequest(ctx context.Context, peerID string, req *RetrievalRequest) (*RetrievalResponse, error)
}

// RetrievalRequest represents a request to retrieve a share
type RetrievalRequest struct {
	Height    uint64 // Block height
	Row       uint32 // Row position
	Col       uint32 // Column position
	SymbolID  uint32 // Symbol ID
	RequestID string // Unique request ID for tracing (optional)
	Timestamp int64  // Request timestamp (optional)
}

// RetrievalResponse represents a response to a retrieval request
type RetrievalResponse struct {
	Data      []byte // The share data
	Success   bool   // Whether retrieval was successful
	Error     string // Error message if unsuccessful
	PeerID    string // Which peer provided the data
	Timestamp int64  // Response timestamp
}

// RDARetrieval implements the RDA share retrieval protocol
//
// Protocol Overview:
// 1. Client requests symbol S from height H, position (r, c)
// 2. Calculate destination column: col = symbolID % GridSize
// 3. Discover peers in that column
// 4. Send retrieval request (with validation)
// 5. Receive share from column owner
// 6. Validate and return
//
// This achieves 1-2 hop retrieval:
// - 1 hop: Direct connection to column peer (if not local)
// - 2 hops: Via bootstrap node to column peer
type RDARetrieval struct {
	config   RDARetrievalConfig
	storage  *RDAStorage
	resolver ColumnPeerResolver
	metrics  *RDAMetrics // RDA metrics recorder
	mu       sync.RWMutex

	// Metrics
	retrievalAttempts  int64
	retrievalSuccesses int64
	retrievalFailures  int64
	localRetrievals    int64
}

// NewRDARetrieval creates a new RDA retrieval service
func NewRDARetrieval(config RDARetrievalConfig, storage *RDAStorage, resolver ColumnPeerResolver) *RDARetrieval {
	rdaRetrievalLog.Infof(
		"RDA Retrieval initialized: node at (row=%d, col=%d), grid_size=%d",
		config.MyRow, config.MyCol, config.GridSize,
	)

	metrics, _ := InitRDAMetrics() // Initialize RDA metrics
	return &RDARetrieval{
		config:   config,
		storage:  storage,
		resolver: resolver,
		metrics:  metrics,
	}
}

// CalculateTargetColumn computes which column should hold symbol S
func (r *RDARetrieval) CalculateTargetColumn(symbolID uint32) uint32 {
	targetCol := symbolID % r.config.GridSize
	rdaRetrievalLog.Debugf(
		"RDA Retrieval: calculated target column - symbolID=%d → column=%d (grid_size=%d)",
		symbolID, targetCol, r.config.GridSize,
	)
	return targetCol
}

// IsLocalColumn checks if this node owns the target column
func (r *RDARetrieval) IsLocalColumn(targetCol uint32) bool {
	isLocal := targetCol == r.config.MyCol
	rdaRetrievalLog.Debugf(
		"RDA Retrieval: column check - target_col=%d, my_col=%d, is_local=%v",
		targetCol, r.config.MyCol, isLocal,
	)
	return isLocal
}

// VerifyRetrievalRequest validates a retrieval request
//
// Checks:
// 1. Grid position is valid
// 2. Symbol maps to correct column
// 3. Target column is this node's column (for local retrieval)
func (r *RDARetrieval) VerifyRetrievalRequest(req *RetrievalRequest) error {
	rdaRetrievalLog.Debugf(
		"RDA Retrieval: VerifyRetrievalRequest START - height=%d, row=%d, col=%d, symbolID=%d",
		req.Height, req.Row, req.Col, req.SymbolID,
	)

	// Check grid position
	if req.Row >= r.config.GridSize {
		return fmt.Errorf("row %d exceeds grid size %d", req.Row, r.config.GridSize)
	}
	if req.Col >= r.config.GridSize {
		return fmt.Errorf("col %d exceeds grid size %d", req.Col, r.config.GridSize)
	}

	// Check symbol→column mapping
	expectedCol := req.SymbolID % r.config.GridSize
	if expectedCol != req.Col {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: VerifyRetrievalRequest FAILED - symbol mapping mismatch: symbolID=%d → col=%d, but request col=%d",
			req.SymbolID, expectedCol, req.Col,
		)
		return fmt.Errorf(
			"symbol %d maps to column %d, but request specifies column %d",
			req.SymbolID, expectedCol, req.Col,
		)
	}

	// Check if we own this column
	if req.Col != r.config.MyCol {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: VerifyRetrievalRequest FAILED - column mismatch: request_col=%d, my_col=%d",
			req.Col, r.config.MyCol,
		)
		return fmt.Errorf(
			"this node is column %d, cannot serve column %d",
			r.config.MyCol, req.Col,
		)
	}

	rdaRetrievalLog.Debugf(
		"RDA Retrieval: VerifyRetrievalRequest SUCCESS - all validations passed ✓",
	)
	return nil
}

// RetrieveShareLocal retrieves a share locally from this node's storage
//
// Used when this node owns the target column.
func (r *RDARetrieval) RetrieveShareLocal(ctx context.Context, req *RetrievalRequest) ([]byte, error) {
	r.mu.Lock()
	r.retrievalAttempts++
	r.localRetrievals++
	r.mu.Unlock()

	rdaRetrievalLog.Infof(
		"RDA Retrieval: LOCAL retrieval START [%s] - height=%d, (row=%d, col=%d), symbolID=%d",
		req.RequestID, req.Height, req.Row, req.Col, req.SymbolID,
	)

	// Verify request is valid for this node
	if err := r.VerifyRetrievalRequest(req); err != nil {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: LOCAL retrieval [%s] FAILED - validation error: %v",
			req.RequestID, err,
		)
		r.mu.Lock()
		r.retrievalFailures++
		r.mu.Unlock()
		return nil, err
	}

	// Retrieve from local storage
	data, err := r.storage.GetShare(ctx, req.Height, req.Row, req.Col, req.SymbolID)
	if err != nil {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: LOCAL retrieval [%s] FAILED - storage error: %v",
			req.RequestID, err,
		)
		r.mu.Lock()
		r.retrievalFailures++
		r.mu.Unlock()
		return nil, err
	}

	r.mu.Lock()
	r.retrievalSuccesses++
	r.mu.Unlock()

	latency := time.Now().UnixMilli() - req.Timestamp
	latencyMs := float64(latency)
	
	// Record metrics
	if r.metrics != nil {
		ctx := context.Background()
		r.metrics.RecordDirectFetchLatency(ctx, latencyMs)
		r.metrics.RecordDirectRecoveryBytes(ctx, int64(len(data)))
		r.metrics.RecordErasureRecoveryAttemptWithCounters(ctx, true)
	}
	
	rdaRetrievalLog.Infof(
		"RDA Retrieval: LOCAL retrieval [%s] SUCCESS - height=%d, (row=%d, col=%d), symbolID=%d, data_size=%d bytes, latency=%dms ✓",
		req.RequestID, req.Height, req.Row, req.Col, req.SymbolID, len(data), latency,
	)

	return data, nil
}

// RetrieveShareRemote retrieves a share from remote column-owning peers
//
// Protocol:
// 1. Discover peers in target column
// 2. Send retrieval request to each peer until success
// 3. Validate response
// 4. Return data
//
// This achieves 1 hop from client to column owner (or 2 hops via bootstrap)
func (r *RDARetrieval) RetrieveShareRemote(ctx context.Context, req *RetrievalRequest) ([]byte, string, error) {
	r.mu.Lock()
	r.retrievalAttempts++
	r.mu.Unlock()

	rdaRetrievalLog.Infof(
		"RDA Retrieval: REMOTE retrieval START [%s] - height=%d, (row=%d, col=%d), symbolID=%d, target_column=%d",
		req.RequestID, req.Height, req.Row, req.Col, req.SymbolID, req.Col,
	)

	// Step 1: Discover peers in target column
	rdaRetrievalLog.Debugf(
		"RDA Retrieval: REMOTE retrieval [%s] - discovering peers in column %d",
		req.RequestID, req.Col,
	)

	peers, err := r.resolver.GetColumnPeers(ctx, req.Col)
	if err != nil {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: REMOTE retrieval [%s] FAILED - peer discovery error: %v",
			req.RequestID, err,
		)
		r.mu.Lock()
		r.retrievalFailures++
		r.mu.Unlock()
		return nil, "", fmt.Errorf("failed to discover peers in column %d: %w", req.Col, err)
	}

	if len(peers) == 0 {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: REMOTE retrieval [%s] FAILED - no peers found in column %d",
			req.RequestID, req.Col,
		)
		r.mu.Lock()
		r.retrievalFailures++
		r.mu.Unlock()
		return nil, "", fmt.Errorf("no peers available in column %d", req.Col)
	}

	rdaRetrievalLog.Debugf(
		"RDA Retrieval: REMOTE retrieval [%s] - discovered %d peers in column %d: %v",
		req.RequestID, len(peers), req.Col, peers,
	)

	// Step 2: Try each peer until success
	var lastErr error
	for i, peerID := range peers {
		rdaRetrievalLog.Debugf(
			"RDA Retrieval: REMOTE retrieval [%s] - attempt %d/%d on peer %s",
			req.RequestID, i+1, len(peers), peerID,
		)

		// Send retrieval request to peer
		response, err := r.resolver.SendRetrievalRequest(ctx, peerID, req)
		if err != nil {
			rdaRetrievalLog.Warnf(
				"RDA Retrieval: REMOTE retrieval [%s] - peer %s failed: %v",
				req.RequestID, peerID, err,
			)
			lastErr = err
			continue
		}

		// Check response success
		if !response.Success {
			rdaRetrievalLog.Warnf(
				"RDA Retrieval: REMOTE retrieval [%s] - peer %s returned error: %s",
				req.RequestID, peerID, response.Error,
			)
			lastErr = fmt.Errorf("peer %s: %s", peerID, response.Error)
			continue
		}

		// Validate response data
		if len(response.Data) == 0 {
			rdaRetrievalLog.Warnf(
				"RDA Retrieval: REMOTE retrieval [%s] - peer %s returned empty data",
				req.RequestID, peerID,
			)
			lastErr = fmt.Errorf("peer %s returned empty data", peerID)
			continue
		}

		// Success!
		r.mu.Lock()
		r.retrievalSuccesses++
		r.mu.Unlock()

		latency := time.Now().UnixMilli() - req.Timestamp
		latencyMs := float64(latency)
		
		// Record metrics
		if r.metrics != nil {
			ctx := context.Background()
			r.metrics.RecordDirectFetchLatency(ctx, latencyMs)
			r.metrics.RecordDirectRecoveryBytes(ctx, int64(len(response.Data)))
			r.metrics.RecordErasureRecoveryAttemptWithCounters(ctx, true)
		}
		
		rdaRetrievalLog.Infof(
			"RDA Retrieval: REMOTE retrieval SUCCESS [%s] - height=%d, (row=%d, col=%d), symbolID=%d, data_size=%d bytes, from peer %s ✓ (latency=%dms, hop=1)",
			req.RequestID, req.Height, req.Row, req.Col, req.SymbolID, len(response.Data), peerID, latency,
		)

		return response.Data, peerID, nil
	}

	// All peers failed
	r.mu.Lock()
	r.retrievalFailures++
	r.mu.Unlock()

	// Record failed recovery attempt
	if r.metrics != nil {
		ctx := context.Background()
		r.metrics.RecordErasureRecoveryAttemptWithCounters(ctx, false)
	}

	rdaRetrievalLog.Warnf(
		"RDA Retrieval: REMOTE retrieval FAILED - all %d peers failed, last error: %v",
		len(peers), lastErr,
	)

	return nil, "", fmt.Errorf("all %d peers failed, last: %w", len(peers), lastErr)
}

// GetShare is the main entry point for retrieving a share
//
// Smart routing:
// - If target column is local: retrieve from local storage (0 hops within node)
// - Otherwise: retrieve from remote column-owning peers (1 hop)
//
// Returns: (data, peerID, error)
// - If local: peerID = "local"
// - If remote: peerID = remote peer ID
func (r *RDARetrieval) GetShare(ctx context.Context, req *RetrievalRequest) ([]byte, string, error) {
	// Generate request ID if not provided
	if req.RequestID == "" {
		req.RequestID = generateRequestID()
	}
	if req.Timestamp == 0 {
		req.Timestamp = time.Now().UnixMilli()
	}

	rdaRetrievalLog.Infof(
		"RDA Retrieval: GetShare START [%s] - height=%d, (row=%d, col=%d), symbolID=%d, timestamp=%d",
		req.RequestID, req.Height, req.Row, req.Col, req.SymbolID, req.Timestamp,
	)

	// Calculate target column
	targetCol := r.CalculateTargetColumn(req.SymbolID)

	// Check if we own this column
	if r.IsLocalColumn(targetCol) {
		rdaRetrievalLog.Infof(
			"RDA Retrieval: GetShare [%s] - routing to LOCAL storage (this node is column %d)",
			req.RequestID, targetCol,
		)

		data, err := r.RetrieveShareLocal(ctx, req)
		if err != nil {
			rdaRetrievalLog.Warnf("RDA Retrieval: GetShare [%s] FAILED - local retrieval error: %v", req.RequestID, err)
			return nil, "", err
		}

		return data, "local", nil
	}

	// Route to remote column peer
	rdaRetrievalLog.Infof(
		"RDA Retrieval: GetShare [%s] - routing to REMOTE peers in column %d (1 hop expected)",
		req.RequestID, targetCol,
	)

	data, peerID, err := r.RetrieveShareRemote(ctx, req)
	if err != nil {
		rdaRetrievalLog.Warnf("RDA Retrieval: GetShare [%s] FAILED - remote retrieval error: %v", req.RequestID, err)
		return nil, "", err
	}

	latency := time.Now().UnixMilli() - req.Timestamp
	rdaRetrievalLog.Infof(
		"RDA Retrieval: GetShare [%s] SUCCESS - peerID=%s, data_size=%d bytes, latency=%dms ✓",
		req.RequestID, peerID, len(data), latency,
	)

	return data, peerID, nil
}

// HandleRetrievalRequest handles an incoming retrieval request from another peer
//
// This node acts as a server for the requesting peer.
// Validates request and returns share from local storage if valid.
func (r *RDARetrieval) HandleRetrievalRequest(ctx context.Context, peerID string, req *RetrievalRequest) *RetrievalResponse {
	// Use request ID if provided, otherwise create one
	if req.RequestID == "" {
		req.RequestID = generateRequestID()
	}
	if req.Timestamp == 0 {
		req.Timestamp = time.Now().UnixMilli()
	}

	rdaRetrievalLog.Infof(
		"RDA Retrieval: HandleRetrievalRequest START [%s] - from peer=%s, height=%d, (row=%d, col=%d), symbolID=%d",
		req.RequestID, peerID[:8], req.Height, req.Row, req.Col, req.SymbolID,
	)

	// Validate request
	if err := r.VerifyRetrievalRequest(req); err != nil {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: HandleRetrievalRequest [%s] REJECTED - peer=%s, reason: %v",
			req.RequestID, peerID[:8], err,
		)

		return &RetrievalResponse{
			Success:   false,
			Error:     fmt.Sprintf("request validation failed: %v", err),
			PeerID:    peerID,
			Timestamp: time.Now().UnixMilli(),
		}
	}

	// Retrieve from local storage
	data, err := r.storage.GetShare(ctx, req.Height, req.Row, req.Col, req.SymbolID)
	if err != nil {
		rdaRetrievalLog.Warnf(
			"RDA Retrieval: HandleRetrievalRequest [%s] FAILED - peer=%s, storage error: %v",
			req.RequestID, peerID[:8], err,
		)

		return &RetrievalResponse{
			Success:   false,
			Error:     fmt.Sprintf("storage error: %v", err),
			PeerID:    peerID,
			Timestamp: time.Now().UnixMilli(),
		}
	}

	latency := time.Now().UnixMilli() - req.Timestamp
	rdaRetrievalLog.Infof(
		"RDA Retrieval: HandleRetrievalRequest [%s] SUCCESS - peer=%s, height=%d, (row=%d, col=%d), symbolID=%d, data_size=%d bytes, latency=%dms ✓",
		req.RequestID, peerID[:8], req.Height, req.Row, req.Col, req.SymbolID, len(data), latency,
	)

	return &RetrievalResponse{
		Data:      data,
		Success:   true,
		PeerID:    peerID,
		Timestamp: time.Now().UnixMilli(),
	}
}

// GetStats returns retrieval statistics
func (r *RDARetrieval) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := map[string]interface{}{
		"node_position": map[string]uint32{
			"row":       r.config.MyRow,
			"col":       r.config.MyCol,
			"grid_size": r.config.GridSize,
		},
		"retrieval_stats": map[string]interface{}{
			"total_attempts":   r.retrievalAttempts,
			"total_successes":  r.retrievalSuccesses,
			"total_failures":   r.retrievalFailures,
			"local_retrievals": r.localRetrievals,
			"success_rate":     calculateRate(r.retrievalSuccesses, r.retrievalAttempts),
		},
	}

	rdaRetrievalLog.Infof(
		"RDA Retrieval: stats - node=(row=%d,col=%d), attempts=%d, success=%d, failures=%d, local=%d, rate=%.2f%%",
		r.config.MyRow, r.config.MyCol,
		r.retrievalAttempts, r.retrievalSuccesses, r.retrievalFailures,
		r.localRetrievals,
		calculateRate(r.retrievalSuccesses, r.retrievalAttempts)*100,
	)

	return stats
}

// Helper function to calculate success rate
func calculateRate(successes, attempts int64) float64 {
	if attempts == 0 {
		return 0.0
	}
	return float64(successes) / float64(attempts)
}
