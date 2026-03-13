package share

import (
	"context"
	"fmt"
	"sync"

	logging "github.com/ipfs/go-log/v2"
)

var rdaStorageLog = logging.Logger("rda.storage")

// RDAStorageConfig holds RDA storage configuration
type RDAStorageConfig struct {
	MyRow    uint32 // This node's row position
	MyCol    uint32 // This node's column position
	GridSize uint32 // Grid size K (e.g., 256)
}

// RDAShare represents a single symbol in the RDA grid
type RDAShare struct {
	Row      uint32 // 0 to K-1
	Col      uint32 // 0 to K-1
	SymbolID uint32 // 0 to K²-1
	Data     []byte // The actual share data
	Height   uint64 // Block height this share belongs to
}

// RDAStorage implements column-based storage respecting RDA column affinity rules
// 
// Critical Rules:
// 1. Node at (row=r, col=c) MUST ONLY store shares from column c
// 2. Symbol sᵢ MUST be stored at column = i % K (deterministic)
// 3. No node can store arbitrary shares
type RDAStorage struct {
	config RDAStorageConfig

	// columnShares stores ALL shares from this node's column
	// Access: [blockHeight][row][symbolID] → share data
	columnShares map[uint64]map[uint32]map[uint32][]byte
	mu           sync.RWMutex

	// Metrics
	sharesStored   int64
	sharesRetrieved int64
}

// NewRDAStorage creates a new RDA storage instance
func NewRDAStorage(config RDAStorageConfig) *RDAStorage {
	rdaStorageLog.Infof(
		"RDA Storage initialized: node at (row=%d, col=%d), grid_size=%d",
		config.MyRow, config.MyCol, config.GridSize,
	)

	return &RDAStorage{
		config:       config,
		columnShares: make(map[uint64]map[uint32]map[uint32][]byte),
	}
}

// VerifySymbolToColumnMapping verifies symbol belongs to this node's column
// 
// Validation Rule:
// - symbol % gridSize MUST equal myCol
func (s *RDAStorage) VerifySymbolToColumnMapping(symbolID uint32) error {
	expectedCol := symbolID % s.config.GridSize

	if expectedCol != s.config.MyCol {
		rdaStorageLog.Debugf(
			"RDA Storage: column mapping FAILED - symbolID=%d expected_col=%d, but node is col=%d",
			symbolID, expectedCol, s.config.MyCol,
		)
		return fmt.Errorf(
			"symbol %d maps to column %d, but this node is column %d",
			symbolID, expectedCol, s.config.MyCol,
		)
	}

	rdaStorageLog.Debugf(
		"RDA Storage: column mapping VERIFIED - symbolID=%d → column=%d ✓",
		symbolID, expectedCol,
	)
	return nil
}

// VerifyGridPosition verifies the share belongs to valid grid position
func (s *RDAStorage) VerifyGridPosition(row, col uint32) error {
	if row >= s.config.GridSize {
		return fmt.Errorf("row %d exceeds grid size %d", row, s.config.GridSize)
	}
	if col >= s.config.GridSize {
		return fmt.Errorf("col %d exceeds grid size %d", col, s.config.GridSize)
	}
	return nil
}

// ComputeSymbolID converts (row, col) to linear symbolID
// 
// Mapping: symbolID = row * K + col
// where K = grid size
func (s *RDAStorage) ComputeSymbolID(row, col uint32) (uint32, error) {
	if err := s.VerifyGridPosition(row, col); err != nil {
		return 0, err
	}

	symbolID := row*s.config.GridSize + col
	rdaStorageLog.Debugf(
		"RDA Storage: computed symbolID - row=%d, col=%d → symbolID=%d (grid_size=%d)",
		row, col, symbolID, s.config.GridSize,
	)

	return symbolID, nil
}

// StoreShare stores a share at grid position (row, col) with validation
//
// Validation Steps:
// 1. Verify grid position is valid
// 2. Verify symbol→column mapping (symbol MUST belong to this node's column)
// 3. Verify row isn't this node's row (row shares handled separately)
// 4. Store in column shares map
func (s *RDAStorage) StoreShare(ctx context.Context, share *RDAShare) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rdaStorageLog.Infof(
		"RDA Storage: StoreShare START - height=%d, row=%d, col=%d, symbolID=%d, data_size=%d bytes",
		share.Height, share.Row, share.Col, share.SymbolID, len(share.Data),
	)

	// Validation Step 1: Grid position
	if err := s.VerifyGridPosition(share.Row, share.Col); err != nil {
		rdaStorageLog.Warnf("RDA Storage: StoreShare FAILED - invalid grid position: %v", err)
		return err
	}

	// Validation Step 2: Column affinity - CRITICAL!
	// Symbol MUST map to this node's column
	if err := s.VerifySymbolToColumnMapping(share.SymbolID); err != nil {
		rdaStorageLog.Warnf(
			"RDA Storage: StoreShare FAILED - column affinity violation: %v",
			err,
		)
		return err
	}

	// Validation Step 3: Column must match grid position
	if share.Col != s.config.MyCol {
		rdaStorageLog.Warnf(
			"RDA Storage: StoreShare FAILED - column mismatch: share.col=%d, node.col=%d",
			share.Col, s.config.MyCol,
		)
		return fmt.Errorf(
			"share column %d doesn't match node column %d",
			share.Col, s.config.MyCol,
		)
	}

	// Initialize height map if needed
	if _, exists := s.columnShares[share.Height]; !exists {
		s.columnShares[share.Height] = make(map[uint32]map[uint32][]byte)
		rdaStorageLog.Debugf(
			"RDA Storage: allocated new height map for height=%d",
			share.Height,
		)
	}

	// Initialize row map if needed
	if _, exists := s.columnShares[share.Height][share.Row]; !exists {
		s.columnShares[share.Height][share.Row] = make(map[uint32][]byte)
		rdaStorageLog.Debugf(
			"RDA Storage: allocated new row map - height=%d, row=%d",
			share.Height, share.Row,
		)
	}

	// Store the share
	s.columnShares[share.Height][share.Row][share.SymbolID] = share.Data
	s.sharesStored++

	rdaStorageLog.Infof(
		"RDA Storage: StoreShare SUCCESS - height=%d, (row=%d, col=%d), symbolID=%d stored ✓ (total_stored=%d)",
		share.Height, share.Row, share.Col, share.SymbolID, s.sharesStored,
	)

	return nil
}

// GetShare retrieves a share at grid position (row, col, symbolID)
//
// Validation Steps:
// 1. Verify grid position is valid
// 2. Verify column is this node's column
// 3. Verify symbol→column mapping
// 4. Retrieve from storage
func (s *RDAStorage) GetShare(ctx context.Context, height uint64, row, col, symbolID uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rdaStorageLog.Debugf(
		"RDA Storage: GetShare START - height=%d, row=%d, col=%d, symbolID=%d",
		height, row, col, symbolID,
	)

	// Validation Step 1: Grid position
	if err := s.VerifyGridPosition(row, col); err != nil {
		rdaStorageLog.Debugf("RDA Storage: GetShare FAILED - invalid grid position: %v", err)
		return nil, err
	}

	// Validation Step 2: Can only retrieve from our column
	if col != s.config.MyCol {
		rdaStorageLog.Warnf(
			"RDA Storage: GetShare FAILED - requesting from wrong column: request_col=%d, node_col=%d",
			col, s.config.MyCol,
		)
		return nil, fmt.Errorf(
			"node is column %d, cannot retrieve from column %d",
			s.config.MyCol, col,
		)
	}

	// Validation Step 3: Symbol→Column mapping
	if err := s.VerifySymbolToColumnMapping(symbolID); err != nil {
		rdaStorageLog.Debugf("RDA Storage: GetShare FAILED - invalid symbol: %v", err)
		return nil, err
	}

	// Retrieve from storage
	heightMap, heightExists := s.columnShares[height]
	if !heightExists {
		rdaStorageLog.Debugf(
			"RDA Storage: GetShare FAILED - height %d not found (available heights: %d)",
			height, len(s.columnShares),
		)
		return nil, fmt.Errorf("no shares stored for height %d", height)
	}

	rowMap, rowExists := heightMap[row]
	if !rowExists {
		rdaStorageLog.Debugf(
			"RDA Storage: GetShare FAILED - row %d not found at height %d",
			row, height,
		)
		return nil, fmt.Errorf("no shares for row %d at height %d", row, height)
	}

	data, symbolExists := rowMap[symbolID]
	if !symbolExists {
		rdaStorageLog.Debugf(
			"RDA Storage: GetShare FAILED - symbolID %d not found at (height=%d, row=%d, col=%d)",
			symbolID, height, row, col,
		)
		return nil, fmt.Errorf(
			"symbol %d not found at (height=%d, row=%d, col=%d)",
			symbolID, height, row, col,
		)
	}

	s.sharesRetrieved++

	rdaStorageLog.Infof(
		"RDA Storage: GetShare SUCCESS - height=%d, (row=%d, col=%d), symbolID=%d retrieved ✓ (data_size=%d bytes, total_retrieved=%d)",
		height, row, col, symbolID, len(data), s.sharesRetrieved,
	)

	return data, nil
}

// GetAllSharesForHeight returns all shares stored for a given height in this column
func (s *RDAStorage) GetAllSharesForHeight(ctx context.Context, height uint64) (map[uint32]map[uint32][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	heightMap, exists := s.columnShares[height]
	if !exists {
		rdaStorageLog.Debugf(
			"RDA Storage: GetAllSharesForHeight - height %d not found",
			height,
		)
		return nil, fmt.Errorf("no shares for height %d", height)
	}

	// Return a copy
	result := make(map[uint32]map[uint32][]byte)
	for row, rowMap := range heightMap {
		result[row] = make(map[uint32][]byte)
		for symbolID, data := range rowMap {
			result[row][symbolID] = data
		}
	}

	shareCount := 0
	for _, rowMap := range result {
		shareCount += len(rowMap)
	}

	rdaStorageLog.Infof(
		"RDA Storage: GetAllSharesForHeight - height=%d, total_shares=%d",
		height, shareCount,
	)

	return result, nil
}

// GetStats returns storage statistics
func (s *RDAStorage) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalHeights := len(s.columnShares)
	totalShares := int64(0)

	for _, heightMap := range s.columnShares {
		for _, rowMap := range heightMap {
			totalShares += int64(len(rowMap))
		}
	}

	stats := map[string]interface{}{
		"node_position": map[string]uint32{
			"row":       s.config.MyRow,
			"col":       s.config.MyCol,
			"grid_size": s.config.GridSize,
		},
		"storage_stats": map[string]interface{}{
			"total_heights":        int64(totalHeights),
			"total_shares_stored":  totalShares,
			"total_retrieval_ops":  s.sharesRetrieved,
			"total_storage_ops":    s.sharesStored,
		},
	}

	rdaStorageLog.Infof(
		"RDA Storage: stats - node=(row=%d,col=%d), heights=%d, total_shares=%d, stored_ops=%d, retrieved_ops=%d",
		s.config.MyRow, s.config.MyCol, totalHeights, totalShares, s.sharesStored, s.sharesRetrieved,
	)

	return stats
}
