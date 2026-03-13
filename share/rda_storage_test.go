package share

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRDAStorage_ColumnAffinityViolation tests that storing share in wrong column fails
func TestRDAStorage_ColumnAffinityViolation(t *testing.T) {
	t.Log("========== TEST: Column Affinity Violation ==========")

	config := RDAStorageConfig{
		MyRow:    0,
		MyCol:    5,    // Node is at column 5
		GridSize: 256,
	}
	storage := NewRDAStorage(config)

	// Try to store symbol that belongs to column 10, not column 5
	share := &RDAShare{
		Row:      10,
		Col:      5,
		SymbolID: 2570, // 2570 % 256 = 10 (belongs to column 10, NOT column 5!)
		Data:     []byte("test data"),
		Height:   1,
	}

	t.Logf("TEST: Attempting to store symbol %d which maps to column %d at node column %d",
		share.SymbolID, share.SymbolID%256, config.MyCol)

	err := storage.StoreShare(context.Background(), share)

	t.Logf("TEST: Store result: %v", err)
	assert.Error(t, err, "Expected error when storing share in wrong column")
	assert.Contains(t, err.Error(), "column", "Error should mention column mismatch")

	t.Log("✓ Test PASSED: Column affinity correctly enforced\n")
}

// TestRDAStorage_CorrectColumnStorage tests storing shares in correct column
func TestRDAStorage_CorrectColumnStorage(t *testing.T) {
	t.Log("========== TEST: Correct Column Storage ==========")

	config := RDAStorageConfig{
		MyRow:    0,
		MyCol:    5,    // Node is at column 5
		GridSize: 256,
	}
	storage := NewRDAStorage(config)

	// Store shares with symbolIDs that map to column 5
	testCases := []struct {
		symbolID uint32
		row      uint32
		desc     string
	}{
		{5, 0, "symbolID=5, row=0 → column=5%256=5 ✓"},
		{261, 1, "symbolID=261, row=1 → column=261%256=5 ✓"},
		{517, 2, "symbolID=517, row=2 → column=517%256=5 ✓"},
		{5 + 256*5, 5, "symbolID=1285, row=5 → column=1285%256=5 ✓"},
	}

	for _, tc := range testCases {
		t.Logf("TEST: %s", tc.desc)

		share := &RDAShare{
			Row:      tc.row,
			Col:      config.MyCol,
			SymbolID: tc.symbolID,
			Data:     []byte(fmt.Sprintf("test data %d", tc.symbolID)),
			Height:   1,
		}

		err := storage.StoreShare(context.Background(), share)
		t.Logf("TEST: Store result: %v", err)

		require.NoError(t, err, "Expected no error storing valid share")
	}

	t.Log("✓ Test PASSED: All shares stored correctly in column\n")
}

// TestRDAStorage_RetrieveShare tests retrieving shares with validation
func TestRDAStorage_RetrieveShare(t *testing.T) {
	t.Log("========== TEST: Retrieve Share With Validation ==========")

	config := RDAStorageConfig{
		MyRow:    0,
		MyCol:    5,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)
	ctx := context.Background()

	// Store a share
	originalData := []byte("my special data")
	share := &RDAShare{
		Row:      10,
		Col:      5,
		SymbolID: 1285, // 1285 % 256 = 5
		Data:     originalData,
		Height:   100,
	}

	t.Logf("TEST: Storing share - height=%d, row=%d, col=%d, symbolID=%d",
		share.Height, share.Row, share.Col, share.SymbolID)

	err := storage.StoreShare(ctx, share)
	require.NoError(t, err)
	t.Log("✓ Share stored")

	// Retrieve it
	t.Logf("TEST: Retrieving share - height=%d, row=%d, col=%d, symbolID=%d",
		share.Height, share.Row, share.Col, share.SymbolID)

	data, err := storage.GetShare(ctx, share.Height, share.Row, share.Col, share.SymbolID)
	require.NoError(t, err)
	assert.Equal(t, originalData, data, "Retrieved data should match original")
	t.Log("✓ Share retrieved correctly")

	t.Log("✓ Test PASSED: Retrieve with validation works\n")
}

// TestRDAStorage_RetrieveFromWrongColumn tests that retrieval from wrong column fails
func TestRDAStorage_RetrieveFromWrongColumn(t *testing.T) {
	t.Log("========== TEST: Retrieve From Wrong Column (Should Fail) ==========")

	config := RDAStorageConfig{
		MyRow:    0,
		MyCol:    5,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)
	ctx := context.Background()

	// Try to retrieve from a different column
	t.Logf("TEST: Attempting to retrieve share from column 10 (node is column %d)", config.MyCol)

	data, err := storage.GetShare(ctx, 100, 10, 10, 2570) // col=10, not col=5

	t.Logf("TEST: Retrieve result: %v", err)
	assert.Error(t, err, "Expected error when retrieving from wrong column")
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "column", "Error should mention column mismatch")

	t.Log("✓ Test PASSED: Correctly rejected retrieval from wrong column\n")
}

// TestRDAStorage_SymbolMapping tests symbol to column mapping
func TestRDAStorage_SymbolMapping(t *testing.T) {
	t.Log("========== TEST: Symbol→Column Mapping ==========")

	config := RDAStorageConfig{
		MyRow:    0,
		MyCol:    7,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)

	testCases := []struct {
		symbolID      uint32
		expectedCol   uint32
		shouldSucceed bool
		desc          string
	}{
		{7, 7, true, "symbolID=7 → col=7 (my col) ✓"},
		{263, 7, true, "symbolID=263 → col=263%256=7 (my col) ✓"},
		{519, 7, true, "symbolID=519 → col=519%256=7 (my col) ✓"},
		{8, 8, false, "symbolID=8 → col=8 (not my col) ✗"},
		{100, 100, false, "symbolID=100 → col=100 (not my col) ✗"},
	}

	for _, tc := range testCases {
		t.Logf("TEST: %s", tc.desc)

		err := storage.VerifySymbolToColumnMapping(tc.symbolID)

		if tc.shouldSucceed {
			require.NoError(t, err, "Expected verification to succeed")
			t.Log("  ✓ Mapping verified")
		} else {
			require.Error(t, err, "Expected verification to fail")
			t.Logf("  ✓ Mapping rejected with: %v", err)
		}
	}

	t.Log("✓ Test PASSED: Symbol mapping works correctly\n")
}

// TestRDAStorage_MultipleHeights tests storage with multiple block heights
func TestRDAStorage_MultipleHeights(t *testing.T) {
	t.Log("========== TEST: Multiple Block Heights ==========")

	config := RDAStorageConfig{
		MyRow:    1,
		MyCol:    3,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)
	ctx := context.Background()

	// Store shares for multiple heights
	heights := []uint64{100, 101, 102}
	symbolBase := uint32(3) // Maps to column 3

	t.Logf("TEST: Storing shares for heights %v", heights)

	for _, height := range heights {
		for row := uint32(0); row < 5; row++ {
			symbolID := symbolBase + row*256
			share := &RDAShare{
				Row:      row,
				Col:      3,
				SymbolID: symbolID,
				Data:     []byte(fmt.Sprintf("height_%d_row_%d", height, row)),
				Height:   height,
			}

			err := storage.StoreShare(ctx, share)
			require.NoError(t, err)
		}
	}

	t.Logf("TEST: Retrieving shares back...")

	// Retrieve and verify
	for _, height := range heights {
		for row := uint32(0); row < 5; row++ {
			symbolID := symbolBase + row*256
			data, err := storage.GetShare(ctx, height, row, 3, symbolID)
			require.NoError(t, err)
			expected := fmt.Sprintf("height_%d_row_%d", height, row)
			assert.Equal(t, []byte(expected), data)
		}
	}

	t.Log("✓ Test PASSED: Multiple heights stored and retrieved\n")
}

// TestRDAStorage_Stats tests statistics collection
func TestRDAStorage_Stats(t *testing.T) {
	t.Log("========== TEST: Storage Statistics ==========")

	config := RDAStorageConfig{
		MyRow:    2,
		MyCol:    10,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)
	ctx := context.Background()

	// Store some shares
	for i := 0; i < 5; i++ {
		share := &RDAShare{
			Row:      uint32(i),
			Col:      10,
			SymbolID: 10 + uint32(i)*256,
			Data:     []byte("data"),
			Height:   1,
		}
		_ = storage.StoreShare(ctx, share)
	}

	// Get and verify stats
	stats := storage.GetStats()

	t.Logf("TEST: Storage stats: %+v", stats)

	nodePos := stats["node_position"].(map[string]uint32)
	assert.Equal(t, uint32(2), nodePos["row"])
	assert.Equal(t, uint32(10), nodePos["col"])
	assert.Equal(t, uint32(256), nodePos["grid_size"])

	storageStats := stats["storage_stats"].(map[string]interface{})
	assert.Equal(t, int64(1), storageStats["total_heights"])
	assert.Equal(t, int64(5), storageStats["total_shares_stored"])

	t.Log("✓ Test PASSED: Statistics correct\n")
}

// TestRDAStorage_GridPositionValidation tests grid position bounds checking
func TestRDAStorage_GridPositionValidation(t *testing.T) {
	t.Log("========== TEST: Grid Position Validation ==========")

	config := RDAStorageConfig{
		MyRow:    0,
		MyCol:    0,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)

	testCases := []struct {
		row           uint32
		col           uint32
		shouldSucceed bool
		desc          string
	}{
		{0, 0, true, "position (0,0) ✓"},
		{255, 255, true, "position (255,255) ✓"},
		{128, 128, true, "position (128,128) ✓"},
		{256, 0, false, "row=256 (out of bounds) ✗"},
		{0, 256, false, "col=256 (out of bounds) ✗"},
		{256, 256, false, "both out of bounds ✗"},
	}

	for _, tc := range testCases {
		t.Logf("TEST: Validating position (%d, %d) - %s", tc.row, tc.col, tc.desc)

		err := storage.VerifyGridPosition(tc.row, tc.col)

		if tc.shouldSucceed {
			require.NoError(t, err)
			t.Log("  ✓ Position valid")
		} else {
			require.Error(t, err)
			t.Logf("  ✓ Position rejected")
		}
	}

	t.Log("✓ Test PASSED: Grid position validation works\n")
}

// TestRDAStorage_AllRulesEnforced comprehensive test of all RDA rules
func TestRDAStorage_AllRulesEnforced(t *testing.T) {
	t.Log("========== TEST: All RDA Rules Enforced (Comprehensive) ==========")

	config := RDAStorageConfig{
		MyRow:    5,
		MyCol:    42,
		GridSize: 256,
	}
	storage := NewRDAStorage(config)
	ctx := context.Background()

	t.Log("Node position: (row=5, col=42)")
	t.Log("Grid size: 256x256")
	t.Log("")

	// Rule 1: Can only store symbols mapping to column 42
	t.Log("RULE 1: Only symbols mapping to column 42 can be stored")
	validSymbols := []uint32{42, 298, 554, 810}  // All map to col=42
	invalidSymbols := []uint32{41, 43, 100, 300} // Don't map to col=42

	for _, sym := range validSymbols {
		col := sym % 256
		share := &RDAShare{
			Row:      0,
			Col:      42,
			SymbolID: sym,
			Data:     []byte("data"),
			Height:   1,
		}
		err := storage.StoreShare(ctx, share)
		status := "ACCEPTED"
		if err != nil {
			status = "REJECTED"
		}
		t.Logf("  Store symbolID=%d (col=%d): %s", sym, col, status)
		require.NoError(t, err)
	}

	for _, sym := range invalidSymbols {
		col := sym % 256
		share := &RDAShare{
			Row:      0,
			Col:      42,
			SymbolID: sym,
			Data:     []byte("data"),
			Height:   1,
		}
		err := storage.StoreShare(ctx, share)
		t.Logf("  Store symbolID=%d (col=%d): REJECTED ✓", sym, col)
		require.Error(t, err)
	}

	// Rule 2: Retrieval only from column 42
	t.Log("\nRULE 2: Retrieval only allowed from column 42")
	for col := uint32(0); col <= 100; col += 25 {
		_, err := storage.GetShare(ctx, 1, 0, col, 42)
		if col == 42 {
			t.Logf("  Retrieve from col=%d (our col): ACCEPTED ✓", col)
		} else {
			t.Logf("  Retrieve from col=%d (not our col): REJECTED ✓", col)
			require.Error(t, err)
		}
	}

	t.Log("\n✓ Test PASSED: All RDA rules properly enforced\n")
}
