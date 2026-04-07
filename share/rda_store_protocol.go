package share

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var storeLog = logging.Logger("rda.store")

// ============================================================================
// STORE Protocol Definition
// ============================================================================

// Protocol IDs for STORE flow
const (
	// /rda/store/1 - Initial STORE from Proposer → (row, col) intersection
	RDAStoreProtocolID protocol.ID = "/celestia/rda/store/1.0.0"

	// /rda/store_fwd/1 - STORE_FWD forward from Column receiver → same column peers
	RDAStoreFwdProtocolID protocol.ID = "/celestia/rda/store_fwd/1.0.0"
)

// StoreMessageType định nghĩa loại thông điệp
type StoreMessageType string

const (
	StoreMessageTypeStore    StoreMessageType = "store"     // (store, h, i, x)
	StoreMessageTypeStoreFwd StoreMessageType = "store_fwd" // (store_fwd, h, i, x)
)

// StoreMessage - "Phong bì" (envelope) cho STORE operation
// Định dạng: (type, h, i, x, timestamp, sender_row, sender_col)
type StoreMessage struct {
	// Message type: "store" hoặc "store_fwd"
	Type StoreMessageType `json:"type"`

	// Data triple (h, i, x dạng simplified)
	Handle     string `json:"handle"`      // h: DataRoot
	ShareIndex uint32 `json:"share_index"` // i: Index
	RowID      uint32 `json:"row_id"`      // Hàng tạo share này
	ColID      uint32 `json:"col_id"`      // Cột (chứa share này)

	// Raw data - ở đây là simplified version (thực tế là RDASymbol)
	Data []byte `json:"data"` // x: Raw share data

	// NMT Proof wrapper
	NMTProof *NMTProofData `json:"nmt_proof,omitempty"`

	// Metadata
	Timestamp   int64  `json:"timestamp"`
	SenderID    string `json:"sender_id"`  // PeerID của người gửi
	SenderRow   uint32 `json:"sender_row"` // Row của người gửi
	SenderCol   uint32 `json:"sender_col"` // Col của người gửi
	BlockHeight uint64 `json:"block_height"`
}

// StoreMessageResponse - Phản hồi sau khi nhận và lưu
type StoreMessageResponse struct {
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
	StorageID   string `json:"storage_id,omitempty"`
	LatencyMs   int64  `json:"latency_ms"`
	ValidatedBy string `json:"validated_by"` // Pred validation result
}

// ============================================================================
// STORE Protocol Handler - Node nhận
// ============================================================================

// RDAStoreProtocolHandler - Xử lý STORE messages từ broadcast
type RDAStoreProtocolHandler struct {
	host             host.Host
	gridManager      *RDAGridManager
	peerManager      *RDAPeerManager
	subnetManager    *RDASubnetManager
	storage          *RDAStorage // Local storage
	predicateChecker *RDAPredicateChecker

	// Subscription listeners
	storeListeners    []StoreMessageListener
	storeFwdListeners []StoreFwdMessageListener

	// Statistics
	mu                    sync.RWMutex
	totalStoreReceived    int64
	totalStoreForwarded   int64
	totalStoreFwdReceived int64
	failedValidations     int64
}

// StoreMessageListener - Callback khi nhận STORE message
type StoreMessageListener func(msg *StoreMessage) error

// StoreFwdMessageListener - Callback khi nhận STORE_FWD message
type StoreFwdMessageListener func(msg *StoreMessage) error

// NewRDAStoreProtocolHandler tạo handler mới
func NewRDAStoreProtocolHandler(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
	subnetManager *RDASubnetManager,
	storage *RDAStorage,
) *RDAStoreProtocolHandler {
	return &RDAStoreProtocolHandler{
		host:              host,
		gridManager:       gridManager,
		peerManager:       peerManager,
		subnetManager:     subnetManager,
		storage:           storage,
		predicateChecker:  NewRDAPredicateChecker(uint32(gridManager.GetGridDimensions().Cols)),
		storeListeners:    make([]StoreMessageListener, 0),
		storeFwdListeners: make([]StoreFwdMessageListener, 0),
	}
}

// Start đăng ký stream handlers và bắt đầu listen
func (h *RDAStoreProtocolHandler) Start(ctx context.Context) error {
	// Register stream handler cho /rda/store/1
	h.host.SetStreamHandler(RDAStoreProtocolID, h.handleStoreStream)
	storeLog.Infof("RDA|STORE|START ✓ registered protocol: %s", RDAStoreProtocolID)

	// Register stream handler cho /rda/store_fwd/1
	h.host.SetStreamHandler(RDAStoreFwdProtocolID, h.handleStoreFwdStream)
	storeLog.Infof("RDA|STORE_FWD|START ✓ registered protocol: %s", RDAStoreFwdProtocolID)

	return nil
}

// Stop unregister handlers
func (h *RDAStoreProtocolHandler) Stop() error {
	h.host.RemoveStreamHandler(RDAStoreProtocolID)
	h.host.RemoveStreamHandler(RDAStoreFwdProtocolID)
	storeLog.Infof("RDA|STORE|STOP ✓ unregistered handlers")
	return nil
}

// ============================================================================
// Stream Handler 1: /rda/store/1 - Thiập nhận từ Proposer
// ============================================================================

// handleStoreStream xử lý incoming stream để nhận STORE messages
func (h *RDAStoreProtocolHandler) handleStoreStream(stream network.Stream) {
	defer stream.Close()

	peerID := stream.Conn().RemotePeer()
	storeLog.Debugf("RDA|STORE|STREAM_OPEN peer=%s", peerID.String()[:8])

	decoder := json.NewDecoder(stream)
	var msg StoreMessage
	if err := decoder.Decode(&msg); err != nil {
		storeLog.Warnf("RDA|STORE|DECODE_ERROR peer=%s error=%v", peerID.String()[:8], err)
		return
	}

	startTime := time.Now()
	storeLog.Debugf("RDA|STORE|RECEIVE peer=%s handle=%s index=%d row=%d col=%d size=%d",
		peerID.String()[:8], msg.Handle[:8], msg.ShareIndex, msg.RowID, msg.ColID, len(msg.Data))

	// Process STORE message
	h.processStoreMessage(stream.Conn().RemoteMultiaddr().String(), &msg)

	latency := time.Since(startTime).Milliseconds()
	storeLog.Infof("RDA|STORE|COMPLETE peer=%s handle=%s index=%d latency=%dms",
		peerID.String()[:8], msg.Handle[:8], msg.ShareIndex, latency)
}

// processStoreMessage chính thức xử lý STORE message
func (h *RDAStoreProtocolHandler) processStoreMessage(senderAddr string, msg *StoreMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()

	storeStartTime := time.Now()

	// ========== BƯỚC 1: VALIDATION ==========
	storeLog.Debugf("RDA|STORE|VALIDATE_START handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)

	// Chạy Pred(h, i, x) để kiểm tra hợp lệ
	symbol := &RDASymbol{
		Handle:      msg.Handle,
		ShareIndex:  msg.ShareIndex,
		Row:         msg.RowID,
		Col:         msg.ColID,
		ShareData:   msg.Data,
		NMTProof:    *msg.NMTProof,
		Timestamp:   msg.Timestamp,
		BlockHeight: msg.BlockHeight,
	}

	if !h.predicateChecker.Pred(msg.Handle, msg.ShareIndex, symbol) {
		storeLog.Warnf("RDA|STORE|VALIDATE_FAIL handle=%s index=%d reason=pred", msg.Handle[:8], msg.ShareIndex)
		h.failedValidations++
		return
	}

	storeLog.Debugf("RDA|STORE|VALIDATE_OK ✓ handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)

	// ========== BƯỚC 2: STORAGE ==========
	storeLog.Debugf("RDA|STORE|STORAGE_WRITE_START row=%d col=%d index=%d size=%d",
		msg.RowID, msg.ColID, msg.ShareIndex, len(msg.Data))

	// Lưu vào local storage
	rdaShare := &RDAShare{
		Handle:   msg.Handle,
		Row:      msg.RowID,
		Col:      msg.ColID,
		SymbolID: msg.ShareIndex,
		Data:     msg.Data,
		Height:   msg.BlockHeight,
	}

	// Simplified: store without context for now
	ctx := context.Background()
	if err := h.storage.StoreShare(ctx, rdaShare); err != nil {
		storeLog.Warnf("RDA|STORE|STORAGE_ERROR handle=%s index=%d error=%v", msg.Handle[:8], msg.ShareIndex, err)
		h.failedValidations++
		return
	}

	h.totalStoreReceived++
	storeLog.Infof("RDA|STORE|STORAGE_SUCCESS ✓ handle=%s index=%d row=%d col=%d size=%d",
		msg.Handle[:8], msg.ShareIndex, msg.RowID, msg.ColID, len(msg.Data))

	// ========== BƯỚC 3: FORWARD (STORE_FWD) ==========
	// Kiểm tra: Node này có trong cùng cột không?
	myPos, _ := h.gridManager.GetPeerPosition(h.host.ID())
	if myPos.Col == int(msg.ColID) {
		storeLog.Debugf("RDA|STORE|FORWARD_CHECK ✓ SAME_COL index=%d myCol=%d targetCol=%d",
			msg.ShareIndex, myPos.Col, msg.ColID)
		// YES: Phát loa cho TẤT CẢ láng giềng cùng cột
		h.forwardToColumnPeers(msg)
	} else {
		storeLog.Debugf("RDA|STORE|FORWARD_CHECK ✗ DIFF_COL index=%d myCol=%d targetCol=%d (skip forward)",
			msg.ShareIndex, myPos.Col, msg.ColID)
		// NO: Không forward - chỉ lưu cục bộ
	}

	latency := time.Since(storeStartTime).Milliseconds()
	storeLog.Infof("RDA|STORE|PROCESS_END handle=%s index=%d latency=%dms",
		msg.Handle[:8], msg.ShareIndex, latency)
}

// ============================================================================
// Stream Handler 2: /rda/store_fwd/1 - Tời nhận từ Column peers
// ============================================================================

// handleStoreFwdStream xử lý STORE_FWD messages từ column peers
func (h *RDAStoreProtocolHandler) handleStoreFwdStream(stream network.Stream) {
	defer stream.Close()

	peerID := stream.Conn().RemotePeer()
	storeLog.Debugf("Incoming STORE_FWD stream from peer %s", peerID.String()[:16])

	decoder := json.NewDecoder(stream)
	var msg StoreMessage
	if err := decoder.Decode(&msg); err != nil {
		storeLog.Warnf("[%s] Failed to decode STORE_FWD message: %v", peerID.String()[:16], err)
		return
	}

	// Verify sender là cùng cột
	myPos, _ := h.gridManager.GetPeerPosition(h.host.ID())
	if myPos.Col != int(msg.ColID) {
		storeLog.Warnf("[%s] STORE_FWD from wrong column (my_col=%d, sender_col=%d), reject",
			peerID.String()[:16], myPos.Col, msg.ColID)
		return
	}

	startTime := time.Now()
	storeLog.Debugf("[%s] Received STORE_FWD msg - h=%s, i=%d from column peer",
		peerID.String()[:16], msg.Handle[:8], msg.ShareIndex)

	// Process STORE_FWD
	h.processStoreFwdMessage(stream.Conn().RemoteMultiaddr().String(), &msg)

	latency := time.Since(startTime).Milliseconds()
	storeLog.Infof("[%s] STORE_FWD processed - latency=%dms ✓", peerID.String()[:16], latency)
}

// processStoreFwdMessage xử lý STORE_FWD message (từ column peers)
func (h *RDAStoreProtocolHandler) processStoreFwdMessage(senderAddr string, msg *StoreMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// ========== BƯỚC 1: VALIDATION ==========
	symbol := &RDASymbol{
		Handle:      msg.Handle,
		ShareIndex:  msg.ShareIndex,
		Row:         msg.RowID,
		Col:         msg.ColID,
		ShareData:   msg.Data,
		NMTProof:    *msg.NMTProof,
		Timestamp:   msg.Timestamp,
		BlockHeight: msg.BlockHeight,
	}

	if !h.predicateChecker.Pred(msg.Handle, msg.ShareIndex, symbol) {
		storeLog.Warnf("STORE_FWD REJECTED - Pred validation failed: h=%s, i=%d", msg.Handle[:8], msg.ShareIndex)
		h.failedValidations++
		return
	}

	storeLog.Debugf("STORE_FWD VALIDATED ✓ - h=%s, i=%d", msg.Handle[:8], msg.ShareIndex)

	// ========== BƯỚC 2: STORAGE ==========
	rdaShare := &RDAShare{
		Handle:   msg.Handle,
		Row:      msg.RowID,
		Col:      msg.ColID,
		SymbolID: msg.ShareIndex,
		Data:     msg.Data,
		Height:   msg.BlockHeight,
	}

	ctx := context.Background()
	if err := h.storage.StoreShare(ctx, rdaShare); err != nil {
		storeLog.Warnf("STORE_FWD FAILED - storage error: %v", err)
		h.failedValidations++
		return
	}

	h.totalStoreFwdReceived++
	storeLog.Infof("STORE_FWD SUCCESS ✓ - h=%s, i=%d saved from column peer", msg.Handle[:8], msg.ShareIndex)

	// ========== BƯỚC 3: NO FORWARD ==========
	// QUAN TRỌNG: Không forward STORE_FWD tiếp - tránh vòng lặp vô hạn
	storeLog.Debugf("STORE_FWD - Terminal node, NOT forwarding further (prevent loops)")
}

// ============================================================================
// Proposer Side - Khi có Block mới
// ============================================================================

// RDAStoreProposer - Node gửi STORE messages (Bridge/Full nodes)
type RDAStoreProposer struct {
	host             host.Host
	gridManager      *RDAGridManager
	peerManager      *RDAPeerManager
	myPosition       GridPosition
	predicateChecker *RDAPredicateChecker

	mu                  sync.RWMutex
	totalStoresSent     int64
	successSends        int64
	failedSends         int64
	totalBytesForwarded int64
}

// NewRDAStoreProposer tạo proposer mới
func NewRDAStoreProposer(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
) *RDAStoreProposer {
	myPos, _ := gridManager.GetPeerPosition(host.ID())
	return &RDAStoreProposer{
		host:             host,
		gridManager:      gridManager,
		peerManager:      peerManager,
		myPosition:       myPos,
		predicateChecker: NewRDAPredicateChecker(uint32(gridManager.GetGridDimensions().Cols)),
	}
}

// DistributeBlock - Gọi khi Bridge node nhận block mới
// Phân phối tất cả shares qua grid
// Luồng:
// 1. Duyệt tất cả shares từ block
// 2. Với mỗi share i: col = Cell(i)
// 3. Tìm peers ở vị trí intersection (myRow, col)
// 4. Gửi STORE message tới các peers này
func (p *RDAStoreProposer) DistributeBlock(
	ctx context.Context,
	blockHandle string,
	blockHeight uint64,
	shares []*RDASymbol,
) error {
	startTime := time.Now()
	storeLog.Infof("PROPOSER START - Distribuye block %s with %d shares", blockHandle[:8], len(shares))

	p.mu.Lock()
	defer p.mu.Unlock()

	successCount := 0
	failCount := 0

	// Duyệt từng share
	for _, share := range shares {
		// ========== BƯỚC 1: TÍNH CỘT ==========
		// col = Cell(i) = i % K
		col := Cell(share.ShareIndex, uint32(p.gridManager.GetGridDimensions().Cols))

		// My Row = r
		row := uint32(p.myPosition.Row)

		storeLog.Debugf("PROPOSER - Processing share i=%d → col=%d (row=%d)",
			share.ShareIndex, col, row)

		// ========== BƯỚC 2: TỚI PEERS TẠI (row, col) ==========
		// Tìm danh sách peers ở vị trí intersection
		intersectionPeers := p.findIntersectionPeers(int(row), int(col))
		if len(intersectionPeers) == 0 {
			storeLog.Warnf("PROPOSER - No peers at intersection (%d, %d)", row, col)
			failCount++
			continue
		}

		storeLog.Debugf("PROPOSER - Found %d peers at (%d, %d)", len(intersectionPeers), row, col)

		// ========== BƯỚC 3: ĐÓNG GÓI & GỬI ==========
		msg := &StoreMessage{
			Type:        StoreMessageTypeStore,
			Handle:      blockHandle,
			ShareIndex:  share.ShareIndex,
			RowID:       row,
			ColID:       col,
			Data:        share.ShareData,
			NMTProof:    &share.NMTProof,
			Timestamp:   time.Now().UnixNano() / 1e6,
			SenderID:    p.host.ID().String(),
			SenderRow:   row,
			SenderCol:   uint32(p.myPosition.Col),
			BlockHeight: blockHeight,
		}

		// Gửi qua STORE protocol
		for _, targetPeer := range intersectionPeers {
			if err := p.sendStoreMessage(ctx, targetPeer, msg); err != nil {
				storeLog.Warnf("PROPOSER - Failed to send to %s: %v", targetPeer.String()[:16], err)
				failCount++
			} else {
				successCount++
				p.totalBytesForwarded += int64(len(msg.Data))
			}
		}
	}

	p.mu.Lock()
	p.totalStoresSent += int64(len(shares))
	p.successSends += int64(successCount)
	p.failedSends += int64(failCount)
	p.mu.Unlock()

	latency := time.Since(startTime).Milliseconds()
	storeLog.Infof("PROPOSER END ✓ - Distributed %d shares (success=%d, failed=%d, latency=%dms)",
		len(shares), successCount, failCount, latency)

	return nil
}

// findIntersectionPeers tìm peers ở vị trí (row, col)
func (p *RDAStoreProposer) findIntersectionPeers(row, col int) []peer.ID {
	// Combine row peers + col peers tại intersection
	allSubnetPeers := p.peerManager.GetSubnetPeers()

	var intersectionPeers []peer.ID
	for _, peerID := range allSubnetPeers {
		if pos, ok := p.gridManager.GetPeerPosition(peerID); ok {
			// Peer này ở intersection nếu:
			// - Ở cùng row VÀ cùng col
			if pos.Row == row && pos.Col == col {
				intersectionPeers = append(intersectionPeers, peerID)
			}
		}
	}

	return intersectionPeers
}

// sendStoreMessage gửi STORE message qua stream
func (p *RDAStoreProposer) sendStoreMessage(
	ctx context.Context,
	targetPeer peer.ID,
	msg *StoreMessage,
) error {
	stream, err := p.host.NewStream(ctx, targetPeer, RDAStoreProtocolID)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	encoder := json.NewEncoder(stream)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	storeLog.Debugf("SENT STORE to %s - h=%s, i=%d, data_size=%d bytes",
		targetPeer.String()[:16], msg.Handle[:8], msg.ShareIndex, len(msg.Data))

	return nil
}

// ============================================================================
// Column Forwarding - Forward từ receiver → Column peers
// ============================================================================

// forwardToColumnPeers - Gửi STORE_FWD tới TẤT CẢ peers cùng cột
func (h *RDAStoreProtocolHandler) forwardToColumnPeers(msg *StoreMessage) {
	myPos, _ := h.gridManager.GetPeerPosition(h.host.ID())

	// Lấy danh sách TẤT CẢ peers trong cùng cột
	columnPeers := h.peerManager.GetColPeers()

	storeLog.Debugf("FORWARD_START - Forwarding to %d column peers (col=%d)", len(columnPeers), myPos.Col)

	// Gửi STORE_FWD message đến từng peer
	for _, targetPeer := range columnPeers {
		if targetPeer == h.host.ID() {
			// Không gửi cho chính mình
			continue
		}

		// Tạo STORE_FWD message (type = store_fwd)
		fwdMsg := &StoreMessage{
			Type:        StoreMessageTypeStoreFwd, // ← Đánh dấu là FORWARD
			Handle:      msg.Handle,
			ShareIndex:  msg.ShareIndex,
			RowID:       msg.RowID,
			ColID:       msg.ColID,
			Data:        msg.Data,
			NMTProof:    msg.NMTProof,
			Timestamp:   msg.Timestamp,
			SenderID:    h.host.ID().String(),
			SenderRow:   uint32(myPos.Row),
			SenderCol:   uint32(myPos.Col),
			BlockHeight: msg.BlockHeight,
		}

		// Gửi async để không block
		go func(peer peer.ID) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			stream, err := h.host.NewStream(ctx, peer, RDAStoreFwdProtocolID)
			if err != nil {
				storeLog.Warnf("Failed to open STORE_FWD stream to %s: %v", peer.String()[:16], err)
				return
			}
			defer stream.Close()

			encoder := json.NewEncoder(stream)
			if err := encoder.Encode(fwdMsg); err != nil {
				storeLog.Warnf("Failed to send STORE_FWD to %s: %v", peer.String()[:16], err)
				return
			}

			storeLog.Debugf("FORWARD SUCCESS ✓ - Sent STORE_FWD to %s (h=%s, i=%d)",
				peer.String()[:16], fwdMsg.Handle[:8], fwdMsg.ShareIndex)
		}(targetPeer)
	}

	h.totalStoreForwarded++
	storeLog.Infof("FORWARD_END ✓ - Initiated forwarding to column (total_fwd_batches=%d)",
		h.totalStoreForwarded)
}

// ============================================================================
// Statistics & Monitoring
// ============================================================================

// GetStoreStats trả về statistics
func (h *RDAStoreProtocolHandler) GetStoreStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return map[string]interface{}{
		"total_store_received":     h.totalStoreReceived,
		"total_store_forwarded":    h.totalStoreForwarded,
		"total_store_fwd_received": h.totalStoreFwdReceived,
		"failed_validations":       h.failedValidations,
	}
}

// GetProposerStats trả về stats từ proposer
func (p *RDAStoreProposer) GetProposerStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return map[string]interface{}{
		"total_stores_sent":     p.totalStoresSent,
		"successful_sends":      p.successSends,
		"failed_sends":          p.failedSends,
		"total_bytes_forwarded": p.totalBytesForwarded,
	}
}
