package share

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var getLog = logging.Logger("rda.get")

// ============================================================================
// GET Protocol Definition - Phase 3: Query Sampling
// ============================================================================
// Thay thế DAS worker: Light Node yêu cầu RDA Grid để lấy mẫu siêu nhanh (1-hop)
// Flow:
//   Phía hỏi (Light Node):
//   1. GetShare(i) gọi Cell(i) → lấy cột c
//   2. Tìm láng giềng ở (myRow, c)
//   3. Gửi (get, h, i) → chờ (get_rsp, h, i, x)
//   4. Validate với Pred → return
//   5. Nếu fail/timeout → retry láng giềng khác
//
//   Phía trả lời (Bridge/Full Node):
//   1. Listener /rda/get/1 → nhận (get, h, i)
//   2. Tra cứu DB cục bộ
//   3. Trả (get_rsp, h, i, x) + Proof

const (
	// /rda/get/1 - Query protocol (Light Node → Grid Node)
	RDAGetProtocolID protocol.ID = "/celestia/rda/get/1.0.0"
)

// GetMessageType định nghĩa loại thông điệp
type GetMessageType string

const (
	GetMessageTypeQuery    GetMessageType = "get"     // (get, h, i)
	GetMessageTypeResponse GetMessageType = "get_rsp" // (get_rsp, h, i, x, proof)
)

// GetMessage - "Phong bè" (envelope) cho GET operation
type GetMessage struct {
	// Message type: "get" hoặc "get_rsp"
	Type GetMessageType `json:"type"`

	// Dữ liệu truy vấn (h, i)
	Handle     string `json:"handle"`      // h: DataRoot
	ShareIndex uint32 `json:"share_index"` // i: Index

	// Response-only fields
	RowID    uint32        `json:"row_id,omitempty"`    // Hàng của node có dữ liệu
	ColID    uint32        `json:"col_id,omitempty"`    // Cột
	Data     []byte        `json:"data,omitempty"`      // x: Share data
	NMTProof *NMTProofData `json:"nmt_proof,omitempty"` // Proof để xác minh

	// Metadata
	SenderID    string `json:"sender_id"`
	Timestamp   int64  `json:"timestamp"` // milliseconds
	BlockHeight uint64 `json:"block_height"`
	RequestID   string `json:"request_id"` // Để match response với request
}

// ============================================================================
// RDAGetProtocolHandler - Phía trả lời (Bridge/Full Node)
// ============================================================================

type RDAGetProtocolHandler struct {
	host         host.Host
	gridManager  *RDAGridManager
	peerManager  *RDAPeerManager
	subnetMgr    *RDASubnetManager
	storage      *RDAStorage // Để truy vấn dữ liệu local
	predicateChk *RDAPredicateChecker

	streamHandlers map[string]bool
	mu             sync.RWMutex

	// Statistics
	totalGetReceived   int64
	totalGetResponded  int64
	totalNotFound      int64
	failedValidations  int64
	totalBytesTransfer int64
}

// NewRDAGetProtocolHandler tạo handler mới
func NewRDAGetProtocolHandler(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
	subnetMgr *RDASubnetManager,
	storage *RDAStorage,
) *RDAGetProtocolHandler {
	gridSize := uint32(gridManager.GetGridDimensions().Cols)
	return &RDAGetProtocolHandler{
		host:               host,
		gridManager:        gridManager,
		peerManager:        peerManager,
		subnetMgr:          subnetMgr,
		storage:            storage,
		predicateChk:       NewRDAPredicateChecker(gridSize),
		streamHandlers:     make(map[string]bool),
		totalGetReceived:   0,
		totalGetResponded:  0,
		totalNotFound:      0,
		failedValidations:  0,
		totalBytesTransfer: 0,
	}
}

// Start registers stream handlers
func (h *RDAGetProtocolHandler) Start(ctx context.Context) error {
	h.host.SetStreamHandler(RDAGetProtocolID, h.handleGetStream)

	h.mu.Lock()
	h.streamHandlers[string(RDAGetProtocolID)] = true
	h.mu.Unlock()

	getLog.Infof("RDA|GET|START ✓ Handler started - protocol: %s", RDAGetProtocolID)
	return nil
}

// Stop unregisters stream handlers
func (h *RDAGetProtocolHandler) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.host.RemoveStreamHandler(RDAGetProtocolID)
	h.streamHandlers[string(RDAGetProtocolID)] = false

	getLog.Infof("RDA|GET|STOP ✓ Handler stopped")
	return nil
}

// handleGetStream handles incoming GET requests from Light Nodes
func (h *RDAGetProtocolHandler) handleGetStream(stream network.Stream) {
	defer stream.Close()

	remoteID := stream.Conn().RemotePeer()
	getLog.Debugf("RDA|GET|STREAM_OPEN peer=%s", remoteID.String()[:8])

	// Đọc thông điệp
	var msg GetMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		getLog.Warnf("RDA|GET|DECODE_ERROR peer=%s error=%v", remoteID.String()[:8], err)
		return
	}

	if msg.Type != GetMessageTypeQuery {
		getLog.Warnf("RDA|GET|TYPE_ERROR expected=query got=%s from=%s", msg.Type, remoteID.String()[:8])
		return
	}

	h.totalGetReceived++
	getLog.Debugf("RDA|GET|RECEIVE peer=%s handle=%s index=%d requestID=%s",
		remoteID.String()[:8], msg.Handle[:8], msg.ShareIndex, msg.RequestID)

	// ========== BƯỚC 1: LOOKUP DATABASE ==========
	getLog.Debugf("RDA|GET|LOOKUP_START handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Real lookup by (handle, shareIndex) in local RDA storage.
	shareData, blockHeight, rowID, colID, err := h.storage.GetShareByHandleAndSymbol(ctx, msg.Handle, msg.ShareIndex)

	if err != nil || len(shareData) == 0 {
		h.totalNotFound++
		getLog.Warnf("RDA|GET|NOT_FOUND handle=%s index=%d error=%v",
			msg.Handle[:8], msg.ShareIndex, err)

		// Trả thông báo "không có" (khống response lại)
		return
	}

	getLog.Debugf("RDA|GET|FOUND ✓ handle=%s index=%d size=%d", msg.Handle[:8], msg.ShareIndex, len(shareData))

	// ========== BƯỚC 2: VALIDATE VỚI PRED ==========
	getLog.Debugf("RDA|GET|VALIDATE_START handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)

	symbol := &RDASymbol{
		Handle:      msg.Handle,
		ShareIndex:  msg.ShareIndex,
		Row:         rowID,
		Col:         colID,
		ShareData:   shareData,
		Timestamp:   time.Now().UnixNano() / 1e6,
		BlockHeight: blockHeight,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{},
			RootHash:    []byte(msg.Handle),
			NamespaceID: "rda",
		},
	}

	if !h.predicateChk.Pred(msg.Handle, msg.ShareIndex, symbol) {
		h.failedValidations++
		getLog.Warnf("RDA|GET|VALIDATE_FAIL handle=%s index=%d reason=pred", msg.Handle[:8], msg.ShareIndex)
		return
	}

	getLog.Debugf("RDA|GET|VALIDATE_OK ✓ handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)

	// ========== BƯỚC 3: ASSEMBLE & SEND RESPONSE ==========
	getLog.Debugf("RDA|GET|RESPONSE_BUILD handle=%s index=%d size=%d", msg.Handle[:8], msg.ShareIndex, len(shareData))

	response := GetMessage{
		Type:        GetMessageTypeResponse,
		Handle:      msg.Handle,
		ShareIndex:  msg.ShareIndex,
		RowID:       rowID,
		ColID:       colID,
		Data:        shareData,
		NMTProof:    &symbol.NMTProof,
		SenderID:    h.host.ID().String(),
		Timestamp:   time.Now().UnixNano() / 1e6,
		BlockHeight: blockHeight,
		RequestID:   msg.RequestID,
	}

	respData, err := json.Marshal(response)
	if err != nil {
		getLog.Warnf("RDA|GET|MARSHAL_ERROR handle=%s error=%v", msg.Handle[:8], err)
		return
	}

	if _, err := stream.Write(respData); err != nil {
		getLog.Warnf("RDA|GET|SEND_ERROR peer=%s handle=%s error=%v", remoteID.String()[:8], msg.Handle[:8], err)
		return
	}

	h.totalGetResponded++
	h.totalBytesTransfer += int64(len(respData))

	getLog.Infof("RDA|GET|RESPONSE_SENT ✓ peer=%s handle=%s index=%d size=%d bytes",
		remoteID.String()[:8], msg.Handle[:8], msg.ShareIndex, len(respData))
}

// GetStats trả về statistics
func (h *RDAGetProtocolHandler) GetStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return map[string]interface{}{
		"total_get_received":   h.totalGetReceived,
		"total_get_responded":  h.totalGetResponded,
		"total_not_found":      h.totalNotFound,
		"failed_validations":   h.failedValidations,
		"total_bytes_transfer": h.totalBytesTransfer,
	}
}

// ============================================================================
// RDAGetProtocolRequester - Phía hỏi (Light Node)
// ============================================================================

type RDAGetProtocolRequester struct {
	host             host.Host
	selfPeerID       peer.ID
	gridManager      *RDAGridManager
	peerManager      *RDAPeerManager
	subnetMgr        *RDASubnetManager
	predicateChk     *RDAPredicateChecker
	sendGetRequestFn func(context.Context, peer.ID, string, uint32) (*RDASymbol, error)

	mu                 sync.RWMutex
	requestTimeout     time.Duration
	retryAttempts      int
	retryBackoffBase   time.Duration
	retryBackoffMax    time.Duration
	totalRequests      int64
	successfulGets     int64
	failedRetries      int64
	totalBytesReceived int64
}

// NewRDAGetProtocolRequester tạo requester mới
func NewRDAGetProtocolRequester(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
	subnetMgr *RDASubnetManager,
) *RDAGetProtocolRequester {
	gridSize := uint32(gridManager.GetGridDimensions().Cols)
	return &RDAGetProtocolRequester{
		host:               host,
		selfPeerID:         host.ID(),
		gridManager:        gridManager,
		peerManager:        peerManager,
		subnetMgr:          subnetMgr,
		predicateChk:       NewRDAPredicateChecker(gridSize),
		sendGetRequestFn:   nil,
		requestTimeout:     3 * time.Second,
		retryAttempts:      3,
		retryBackoffBase:   50 * time.Millisecond,
		retryBackoffMax:    1 * time.Second,
		totalRequests:      0,
		successfulGets:     0,
		failedRetries:      0,
		totalBytesReceived: 0,
	}
}

func (r *RDAGetProtocolRequester) backoffDuration(attempt int) time.Duration {
	if r.retryBackoffBase <= 0 {
		return 0
	}

	if attempt < 0 {
		attempt = 0
	}

	backoff := r.retryBackoffBase
	for i := 0; i < attempt; i++ {
		if backoff > r.retryBackoffMax/2 {
			backoff = r.retryBackoffMax
			break
		}
		backoff *= 2
	}

	if r.retryBackoffMax > 0 && backoff > r.retryBackoffMax {
		return r.retryBackoffMax
	}

	return backoff
}

func (r *RDAGetProtocolRequester) shouldBackoff(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, ErrRDAQueryTimeout) || errors.Is(err, ErrRDAPeerUnavailable)
}

func (r *RDAGetProtocolRequester) waitBackoff(ctx context.Context, attempt int) error {
	d := r.backoffDuration(attempt)
	if d <= 0 {
		return nil
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// QueryShare implements the GetShare override for Light Nodes
// Throws: (h, i) request → Cell(i) → (myRow, c) → find peers → Send GET → Validate → Return
func (r *RDAGetProtocolRequester) QueryShare(
	ctx context.Context,
	handle string,
	shareIndex uint32,
) (*RDASymbol, error) {
	selfID := r.selfPeerID
	if selfID == "" && r.host != nil {
		selfID = r.host.ID()
	}
	if selfID == "" {
		return nil, fmt.Errorf("%w: requester peer id is unavailable", ErrRDAProtocolDecode)
	}

	// Step 1: Calculate target column c = Cell(i)
	targetCol := Cell(shareIndex, uint32(r.gridManager.GetGridDimensions().Cols))
	myPos, found := r.gridManager.GetPeerPosition(selfID)
	if !found {
		return nil, fmt.Errorf("%w: cannot determine my grid position", ErrRDAProtocolDecode)
	}

	myRow := uint32(myPos.Row)
	getLog.Infof("RDA|GET|QUERY_START handle=%s index=%d targetCol=%d myRow=%d",
		handle[:8], shareIndex, targetCol, myRow)

	// Step 2: Find peers at (myRow, targetCol) intersection
	intersectionPeers := r.findIntersectionPeers(myRow, targetCol)
	if len(intersectionPeers) == 0 {
		getLog.Warnf("RDA|GET|NO_PEERS row=%d col=%d", myRow, targetCol)
		return nil, fmt.Errorf("%w: no peers available at grid intersection", ErrRDAPeerUnavailable)
	}

	getLog.Debugf("RDA|GET|PEERS_FOUND count=%d row=%d col=%d", len(intersectionPeers), myRow, targetCol)

	// Step 3: Try to query peers with retries
	sendFn := r.sendGetRequestFn
	if sendFn == nil {
		sendFn = r.sendGetRequest
	}

	var lastErr error
	for attempt := 0; attempt < r.retryAttempts; attempt++ {
		for _, peer := range intersectionPeers {
			symbol, err := sendFn(ctx, peer, handle, shareIndex)
			if err == nil {
				r.successfulGets++
				getLog.Infof("RDA|GET|SUCCESS ✓ handle=%s index=%d peer=%s attempt=%d",
					handle[:8], shareIndex, peer.String()[:8], attempt+1)
				return symbol, nil
			}
			lastErr = err
			getLog.Debugf("RDA|GET|RETRY peer=%s attempt=%d error=%v", peer.String()[:8], attempt+1, err)
		}

		if attempt < r.retryAttempts-1 && r.shouldBackoff(lastErr) {
			backoff := r.backoffDuration(attempt)
			getLog.Debugf("RDA|GET|BACKOFF attempt=%d delay=%s", attempt+1, backoff)
			if err := r.waitBackoff(ctx, attempt); err != nil {
				return nil, fmt.Errorf("%w: retry backoff interrupted: %v", ErrRDAQueryTimeout, err)
			}
		}
	}

	r.failedRetries++
	getLog.Warnf("RDA|GET|FAILED_ALL handle=%s index=%d row=%d col=%d",
		handle[:8], shareIndex, myRow, targetCol)
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("%w: all peers at intersection (%d, %d) failed to respond", ErrRDAPeerUnavailable, myRow, targetCol)
}

// sendGetRequest gửi GET request đến một peer
func (r *RDAGetProtocolRequester) sendGetRequest(
	ctx context.Context,
	targetPeerID peer.ID,
	handle string,
	shareIndex uint32,
) (*RDASymbol, error) {
	getLog.Debugf("RDA|GET|SEND_START peer=%s handle=%s index=%d", targetPeerID.String()[:8], handle[:8], shareIndex)

	// Open stream
	stream, err := r.host.NewStream(ctx, targetPeerID, RDAGetProtocolID)
	if err != nil {
		getLog.Warnf("RDA|GET|STREAM_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("%w: failed to open stream to %s: %w", ErrRDAPeerUnavailable, targetPeerID.String()[:8], err)
	}
	defer stream.Close()

	r.totalRequests++
	requestID := fmt.Sprintf("%s-%d-%d", r.host.ID().String()[:8], shareIndex, time.Now().UnixNano())

	// Build request
	req := GetMessage{
		Type:       GetMessageTypeQuery,
		Handle:     handle,
		ShareIndex: shareIndex,
		SenderID:   r.host.ID().String(),
		Timestamp:  time.Now().UnixNano() / 1e6,
		RequestID:  requestID,
	}

	// Send request
	if err := stream.SetWriteDeadline(time.Now().Add(r.requestTimeout)); err != nil {
		getLog.Warnf("RDA|GET|WRITE_DEADLINE_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("%w: failed to set write deadline: %w", ErrRDAProtocolDecode, err)
	}

	encoder := json.NewEncoder(stream)
	if err := encoder.Encode(req); err != nil {
		getLog.Warnf("RDA|GET|ENCODE_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("%w: failed to send request: %w", ErrRDAProtocolDecode, err)
	}

	getLog.Debugf("RDA|GET|SEND_OK peer=%s reqID=%s", targetPeerID.String()[:8], requestID)

	// Set stream read deadline for response
	if err := stream.SetReadDeadline(time.Now().Add(r.requestTimeout)); err != nil {
		getLog.Warnf("RDA|GET|DEADLINE_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("%w: failed to set deadline: %w", ErrRDAProtocolDecode, err)
	}

	// Read response
	var resp GetMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&resp); err != nil {
		getLog.Warnf("RDA|GET|RESPONSE_ERROR peer=%s timeout=%v error=%v",
			targetPeerID.String()[:8], r.requestTimeout, err)
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, fmt.Errorf("%w: failed to decode response: %w", ErrRDAQueryTimeout, err)
		}
		return nil, fmt.Errorf("%w: failed to decode response: %w", ErrRDAProtocolDecode, err)
	}

	if err := validateGetResponse(req, resp); err != nil {
		getLog.Warnf("RDA|GET|RESPONSE_INVALID peer=%s reqID=%s error=%v", targetPeerID.String()[:8], requestID, err)
		return nil, err
	}

	// Validate response with Pred
	getLog.Debugf("RDA|GET|VALIDATE handle=%s index=%d size=%d", handle[:8], resp.ShareIndex, len(resp.Data))

	symbol := &RDASymbol{
		Handle:     resp.Handle,
		ShareIndex: resp.ShareIndex,
		Row:        resp.RowID,
		Col:        resp.ColID,
		ShareData:  resp.Data,
		Timestamp:  resp.Timestamp,
		NMTProof:   *resp.NMTProof,
	}

	if !r.predicateChk.Pred(resp.Handle, resp.ShareIndex, symbol) {
		getLog.Warnf("RDA|GET|VALIDATE_FAIL peer=%s handle=%s", targetPeerID.String()[:8], handle[:8])
		return nil, fmt.Errorf("%w: predicate validation failed", ErrRDAProofInvalid)
	}

	r.totalBytesReceived += int64(len(resp.Data))
	getLog.Infof("RDA|GET|VALIDATED ✓ peer=%s handle=%s index=%d size=%d",
		targetPeerID.String()[:8], handle[:8], resp.ShareIndex, len(resp.Data))

	return symbol, nil
}

func validateGetResponse(req, resp GetMessage) error {
	if resp.Type != GetMessageTypeResponse {
		return fmt.Errorf("%w: unexpected response type: %s", ErrRDAProtocolDecode, resp.Type)
	}

	if req.RequestID == "" || resp.RequestID != req.RequestID {
		return fmt.Errorf("%w: request id mismatch", ErrRDAProtocolDecode)
	}

	if resp.Handle != req.Handle || resp.ShareIndex != req.ShareIndex {
		return fmt.Errorf("%w: response identity mismatch", ErrRDAProtocolDecode)
	}

	if len(resp.Data) == 0 {
		return fmt.Errorf("%w: empty payload", ErrRDASymbolNotFound)
	}

	if resp.NMTProof == nil {
		return fmt.Errorf("%w: missing proof payload", ErrRDAProtocolDecode)
	}

	return nil
}

func chooseQueryPeers(rowPeers, colPeers []peer.ID) []peer.ID {
	if len(colPeers) == 0 {
		return nil
	}

	rowSet := make(map[peer.ID]struct{}, len(rowPeers))
	for _, p := range rowPeers {
		rowSet[p] = struct{}{}
	}

	intersection := make([]peer.ID, 0, len(colPeers))
	for _, p := range colPeers {
		if _, ok := rowSet[p]; ok {
			intersection = append(intersection, p)
		}
	}

	if len(intersection) > 0 {
		return dedupeAndSortPeers(intersection)
	}

	// If row+column intersection is empty, keep column-subnet preference as fallback.
	return dedupeAndSortPeers(colPeers)
}

func dedupeAndSortPeers(peers []peer.ID) []peer.ID {
	if len(peers) == 0 {
		return nil
	}

	seen := make(map[peer.ID]struct{}, len(peers))
	out := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out
}

// findIntersectionPeers tìm tất cả peers tại (row, col)
func (r *RDAGetProtocolRequester) findIntersectionPeers(row, col uint32) []peer.ID {
	// Query grid để tìm peers tại giao điểm (row, col)
	// Sử dụng row peers và col peers từ PeerManager
	rowPeers := r.peerManager.GetRowPeers()
	colPeers := r.peerManager.GetColPeers()
	_ = row
	_ = col
	return chooseQueryPeers(rowPeers, colPeers)
}

// GetStats trả về statistics
func (r *RDAGetProtocolRequester) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]interface{}{
		"total_requests":       r.totalRequests,
		"successful_gets":      r.successfulGets,
		"failed_retries":       r.failedRetries,
		"total_bytes_received": r.totalBytesReceived,
		"request_timeout":      r.requestTimeout.String(),
		"retry_attempts":       r.retryAttempts,
		"retry_backoff_base":   r.retryBackoffBase.String(),
		"retry_backoff_max":    r.retryBackoffMax.String(),
	}
}
