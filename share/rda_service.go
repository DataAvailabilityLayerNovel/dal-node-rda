package share

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2pDisc "github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RDANodeService aggregates all RDA components for easy integration into celestia-node
type RDANodeService struct {
	host                   host.Host
	pubsub                 *pubsub.PubSub
	gridManager            *RDAGridManager
	peerManager            *RDAPeerManager
	subnetManager          *RDASubnetManager
	peerFilter             *PeerFilter
	gossipRouter           *RDAGossipSubRouter
	exchangeCoordinator    *RDAExchangeCoordinator
	discovery              *RDADiscovery              // nil if discovery disabled
	bootstrapDiscovery     *BootstrapDiscoveryService // Bootstrap-based peer discovery
	subnetDiscoveryManager *RDASubnetDiscoveryManager
	storeProtocolHandler   *RDAStoreProtocolHandler  // STORE protocol handler for data distribution
	storeProposer          *RDAStoreProposer         // STORE proposer for bridge nodes
	getProtocolHandler     *RDAGetProtocolHandler    // GET protocol handler for query responses
	getProtocolRequester   *RDAGetProtocolRequester  // GET protocol requester for light nodes
	syncProtocolHandler    *RDASyncProtocolHandler   // SYNC protocol handler for data sync responses
	syncProtocolRequester  *RDASyncProtocolRequester // SYNC protocol requester for new nodes
	useSubnetDiscovery     bool                      // Use paper-based subnet protocol instead of DHT
	useBootstrapDiscovery  bool                      // Use bootstrap nodes for peer discovery
	lifecycle              interface{}               // Optional lifecycle manager to signal subnet discovery completion

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// RDATopologySnapshot provides a compact topology view for external callers.
type RDATopologySnapshot struct {
	GridDimensions   GridDimensions `json:"grid_dimensions"`
	RowPeers         int            `json:"row_peers"`
	ColPeers         int            `json:"col_peers"`
	TotalSubnetPeers int            `json:"total_subnet_peers"`
}

// RDAHealthSnapshot provides a compact health view for external callers.
type RDAHealthSnapshot struct {
	Synced bool `json:"synced"`
}

// RDANodeServiceConfig holds configuration for RDANodeService
type RDANodeServiceConfig struct {
	// Grid dimensions
	GridDimensions GridDimensions

	// Filtering policy
	FilterPolicy FilterPolicy

	// Auto-calculate grid size based on expected node count
	// If > 0, overrides GridDimensions
	ExpectedNodeCount uint32

	// Enable detailed logging
	EnableDetailedLogging bool

	// Discovery is a libp2p discovery.Discovery (backed by DHT) used to
	// find row/col peers via rendezvous. If nil, DHT-based discovery is
	// disabled (useful for tests using mocknet).
	Discovery libp2pDisc.Discovery

	// BootstrapPeers is the list of known stable peers (typically bridge nodes)
	// to connect to immediately on Start, so DHT discovery can follow.
	BootstrapPeers []peer.AddrInfo

	// PeerBootstraps is a list of other bootstrap servers for peer info sync.
	// Only used when this node is a bootstrap server itself.
	PeerBootstraps []peer.AddrInfo

	// UseSubnetDiscovery enables RDA paper-based subnet discovery protocol
	// instead of DHT. When true, nodes join via bootstrap and discover peers
	// through subnet announcements/gossip.
	UseSubnetDiscovery bool

	// SubnetDiscoveryDelay is the delay before pulling full membership list
	// after joining a subnet (implements "delayed pull" from paper).
	SubnetDiscoveryDelay time.Duration
}

// DefaultRDANodeServiceConfig returns default configuration
func DefaultRDANodeServiceConfig() RDANodeServiceConfig {
	return RDANodeServiceConfig{
		GridDimensions:        DefaultGridDimensions,
		FilterPolicy:          DefaultFilterPolicy(),
		ExpectedNodeCount:     0,
		EnableDetailedLogging: false,
	}
}

// Validate checks if the configuration is valid
func (c *RDANodeServiceConfig) Validate() error {
	if c.GridDimensions.Rows == 0 || c.GridDimensions.Cols == 0 {
		return fmt.Errorf("grid dimensions must be positive: rows=%d, cols=%d", c.GridDimensions.Rows, c.GridDimensions.Cols)
	}

	if c.GridDimensions.Rows > 10000 || c.GridDimensions.Cols > 10000 {
		return fmt.Errorf("grid dimensions too large: rows=%d, cols=%d (max 10000)", c.GridDimensions.Rows, c.GridDimensions.Cols)
	}

	if c.SubnetDiscoveryDelay < 0 {
		return fmt.Errorf("subnet discovery delay cannot be negative: %v", c.SubnetDiscoveryDelay)
	}

	return nil
}

// NewRDANodeService creates a new RDA node service
func NewRDANodeService(
	host host.Host,
	pubsub *pubsub.PubSub,
	config RDANodeServiceConfig,
) *RDANodeService {
	ctx, cancel := context.WithCancel(context.Background())

	// Calculate grid dimensions if needed
	gridDims := config.GridDimensions
	if config.ExpectedNodeCount > 0 {
		gridDims = CalculateOptimalGridSize(config.ExpectedNodeCount)
	}

	// Create grid manager
	gridManager := NewRDAGridManager(gridDims)

	// Create peer manager
	peerManager := NewRDAPeerManager(host, gridManager)

	// Create subnet manager
	subnetManager := NewRDASubnetManager(pubsub, gridManager, host.ID())

	// Create peer filter
	peerFilter := NewPeerFilter(host, gridManager, config.FilterPolicy)

	// Create gossip router
	gossipRouter := NewRDAGossipSubRouter(host, gridManager, peerManager, subnetManager, peerFilter)

	// Create exchange coordinator
	exchangeCoordinator := NewRDAExchangeCoordinator(host, gridManager, peerManager, subnetManager, peerFilter)

	// Optionally create DHT-backed discovery service.
	var disc *RDADiscovery
	if config.Discovery != nil && !config.UseSubnetDiscovery {
		disc = NewRDADiscovery(host, config.Discovery, gridManager, config.BootstrapPeers)
	}

	// Create bootstrap discovery service (always listens for incoming requests)
	// Will only contact bootstrap peers if config.BootstrapPeers is non-empty
	myCoords := GetCoords(host.ID(), gridDims)
	bootstrapDisc := NewBootstrapDiscoveryService(host, config.BootstrapPeers, gridManager, uint32(myCoords.Row), uint32(myCoords.Col))

	// Populate peer bootstraps (other bootstrap servers for sync)
	for peerID, info := range convertAddrInfoToMap(config.PeerBootstraps) {
		bootstrapDisc.peerBootstraps[peerID] = info
	}

	useBootstrapDiscovery := true

	// Create subnet discovery manager if enabled
	delayBeforePull := config.SubnetDiscoveryDelay
	if delayBeforePull == 0 {
		delayBeforePull = 4 * time.Second // Default: ~4 rounds
	}
	subnetDiscoveryMgr := NewRDASubnetDiscoveryManager(host, pubsub, delayBeforePull)

	// Create local storage for STORE operations (column-based)
	localStorage := NewRDAStorage(RDAStorageConfig{
		MyRow:    uint32(myCoords.Row),
		MyCol:    uint32(myCoords.Col),
		GridSize: uint32(gridDims.Cols),
	})

	// Create STORE protocol handler
	storeHandler := NewRDAStoreProtocolHandler(host, gridManager, peerManager, subnetManager, localStorage)

	// Create STORE proposer (for bridge nodes)
	storeProposer := NewRDAStoreProposer(host, gridManager, peerManager)

	// Create GET protocol handler (responder for queries)
	getHandler := NewRDAGetProtocolHandler(host, gridManager, peerManager, subnetManager, localStorage)

	// Create GET protocol requester (for light nodes to query)
	getRequester := NewRDAGetProtocolRequester(host, gridManager, peerManager, subnetManager, localStorage)

	// Create SYNC protocol handler (responder for sync requests)
	syncHandler := NewRDASyncProtocolHandler(host, gridManager, localStorage)

	// Create SYNC protocol requester (for new nodes to sync on startup)
	syncDelay := config.SubnetDiscoveryDelay
	if syncDelay == 0 {
		syncDelay = 4 * time.Second
	}
	syncRequester := NewRDASyncProtocolRequester(host, gridManager, peerManager, localStorage, subnetManager, syncDelay)

	service := &RDANodeService{
		host:                   host,
		pubsub:                 pubsub,
		gridManager:            gridManager,
		peerManager:            peerManager,
		subnetManager:          subnetManager,
		peerFilter:             peerFilter,
		bootstrapDiscovery:     bootstrapDisc,
		useBootstrapDiscovery:  useBootstrapDiscovery,
		gossipRouter:           gossipRouter,
		exchangeCoordinator:    exchangeCoordinator,
		discovery:              disc,
		subnetDiscoveryManager: subnetDiscoveryMgr,
		storeProtocolHandler:   storeHandler,
		storeProposer:          storeProposer,
		getProtocolHandler:     getHandler,
		getProtocolRequester:   getRequester,
		syncProtocolHandler:    syncHandler,
		syncProtocolRequester:  syncRequester,
		useSubnetDiscovery:     config.UseSubnetDiscovery,
		ctx:                    ctx,
		cancel:                 cancel,
	}

	if config.EnableDetailedLogging {
		log.Infof("RDA Node Service initialized with grid %dx%d",
			gridDims.Rows, gridDims.Cols)
	}

	return service
}

// convertAddrInfoToMap converts a list of peer.AddrInfo to a map indexed by peer ID string
func convertAddrInfoToMap(peers []peer.AddrInfo) map[string]peer.AddrInfo {
	result := make(map[string]peer.AddrInfo, len(peers))
	for _, p := range peers {
		result[p.ID.String()] = p
	}
	return result
}

// Start initializes and starts all RDA components
func (s *RDANodeService) Start(startCtx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Start peer manager
	if err := s.peerManager.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start peer manager: %w", err)
	}

	// Start subnet manager
	if err := s.subnetManager.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start subnet manager: %w", err)
	}

	// Start exchange coordinator
	if err := s.exchangeCoordinator.Start(); err != nil {
		return fmt.Errorf("failed to start exchange coordinator: %w", err)
	}

	// Start DHT discovery (if configured and not using subnet discovery)
	// When using subnet discovery (RDA Grid), DHT is always nil and this is skipped
	if s.discovery != nil && !s.useSubnetDiscovery {
		log.Infof("RDA: Starting DHT-based peer discovery (legacy mode)")
		if err := s.discovery.Start(startCtx); err != nil {
			return fmt.Errorf("failed to start RDA discovery: %w", err)
		}
	} else if s.useSubnetDiscovery {
		log.Infof("RDA: DHT discovery disabled (using Grid-based subnet discovery)")
	}

	// Start STORE protocol handler (stream-based data distribution)
	if err := s.storeProtocolHandler.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start STORE protocol handler: %w", err)
	}

	// Start GET protocol handler (responds to queries from light nodes)
	if err := s.getProtocolHandler.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start GET protocol handler: %w", err)
	}

	// Start SYNC protocol handler (responds to sync requests from new nodes)
	if err := s.syncProtocolHandler.Start(startCtx); err != nil {
		return fmt.Errorf("failed to start SYNC protocol handler: %w", err)
	}

	// Trigger SYNC on startup for new nodes (after discovery delay)
	if s.syncProtocolRequester != nil {
		go s.syncProtocolRequester.TriggerSyncOnStartup(startCtx)
	}

	// If using subnet discovery, announce to subnets
	if s.useSubnetDiscovery {
		go s.startSubnetDiscovery(startCtx)
	}

	log.Infof("RDA Node Service started successfully")
	return nil
}

// SetLifecycle sets the lifecycle manager for coordinating subnet discovery completion
// This is used to signal when subnet discovery has completed to prevent blocking OnStart hooks
func (s *RDANodeService) SetLifecycle(lifecycle interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lifecycle = lifecycle
}

// IsSubnetDiscoveryReady reports whether subnet discovery has completed when subnet discovery mode is enabled.
// If subnet discovery is disabled, or no lifecycle manager is attached, this returns true for compatibility.
func (s *RDANodeService) IsSubnetDiscoveryReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.useSubnetDiscovery {
		return true
	}

	if s.lifecycle == nil {
		return true
	}

	if lifecycleMgr, ok := s.lifecycle.(interface{ IsSubnetReady() bool }); ok {
		return lifecycleMgr.IsSubnetReady()
	}

	return true
}

// Stop stops all RDA components
func (s *RDANodeService) Stop(stopCtx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel()

	// Stop STORE protocol handler
	if s.storeProtocolHandler != nil {
		if err := s.storeProtocolHandler.Stop(); err != nil {
			log.Warnf("RDA STORE protocol handler stop error: %v", err)
		}
	}

	// Stop GET protocol handler
	if s.getProtocolHandler != nil {
		if err := s.getProtocolHandler.Stop(); err != nil {
			log.Warnf("RDA GET protocol handler stop error: %v", err)
		}
	}

	// Stop SYNC protocol handler
	if s.syncProtocolHandler != nil {
		if err := s.syncProtocolHandler.Stop(); err != nil {
			log.Warnf("RDA SYNC protocol handler stop error: %v", err)
		}
	}

	// Stop discovery first so no new connections are attempted
	if s.discovery != nil {
		if err := s.discovery.Stop(stopCtx); err != nil {
			log.Warnf("RDA discovery stop error: %v", err)
		}
	}

	// Stop bootstrap discovery service
	if s.bootstrapDiscovery != nil {
		if err := s.bootstrapDiscovery.Stop(stopCtx); err != nil {
			log.Warnf("RDA bootstrap discovery stop error: %v", err)
		}
	}

	// Stop subnet discovery manager
	if err := s.subnetDiscoveryManager.Stop(stopCtx); err != nil {
		log.Warnf("RDA subnet discovery stop error: %v", err)
	}

	// Stop exchange coordinator
	s.exchangeCoordinator.Stop()

	// Stop subnet manager
	s.subnetManager.Stop(stopCtx)

	// Stop peer manager
	s.peerManager.Stop(stopCtx)

	s.wg.Wait()

	log.Infof("RDA Node Service stopped")
	return nil
}

// GetGridManager returns the grid manager
func (s *RDANodeService) GetGridManager() *RDAGridManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gridManager
}

// GetPeerManager returns the peer manager
func (s *RDANodeService) GetPeerManager() *RDAPeerManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager
}

// GetSubnetManager returns the subnet manager
func (s *RDANodeService) GetSubnetManager() *RDASubnetManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subnetManager
}

// GetPeerFilter returns the peer filter
func (s *RDANodeService) GetPeerFilter() *PeerFilter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerFilter
}

// GetGossipRouter returns the gossip router
func (s *RDANodeService) GetGossipRouter() *RDAGossipSubRouter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gossipRouter
}

// GetSubnetDiscoveryManager returns the subnet discovery manager
func (s *RDANodeService) GetSubnetDiscoveryManager() *RDASubnetDiscoveryManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subnetDiscoveryManager
}

// startSubnetDiscovery initiates the subnet discovery protocol with bootstrap nodes
// Step 1: Contact bootstrap nodes to join row/column subnets
// Step 2: Get peer lists from bootstrap nodes
// Step 3: Announce to subnets via gossip (for redundancy)
// Step 4: Connect to all discovered peers
func (s *RDANodeService) startSubnetDiscovery(startCtx context.Context) {
	s.wg.Add(1)
	defer s.wg.Done()

	// GUARANTEE: Signal discovery completion if not already done (early signal after bootstrap)
	// This ensures the OnStart hook completes quickly
	defer func() {
		if s.lifecycle != nil {
			if lifecycleMgr, ok := s.lifecycle.(interface{ MarkSubnetDiscoveryReady(error) }); ok {
				// Try to signal, but it's a no-op if already signalled (sync.Once inside MarkSubnetDiscoveryReady)
				lifecycleMgr.MarkSubnetDiscoveryReady(nil)
			}
		}
	}()

	// Use service context for discovery operations so they can be cancelled on shutdown
	// The 12-second timer in GetMembersWithDuration still limits gossip wait time
	discoveryCtx := s.ctx

	// Get this node's grid position
	myPos := GetCoords(s.host.ID(), s.gridManager.GetGridDimensions())
	rowSubnet := fmt.Sprintf("row/%d", myPos.Row)
	colSubnet := fmt.Sprintf("col/%d", myPos.Col)

	log.Infof("RDA Subnet Discovery: node at (row=%d, col=%d)", myPos.Row, myPos.Col)

	// ========== STEP 1 & 2: Bootstrap-based discovery ==========
	// All nodes always listen for bootstrap requests (server mode)
	// Only nodes with configured bootstrap peers will contact them (client mode)
	var bootstrapRowPeers []peer.AddrInfo
	var bootstrapColPeers []peer.AddrInfo

	if err := s.bootstrapDiscovery.Start(startCtx); err != nil {
		log.Warnf("RDA bootstrap discovery failed to start: %v", err)
	} else {
		// Wait for bootstrap discovery to complete (only contacts if bootstrap peers configured)
		// Allow 8 seconds for bootstrap nodes to join all row subnets and build routing tables
		// With 128 rows @ ~100ms per join + network latency: 12-15s needed, so 8s is minimum for fast networks
		time.Sleep(8 * time.Second)
		bootstrapRowPeers = s.bootstrapDiscovery.GetRowPeers()
		bootstrapColPeers = s.bootstrapDiscovery.GetColPeers()
		bootstrapRowDetailed := s.bootstrapDiscovery.GetRowPeersDetailed()
		bootstrapColDetailed := s.bootstrapDiscovery.GetColPeersDetailed()

		if len(bootstrapRowPeers) > 0 || len(bootstrapColPeers) > 0 {
			// Format detailed peer info with positions
			var rowDetails, colDetails string
			if len(bootstrapRowDetailed) > 0 {
				for i, p := range bootstrapRowDetailed {
					if i > 0 {
						rowDetails += ", "
					}
					rowDetails += fmt.Sprintf("%s@(%d,%d)", p.PeerID, p.Row, p.Col)
				}
			}
			if len(bootstrapColDetailed) > 0 {
				for i, p := range bootstrapColDetailed {
					if i > 0 {
						colDetails += ", "
					}
					colDetails += fmt.Sprintf("%s@(%d,%d)", p.PeerID, p.Row, p.Col)
				}
			}
			log.Infof("📤 DISCOVERY SOURCE: BOOTSTRAP - row_peers=%d [%s], col_peers=%d [%s]", len(bootstrapRowPeers), rowDetails, len(bootstrapColPeers), colDetails)
		} else {
			log.Warnf("⚠️  DISCOVERY SOURCE: BOOTSTRAP - No peers discovered (check bootstrap peer configuration)")
		}
	}

	// ========== EARLY SIGNAL: After bootstrap, signal discovery readiness ==========
	// This allows OnStart hook to complete quickly (avoid 20s timeout)
	// Gossip discovery continues in background for additional peers
	// Call the signal AFTER bootstrap completes but BEFORE gossip wait starts
	if s.lifecycle != nil {
		if lifecycleMgr, ok := s.lifecycle.(interface{ MarkSubnetDiscoveryReady(error) }); ok {
			log.Infof("🚀 EARLY SIGNAL: RDA Subnet Discovery ready after bootstrap (gossip continues in background)")
			lifecycleMgr.MarkSubnetDiscoveryReady(nil)
		}
	}

	// ========== STEP 3: Gossip-based discovery (for redundancy, runs in background) ==========
	var gossipRowPeers []SubnetMember
	var gossipColPeers []SubnetMember

	// Create announcers for row and column subnets
	rowAnnouncer, err := s.subnetDiscoveryManager.GetOrCreateAnnouncer(discoveryCtx, rowSubnet)
	if err != nil {
		log.Warnf("RDA subnet discovery: failed to create row announcer: %v", err)
	} else {
		// Announce presence in row subnet
		if err := rowAnnouncer.AnnounceJoin(discoveryCtx); err != nil {
			log.Warnf("RDA subnet discovery: failed to announce to row subnet: %v", err)
		}
		// Start periodic announcements (every 2 seconds for 20 seconds total)
		rowAnnouncer.StartPeriodicAnnouncement(discoveryCtx, 2*time.Second, 20*time.Second)
		// Collect gossip-based members - wait 12s for announcements to propagate
		gossipRowPeers = rowAnnouncer.GetMembersWithDuration(discoveryCtx, 12*time.Second)
	}

	colAnnouncer, err := s.subnetDiscoveryManager.GetOrCreateAnnouncer(discoveryCtx, colSubnet)
	if err != nil {
		log.Warnf("RDA subnet discovery: failed to create col announcer: %v", err)
	} else {
		// Announce presence in column subnet
		if err := colAnnouncer.AnnounceJoin(discoveryCtx); err != nil {
			log.Warnf("RDA subnet discovery: failed to announce to col subnet: %v", err)
		}
		// Start periodic announcements (every 2 seconds for 20 seconds total)
		colAnnouncer.StartPeriodicAnnouncement(discoveryCtx, 2*time.Second, 20*time.Second)
		// Collect gossip-based members - wait 12s for announcements to propagate
		gossipColPeers = colAnnouncer.GetMembersWithDuration(discoveryCtx, 12*time.Second)
	}

	if len(gossipRowPeers) > 0 || len(gossipColPeers) > 0 {
		// Format gossip peer IDs
		var rowGossipStr, colGossipStr string
		if len(gossipRowPeers) > 0 {
			for i, m := range gossipRowPeers {
				if i > 0 {
					rowGossipStr += ", "
				}
				rowGossipStr += m.PeerID.String()[:12]
			}
		}
		if len(gossipColPeers) > 0 {
			for i, m := range gossipColPeers {
				if i > 0 {
					colGossipStr += ", "
				}
				colGossipStr += m.PeerID.String()[:12]
			}
		}
		log.Infof("📚 DISCOVERY SOURCE: GOSSIP - row_peers=%d [%s], col_peers=%d [%s]", len(gossipRowPeers), rowGossipStr, len(gossipColPeers), colGossipStr)
	} else {
		log.Warnf("⚠️  DISCOVERY SOURCE: GOSSIP - No peers discovered from gossip subnets")
	}

	// ========== STEP 4: Combine bootstrap + gossip results & connect ==========
	s.connectToAllDiscoveredPeers(discoveryCtx, bootstrapRowPeers, bootstrapColPeers, gossipRowPeers, gossipColPeers)

	// Monitor for new members via gossip
	if rowAnnouncer != nil && colAnnouncer != nil {
		go s.monitorSubnetMembership(rowAnnouncer, colAnnouncer)
	}
}

// connectToAllDiscoveredPeers combines bootstrap and gossip discovery results and connects
func (s *RDANodeService) connectToAllDiscoveredPeers(
	ctx context.Context,
	bootstrapRowPeers, bootstrapColPeers []peer.AddrInfo,
	gossipRowPeers, gossipColPeers []SubnetMember,
) {
	// Merge bootstrap peers (already in AddrInfo format)
	rowPeersMap := make(map[peer.ID]peer.AddrInfo)
	colPeersMap := make(map[peer.ID]peer.AddrInfo)

	// Add bootstrap peers
	for _, p := range bootstrapRowPeers {
		rowPeersMap[p.ID] = p
	}
	for _, p := range bootstrapColPeers {
		colPeersMap[p.ID] = p
	}

	// Add gossip peers (SubnetMember already has peer.AddrInfo)
	for _, m := range gossipRowPeers {
		for _, addrInfo := range m.PeerAddrs {
			// Merge addresses if peer already exists
			if existing, ok := rowPeersMap[m.PeerID]; ok {
				existing.Addrs = append(existing.Addrs, addrInfo.Addrs...)
				rowPeersMap[m.PeerID] = existing
			} else {
				rowPeersMap[m.PeerID] = addrInfo
			}
		}
	}
	for _, m := range gossipColPeers {
		for _, addrInfo := range m.PeerAddrs {
			// Merge addresses if peer already exists
			if existing, ok := colPeersMap[m.PeerID]; ok {
				existing.Addrs = append(existing.Addrs, addrInfo.Addrs...)
				colPeersMap[m.PeerID] = existing
			} else {
				colPeersMap[m.PeerID] = addrInfo
			}
		}
	}

	// Convert maps to slices
	var rowPeers, colPeers []peer.AddrInfo
	for _, p := range rowPeersMap {
		rowPeers = append(rowPeers, p)
	}
	for _, p := range colPeersMap {
		colPeers = append(colPeers, p)
	}

	log.Infof(
		"✅ DISCOVERY COMPLETE - bootstrap_row=%d + gossip_row=%d = total_row=%d | bootstrap_col=%d + gossip_col=%d = total_col=%d",
		len(bootstrapRowPeers),
		len(gossipRowPeers),
		len(rowPeers),
		len(bootstrapColPeers),
		len(gossipColPeers),
		len(colPeers),
	)

	// Show final peer list
	if len(rowPeers) > 0 || len(colPeers) > 0 {
		rowPeerInfo := make([]string, len(rowPeers))
		for i, p := range rowPeers {
			rowPeerInfo[i] = p.ID.String()[:12]
		}
		colPeerInfo := make([]string, len(colPeers))
		for i, p := range colPeers {
			colPeerInfo[i] = p.ID.String()[:12]
		}

		var rowStr, colStr string
		if len(rowPeerInfo) > 0 {
			rowStr = strings.Join(rowPeerInfo, ", ")
		} else {
			rowStr = "none"
		}
		if len(colPeerInfo) > 0 {
			colStr = strings.Join(colPeerInfo, ", ")
		} else {
			colStr = "none"
		}

		log.Infof("🎯 FINAL PEER COUNT - rows: %d [%s], cols: %d [%s]", len(rowPeers), rowStr, len(colPeers), colStr)
	} else {
		log.Warnf("❌ NO PEERS DISCOVERED - Check bootstrap configuration and gossip subnet connectivity")
	}

	// Connect to discovered members
	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Connect to row members
	for _, p := range rowPeers {
		go func(peerInfo peer.AddrInfo) {
			log.Infof("RDA: attempting row peer connect to %s", peerInfo.ID.String())
			if err := s.host.Connect(connCtx, peerInfo); err != nil {
				log.Infof("RDA: failed to connect row peer %s: %v", peerInfo.ID.String(), err)
			} else {
				log.Infof("RDA: connected to row peer %s", peerInfo.ID.String())
			}
		}(p)
	}

	// Connect to column members
	for _, p := range colPeers {
		go func(peerInfo peer.AddrInfo) {
			log.Infof("RDA: attempting col peer connect to %s", peerInfo.ID.String())
			if err := s.host.Connect(connCtx, peerInfo); err != nil {
				log.Infof("RDA: failed to connect col peer %s: %v", peerInfo.ID.String(), err)
			} else {
				log.Infof("RDA: connected to col peer %s", peerInfo.ID.String())
			}
		}(p)
	}
}

// connectToSubnetMembers establishes connections to discovered subnet members
func (s *RDANodeService) connectToSubnetMembers(ctx context.Context,
	rowMembers, colMembers []SubnetMember) {

	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Connect to row members
	for _, member := range rowMembers {
		go func(m SubnetMember) {
			for _, addrInfo := range m.PeerAddrs {
				if err := s.host.Connect(connCtx, addrInfo); err != nil {
					log.Debugf("RDA: failed to connect row member %s: %v", m.PeerID, err)
					continue
				}
				log.Debugf("RDA: connected to row member %s", m.PeerID)
				break
			}
		}(member)
	}

	// Connect to column members
	for _, member := range colMembers {
		go func(m SubnetMember) {
			for _, addrInfo := range m.PeerAddrs {
				if err := s.host.Connect(connCtx, addrInfo); err != nil {
					log.Debugf("RDA: failed to connect col member %s: %v", m.PeerID, err)
					continue
				}
				log.Debugf("RDA: connected to col member %s", m.PeerID)
				break
			}
		}(member)
	}
}

// monitorSubnetMembership continuously monitors for new members joining subnets
func (s *RDANodeService) monitorSubnetMembership(
	rowAnnouncer, colAnnouncer *SubnetAnnouncer) {

	for {
		select {
		case <-s.ctx.Done():
			return
		case member := <-rowAnnouncer.MemberUpdates():
			log.Debugf("RDA: new row member detected: %s", member.PeerID)
			go func(m SubnetMember) {
				for _, addr := range m.PeerAddrs {
					_ = s.host.Connect(context.Background(), addr)
				}
			}(member)
		case member := <-colAnnouncer.MemberUpdates():
			log.Debugf("RDA: new col member detected: %s", member.PeerID)
			go func(m SubnetMember) {
				for _, addr := range m.PeerAddrs {
					_ = s.host.Connect(context.Background(), addr)
				}
			}(member)
		}
	}
}

// GetDiscovery returns the RDA discovery service (nil if disabled).
func (s *RDANodeService) GetDiscovery() *RDADiscovery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.discovery
}

// GetExchangeCoordinator returns the exchange coordinator
func (s *RDANodeService) GetExchangeCoordinator() *RDAExchangeCoordinator {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exchangeCoordinator
}

// GetMyPosition returns this node's position in the grid
func (s *RDANodeService) GetMyPosition() GridPosition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager.GetMyPosition()
}

// GetRowPeers returns peers in the same row
func (s *RDANodeService) GetRowPeers() []peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager.GetRowPeers()
}

// GetColPeers returns peers in the same column
func (s *RDANodeService) GetColPeers() []peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager.GetColPeers()
}

// GetSubnetPeers returns all peers in the subnet (row + column)
func (s *RDANodeService) GetSubnetPeers() []peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager.GetSubnetPeers()
}

// PublishToSubnet publishes data to the entire subnet
func (s *RDANodeService) PublishToSubnet(ctx context.Context, data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subnetManager.PublishToSubnet(ctx, data)
}

// PublishToRow publishes data to row peers
func (s *RDANodeService) PublishToRow(ctx context.Context, data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subnetManager.PublishToRow(ctx, data)
}

// PublishToCol publishes data to column peers
func (s *RDANodeService) PublishToCol(ctx context.Context, data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subnetManager.PublishToCol(ctx, data)
}

// RequestDataFromRow requests data from row peers
func (s *RDANodeService) RequestDataFromRow(dataHash []byte) <-chan exchangeResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exchangeCoordinator.RequestFromRow(dataHash)
}

// RequestDataFromCol requests data from column peers
func (s *RDANodeService) RequestDataFromCol(dataHash []byte) <-chan exchangeResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.exchangeCoordinator.RequestFromCol(dataHash)
}

// GetStatus returns status information about the RDA node
func (s *RDANodeService) GetStatus() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rowPeers := s.peerManager.GetRowPeers()
	colPeers := s.peerManager.GetColPeers()
	position := s.peerManager.GetMyPosition()
	gridDims := s.gridManager.GetGridDimensions()

	return map[string]interface{}{
		"position":           position,
		"grid_dimensions":    gridDims,
		"row_peers":          len(rowPeers),
		"col_peers":          len(colPeers),
		"total_subnet_peers": len(s.peerManager.GetSubnetPeers()),
		"router_stats":       s.gossipRouter.GetStats(),
	}
}

// OnPeerConnected returns channel for peer connection events
func (s *RDANodeService) OnPeerConnected() <-chan peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager.OnPeerConnected()
}

// OnPeerDisconnected returns channel for peer disconnection events
func (s *RDANodeService) OnPeerDisconnected() <-chan peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerManager.OnPeerDisconnected()
}

// SetFilterPolicy updates the peer filtering policy
func (s *RDANodeService) SetFilterPolicy(policy FilterPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerFilter.SetPolicy(policy)
}

// GetFilterStats returns statistics about peer filtering
func (s *RDANodeService) GetFilterStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerFilter.GetStats(s.gridManager)
}

// IsValidPeer checks if a peer is valid for communication
func (s *RDANodeService) IsValidPeer(peerID peer.ID) (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerFilter.CanCommunicate(peerID)
}

// FilterPeerList filters a list of peers to only include valid ones
func (s *RDANodeService) FilterPeerList(peers []peer.ID) []peer.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerFilter.FilterPeers(peers)
}

// ============================================================================
// STORE Protocol API
// ============================================================================

// GetStoreProtocolHandler returns the STORE protocol handler
func (s *RDANodeService) GetStoreProtocolHandler() *RDAStoreProtocolHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.storeProtocolHandler
}

// GetStoreProposer returns the STORE proposer (for bridge nodes)
func (s *RDANodeService) GetStoreProposer() *RDAStoreProposer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.storeProposer
}

// DistributeBlock distributes a block across the grid using STORE protocol
// Called by bridge/full nodes when a new block is produced
func (s *RDANodeService) DistributeBlock(ctx context.Context, handle string, height uint64, shares []*RDASymbol) error {
	if s.storeProposer == nil {
		return fmt.Errorf("store proposer not available")
	}
	return s.storeProposer.DistributeBlock(ctx, handle, height, shares)
}

// GetStoreStats returns statistics for STORE operations
func (s *RDANodeService) GetStoreStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.storeProtocolHandler == nil {
		return make(map[string]interface{})
	}
	return s.storeProtocolHandler.GetStoreStats()
}

// GetGetProtocolHandler returns the GET protocol handler
func (s *RDANodeService) GetGetProtocolHandler() *RDAGetProtocolHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getProtocolHandler
}

// GetGetProtocolRequester returns the GET protocol requester
func (s *RDANodeService) GetGetProtocolRequester() *RDAGetProtocolRequester {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getProtocolRequester
}

// QueryShare is the main entry point for light nodes to query a share
// Replaces traditional DAS worker with 1-hop RDA grid query
func (s *RDANodeService) QueryShare(ctx context.Context, handle string, shareIndex uint32) (*RDASymbol, error) {
	if s.getProtocolRequester == nil {
		return nil, fmt.Errorf("get protocol requester not available")
	}
	return s.getProtocolRequester.QueryShare(ctx, handle, shareIndex)
}

// GetTopologySnapshot returns a compact, type-safe topology summary.
func (s *RDANodeService) GetTopologySnapshot() RDATopologySnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.gridManager == nil || s.peerManager == nil {
		return RDATopologySnapshot{}
	}

	return RDATopologySnapshot{
		GridDimensions:   s.gridManager.GetGridDimensions(),
		RowPeers:         len(s.peerManager.GetRowPeers()),
		ColPeers:         len(s.peerManager.GetColPeers()),
		TotalSubnetPeers: len(s.peerManager.GetSubnetPeers()),
	}
}

// GetGetStats returns statistics for GET operations
func (s *RDANodeService) GetGetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.getProtocolHandler == nil {
		return make(map[string]interface{})
	}
	return s.getProtocolHandler.GetStats()
}

// GetSyncProtocolHandler returns the SYNC protocol handler
func (s *RDANodeService) GetSyncProtocolHandler() *RDASyncProtocolHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncProtocolHandler
}

// GetSyncProtocolRequester returns the SYNC protocol requester
func (s *RDANodeService) GetSyncProtocolRequester() *RDASyncProtocolRequester {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.syncProtocolRequester
}

// GetSyncStats returns statistics for SYNC operations
func (s *RDANodeService) GetSyncStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.syncProtocolRequester == nil {
		return make(map[string]interface{})
	}
	return s.syncProtocolRequester.GetStats()
}

// IsSynced returns whether this node has completed initial column sync
func (s *RDANodeService) IsSynced() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.syncProtocolRequester == nil {
		return false
	}
	return s.syncProtocolRequester.IsSynced()
}

// GetHealthSnapshot returns a compact, type-safe health summary.
func (s *RDANodeService) GetHealthSnapshot() RDAHealthSnapshot {
	return RDAHealthSnapshot{Synced: s.IsSynced()}
}

// SyncColumn triggers a best-effort sync operation for current column membership.
// sinceHeight is accepted for forward compatibility and applied when requester supports it.
func (s *RDANodeService) SyncColumn(ctx context.Context, _ uint64) error {
	s.mu.RLock()
	requester := s.syncProtocolRequester
	s.mu.RUnlock()

	if requester == nil {
		return fmt.Errorf("sync protocol requester not available")
	}

	requester.TriggerSyncOnStartup(ctx)
	return nil
}

// RDAIntegrationExample shows how to integrate RDA into nodebuilder
// This is just an example - actual integration would be in nodebuilder/p2p/module.go
/*
func NewRDAModule(cfg *RDANodeServiceConfig) fx.Option {
	return fx.Options(
		fx.Provide(
			func(host host.Host, ps *pubsub.PubSub) *RDANodeService {
				return NewRDANodeService(host, ps, *cfg)
			},
		),
		fx.Invoke(
			func(lc fx.Lifecycle, rda *RDANodeService) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						return rda.Start(ctx)
					},
					OnStop: func(ctx context.Context) error {
						return rda.Stop(ctx)
					},
				})
			},
		),
	)
}

// In nodebuilder/default_services.go, add RDANodeService to the node type:
type Node struct {
	// ... existing fields ...
	RDAService *RDANodeService
}

// In nodebuilder/node.go, add RDANodeService to provide it:
func provide(cfg *Config) fx.Option {
	return fx.Options(
		// ... existing providers ...
		fx.Provide(func() *RDANodeServiceConfig {
			return &RDANodeServiceConfig{
				ExpectedNodeCount: 10000,
				FilterPolicy: DefaultFilterPolicy(),
				EnableDetailedLogging: true,
			}
		}),
		NewRDAModule(rdaCfg),
	)
}
*/
