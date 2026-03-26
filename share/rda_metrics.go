package share

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var rdaMeter = otel.Meter("rda")

// RDAMetrics chứa tất cả metrics RDA-specific
type RDAMetrics struct {
	// Network & Topology
	matrixRowSyncLatency     metric.Float64Histogram // Milliseconds
	matrixColSyncLatency     metric.Float64Histogram // Milliseconds
	connectedPeersPerNode    metric.Int64Gauge       // Number of peers
	subnetBandwidthReduction metric.Float64Gauge     // Percentage 0-100
	controlMessageOverhead   metric.Float64Gauge     // Bytes/sec

	// DAS Performance
	erasureRecoverySuccess   metric.Float64Gauge     // Percentage 0-100
	matrixDirectFetchLatency metric.Float64Histogram // Milliseconds
	samplingThroughput       metric.Int64Counter     // Samples/sec

	// Fault Tolerance
	nodeFailureRecoveryRate metric.Float64Gauge     // Percentage 0-100
	dataRecoveryTime        metric.Float64Histogram // Milliseconds

	// System Resources
	bandwidthSavingsVsGossip metric.Float64Gauge // Percentage (30-50% target)
	cpuLoadReduction         metric.Float64Gauge // Percentage
	memoryUsagePerNode       metric.Int64Gauge   // Bytes

	// Additional counters for dashboard compatibility
	peerChurnTotal              metric.Int64Counter // Total peer churn events
	controlMessageBytesTotal    metric.Int64Counter // Total control message bytes
	samplingLatencyP99          metric.Float64Gauge // P99 latency in ms
	erasureRecoveryTotal        metric.Int64Counter // Total recovery attempts
	erasureRecoverySuccessTotal metric.Int64Counter // Total successful recoveries
	directRecoveryBytesTotal    metric.Int64Counter // Total bytes recovered directly

	// Internal state for calculations
	mu                    sync.RWMutex
	totalBytesWithRDA     atomic.Int64
	totalBytesWithGossip  atomic.Int64
	successfulRecoveries  atomic.Int64
	totalRecoveryAttempts atomic.Int64
	connectedPeersCount   atomic.Int64
	lastP99Latency        atomic.Int64
}

// InitRDAMetrics initializes all RDA metrics
func InitRDAMetrics() (*RDAMetrics, error) {
	m := &RDAMetrics{}
	var err error

	// Network & Topology Metrics
	m.matrixRowSyncLatency, err = rdaMeter.Float64Histogram(
		"rda_matrix_row_sync_latency_ms",
		metric.WithDescription("Direct fetch latency for row-based data retrieval (milliseconds)"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create matrixRowSyncLatency: %w", err)
	}

	m.matrixColSyncLatency, err = rdaMeter.Float64Histogram(
		"rda_matrix_col_sync_latency_ms",
		metric.WithDescription("Direct fetch latency for column-based data retrieval (milliseconds)"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create matrixColSyncLatency: %w", err)
	}

	m.connectedPeersPerNode, err = rdaMeter.Int64Gauge(
		"rda_connected_peers_per_node",
		metric.WithDescription("Number of connected peers per node - should stay at O(√N)"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connectedPeersPerNode: %w", err)
	}

	m.subnetBandwidthReduction, err = rdaMeter.Float64Gauge(
		"rda_subnet_bandwidth_reduction_percent",
		metric.WithDescription("Bandwidth reduction vs standard Gossip (target: 30-50%)"),
		metric.WithUnit("%"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create subnetBandwidthReduction: %w", err)
	}

	m.controlMessageOverhead, err = rdaMeter.Float64Gauge(
		"rda_control_message_overhead_bytes_per_sec",
		metric.WithDescription("Network overhead for topology maintenance - should approach 0"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create controlMessageOverhead: %w", err)
	}

	// DAS Performance Metrics
	m.erasureRecoverySuccess, err = rdaMeter.Float64Gauge(
		"rda_erasure_recovery_success_rate_percent",
		metric.WithDescription("Percentage of successful data recovery from erasure coded shares"),
		metric.WithUnit("%"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create erasureRecoverySuccess: %w", err)
	}

	m.matrixDirectFetchLatency, err = rdaMeter.Float64Histogram(
		"rda_matrix_direct_fetch_latency_ms",
		metric.WithDescription("Latency of direct peer fetch within RDA matrix (milliseconds)"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create matrixDirectFetchLatency: %w", err)
	}

	m.samplingThroughput, err = rdaMeter.Int64Counter(
		"rda_sampling_throughput_total",
		metric.WithDescription("Total number of successful samples processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create samplingThroughput: %w", err)
	}

	// Fault Tolerance Metrics
	m.nodeFailureRecoveryRate, err = rdaMeter.Float64Gauge(
		"rda_node_failure_recovery_rate_percent",
		metric.WithDescription("Percentage of light nodes that maintain sync when other nodes fail"),
		metric.WithUnit("%"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create nodeFailureRecoveryRate: %w", err)
	}

	m.dataRecoveryTime, err = rdaMeter.Float64Histogram(
		"rda_data_recovery_time_ms",
		metric.WithDescription("Time to recover data when shares are unavailable (milliseconds)"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataRecoveryTime: %w", err)
	}

	// System Resources Metrics
	m.bandwidthSavingsVsGossip, err = rdaMeter.Float64Gauge(
		"rda_bandwidth_savings_vs_gossip_percent",
		metric.WithDescription("Percentage bandwidth savings compared to standard Gossip protocol (target: 30-50%)"),
		metric.WithUnit("%"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create bandwidthSavingsVsGossip: %w", err)
	}

	m.cpuLoadReduction, err = rdaMeter.Float64Gauge(
		"rda_cpu_load_reduction_percent",
		metric.WithDescription("CPU load reduction compared to standard Celestia"),
		metric.WithUnit("%"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create cpuLoadReduction: %w", err)
	}

	m.memoryUsagePerNode, err = rdaMeter.Int64Gauge(
		"rda_memory_usage_per_node_bytes",
		metric.WithDescription("Memory usage per node - RDA should be minimal due to O(√N) peers"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create memoryUsagePerNode: %w", err)
	}

	// Additional counters for dashboard
	m.peerChurnTotal, err = rdaMeter.Int64Counter(
		"rda_peer_churn_total",
		metric.WithDescription("Total number of peer churn events (additions/removals)"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create peerChurnTotal: %w", err)
	}

	m.controlMessageBytesTotal, err = rdaMeter.Int64Counter(
		"rda_control_message_bytes_total",
		metric.WithDescription("Total bytes used for control messages (Ping, Pong, FindNode)"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create controlMessageBytesTotal: %w", err)
	}

	m.samplingLatencyP99, err = rdaMeter.Float64Gauge(
		"rda_sampling_latency_p99_ms",
		metric.WithDescription("P99 latency for sampling operations (milliseconds)"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create samplingLatencyP99: %w", err)
	}

	m.erasureRecoveryTotal, err = rdaMeter.Int64Counter(
		"rda_erasure_recovery_total",
		metric.WithDescription("Total number of erasure recovery attempts"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create erasureRecoveryTotal: %w", err)
	}

	m.erasureRecoverySuccessTotal, err = rdaMeter.Int64Counter(
		"rda_erasure_recovery_success_total",
		metric.WithDescription("Total number of successful erasure recoveries"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create erasureRecoverySuccessTotal: %w", err)
	}

	m.directRecoveryBytesTotal, err = rdaMeter.Int64Counter(
		"rda_direct_recovery_bytes_total",
		metric.WithDescription("Total bytes recovered directly from determined peers"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create directRecoveryBytesTotal: %w", err)
	}

	return m, nil
}

// RecordRowSyncLatency records latency for row-based direct fetch
func (m *RDAMetrics) RecordRowSyncLatency(ctx context.Context, latencyMs float64) {
	if m == nil {
		return
	}
	m.matrixRowSyncLatency.Record(ctx, latencyMs, metric.WithAttributes(
		attribute.String("sync_type", "row_direct_fetch"),
	))
}

// RecordColSyncLatency records latency for column-based direct fetch
func (m *RDAMetrics) RecordColSyncLatency(ctx context.Context, latencyMs float64) {
	if m == nil {
		return
	}
	m.matrixColSyncLatency.Record(ctx, latencyMs, metric.WithAttributes(
		attribute.String("sync_type", "col_direct_fetch"),
	))
}

// UpdateConnectedPeers updates the connected peers gauge
func (m *RDAMetrics) UpdateConnectedPeers(ctx context.Context, peerCount int64) {
	if m == nil {
		return
	}
	m.connectedPeersPerNode.Record(ctx, peerCount, metric.WithAttributes(
		attribute.String("topology", "deterministic_matrix"),
	))
	m.connectedPeersCount.Store(peerCount)
}

// UpdateBandwidthReduction updates the bandwidth reduction percentage
// Expected: 30-50% savings vs standard Gossip
func (m *RDAMetrics) UpdateBandwidthReduction(ctx context.Context, reductionPercent float64) {
	if m == nil {
		return
	}
	m.subnetBandwidthReduction.Record(ctx, reductionPercent, metric.WithAttributes(
		attribute.String("comparison", "vs_gossip_amplification"),
	))
}

// UpdateControlMessageOverhead records network overhead for maintaining topology
// RDA should approach 0 since it uses deterministic subnets
func (m *RDAMetrics) UpdateControlMessageOverhead(ctx context.Context, bytesPerSec float64) {
	if m == nil {
		return
	}
	m.controlMessageOverhead.Record(ctx, bytesPerSec, metric.WithAttributes(
		attribute.String("overhead_type", "discovery_maintenance"),
	))
}

// RecordErasureRecoveryAttempt records success/failure of erasure code recovery
func (m *RDAMetrics) RecordErasureRecoveryAttempt(ctx context.Context, success bool) {
	if m == nil {
		return
	}

	m.totalRecoveryAttempts.Add(1)
	if success {
		m.successfulRecoveries.Add(1)
	}

	// Update success rate gauge
	total := m.totalRecoveryAttempts.Load()
	if total > 0 {
		successRate := float64(m.successfulRecoveries.Load()) * 100.0 / float64(total)
		m.erasureRecoverySuccess.Record(ctx, successRate, metric.WithAttributes(
			attribute.String("recovery_type", "erasure_coding"),
		))
	}
}

// RecordDirectFetchLatency records direct fetch latency within RDA matrix
func (m *RDAMetrics) RecordDirectFetchLatency(ctx context.Context, latencyMs float64) {
	if m == nil {
		return
	}
	m.matrixDirectFetchLatency.Record(ctx, latencyMs, metric.WithAttributes(
		attribute.String("fetch_type", "determined_peer"),
	))
}

// IncrementSamplingThroughput increments the sampling counter
func (m *RDAMetrics) IncrementSamplingThroughput(ctx context.Context) {
	if m == nil {
		return
	}
	m.samplingThroughput.Add(ctx, 1, metric.WithAttributes(
		attribute.String("sample_type", "rda_matrix"),
	))
}

// RecordNodeFailureRecovery updates recovery rate when nodes fail
func (m *RDAMetrics) RecordNodeFailureRecovery(ctx context.Context, recoveryPercent float64) {
	if m == nil {
		return
	}
	m.nodeFailureRecoveryRate.Record(ctx, recoveryPercent, metric.WithAttributes(
		attribute.String("failure_scenario", "detached_nodes"),
	))
}

// RecordDataRecoveryTime records time to recover data when shares unavailable
func (m *RDAMetrics) RecordDataRecoveryTime(ctx context.Context, timeMs float64) {
	if m == nil {
		return
	}
	m.dataRecoveryTime.Record(ctx, timeMs, metric.WithAttributes(
		attribute.String("recovery_method", "erasure_reconstruction"),
	))
}

// UpdateBandwidthSavings updates bandwidth savings vs standard Gossip
func (m *RDAMetrics) UpdateBandwidthSavings(ctx context.Context, savingsPercent float64) {
	if m == nil {
		return
	}
	m.bandwidthSavingsVsGossip.Record(ctx, savingsPercent, metric.WithAttributes(
		attribute.String("baseline", "standard_gossip"),
	))
}

// UpdateCPUReduction updates CPU load reduction percentage
func (m *RDAMetrics) UpdateCPUReduction(ctx context.Context, reductionPercent float64) {
	if m == nil {
		return
	}
	m.cpuLoadReduction.Record(ctx, reductionPercent, metric.WithAttributes(
		attribute.String("baseline", "standard_celestia"),
	))
}

// UpdateMemoryUsage updates per-node memory usage
func (m *RDAMetrics) UpdateMemoryUsage(ctx context.Context, bytes int64) {
	if m == nil {
		return
	}
	m.memoryUsagePerNode.Record(ctx, bytes, metric.WithAttributes(
		attribute.String("memory_type", "peer_state_storage"),
	))
}

// RecordBandwidthUsage for calculating savings
func (m *RDAMetrics) RecordBandwidthUsage(rdaBytes, gossipBytes int64) {
	m.totalBytesWithRDA.Store(rdaBytes)
	m.totalBytesWithGossip.Store(gossipBytes)
}

// RecordPeerChurn records a peer churn event (peer added/removed)
func (m *RDAMetrics) RecordPeerChurn(ctx context.Context) {
	if m == nil {
		return
	}
	m.peerChurnTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", "peer_change"),
	))
}

// RecordControlMessageBytes records control message bytes sent
func (m *RDAMetrics) RecordControlMessageBytes(ctx context.Context, bytes int64) {
	if m == nil {
		return
	}
	m.controlMessageBytesTotal.Add(ctx, bytes, metric.WithAttributes(
		attribute.String("message_type", "control"),
	))
}

// UpdateSamplingLatencyP99 updates the P99 latency gauge
func (m *RDAMetrics) UpdateSamplingLatencyP99(ctx context.Context, latencyMs float64) {
	if m == nil {
		return
	}
	m.samplingLatencyP99.Record(ctx, latencyMs, metric.WithAttributes(
		attribute.String("percentile", "p99"),
	))
	m.lastP99Latency.Store(int64(latencyMs))
}

// RecordErasureRecoveryAttemptWithCounters records both total attempts and successes
func (m *RDAMetrics) RecordErasureRecoveryAttemptWithCounters(ctx context.Context, success bool) {
	if m == nil {
		return
	}
	m.erasureRecoveryTotal.Add(ctx, 1)
	if success {
		m.erasureRecoverySuccessTotal.Add(ctx, 1)
	}
}

// RecordDirectRecoveryBytes records bytes recovered through direct peers
func (m *RDAMetrics) RecordDirectRecoveryBytes(ctx context.Context, bytes int64) {
	if m == nil {
		return
	}
	m.directRecoveryBytesTotal.Add(ctx, bytes, metric.WithAttributes(
		attribute.String("recovery_type", "direct_peer"),
	))
}
