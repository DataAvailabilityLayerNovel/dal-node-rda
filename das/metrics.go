package das

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/libs/utils"
)

const (
	jobTypeLabel     = "job_type"
	headerWidthLabel = "header_width"
	failedLabel      = "failed"
	modeLabel        = "mode"
	outcomeLabel     = "outcome"
	fallbackLabel    = "fallback"
)

var meter = otel.Meter("das")

type metrics struct {
	sampled       metric.Int64Counter
	sampleTime    metric.Float64Histogram
	getHeaderTime metric.Float64Histogram
	newHead       metric.Int64Counter

	lastSampledTS atomic.Uint64

	clientReg metric.Registration

	// RDA-specific metrics
	rdaMatrixDirect             metric.Float64Histogram // Direct matrix fetch latency
	rdaErasureRecovery          metric.Int64Counter     // Successful erasure recoveries
	rdaRecoveryLatency          metric.Float64Histogram // Recovery operation latency
	rdaBandwidthSavings         metric.Float64Gauge     // Bandwidth savings %
	rdaThroughput               metric.Int64Counter     // Sampling operations/sec
	rdaDirectHits               metric.Int64Counter
	rdaFallbackHits             metric.Int64Counter
	rdaGetRequestTotal          metric.Int64Counter
	rdaGetSuccessTotal          metric.Int64Counter
	rdaGetTimeoutTotal          metric.Int64Counter
	rdaSyncRequestTotal         metric.Int64Counter
	rdaSyncSymbolsReceivedTotal metric.Int64Counter
	rdaPredValidationFailTotal  metric.Int64Counter
}

func (d *DASer) InitMetrics() error {
	if d.sampler != nil && d.sampler.metrics != nil {
		if err := d.sampler.metrics.close(); err != nil {
			return fmt.Errorf("closing previous metrics registration: %w", err)
		}
	}

	sampled, err := meter.Int64Counter("das_sampled_headers_counter",
		metric.WithDescription("sampled headers counter"))
	if err != nil {
		return err
	}

	sampleTime, err := meter.Float64Histogram("das_sample_time_hist",
		metric.WithDescription("duration of sampling a single header"))
	if err != nil {
		return err
	}

	getHeaderTime, err := meter.Float64Histogram("das_get_header_time_hist",
		metric.WithDescription("duration of getting header from header store"))
	if err != nil {
		return err
	}

	newHead, err := meter.Int64Counter("das_head_updated_counter",
		metric.WithDescription("amount of times DAS'er advanced network head"))
	if err != nil {
		return err
	}

	lastSampledTS, err := meter.Int64ObservableGauge("das_latest_sampled_ts",
		metric.WithDescription("latest sampled timestamp"))
	if err != nil {
		return err
	}

	busyWorkers, err := meter.Int64ObservableGauge("das_busy_workers_amount",
		metric.WithDescription("number of active parallel workers in DAS'er"))
	if err != nil {
		return err
	}

	networkHead, err := meter.Int64ObservableGauge("das_network_head",
		metric.WithDescription("most recent network head"))
	if err != nil {
		return err
	}

	sampledChainHead, err := meter.Int64ObservableGauge("das_sampled_chain_head",
		metric.WithDescription("height of the sampled chain - all previous headers have been successfully sampled"))
	if err != nil {
		return err
	}

	totalSampled, err := meter.Int64ObservableGauge("das_total_sampled_headers",
		metric.WithDescription("total sampled headers gauge"),
	)
	if err != nil {
		return err
	}

	d.sampler.metrics = &metrics{
		sampled:       sampled,
		sampleTime:    sampleTime,
		getHeaderTime: getHeaderTime,
		newHead:       newHead,
	}

	// Initialize RDA-specific metrics
	rdaMatrixDirect, err := meter.Float64Histogram("rda_matrix_direct_fetch_latency_ms",
		metric.WithDescription("Direct fetch latency within RDA matrix (milliseconds)"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaMatrixDirect = rdaMatrixDirect

	rdaErasureRecovery, err := meter.Int64Counter("rda_erasure_recovery_total",
		metric.WithDescription("Total successful erasure code recoveries"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaErasureRecovery = rdaErasureRecovery

	rdaRecoveryLatency, err := meter.Float64Histogram("rda_data_recovery_latency_ms",
		metric.WithDescription("Time to recover unavailable data (milliseconds)"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaRecoveryLatency = rdaRecoveryLatency

	rdaBandwidthSavings, err := meter.Float64Gauge("rda_bandwidth_savings_percent",
		metric.WithDescription("Bandwidth savings vs standard Gossip (%)"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaBandwidthSavings = rdaBandwidthSavings

	rdaThroughput, err := meter.Int64Counter("rda_sampling_operations_total",
		metric.WithDescription("Total RDA sampling operations completed"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaThroughput = rdaThroughput

	rdaDirectHits, err := meter.Int64Counter("das_rda_direct_hits_total",
		metric.WithDescription("Total successful RDA direct hits"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaDirectHits = rdaDirectHits

	rdaFallbackHits, err := meter.Int64Counter("das_rda_fallback_hits_total",
		metric.WithDescription("Total successful classic fallback hits after RDA failure"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaFallbackHits = rdaFallbackHits

	rdaGetRequestTotal, err := meter.Int64Counter("rda_get_request_total",
		metric.WithDescription("Total RDA GET requests attempted"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaGetRequestTotal = rdaGetRequestTotal

	rdaGetSuccessTotal, err := meter.Int64Counter("rda_get_success_total",
		metric.WithDescription("Total successful RDA GET requests"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaGetSuccessTotal = rdaGetSuccessTotal

	rdaGetTimeoutTotal, err := meter.Int64Counter("rda_get_timeout_total",
		metric.WithDescription("Total timed out RDA GET requests"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaGetTimeoutTotal = rdaGetTimeoutTotal

	rdaSyncRequestTotal, err := meter.Int64Counter("rda_sync_request_total",
		metric.WithDescription("Total RDA SYNC requests attempted"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaSyncRequestTotal = rdaSyncRequestTotal

	rdaSyncSymbolsReceivedTotal, err := meter.Int64Counter("rda_sync_symbols_received_total",
		metric.WithDescription("Total symbols received by RDA SYNC"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaSyncSymbolsReceivedTotal = rdaSyncSymbolsReceivedTotal

	rdaPredValidationFailTotal, err := meter.Int64Counter("rda_pred_validation_fail_total",
		metric.WithDescription("Total RDA predicate validation failures"))
	if err != nil {
		return err
	}
	d.sampler.metrics.rdaPredValidationFailTotal = rdaPredValidationFailTotal

	rdaQuerySuccessRatio, err := meter.Float64ObservableGauge("rda_query_success_ratio",
		metric.WithDescription("RDA GET success ratio [0,1]"))
	if err != nil {
		return err
	}

	rdaFallbackRatio, err := meter.Float64ObservableGauge("rda_fallback_ratio",
		metric.WithDescription("RDA fallback ratio [0,1]"))
	if err != nil {
		return err
	}

	retryDepthLow, err := meter.Int64ObservableGauge("rda_retry_depth_bucket_low",
		metric.WithDescription("Failed headers with retry depth <= 1"))
	if err != nil {
		return err
	}

	retryDepthMid, err := meter.Int64ObservableGauge("rda_retry_depth_bucket_mid",
		metric.WithDescription("Failed headers with retry depth in [2,3]"))
	if err != nil {
		return err
	}

	retryDepthHigh, err := meter.Int64ObservableGauge("rda_retry_depth_bucket_high",
		metric.WithDescription("Failed headers with retry depth >= 4"))
	if err != nil {
		return err
	}

	callback := func(ctx context.Context, observer metric.Observer) error {
		stats, err := d.sampler.stats(ctx)
		if err != nil {
			log.Errorf("observing stats: %s", err.Error())
			return err
		}

		for jobType, amount := range stats.workersByJobType() {
			observer.ObserveInt64(busyWorkers, amount,
				metric.WithAttributes(
					attribute.String(jobTypeLabel, string(jobType)),
				))
		}

		observer.ObserveInt64(networkHead, int64(stats.NetworkHead))
		observer.ObserveInt64(sampledChainHead, int64(stats.SampledChainHead))

		if ts := d.sampler.metrics.lastSampledTS.Load(); ts != 0 {
			observer.ObserveInt64(lastSampledTS, int64(ts))
		}

		observer.ObserveInt64(totalSampled, int64(stats.totalSampled()))

		requests := d.rdaGetRequests.Load()
		successes := d.rdaGetSuccesses.Load()
		if requests > 0 {
			observer.ObserveFloat64(rdaQuerySuccessRatio, float64(successes)/float64(requests))
		}

		directHits := d.rdaDirectHits.Load()
		fallbackHits := d.rdaFallbackHits.Load()
		totalOutcomes := directHits + fallbackHits
		if totalOutcomes > 0 {
			observer.ObserveFloat64(rdaFallbackRatio, float64(fallbackHits)/float64(totalOutcomes))
		}

		var lowDepth, midDepth, highDepth int64
		for _, attempts := range stats.Failed {
			switch {
			case attempts <= 1:
				lowDepth++
			case attempts <= 3:
				midDepth++
			default:
				highDepth++
			}
		}
		observer.ObserveInt64(retryDepthLow, lowDepth)
		observer.ObserveInt64(retryDepthMid, midDepth)
		observer.ObserveInt64(retryDepthHigh, highDepth)
		return nil
	}

	d.sampler.metrics.clientReg, err = meter.RegisterCallback(callback,
		lastSampledTS,
		busyWorkers,
		networkHead,
		sampledChainHead,
		totalSampled,
		rdaQuerySuccessRatio,
		rdaFallbackRatio,
		retryDepthLow,
		retryDepthMid,
		retryDepthHigh,
	)
	if err != nil {
		return fmt.Errorf("registering metrics callback: %w", err)
	}

	return nil
}

func (m *metrics) close() error {
	if m == nil {
		return nil
	}
	if m.clientReg == nil {
		return nil
	}
	return m.clientReg.Unregister()
}

// observeSample records the time it took to sample a header +
// the amount of sampled contiguous headers
func (m *metrics) observeSample(
	ctx context.Context,
	h *header.ExtendedHeader,
	sampleTime time.Duration,
	jobType jobType,
	err error,
) {
	if m == nil {
		return
	}

	ctx = utils.ResetContextOnError(ctx)

	m.sampleTime.Record(ctx, sampleTime.Seconds(),
		metric.WithAttributes(
			attribute.Bool(failedLabel, err != nil),
			attribute.Int(headerWidthLabel, len(h.DAH.RowRoots)),
			attribute.String(jobTypeLabel, string(jobType)),
		))

	m.sampled.Add(ctx, 1,
		metric.WithAttributes(
			attribute.Bool(failedLabel, err != nil),
			attribute.Int(headerWidthLabel, len(h.DAH.RowRoots)),
			attribute.String(jobTypeLabel, string(jobType)),
		))

	m.lastSampledTS.Store(uint64(time.Now().UTC().Unix()))
}

// observeGetHeader records the time it took to get a header from the header store.
func (m *metrics) observeGetHeader(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.getHeaderTime.Record(ctx, d.Seconds())
}

// observeNewHead records the network head.
func (m *metrics) observeNewHead(ctx context.Context) {
	if m == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.newHead.Add(ctx, 1)
}

// observeRDAMatrixDirectFetch records direct fetch latency within RDA matrix
func (m *metrics) observeRDAMatrixDirectFetch(ctx context.Context, latencyMs float64, mode Mode, outcome string) {
	if m == nil || m.rdaMatrixDirect == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaMatrixDirect.Record(ctx, latencyMs,
		metric.WithAttributes(
			attribute.String("fetch_type", "rda_direct"),
			attribute.String(modeLabel, string(mode)),
			attribute.String(outcomeLabel, outcome),
		))
}

// recordRDAErasureRecovery records successful erasure code recovery
func (m *metrics) recordRDAErasureRecovery(ctx context.Context) {
	if m == nil || m.rdaErasureRecovery == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaErasureRecovery.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("recovery_type", "erasure_coding"),
		))
}

// observeRDARecoveryLatency records data recovery operation latency
func (m *metrics) observeRDARecoveryLatency(ctx context.Context, latencyMs float64) {
	if m == nil || m.rdaRecoveryLatency == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaRecoveryLatency.Record(ctx, latencyMs,
		metric.WithAttributes(
			attribute.String("recovery_method", "erasure_reconstruction"),
		))
}

// updateRDABandwidthSavings updates bandwidth savings percentage
func (m *metrics) updateRDABandwidthSavings(ctx context.Context, savingsPercent float64) {
	if m == nil || m.rdaBandwidthSavings == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaBandwidthSavings.Record(ctx, savingsPercent,
		metric.WithAttributes(
			attribute.String("baseline", "gossip_protocol"),
		))
}

// recordRDASamplingOperation records a completed RDA sampling operation
func (m *metrics) recordRDASamplingOperation(ctx context.Context, mode Mode, outcome string) {
	if m == nil || m.rdaThroughput == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaThroughput.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("sample_type", "rda_matrix"),
			attribute.String(modeLabel, string(mode)),
			attribute.String(outcomeLabel, outcome),
		))
}

func (m *metrics) recordRDADirectHit(ctx context.Context, mode Mode) {
	if m == nil || m.rdaDirectHits == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaDirectHits.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
			attribute.String(outcomeLabel, "success"),
			attribute.Bool(fallbackLabel, false),
		))
}

func (m *metrics) recordRDAFallbackHit(ctx context.Context, mode Mode) {
	if m == nil || m.rdaFallbackHits == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaFallbackHits.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
			attribute.String(outcomeLabel, "success"),
			attribute.Bool(fallbackLabel, true),
		))
}

func (m *metrics) recordRDAGetRequest(ctx context.Context, mode Mode) {
	if m == nil || m.rdaGetRequestTotal == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaGetRequestTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
		))
}

func (m *metrics) recordRDAGetSuccess(ctx context.Context, mode Mode) {
	if m == nil || m.rdaGetSuccessTotal == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaGetSuccessTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
		))
}

func (m *metrics) recordRDAGetTimeout(ctx context.Context, mode Mode) {
	if m == nil || m.rdaGetTimeoutTotal == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaGetTimeoutTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
		))
}

func (m *metrics) recordRDASyncRequest(ctx context.Context, mode Mode) {
	if m == nil || m.rdaSyncRequestTotal == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaSyncRequestTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
		))
}

func (m *metrics) recordRDASyncSymbolsReceived(ctx context.Context, mode Mode, symbols int64) {
	if m == nil || m.rdaSyncSymbolsReceivedTotal == nil {
		return
	}
	if symbols <= 0 {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaSyncSymbolsReceivedTotal.Add(ctx, symbols,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
		))
}

func (m *metrics) recordRDAPredValidationFail(ctx context.Context, mode Mode) {
	if m == nil || m.rdaPredValidationFailTotal == nil {
		return
	}
	ctx = utils.ResetContextOnError(ctx)
	m.rdaPredValidationFailTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String(modeLabel, string(mode)),
		))
}
