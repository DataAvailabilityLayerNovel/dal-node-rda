package das

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Mode defines the DAS sampling execution strategy.
type Mode string

const (
	ModeClassic Mode = "classic"
	ModeRDA     Mode = "rda"
	ModeHybrid  Mode = "hybrid"
)

func parseMode(mode string) (Mode, error) {
	m := Mode(strings.ToLower(mode))
	switch m {
	case ModeClassic, ModeRDA, ModeHybrid:
		return m, nil
	default:
		return "", fmt.Errorf("%w: unsupported DAS mode %q", ErrInvalidOption, mode)
	}
}

// ErrInvalidOption is an error that is returned by Parameters.Validate
// when supplied with invalid values.
// This error will also be returned by NewDASer if supplied with an invalid option
var ErrInvalidOption = errors.New("das: invalid option")

// errInvalidOptionValue is a utility function to dedup code for error-returning
// when dealing with invalid parameter values
func errInvalidOptionValue(optionName, value string) error {
	return fmt.Errorf("%w: value %s cannot be %s", ErrInvalidOption, optionName, value)
}

// Option is the functional option that is applied to the daser instance
// to configure DASing parameters (the Parameters struct)
type Option func(*DASer)

// Parameters is the set of parameters that must be configured for the daser
type Parameters struct {
	// Mode controls which sampling strategy is used by DASer.
	Mode Mode

	// RDAQueryTimeout is the maximum time allowed for a single RDA query.
	RDAQueryTimeout time.Duration

	// RDAMaxRetries is the maximum retry attempts for a failed RDA query.
	RDAMaxRetries int

	// RDAParallelQueries defines parallel RDA query fanout per sample operation.
	RDAParallelQueries int

	// RDAFallbackEnabled controls whether classic path fallback can be used in RDA/hybrid modes.
	RDAFallbackEnabled bool

	// RDAFallbackAfterRetries defines when fallback is allowed after retry attempts.
	RDAFallbackAfterRetries int

	// RDASyncOnCatchup enables SYNC-assisted behavior during catchup flows.
	RDASyncOnCatchup bool

	// RDAModeStrictPredicate controls strict predicate enforcement behavior in RDA path.
	RDAModeStrictPredicate bool

	// RDAMinPeersPerSubnet is the minimum peers required per subnet for RDA operations.
	RDAMinPeersPerSubnet int

	// RDASyncBatchSize is the upper bound for one SYNC batch.
	RDASyncBatchSize int

	// RDACheckpointVersion stores schema version used by RDA-related checkpoints.
	RDACheckpointVersion int

	//  SamplingRange is the maximum amount of headers processed in one job.
	SamplingRange uint64

	// ConcurrencyLimit defines the maximum amount of sampling workers running in parallel.
	ConcurrencyLimit int

	// BackgroundStoreInterval is the period of time for background checkpointStore to perform a
	// checkpoint backup.
	BackgroundStoreInterval time.Duration

	// SampleTimeout is a maximum amount time sampling of single block may take until it will be
	// canceled. High ConcurrencyLimit value may increase sampling time due to node resources being
	// divided between parallel workers. SampleTimeout should be adjusted proportionally to
	// ConcurrencyLimit.
	SampleTimeout time.Duration
}

// DefaultParameters returns the default configuration values for the daser parameters
func DefaultParameters() Parameters {
	// TODO(@derrandz): parameters needs performance testing on real network to define optimal values
	// (#1261)
	concurrencyLimit := 16
	return Parameters{
		Mode:                    ModeClassic,
		RDAQueryTimeout:         5 * time.Second,
		RDAMaxRetries:           3,
		RDAParallelQueries:      4,
		RDAFallbackEnabled:      true,
		RDAFallbackAfterRetries: 2,
		RDASyncOnCatchup:        false,
		RDAModeStrictPredicate:  true,
		RDAMinPeersPerSubnet:    2,
		RDASyncBatchSize:        64,
		RDACheckpointVersion:    1,
		SamplingRange:           100,
		ConcurrencyLimit:        concurrencyLimit,
		BackgroundStoreInterval: 10 * time.Minute,
		// SampleTimeout = approximate block time (with a bit of wiggle room) * max amount of catchup
		// workers
		SampleTimeout: 15 * time.Second * time.Duration(concurrencyLimit),
	}
}

// Validate validates the values in Parameters
//
//	All parameters must be positive and non-zero, except:
//		BackgroundStoreInterval = 0 disables background storer,
//		PriorityQueueSize = 0 disables prioritization of recently produced blocks for sampling
func (p *Parameters) Validate() error {
	if _, err := parseMode(string(p.Mode)); err != nil {
		return err
	}

	if p.RDAQueryTimeout <= 0 {
		return errInvalidOptionValue(
			"RDAQueryTimeout",
			"negative or 0",
		)
	}

	if p.RDAMaxRetries < 0 {
		return errInvalidOptionValue(
			"RDAMaxRetries",
			"negative",
		)
	}

	if p.RDAParallelQueries <= 0 {
		return errInvalidOptionValue(
			"RDAParallelQueries",
			"negative or 0",
		)
	}

	if p.RDAFallbackAfterRetries < 0 {
		return errInvalidOptionValue(
			"RDAFallbackAfterRetries",
			"negative",
		)
	}

	if p.RDAFallbackAfterRetries > p.RDAMaxRetries {
		return fmt.Errorf("%w: RDAFallbackAfterRetries (%d) cannot be greater than RDAMaxRetries (%d)",
			ErrInvalidOption,
			p.RDAFallbackAfterRetries,
			p.RDAMaxRetries,
		)
	}

	if p.RDAMinPeersPerSubnet <= 0 {
		return errInvalidOptionValue(
			"RDAMinPeersPerSubnet",
			"negative or 0",
		)
	}

	if p.RDASyncBatchSize <= 0 {
		return errInvalidOptionValue(
			"RDASyncBatchSize",
			"negative or 0",
		)
	}

	if p.RDACheckpointVersion <= 0 {
		return errInvalidOptionValue(
			"RDACheckpointVersion",
			"negative or 0",
		)
	}

	if p.Mode == ModeHybrid && !p.RDAFallbackEnabled {
		return fmt.Errorf("%w: hybrid mode requires RDA fallback to be enabled", ErrInvalidOption)
	}

	// SamplingRange = 0 will cause the jobs' queue to be empty
	// Therefore no sampling jobs will be reserved and more importantly the DASer will break
	if p.SamplingRange <= 0 {
		return errInvalidOptionValue(
			"SamplingRange",
			"negative or 0",
		)
	}

	// ConcurrencyLimit = 0 will cause the number of workers to be 0 and
	// Thus no threads will be assigned to the waiting jobs therefore breaking the DASer
	if p.ConcurrencyLimit <= 0 {
		return errInvalidOptionValue(
			"ConcurrencyLimit",
			"negative or 0",
		)
	}

	// SampleTimeout = 0 would fail every sample operation with timeout error
	if p.SampleTimeout <= 0 {
		return errInvalidOptionValue(
			"SampleTimeout",
			"negative or 0",
		)
	}

	return nil
}

// WithMode is a functional option to configure the daser's strategy mode.
func WithMode(mode Mode) Option {
	return func(d *DASer) {
		d.params.Mode = mode
	}
}

// WithRDAQueryTimeout is a functional option to configure RDA query timeout.
func WithRDAQueryTimeout(timeout time.Duration) Option {
	return func(d *DASer) {
		d.params.RDAQueryTimeout = timeout
	}
}

// WithRDAMaxRetries is a functional option to configure RDA query retry limit.
func WithRDAMaxRetries(retries int) Option {
	return func(d *DASer) {
		d.params.RDAMaxRetries = retries
	}
}

// WithRDAParallelQueries is a functional option to configure RDA query fanout.
func WithRDAParallelQueries(parallel int) Option {
	return func(d *DASer) {
		d.params.RDAParallelQueries = parallel
	}
}

// WithRDAFallbackEnabled toggles classic fallback for RDA/hybrid strategy.
func WithRDAFallbackEnabled(enabled bool) Option {
	return func(d *DASer) {
		d.params.RDAFallbackEnabled = enabled
	}
}

// WithRDAFallbackAfterRetries configures when fallback can be used.
func WithRDAFallbackAfterRetries(retries int) Option {
	return func(d *DASer) {
		d.params.RDAFallbackAfterRetries = retries
	}
}

// WithRDAAdapter injects a narrow RDA adapter for DAS strategy integration.
func WithRDAAdapter(adapter RDAAdapter) Option {
	return func(d *DASer) {
		d.rda = adapter
	}
}

// WithRDASyncOnCatchup toggles SYNC-assisted catchup behavior in RDA mode.
func WithRDASyncOnCatchup(enabled bool) Option {
	return func(d *DASer) {
		d.params.RDASyncOnCatchup = enabled
	}
}

// WithRDAModeStrictPredicate toggles strict predicate checks in RDA flow.
func WithRDAModeStrictPredicate(enabled bool) Option {
	return func(d *DASer) {
		d.params.RDAModeStrictPredicate = enabled
	}
}

// WithRDAMinPeersPerSubnet configures minimum peers required for subnet operations.
func WithRDAMinPeersPerSubnet(minPeers int) Option {
	return func(d *DASer) {
		d.params.RDAMinPeersPerSubnet = minPeers
	}
}

// WithRDASyncBatchSize configures maximum symbols processed per SYNC batch.
func WithRDASyncBatchSize(batchSize int) Option {
	return func(d *DASer) {
		d.params.RDASyncBatchSize = batchSize
	}
}

// WithRDACheckpointVersion configures RDA checkpoint schema version.
func WithRDACheckpointVersion(version int) Option {
	return func(d *DASer) {
		d.params.RDACheckpointVersion = version
	}
}

// WithSamplingRange is a functional option to configure the daser's `SamplingRange` parameter
//
//	Usage:
//	```
//		WithSamplingRange(10)(daser)
//	```
//
// or
//
//	```
//		option := WithSamplingRange(10)
//		// shenanigans to create daser
//		option(daser)
//
// ```
func WithSamplingRange(samplingRange uint64) Option {
	return func(d *DASer) {
		d.params.SamplingRange = samplingRange
	}
}

// WithConcurrencyLimit is a functional option to configure the daser's `ConcurrencyLimit` parameter
// Refer to WithSamplingRange documentation to see an example of how to use this
func WithConcurrencyLimit(concurrencyLimit int) Option {
	return func(d *DASer) {
		d.params.ConcurrencyLimit = concurrencyLimit
	}
}

// WithBackgroundStoreInterval is a functional option to configure the daser's
// `backgroundStoreInterval` parameter Refer to WithSamplingRange documentation to see an example
// of how to use this
func WithBackgroundStoreInterval(backgroundStoreInterval time.Duration) Option {
	return func(d *DASer) {
		d.params.BackgroundStoreInterval = backgroundStoreInterval
	}
}

// WithSampleTimeout is a functional option to configure the daser's `SampleTimeout` parameter
// Refer to WithSamplingRange documentation to see an example of how to use this
func WithSampleTimeout(sampleTimeout time.Duration) Option {
	return func(d *DASer) {
		d.params.SampleTimeout = sampleTimeout
	}
}
