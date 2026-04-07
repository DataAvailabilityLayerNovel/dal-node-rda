package das

import (
	"context"

	"go.uber.org/fx"

	"github.com/celestiaorg/celestia-node/das"
)

func ConstructModule(cfg *Config) fx.Option {
	// If DASer is disabled, provide the stub implementation for any node type
	if !cfg.Enabled {
		return fx.Module(
			"das",
			fx.Provide(newDaserStub),
		)
	}

	return fx.Module(
		"das",
		fx.Supply(*cfg),
		fx.Error(cfg.Validate()),
		fx.Provide(
			func(c Config) []das.Option {
				return []das.Option{
					das.WithMode(c.Mode),
					das.WithRDAQueryTimeout(c.RDAQueryTimeout),
					das.WithRDAMaxRetries(c.RDAMaxRetries),
					das.WithRDAParallelQueries(c.RDAParallelQueries),
					das.WithRDAFallbackEnabled(c.RDAFallbackEnabled),
					das.WithRDAFallbackAfterRetries(c.RDAFallbackAfterRetries),
					das.WithRDASyncOnCatchup(c.RDASyncOnCatchup),
					das.WithRDAModeStrictPredicate(c.RDAModeStrictPredicate),
					das.WithRDAMinPeersPerSubnet(c.RDAMinPeersPerSubnet),
					das.WithRDASyncBatchSize(c.RDASyncBatchSize),
					das.WithRDACheckpointVersion(c.RDACheckpointVersion),
					das.WithSamplingRange(c.SamplingRange),
					das.WithConcurrencyLimit(c.ConcurrencyLimit),
					das.WithBackgroundStoreInterval(c.BackgroundStoreInterval),
					das.WithSampleTimeout(c.SampleTimeout),
				}
			},
		),
		fx.Provide(fx.Annotate(
			newDASer,
			fx.OnStart(func(ctx context.Context, daser *das.DASer) error {
				return daser.Start(ctx)
			}),
			fx.OnStop(func(ctx context.Context, daser *das.DASer) error {
				return daser.Stop(ctx)
			}),
		)),
		// Module is needed for the RPC handler
		fx.Provide(func(das *das.DASer) Module {
			return das
		}),
	)
}
