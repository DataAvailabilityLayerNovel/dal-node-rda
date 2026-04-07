package das

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/das"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
)

func TestConfigValidateAcceptsKnownModes(t *testing.T) {
	testCases := []das.Mode{das.ModeClassic, das.ModeRDA, das.ModeHybrid}

	for _, mode := range testCases {
		t.Run(string(mode), func(t *testing.T) {
			cfg := DefaultConfig(node.Light)
			cfg.Mode = mode
			require.NoError(t, cfg.Validate())
		})
	}
}

func TestConfigValidateRejectsUnknownMode(t *testing.T) {
	cfg := DefaultConfig(node.Light)
	cfg.Mode = das.Mode("unsupported")
	require.Error(t, cfg.Validate())
}

func TestConfigValidateRejectsHybridWithoutFallback(t *testing.T) {
	cfg := DefaultConfig(node.Light)
	cfg.Mode = das.ModeHybrid
	cfg.RDAFallbackEnabled = false
	require.Error(t, cfg.Validate())
}

func TestConfigValidateRejectsFallbackAfterRetriesGreaterThanMax(t *testing.T) {
	cfg := DefaultConfig(node.Light)
	cfg.Mode = das.ModeRDA
	cfg.RDAMaxRetries = 2
	cfg.RDAFallbackAfterRetries = 3
	require.Error(t, cfg.Validate())
}

func TestConfigValidateRejectsInvalidRDAKnobs(t *testing.T) {
	t.Run("query timeout", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDAQueryTimeout = 0
		require.Error(t, cfg.Validate())
	})

	t.Run("parallel queries", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDAParallelQueries = 0
		require.Error(t, cfg.Validate())
	})

	t.Run("max retries", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDAMaxRetries = -1
		require.Error(t, cfg.Validate())
	})

	t.Run("fallback after retries", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDAFallbackAfterRetries = -1
		require.Error(t, cfg.Validate())
	})
}

func TestConfigValidateAcceptsValidRDAKnobs(t *testing.T) {
	cfg := DefaultConfig(node.Light)
	cfg.Mode = das.ModeHybrid
	cfg.RDAQueryTimeout = 3 * time.Second
	cfg.RDAMaxRetries = 4
	cfg.RDAParallelQueries = 3
	cfg.RDAFallbackEnabled = true
	cfg.RDAFallbackAfterRetries = 2
	cfg.RDASyncOnCatchup = true
	cfg.RDAModeStrictPredicate = true
	cfg.RDAMinPeersPerSubnet = 2
	cfg.RDASyncBatchSize = 32
	cfg.RDACheckpointVersion = 1
	require.NoError(t, cfg.Validate())
}

func TestConfigValidateRejectsInvalidNewRDAKnobs(t *testing.T) {
	t.Run("min peers per subnet", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDAMinPeersPerSubnet = 0
		require.Error(t, cfg.Validate())
	})

	t.Run("sync batch size", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDASyncBatchSize = 0
		require.Error(t, cfg.Validate())
	})

	t.Run("checkpoint version", func(t *testing.T) {
		cfg := DefaultConfig(node.Light)
		cfg.Mode = das.ModeRDA
		cfg.RDACheckpointVersion = 0
		require.Error(t, cfg.Validate())
	})
}
