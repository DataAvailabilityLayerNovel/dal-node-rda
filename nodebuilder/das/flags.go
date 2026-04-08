package das

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	moddas "github.com/celestiaorg/celestia-node/das"
)

const (
	dasEnabledFlag            = "das.enabled"
	dasModeFlag               = "das.mode"
	dasRDAFallbackEnabledFlag = "das.rda-fallback-enabled"
)

// Flags gives a set of DAS flags.
func Flags() *flag.FlagSet {
	flags := &flag.FlagSet{}

	flags.Bool(
		dasEnabledFlag,
		true,
		"Enable or disable DAS module startup",
	)

	flags.String(
		dasModeFlag,
		"",
		"DAS mode: classic | rda | hybrid",
	)

	flags.Bool(
		dasRDAFallbackEnabledFlag,
		true,
		"Enable classic fallback when RDA path fails (required in hybrid mode)",
	)

	return flags
}

// ParseFlags parses DAS flags from the given cmd and saves them to the passed config.
func ParseFlags(cmd *cobra.Command, cfg *Config) error {
	if cmd.Flags().Changed(dasEnabledFlag) {
		enabled, err := cmd.Flags().GetBool(dasEnabledFlag)
		if err != nil {
			return err
		}
		cfg.Enabled = enabled
	}

	if cmd.Flags().Changed(dasModeFlag) {
		mode, err := cmd.Flags().GetString(dasModeFlag)
		if err != nil {
			return err
		}

		cfg.Mode = moddas.Mode(strings.ToLower(mode))
	}

	if cmd.Flags().Changed(dasRDAFallbackEnabledFlag) {
		enabled, err := cmd.Flags().GetBool(dasRDAFallbackEnabledFlag)
		if err != nil {
			return err
		}
		cfg.RDAFallbackEnabled = enabled
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("cmd: while parsing DAS flags: %w", err)
	}

	return nil
}
