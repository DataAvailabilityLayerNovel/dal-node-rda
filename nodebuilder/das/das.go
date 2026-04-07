package das

import (
	"context"

	"github.com/celestiaorg/celestia-node/das"
)

var _ Module = (*API)(nil)

//go:generate mockgen -destination=mocks/api.go -package=mocks . Module
type Module interface {
	// SamplingStats returns the current statistics over the DA sampling process.
	//
	// Mode behavior:
	// - classic: stats reflect classic availability sampling only.
	// - rda: stats include RDA runtime fields (rda_definition, mode, fallback_active) and RDA-driven progress.
	// - hybrid: stats reflect RDA-first sampling with fallback activity surfaced via fallback_active.
	SamplingStats(ctx context.Context) (das.SamplingStats, error)
	// RDADiagnostics returns RDA-specific runtime diagnostics for operators.
	//
	// Mode behavior:
	// - classic: diagnostics are still accessible, but RDA adapter-dependent fields may be empty and include error hints.
	// - rda: diagnostics expose direct RDA protocol counters and topology/health snapshots.
	// - hybrid: diagnostics expose both RDA protocol counters and fallback ratio for operational canarying.
	RDADiagnostics(ctx context.Context) (das.RDADiagnosticsStatus, error)
	// RuntimeMode returns active DAS mode and fallback status.
	//
	// Mode behavior:
	// - classic: fallback_active remains false because classic path is primary.
	// - rda/hybrid: fallback_active indicates whether classic fallback has been exercised at runtime.
	RuntimeMode(ctx context.Context) (das.RuntimeModeStatus, error)
	// WaitCatchUp blocks until DASer finishes catching up to the network head.
	WaitCatchUp(ctx context.Context) error
}

// API is a wrapper around Module for the RPC.
type API struct {
	Internal struct {
		SamplingStats  func(ctx context.Context) (das.SamplingStats, error)        `perm:"read"`
		RDADiagnostics func(ctx context.Context) (das.RDADiagnosticsStatus, error) `perm:"read"`
		RuntimeMode    func(ctx context.Context) (das.RuntimeModeStatus, error)    `perm:"read"`
		WaitCatchUp    func(ctx context.Context) error                             `perm:"read"`
	}
}

func (api *API) SamplingStats(ctx context.Context) (das.SamplingStats, error) {
	return api.Internal.SamplingStats(ctx)
}

func (api *API) RDADiagnostics(ctx context.Context) (das.RDADiagnosticsStatus, error) {
	return api.Internal.RDADiagnostics(ctx)
}

func (api *API) WaitCatchUp(ctx context.Context) error {
	return api.Internal.WaitCatchUp(ctx)
}

func (api *API) RuntimeMode(ctx context.Context) (das.RuntimeModeStatus, error) {
	return api.Internal.RuntimeMode(ctx)
}
