package das

import (
	"context"
	"fmt"

	moddas "github.com/celestiaorg/celestia-node/das"
	sharecore "github.com/celestiaorg/celestia-node/share"
)

type rdaServiceAdapter struct {
	service *sharecore.RDANodeService
}

func newRDAServiceAdapter(service *sharecore.RDANodeService) moddas.RDAAdapter {
	if service == nil {
		return nil
	}
	return &rdaServiceAdapter{service: service}
}

func (a *rdaServiceAdapter) QuerySymbol(ctx context.Context, handle string, shareIndex uint32) (*moddas.RDASymbol, error) {
	if !a.service.IsSubnetDiscoveryReady() {
		return nil, fmt.Errorf("rda adapter: subnet discovery not ready")
	}

	sym, err := a.service.QueryShare(ctx, handle, shareIndex)
	if err != nil {
		return nil, err
	}
	if sym == nil {
		return nil, fmt.Errorf("rda adapter: query returned nil symbol")
	}

	return &moddas.RDASymbol{
		Handle:      sym.Handle,
		ShareIndex:  sym.ShareIndex,
		BlockHeight: sym.BlockHeight,
		ShareSize:   sym.ShareSize,
	}, nil
}

func (a *rdaServiceAdapter) SyncColumn(ctx context.Context, sinceHeight uint64) error {
	if !a.service.IsSubnetDiscoveryReady() {
		return fmt.Errorf("rda adapter: subnet discovery not ready")
	}

	return a.service.SyncColumn(ctx, sinceHeight)
}

func (a *rdaServiceAdapter) GetTopologySnapshot(context.Context) (moddas.RDATopologySnapshot, error) {
	topo := a.service.GetTopologySnapshot()

	snap := moddas.RDATopologySnapshot{
		Rows:             int(topo.GridDimensions.Rows),
		Cols:             int(topo.GridDimensions.Cols),
		RowPeers:         topo.RowPeers,
		ColPeers:         topo.ColPeers,
		TotalSubnetPeers: topo.TotalSubnetPeers,
	}
	return snap, nil
}

func (a *rdaServiceAdapter) GetHealthSnapshot(context.Context) (moddas.RDAHealthSnapshot, error) {
	health := a.service.GetHealthSnapshot()
	return moddas.RDAHealthSnapshot{Synced: health.Synced}, nil
}
