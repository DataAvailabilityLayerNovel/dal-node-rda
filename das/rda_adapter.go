package das

import "context"

// RDASymbol is a narrow representation used by DAS to avoid coupling with share internals.
type RDASymbol struct {
	Handle      string
	ShareIndex  uint32
	BlockHeight uint64
	ShareSize   uint32
}

// RDATopologySnapshot is a minimal topology view needed by DAS strategies.
type RDATopologySnapshot struct {
	Rows             int
	Cols             int
	RowPeers         int
	ColPeers         int
	TotalSubnetPeers int
}

// RDAHealthSnapshot is a minimal health view used for strategy decisions.
type RDAHealthSnapshot struct {
	Synced bool
}

// RDAAdapter defines the narrow bridge between DAS and RDA service implementations.
type RDAAdapter interface {
	QuerySymbol(ctx context.Context, handle string, shareIndex uint32) (*RDASymbol, error)
	SyncColumn(ctx context.Context, sinceHeight uint64) error
	GetTopologySnapshot(ctx context.Context) (RDATopologySnapshot, error)
	GetHealthSnapshot(ctx context.Context) (RDAHealthSnapshot, error)
}
