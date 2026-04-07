package das

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"

	"github.com/celestiaorg/go-fraud"
	libhead "github.com/celestiaorg/go-header"
	"go.uber.org/fx"

	"github.com/celestiaorg/celestia-node/das"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/shrexsub"
)

var _ Module = (*daserStub)(nil)

var errStub = fmt.Errorf("module/das: stubbed: dasing is disabled")

// daserStub is a stub implementation of the DASer that is used when DASer is disabled,
// so that we can provide a friendlier error when users try to access the daser over the API.
type daserStub struct{}

func (d daserStub) SamplingStats(context.Context) (das.SamplingStats, error) {
	return das.SamplingStats{}, errStub
}

func (d daserStub) RDADiagnostics(context.Context) (das.RDADiagnosticsStatus, error) {
	return das.RDADiagnosticsStatus{}, errStub
}

func (d daserStub) RuntimeMode(context.Context) (das.RuntimeModeStatus, error) {
	return das.RuntimeModeStatus{}, errStub
}

func (d daserStub) WaitCatchUp(context.Context) error {
	return errStub
}

func newDaserStub() Module {
	return &daserStub{}
}

type daserInputs struct {
	fx.In

	DA         share.Availability
	HSub       libhead.Subscriber[*header.ExtendedHeader]
	Store      libhead.Store[*header.ExtendedHeader]
	Batching   datastore.Batching
	FraudServ  fraud.Service[*header.ExtendedHeader]
	Broadcast  shrexsub.BroadcastFn
	Options    []das.Option
	RDAService *share.RDANodeService `optional:"true"`
}

func newDASer(
	in daserInputs,
) (*das.DASer, error) {
	options := make([]das.Option, 0, len(in.Options)+1)
	options = append(options, in.Options...)
	if adapter := newRDAServiceAdapter(in.RDAService); adapter != nil {
		options = append(options, das.WithRDAAdapter(adapter))
	}

	return das.NewDASer(in.DA, in.HSub, in.Store, in.Batching, in.FraudServ, in.Broadcast, options...)
}
