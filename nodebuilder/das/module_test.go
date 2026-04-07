package das

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/celestiaorg/go-fraud"
	libhead "github.com/celestiaorg/go-header"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/header/headertest"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/availability/mocks"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/shrexsub"
)

// TestConstructModule_DASDisabledStub verifies that when DAS is disabled, it implements a stub
// daser that returns an error and empty das.SamplingStats
func TestConstructModule_DASDisabledStub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var mod Module

	cfg := DefaultConfig(node.Light)
	cfg.Enabled = false // Explicitly disable DAS
	app := fxtest.New(t,
		ConstructModule(&cfg),
		fx.Populate(&mod)).
		RequireStart()
	defer app.RequireStop()

	_, err := mod.SamplingStats(ctx)
	assert.ErrorIs(t, err, errStub)

	_, err = mod.RuntimeMode(ctx)
	assert.ErrorIs(t, err, errStub)

	_, err = mod.RDADiagnostics(ctx)
	assert.ErrorIs(t, err, errStub)
}

func TestConstructModule_NoCircularDependency_WithoutRDAService(t *testing.T) {
	cfg := DefaultConfig(node.Light)
	cfg.Enabled = true
	ctrl := gomock.NewController(t)
	avail := mocks.NewMockAvailability(ctrl)
	avail.EXPECT().SharesAvailable(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	store := headertest.NewStore(t)
	sub := headertest.NewSubscriber(t, store, headertest.NewTestSuiteDefaults(t), 1)
	fraudServ := &fraudServiceStub{}
	batching := datastore.Batching(ds_sync.MutexWrap(datastore.NewMapDatastore()))

	err := fx.ValidateApp(
		ConstructModule(&cfg),
		fx.Provide(func() share.Availability { return avail }),
		fx.Provide(func() libhead.Subscriber[*header.ExtendedHeader] { return sub }),
		fx.Provide(func() libhead.Store[*header.ExtendedHeader] { return store }),
		fx.Provide(func() datastore.Batching { return batching }),
		fx.Provide(func() fraud.Service[*header.ExtendedHeader] { return fraudServ }),
		fx.Supply(shrexsub.BroadcastFn(func(context.Context, shrexsub.Notification) error { return nil })),
		fx.Invoke(func(Module) {}),
	)
	require.NoError(t, err)

	app := fxtest.New(t,
		ConstructModule(&cfg),
		fx.Provide(func() share.Availability { return avail }),
		fx.Provide(func() libhead.Subscriber[*header.ExtendedHeader] { return sub }),
		fx.Provide(func() libhead.Store[*header.ExtendedHeader] { return store }),
		fx.Provide(func() datastore.Batching { return batching }),
		fx.Provide(func() fraud.Service[*header.ExtendedHeader] { return fraudServ }),
		fx.Supply(shrexsub.BroadcastFn(func(context.Context, shrexsub.Notification) error { return nil })),
		fx.Invoke(func(Module) {}),
	)
	app.RequireStart()
	app.RequireStop()

	assert.NoError(t, err)
}

func TestConstructModule_NoCircularDependency_WithRDAService(t *testing.T) {
	cfg := DefaultConfig(node.Light)
	cfg.Enabled = true
	ctrl := gomock.NewController(t)
	avail := mocks.NewMockAvailability(ctrl)
	avail.EXPECT().SharesAvailable(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

	store := headertest.NewStore(t)
	sub := headertest.NewSubscriber(t, store, headertest.NewTestSuiteDefaults(t), 1)
	fraudServ := &fraudServiceStub{}
	batching := datastore.Batching(ds_sync.MutexWrap(datastore.NewMapDatastore()))

	err := fx.ValidateApp(
		ConstructModule(&cfg),
		fx.Provide(func() share.Availability { return avail }),
		fx.Provide(func() libhead.Subscriber[*header.ExtendedHeader] { return sub }),
		fx.Provide(func() libhead.Store[*header.ExtendedHeader] { return store }),
		fx.Provide(func() datastore.Batching { return batching }),
		fx.Provide(func() fraud.Service[*header.ExtendedHeader] { return fraudServ }),
		fx.Supply(shrexsub.BroadcastFn(func(context.Context, shrexsub.Notification) error { return nil })),
		fx.Supply(&share.RDANodeService{}),
		fx.Invoke(func(Module) {}),
	)
	require.NoError(t, err)

	app := fxtest.New(t,
		ConstructModule(&cfg),
		fx.Provide(func() share.Availability { return avail }),
		fx.Provide(func() libhead.Subscriber[*header.ExtendedHeader] { return sub }),
		fx.Provide(func() libhead.Store[*header.ExtendedHeader] { return store }),
		fx.Provide(func() datastore.Batching { return batching }),
		fx.Provide(func() fraud.Service[*header.ExtendedHeader] { return fraudServ }),
		fx.Supply(shrexsub.BroadcastFn(func(context.Context, shrexsub.Notification) error { return nil })),
		fx.Supply(&share.RDANodeService{}),
		fx.Invoke(func(Module) {}),
	)
	app.RequireStart()
	app.RequireStop()

	assert.NoError(t, err)
}

type fraudServiceStub struct{}

func (fraudServiceStub) Subscribe(fraud.ProofType) (fraud.Subscription[*header.ExtendedHeader], error) {
	return &fraudSubscriptionStub{}, nil
}

func (fraudServiceStub) AddVerifier(fraud.ProofType, fraud.Verifier[*header.ExtendedHeader]) error {
	return nil
}

func (fraudServiceStub) Broadcast(context.Context, fraud.Proof[*header.ExtendedHeader]) error {
	return nil
}

func (fraudServiceStub) Get(context.Context, fraud.ProofType) ([]fraud.Proof[*header.ExtendedHeader], error) {
	return nil, nil
}

type fraudSubscriptionStub struct{}

func (fraudSubscriptionStub) Proof(context.Context) (fraud.Proof[*header.ExtendedHeader], error) {
	return nil, nil
}

func (fraudSubscriptionStub) Cancel() {}
