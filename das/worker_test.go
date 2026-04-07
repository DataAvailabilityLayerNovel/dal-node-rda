package das

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share/availability"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/shrexsub"
)

func TestWorker_CancelStopsWithoutPublishingResult(t *testing.T) {
	w := newWorker(
		job{id: 1, jobType: catchupJob, from: 1, to: 3},
		getterStub{},
		func(context.Context, *header.ExtendedHeader) error { return context.Canceled },
		func(context.Context, shrexsub.Notification) error { return nil },
		nil,
	)

	resultCh := make(chan result, 1)
	go w.run(context.Background(), time.Second, resultCh)

	select {
	case <-resultCh:
		t.Fatal("worker published result despite cancellation")
	case <-time.After(200 * time.Millisecond):
	}

	state := w.getState()
	require.EqualValues(t, 1, state.curr)
	require.Empty(t, state.failed)
}

func TestWorker_RetryFailureCapturesFailedDetails(t *testing.T) {
	w := newWorker(
		job{id: 2, jobType: retryJob, from: 7, to: 7},
		getterStub{},
		func(context.Context, *header.ExtendedHeader) error {
			return errors.New("rda query share_index=7: rda: query timeout")
		},
		func(context.Context, shrexsub.Notification) error { return nil },
		nil,
	)

	resultCh := make(chan result, 1)
	w.run(context.Background(), time.Second, resultCh)

	res := <-resultCh
	require.Equal(t, 1, res.failed[7])
	require.NotNil(t, res.failedDetails[7].sampleIndex)
	require.EqualValues(t, 7, *res.failedDetails[7].sampleIndex)
	require.True(t, res.failedDetails[7].retryable)
	require.Error(t, res.err)
}

func TestWorker_OutsideSamplingWindowIsSkippedNotFailed(t *testing.T) {
	w := newWorker(
		job{id: 3, jobType: catchupJob, from: 2, to: 2},
		getterStub{},
		func(context.Context, *header.ExtendedHeader) error { return availability.ErrOutsideSamplingWindow },
		func(context.Context, shrexsub.Notification) error { return nil },
		nil,
	)

	resultCh := make(chan result, 1)
	w.run(context.Background(), time.Second, resultCh)

	res := <-resultCh
	require.Empty(t, res.failed)
	require.NoError(t, res.err)

	state := w.getState()
	require.EqualValues(t, 2, state.curr)
}
