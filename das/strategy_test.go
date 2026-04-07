package das

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/celestiaorg/celestia-node/share"
	"github.com/stretchr/testify/require"
)

func TestClassifyRetryableDeterministic(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "rda timeout", err: fmt.Errorf("wrapped: %w", share.ErrRDAQueryTimeout), want: true},
		{name: "rda peer unavailable", err: fmt.Errorf("wrapped: %w", share.ErrRDAPeerUnavailable), want: true},
		{name: "rda proof invalid", err: fmt.Errorf("wrapped: %w", share.ErrRDAProofInvalid), want: false},
		{name: "rda symbol not found", err: fmt.Errorf("wrapped: %w", share.ErrRDASymbolNotFound), want: false},
		{name: "rda protocol decode", err: fmt.Errorf("wrapped: %w", share.ErrRDAProtocolDecode), want: true},
		{name: "generic", err: errors.New("temporary failure"), want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotFirst := classifyRetryable(tc.err)
			gotSecond := classifyRetryable(tc.err)
			require.Equal(t, tc.want, gotFirst)
			require.Equal(t, gotFirst, gotSecond)
		})
	}
}

func TestMakeResultContract(t *testing.T) {
	resOK := makeResult(ModeClassic, nil)
	require.True(t, resOK.Success)
	require.False(t, resOK.Retryable)
	require.NoError(t, resOK.Err)
	require.Equal(t, ModeClassic, resOK.Telemetry.Mode)

	err := errors.New("boom")
	resErr := makeResult(ModeRDA, err)
	require.False(t, resErr.Success)
	require.Error(t, resErr.Err)
	require.True(t, resErr.Retryable)
	require.Equal(t, "boom", resErr.Telemetry.Error)
	require.Equal(t, ModeRDA, resErr.Telemetry.Mode)
}

func TestShouldFallbackPolicy(t *testing.T) {
	base := DefaultParameters()

	testCases := []struct {
		name    string
		params  Parameters
		err     error
		retries int
		want    bool
	}{
		{
			name:    "no error never falls back",
			params:  base,
			err:     nil,
			retries: 0,
			want:    false,
		},
		{
			name: "fallback disabled",
			params: func() Parameters {
				p := base
				p.RDAFallbackEnabled = false
				return p
			}(),
			err:     share.ErrRDAQueryTimeout,
			retries: 99,
			want:    false,
		},
		{
			name:    "terminal error falls back immediately",
			params:  base,
			err:     share.ErrRDAProofInvalid,
			retries: 0,
			want:    true,
		},
		{
			name: "retryable below threshold",
			params: func() Parameters {
				p := base
				p.RDAFallbackAfterRetries = 3
				return p
			}(),
			err:     share.ErrRDAQueryTimeout,
			retries: 2,
			want:    false,
		},
		{
			name: "retryable at threshold",
			params: func() Parameters {
				p := base
				p.RDAFallbackAfterRetries = 3
				return p
			}(),
			err:     share.ErrRDAQueryTimeout,
			retries: 3,
			want:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldFallback(tc.params, tc.err, tc.retries)
			require.Equal(t, tc.want, got)
		})
	}
}
