package share

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRDAGetRequester_BackoffDuration_ExponentialCapped(t *testing.T) {
	r := &RDAGetProtocolRequester{
		retryBackoffBase: 10 * time.Millisecond,
		retryBackoffMax:  40 * time.Millisecond,
	}

	require.Equal(t, 10*time.Millisecond, r.backoffDuration(0))
	require.Equal(t, 20*time.Millisecond, r.backoffDuration(1))
	require.Equal(t, 40*time.Millisecond, r.backoffDuration(2))
	require.Equal(t, 40*time.Millisecond, r.backoffDuration(5))
}

func TestRDAGetRequester_BackoffDuration_DisabledBase(t *testing.T) {
	r := &RDAGetProtocolRequester{
		retryBackoffBase: 0,
		retryBackoffMax:  time.Second,
	}

	require.Equal(t, time.Duration(0), r.backoffDuration(0))
	require.Equal(t, time.Duration(0), r.backoffDuration(3))
}

func TestRDAGetRequester_ShouldBackoff(t *testing.T) {
	r := &RDAGetProtocolRequester{}

	require.True(t, r.shouldBackoff(ErrRDAQueryTimeout))
	require.True(t, r.shouldBackoff(ErrRDAPeerUnavailable))
	require.True(t, r.shouldBackoff(fmt.Errorf("wrapped: %w", ErrRDAQueryTimeout)))
	require.False(t, r.shouldBackoff(ErrRDAProofInvalid))
	require.False(t, r.shouldBackoff(nil))
}

func TestRDAGetRequester_WaitBackoff_ContextCanceled(t *testing.T) {
	r := &RDAGetProtocolRequester{
		retryBackoffBase: 100 * time.Millisecond,
		retryBackoffMax:  100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := r.waitBackoff(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
}

func TestChooseQueryPeers_IntersectionDeterministic(t *testing.T) {
	rowPeers := []peer.ID{"peerC", "peerA"}
	colPeers := []peer.ID{"peerB", "peerA", "peerC", "peerA"}

	got := chooseQueryPeers(rowPeers, colPeers)
	require.Equal(t, []peer.ID{"peerA", "peerC"}, got)
}

func TestChooseQueryPeers_FallbackToColumnDeterministic(t *testing.T) {
	rowPeers := []peer.ID{"peerX"}
	colPeers := []peer.ID{"peerB", "peerA", "peerA"}

	got := chooseQueryPeers(rowPeers, colPeers)
	require.Equal(t, []peer.ID{"peerA", "peerB"}, got)
}

func TestValidateGetResponse_OK(t *testing.T) {
	req := GetMessage{Type: GetMessageTypeQuery, Handle: "h1", ShareIndex: 7, RequestID: "req-1"}
	resp := GetMessage{
		Type:       GetMessageTypeResponse,
		Handle:     "h1",
		ShareIndex: 7,
		RequestID:  "req-1",
		Data:       []byte{1, 2, 3},
		NMTProof:   &NMTProofData{},
	}

	require.NoError(t, validateGetResponse(req, resp))
}

func TestValidateGetResponse_RequestIDMismatch(t *testing.T) {
	req := GetMessage{Type: GetMessageTypeQuery, Handle: "h1", ShareIndex: 7, RequestID: "req-1"}
	resp := GetMessage{
		Type:       GetMessageTypeResponse,
		Handle:     "h1",
		ShareIndex: 7,
		RequestID:  "req-2",
		Data:       []byte{1},
		NMTProof:   &NMTProofData{},
	}

	err := validateGetResponse(req, resp)
	require.ErrorIs(t, err, ErrRDAProtocolDecode)
}

func TestValidateGetResponse_IdentityMismatch(t *testing.T) {
	req := GetMessage{Type: GetMessageTypeQuery, Handle: "h1", ShareIndex: 7, RequestID: "req-1"}
	resp := GetMessage{
		Type:       GetMessageTypeResponse,
		Handle:     "h2",
		ShareIndex: 7,
		RequestID:  "req-1",
		Data:       []byte{1},
		NMTProof:   &NMTProofData{},
	}

	err := validateGetResponse(req, resp)
	require.ErrorIs(t, err, ErrRDAProtocolDecode)
}

func TestValidateGetResponse_EmptyPayload(t *testing.T) {
	req := GetMessage{Type: GetMessageTypeQuery, Handle: "h1", ShareIndex: 7, RequestID: "req-1"}
	resp := GetMessage{
		Type:       GetMessageTypeResponse,
		Handle:     "h1",
		ShareIndex: 7,
		RequestID:  "req-1",
		NMTProof:   &NMTProofData{},
	}

	err := validateGetResponse(req, resp)
	require.ErrorIs(t, err, ErrRDASymbolNotFound)
}

func TestValidateGetResponse_MissingProof(t *testing.T) {
	req := GetMessage{Type: GetMessageTypeQuery, Handle: "h1", ShareIndex: 7, RequestID: "req-1"}
	resp := GetMessage{
		Type:       GetMessageTypeResponse,
		Handle:     "h1",
		ShareIndex: 7,
		RequestID:  "req-1",
		Data:       []byte{1},
		NMTProof:   nil,
	}

	err := validateGetResponse(req, resp)
	require.ErrorIs(t, err, ErrRDAProtocolDecode)
}
