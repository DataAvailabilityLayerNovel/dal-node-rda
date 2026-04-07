package share

import "errors"

var (
	// RDA-related errors
	ErrSubnetNotInitialized = errors.New("subnet not initialized")
	ErrInvalidGridPosition  = errors.New("invalid grid position")
	ErrNoPeersAvailable     = errors.New("no peers available for communication")
	ErrCommunicationDenied  = errors.New("communication denied by filter policy")
	ErrRDANotStarted        = errors.New("RDA service not started")

	// Stable GET taxonomy errors for RDA-aware DAS integration.
	ErrRDAQueryTimeout    = errors.New("rda: query timeout")
	ErrRDAPeerUnavailable = errors.New("rda: peer unavailable")
	ErrRDAProofInvalid    = errors.New("rda: proof invalid")
	ErrRDASymbolNotFound  = errors.New("rda: symbol not found")
	ErrRDAProtocolDecode  = errors.New("rda: protocol decode")
)
