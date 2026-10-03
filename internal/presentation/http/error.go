package http

import "errors"

var (
	// ErrNilLogger indicates that HTTP wiring received no logger.
	ErrNilLogger = errors.New("logger is nil")
	// ErrMissingToken indicates that the websocket handshake did not provide a token.
	ErrMissingToken = errors.New("missing token")
)
