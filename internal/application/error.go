package application

import "errors"

var (
	// ErrNilLogger indicates that application wiring received no logger.
	ErrNilLogger = errors.New("logger is nil")
	// ErrNilConnectionManager indicates that realtime routing was constructed without a registry.
	ErrNilConnectionManager = errors.New("connection manager is nil")
	// ErrNilMessage is returned when a broker payload is empty.
	ErrNilMessage = errors.New("realtime delivery message is nil")
	// ErrNilEventStore indicates that durable delivery was not wired.
	ErrNilEventStore = errors.New("event store is nil")
	// ErrRegistrationComplete tells the transport to close a terminal registration socket.
	ErrRegistrationComplete = errors.New("registration websocket completed")
)
