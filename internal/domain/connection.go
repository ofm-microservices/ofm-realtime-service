package domain

import "time"

// Connection is the local websocket transport abstraction owned by realtime-service.
type Connection interface {
	ConnectionID() string
	UserID() string
	Send([]byte) error
	Close() error
	ConnectedAt() time.Time
}
