package domain

import "context"

// EventRoute identifies the websocket audience for a client notification.
type EventRoute struct {
	Kind string
	ID   string
}

// EventStore is the durable realtime buffer between Kafka and WebSocket.
type EventStore interface {
	Append(context.Context, EventRoute, string, []byte) error
	Watch(context.Context, EventRoute, string, func([]byte) error) error
	Close() error
}
