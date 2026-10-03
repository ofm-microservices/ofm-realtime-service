package application

import (
	"context"

	"realtime-service/internal/domain"
)

// Service owns realtime message routing inside one process.
type Service interface {
	HandleDelivery(ctx context.Context, msg domain.DeliveryMessage) error
	WatchConnection(ctx context.Context, route domain.EventRoute, connectionID, lastEventID string) error
}

// ConnectionManager owns the in-memory websocket registry.
type ConnectionManager interface {
	Add(conn domain.Connection)
	Remove(connectionID string)
	Get(connectionID string) (domain.Connection, bool)
	SendToConnection(connectionID string, payload []byte) error
	SendToUser(userID string, payload []byte) error
	ActiveUserConnections(userID string) int
	AddRegistration(sessionID string, conn domain.Connection)
	SendToRegistration(sessionID string, payload []byte) bool
	RemoveRegistration(sessionID string)
}
