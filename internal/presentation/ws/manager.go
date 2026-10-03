package ws

import (
	"sync"
	"time"

	"realtime-service/internal/domain"
)

// ConnectionManager stores active websocket connections in memory.
type ConnectionManager struct {
	mu                  sync.RWMutex
	byID                map[string]domain.Connection
	byUser              map[string]map[string]domain.Connection
	byRegistration      map[string]map[string]domain.Connection
	pendingRegistration map[string][]pendingRegistrationEvent
	pendingUser         map[string][]pendingUserEvent
}

type pendingRegistrationEvent struct {
	payload   []byte
	expiresAt time.Time
}

type pendingUserEvent struct {
	payload   []byte
	expiresAt time.Time
}

// NewConnectionManager constructs the in-memory registry.
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		byID:                make(map[string]domain.Connection),
		byUser:              make(map[string]map[string]domain.Connection),
		byRegistration:      make(map[string]map[string]domain.Connection),
		pendingRegistration: make(map[string][]pendingRegistrationEvent),
		pendingUser:         make(map[string][]pendingUserEvent),
	}
}

// AddRegistration stores a temporary unsigned registration connection locally.
func (m *ConnectionManager) AddRegistration(sessionID string, conn domain.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Registration connections are unsigned, but they still need to be
	// addressable by their connection ID. The durable Redis-stream watcher
	// delivers through SendToConnection, which resolves connections from
	// byID. Keeping registration connections only in byRegistration caused
	// buffered registration events to be silently discarded.
	m.byID[conn.ConnectionID()] = conn
	if _, ok := m.byRegistration[sessionID]; !ok {
		m.byRegistration[sessionID] = make(map[string]domain.Connection)
	}
	m.byRegistration[sessionID][conn.ConnectionID()] = conn
	if pending := m.pendingRegistration[sessionID]; len(pending) > 0 {
		now := time.Now()
		for _, event := range pending {
			if now.Before(event.expiresAt) {
				_ = conn.Send(event.payload)
			}
		}
		delete(m.pendingRegistration, sessionID)
	}
}

// Add stores one active connection.
func (m *ConnectionManager) Add(conn domain.Connection) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID[conn.ConnectionID()] = conn
	if _, ok := m.byUser[conn.UserID()]; !ok {
		m.byUser[conn.UserID()] = make(map[string]domain.Connection)
	}
	m.byUser[conn.UserID()][conn.ConnectionID()] = conn
	if pending := m.pendingUser[conn.UserID()]; len(pending) > 0 {
		now := time.Now()
		for _, event := range pending {
			if now.Before(event.expiresAt) {
				_ = conn.Send(event.payload)
			}
		}
		delete(m.pendingUser, conn.UserID())
	}
}

// Remove deletes a connection from the registry.
func (m *ConnectionManager) Remove(connectionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	conn, ok := m.byID[connectionID]
	if !ok {
		return
	}
	delete(m.byID, connectionID)
	for sessionID, connections := range m.byRegistration {
		delete(connections, connectionID)
		if len(connections) == 0 {
			delete(m.byRegistration, sessionID)
		}
	}
	if userConnections, ok := m.byUser[conn.UserID()]; ok {
		delete(userConnections, connectionID)
		if len(userConnections) == 0 {
			delete(m.byUser, conn.UserID())
		}
	}
}

// SendToRegistration delivers a registration event to local session listeners.
func (m *ConnectionManager) SendToRegistration(sessionID string, payload []byte) bool {
	// Pending registration events are written when the saga wins the race
	// against the websocket handshake. This path must use the write lock;
	// mutating the pending map while holding RLock caused lost events and a
	// data race under concurrent registrations.
	m.mu.Lock()
	defer m.mu.Unlock()
	connections := m.byRegistration[sessionID]
	delivered := false
	for _, conn := range connections {
		if err := conn.Send(payload); err == nil {
			delivered = true
		}
	}
	if len(connections) == 0 {
		pending := m.pendingRegistration[sessionID]
		now := time.Now()
		filtered := pending[:0]
		for _, event := range pending {
			if now.Before(event.expiresAt) {
				filtered = append(filtered, event)
			}
		}
		if len(filtered) < 3 {
				filtered = append(filtered, pendingRegistrationEvent{payload: append([]byte(nil), payload...), expiresAt: now.Add(10 * time.Minute)})
		}
		m.pendingRegistration[sessionID] = filtered
	}
	return delivered
}

// RemoveRegistration closes and removes all local listeners for a session.
func (m *ConnectionManager) RemoveRegistration(sessionID string) {
	m.mu.Lock()
	connections := m.byRegistration[sessionID]
	delete(m.byRegistration, sessionID)
	m.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
		m.Remove(conn.ConnectionID())
	}
}

// Get returns a local connection if it exists.
func (m *ConnectionManager) Get(connectionID string) (domain.Connection, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conn, ok := m.byID[connectionID]
	return conn, ok
}

// SendToConnection writes one payload to the exact connection.
func (m *ConnectionManager) SendToConnection(connectionID string, payload []byte) error {
	conn, ok := m.Get(connectionID)
	if !ok {
		return nil
	}
	return conn.Send(payload)
}

// SendToUser writes one payload to every local connection for a user.
func (m *ConnectionManager) SendToUser(userID string, payload []byte) error {
	m.mu.RLock()
	connections := m.byUser[userID]
	if len(connections) > 0 {
		defer m.mu.RUnlock()
		for _, conn := range connections {
			if err := conn.Send(payload); err != nil {
				return err
			}
		}
		return nil
	}
	m.mu.RUnlock()

	// A Kafka notification may race the websocket handshake or a client
	// reconnect. Keep a small bounded buffer so a successful business command
	// cannot lose its realtime result merely because this process had no local
	// socket at that instant. The event remains best-effort and expires quickly;
	// Kafka/operation state remains the durable source of truth.
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	pending := m.pendingUser[userID]
	filtered := pending[:0]
	for _, event := range pending {
		if now.Before(event.expiresAt) {
			filtered = append(filtered, event)
		}
	}
	if len(filtered) < 32 {
		filtered = append(filtered, pendingUserEvent{
			payload:   append([]byte(nil), payload...),
			expiresAt: now.Add(2 * time.Minute),
		})
	}
	m.pendingUser[userID] = filtered
	return nil
}

// ActiveUserConnections reports the local socket count for observability.
func (m *ConnectionManager) ActiveUserConnections(userID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUser[userID])
}
