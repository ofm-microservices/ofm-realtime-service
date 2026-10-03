package ws

import (
	"sync"
	"time"
)

// ClientConn is the local websocket connection wrapper stored in memory.
type ClientConn struct {
	ConnectionIDValue          string
	UserIDValue                string
	RegistrationSessionIDValue string
	Conn                       Conn
	SendQueue                  chan []byte
	Done                       chan struct{}
	ConnectedAtValue           time.Time
	closeOnce                  sync.Once
}

// Conn is the websocket transport contract used by the connection wrapper.
type Conn interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	Close() error
}
