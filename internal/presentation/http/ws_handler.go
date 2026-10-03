package http

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	commonjwt "github.com/ofm-microservices/ofm-common/pkg/jwt"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/oklog/ulid/v2"
	"realtime-service/config"
	app "realtime-service/internal/application"
	"realtime-service/internal/domain"
	"realtime-service/internal/presentation/ws"
)

type wsHandler struct {
	app app.Service
	mgr app.ConnectionManager
	cfg *config.Config
	log logging.Logger
}

// NewWSHandler constructs the websocket handshake handler.
func NewWSHandler(appSvc app.Service, mgr app.ConnectionManager, cfg *config.Config, log logging.Logger) *wsHandler {
	return &wsHandler{app: appSvc, mgr: mgr, cfg: cfg, log: log.With(logging.String("module", "ws"))}
}

// Handle upgrades the connection and registers it in local memory.
func (h *wsHandler) Handle(c *fiber.Ctx) error {
	registrationSessionID := strings.TrimSpace(c.Query("session_id"))
	lastEventID := strings.TrimSpace(c.Query("last_event_id"))
	token := c.Query("token")
	if registrationSessionID == "" && strings.TrimSpace(token) == "" {
		return c.SendStatus(fiber.StatusUnauthorized)
	}
	userID := ""
	if registrationSessionID == "" {
		verifier, err := commonjwt.NewVerifier(commonjwt.Config{Secret: h.cfg.JWT.AccessSecret, PublicKey: h.cfg.JWT.PublicKey})
		if err != nil {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		claims, err := verifier.Validate(token)
		if err != nil {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		userID = claims.Subject
	}
	connID := ulid.Make().String()
	client := &ws.ClientConn{
		ConnectionIDValue:          connID,
		UserIDValue:                userID,
		RegistrationSessionIDValue: registrationSessionID,
		// Registration bursts can deliver ready, code, and terminal events while
		// the client is between websocket phases. Keep enough per-connection
		// buffer to absorb the burst without dropping the socket.
		SendQueue:        make(chan []byte, 1024),
		Done:             make(chan struct{}),
		ConnectedAtValue: time.Now().UTC(),
	}
	c.Locals("ws.client", client)
	return websocket.New(func(conn *websocket.Conn) {
		client.Conn = conn
		if registrationSessionID != "" {
			h.mgr.AddRegistration(registrationSessionID, client)
		} else {
			h.mgr.Add(client)
		}
		h.log.Info("realtime websocket route registered",
			logging.String("connection_id", connID),
			logging.String("route_kind", map[bool]string{true: "session", false: "user"}[registrationSessionID != ""]),
			logging.String("route_id", map[bool]string{true: registrationSessionID, false: userID}[registrationSessionID != ""]),
		)
		defer func() {
			h.mgr.Remove(connID)
			_ = client.Close()
		}()
		watchCtx, cancelWatch := context.WithCancel(context.Background())
		defer cancelWatch()
		route := domain.EventRoute{Kind: "user", ID: userID}
		if registrationSessionID != "" {
			route = domain.EventRoute{Kind: "session", ID: registrationSessionID}
		}
		go func() {
			h.log.Info("realtime websocket stream watcher started",
				logging.String("connection_id", connID),
				logging.String("route_kind", route.Kind),
				logging.String("route_id", route.ID),
			)
			if err := h.app.WatchConnection(watchCtx, route, connID, lastEventID); err != nil && watchCtx.Err() == nil {
				if !errors.Is(err, app.ErrRegistrationComplete) {
					h.log.Warn("realtime websocket stream watcher stopped", logging.Err(err))
				}
				_ = client.Close()
			}
		}()
		var writeMu sync.Mutex
		writeFrame := func(payload []byte) error {
			writeMu.Lock()
			defer writeMu.Unlock()
			return conn.WriteMessage(websocket.TextMessage, payload)
		}
		ready, _ := json.Marshal(map[string]string{"type": "connection.ready", "connection_id": connID})
		_ = writeFrame(ready)
		if registrationSessionID != "" {
			timer := time.AfterFunc(10*time.Minute, func() { _ = conn.Close() })
			defer timer.Stop()
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-client.Done:
					return
				case payload := <-client.SendQueue:
					if err := writeFrame(payload); err != nil {
						_ = client.Close()
						return
					}
				}
			}
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				_ = client.Close()
				<-done
				return
			}
		}
	})(c)
}
