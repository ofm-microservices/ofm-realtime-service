package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"realtime-service/config"
	app "realtime-service/internal/application"
)

// Server owns the realtime HTTP listener and websocket route.
type Server struct {
	app app.Service
	mgr app.ConnectionManager
	cfg *config.Config
	log logging.Logger
}

// NewServer constructs the realtime HTTP server.
func NewServer(appSvc app.Service, mgr app.ConnectionManager, cfg *config.Config, log logging.Logger) (*Server, error) {
	if log == nil {
		return nil, ErrNilLogger
	}
	return &Server{app: appSvc, mgr: mgr, cfg: cfg, log: log.With(logging.String("module", "http"))}, nil
}

// Run starts the HTTP listener and blocks.
func (s *Server) Run(ctx context.Context) error {
	f := fiber.New()
	f.Use(transportLoggingMiddleware(s.log))
	wh := NewWSHandler(s.app, s.mgr, s.cfg, s.log)
	f.Get("/ws", wh.Handle)
	f.Get("/metrics", func(c *fiber.Ctx) error { return c.SendStatus(fiber.StatusOK) })
	addr := fmt.Sprintf("0.0.0.0:%s", s.cfg.HTTP.Port)
	go func() {
		_ = http.ListenAndServe("0.0.0.0:9610", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	}()
	go func() { <-ctx.Done(); _ = f.Shutdown() }()
	return f.Listen(addr)
}
