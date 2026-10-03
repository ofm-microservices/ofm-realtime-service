package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	realtimehttp "realtime-service/internal/presentation/http"
)

// PresentationModule wires the HTTP listener.
var PresentationModule = fx.Options(
	fx.Provide(realtimehttp.NewServer),
	fx.Invoke(InvokeRunHTTPServer),
)

// InvokeRunHTTPServer starts the HTTP server during app startup.
func InvokeRunHTTPServer(lc fx.Lifecycle, srv *realtimehttp.Server, log logging.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() { _ = srv.Run(context.Background()) }()
			return nil
		},
		OnStop: func(context.Context) error { return nil },
	})
}
