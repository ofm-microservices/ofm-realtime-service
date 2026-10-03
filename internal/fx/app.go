package appfx

import (
	"context"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"realtime-service/config"
	"realtime-service/internal/application"
	"realtime-service/internal/domain"
	redisinfra "realtime-service/internal/infra/redis"
	"realtime-service/internal/presentation/ws"
)

// AppModule provides the in-memory websocket registry.
var AppModule = fx.Options(
	fx.Provide(ProvideConnectionManager),
	fx.Provide(ProvideEventStore),
	fx.Invoke(InvokeCloseEventStore),
)

// ProvideConnectionManager constructs the in-memory websocket registry.
func ProvideConnectionManager() application.ConnectionManager {
	return ws.NewConnectionManager()
}

// ProvideEventStore constructs the durable Redis event buffer.
func ProvideEventStore(cfg *config.Config, log logging.Logger) (domain.EventStore, error) {
	return redisinfra.NewStore(cfg.Redis, log)
}

// InvokeCloseEventStore closes Redis when the realtime application stops.
func InvokeCloseEventStore(lc fx.Lifecycle, store domain.EventStore) {
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return store.Close() }})
}
