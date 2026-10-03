package appfx

import (
	app "realtime-service/internal/application"
	"realtime-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// ServiceModule provides the realtime application coordinator.
var ServiceModule = fx.Options(
	fx.Provide(ProvideService),
)

// ProvideService constructs the realtime application service.
func ProvideService(manager app.ConnectionManager, store domain.EventStore, log logging.Logger) (app.Service, error) {
	svc, err := app.New(manager, store, log)
	if err != nil {
		return nil, err
	}
	return svc, nil
}
