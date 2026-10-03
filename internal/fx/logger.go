package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"realtime-service/config"
)

// LoggerModule provides the shared structured logger.
var LoggerModule = fx.Provide(ProvideLogger)

// ProvideLogger constructs a zap-backed logger.
func ProvideLogger(cfg *config.Config) (logging.Logger, error) {
	return logging.NewWithMode(cfg.App.Name, cfg.App.Env, cfg.App.ObservabilityMode, "")
}
