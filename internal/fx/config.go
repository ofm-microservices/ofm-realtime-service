package appfx

import "realtime-service/config"

import "go.uber.org/fx"

// ConfigModule loads the environment-backed configuration.
var ConfigModule = fx.Provide(config.Load)
