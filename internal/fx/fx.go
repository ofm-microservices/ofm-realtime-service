package appfx

import "go.uber.org/fx"

// Module keeps the compatibility bundle for the realtime service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	AppModule,
	MessagingModule,
	ServiceModule,
	PresentationModule,
)
