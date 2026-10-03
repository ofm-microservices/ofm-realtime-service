package main

import (
	appfx "realtime-service/internal/fx"

	"go.uber.org/fx"
)

var newApp = fx.New
var runApp = (*fx.App).Run

func main() {
	runApp(newApp(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.AppModule,
		appfx.MessagingModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	))
}
