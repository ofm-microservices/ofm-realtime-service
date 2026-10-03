package config

// AppConfig defines application-level configuration.
type AppConfig struct {
	Name string `env:"NAME" envDefault:"realtime-service"`
	Env  string `env:"ENV" envDefault:"local"`
	// ObservabilityMode controls verbose transport logging; use dev locally and
	// production to avoid request/message payload diagnostics.
	ObservabilityMode string `env:"OBSERVABILITY_MODE" envDefault:"production"`
}
