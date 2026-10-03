package config

// HTTPConfig defines the HTTP listener configuration.
type HTTPConfig struct {
	Port string `env:"PORT" envDefault:"8082"`
}
