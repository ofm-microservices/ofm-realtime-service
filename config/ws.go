package config

// WSConfig defines websocket timing settings.
type WSConfig struct {
	ReadTimeoutSeconds  int `env:"READ_TIMEOUT_SECONDS" envDefault:"60"`
	WriteTimeoutSeconds int `env:"WRITE_TIMEOUT_SECONDS" envDefault:"10"`
	PingIntervalSeconds int `env:"PING_INTERVAL_SECONDS" envDefault:"30"`
}
