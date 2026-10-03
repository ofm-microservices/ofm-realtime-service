package config

import (
	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config groups the realtime-service runtime configuration.
type Config struct {
	App   AppConfig  `envPrefix:"APP_"`
	HTTP  HTTPConfig `envPrefix:"HTTP_"`
	Kafka KafkaConfig
	Redis RedisConfig
	JWT   JWTConfig `envPrefix:"JWT_"`
	WS    WSConfig  `envPrefix:"WS_"`
}

// Load parses the realtime-service configuration.
func Load() (*Config, error) {
	_ = godotenv.Load()
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
