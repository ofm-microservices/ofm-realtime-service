package config

import "fmt"

// RedisConfig defines the durable event-claim store used by realtime-service.
type RedisConfig struct {
	Host     string `env:"REDIS_HOST" envDefault:"localhost"`
	Port     int    `env:"REDIS_PORT" envDefault:"6379"`
	Password string `env:"REDIS_PASSWORD"`
	DB       int    `env:"REDIS_DB" envDefault:"0"`
	PoolSize int    `env:"REDIS_POOL_SIZE" envDefault:"150"`
}

// Address returns the host:port endpoint used by the Redis adapter.
func (c RedisConfig) Address() string { return fmt.Sprintf("%s:%d", c.Host, c.Port) }
