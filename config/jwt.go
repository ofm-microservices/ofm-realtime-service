package config

// JWTConfig defines access-token validation settings.
type JWTConfig struct {
	AccessSecret string `env:"ACCESS_SECRET" envDefault:"local-dev-access-secret-change-me"`
	PublicKey    string `env:"PUBLIC_KEY" envDefault:""`
}
