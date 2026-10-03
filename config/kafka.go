package config

// KafkaConfig defines the Kafka delivery topic and consumer group for realtime-service.
type KafkaConfig struct {
	Brokers         []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GroupID         string   `env:"KAFKA_REALTIME_GROUP_ID" envDefault:"realtime-service"`
	ChatEventsTopic string   `env:"KAFKA_CHAT_EVENTS_TOPIC" envDefault:"migration.chat-service.chat.changed"`
}
