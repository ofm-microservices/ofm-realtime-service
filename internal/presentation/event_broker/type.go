package eventbroker

import "context"

// MessageHandler processes one Kafka payload.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// EventBroker abstracts the runtime Kafka connection used by realtime-service.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	Replay(ctx context.Context, subject string, handler MessageHandler) error
	Close()
}
