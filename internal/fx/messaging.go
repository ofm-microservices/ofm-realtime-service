package appfx

import (
	"context"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
	"realtime-service/config"
	app "realtime-service/internal/application"
	eb "realtime-service/internal/presentation/event_broker"
	"realtime-service/internal/presentation/event_broker/kafka"
)

// MessagingModule wires the runtime Kafka connection.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBrokerWithClaims),
	fx.Provide(ProvideInstanceSubscriber),
	fx.Invoke(InvokeSubscribeInstance),
)

// ProvideEventBroker constructs the Kafka broker.
func ProvideEventBroker(cfg *config.Config, log logging.Logger) (eb.EventBroker, error) {
	return kafka.NewBroker(cfg.Kafka, log)
}

// ProvideEventBrokerWithClaims wires Kafka with Redis-backed durable claims.
func ProvideEventBrokerWithClaims(lc fx.Lifecycle, cfg *config.Config, log logging.Logger) (eb.EventBroker, error) {
	b, err := kafka.NewBroker(cfg.Kafka, log)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { b.Close(); return nil }})
	return b, nil
}

// ProvideInstanceSubscriber constructs the instance subscriber.
// cfg is injected through the provider below so the CDC topic remains
// configurable for local Compose and Helm deployments.
func ProvideInstanceSubscriber(broker eb.EventBroker, svc app.Service, log logging.Logger, cfg *config.Config) *kafka.InstanceSubscriber {
	return kafka.NewInstanceSubscriber(broker, svc, log, cfg.Kafka)
}

// InvokeSubscribeInstance starts the instance subscription.
func InvokeSubscribeInstance(lc fx.Lifecycle, sub *kafka.InstanceSubscriber, log logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer recoveryCancel()
				if err := sub.Replay(recoveryCtx); err != nil {
					log.Warn("realtime Kafka recovery replay failed; live consumer will continue", logging.Err(err))
					return
				}
				log.Info("realtime Kafka recovery replay completed")
			}()
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				backoff := 100 * time.Millisecond
				for runCtx.Err() == nil {
					err := sub.Subscribe(runCtx)
					if runCtx.Err() != nil {
						return
					}
					if err != nil {
						log.Error("realtime Kafka consumer stopped; reconnecting", logging.Err(err), logging.String("backoff", backoff.String()))
					} else {
						log.Warn("realtime Kafka consumer stopped; reconnecting", logging.String("backoff", backoff.String()))
					}
					timer := time.NewTimer(backoff)
					select {
					case <-runCtx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					if backoff < 30*time.Second {
						backoff *= 2
						if backoff > 30*time.Second {
							backoff = 30 * time.Second
						}
					}
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}
