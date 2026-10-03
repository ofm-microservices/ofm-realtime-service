package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"realtime-service/config"
	app "realtime-service/internal/application"
	"realtime-service/internal/domain"
	eb "realtime-service/internal/presentation/event_broker"
)

// InstanceSubscriber wires the Kafka delivery topic to the local websocket registry.
type InstanceSubscriber struct {
	broker    eb.EventBroker
	svc       app.Service
	log       logging.Logger
	chatTopic string
}

// NewInstanceSubscriber constructs the Kafka delivery subscriber.
func NewInstanceSubscriber(broker eb.EventBroker, svc app.Service, log logging.Logger, cfg config.KafkaConfig) *InstanceSubscriber {
	chatTopic := "migration.chat-service.chat.changed"
	if strings.TrimSpace(cfg.ChatEventsTopic) != "" {
		chatTopic = strings.TrimSpace(cfg.ChatEventsTopic)
	}
	return &InstanceSubscriber{broker: broker, svc: svc, log: log, chatTopic: chatTopic}
}

// Subscribe registers the shared delivery topic owned by this process.
func (s *InstanceSubscriber) Subscribe(ctx context.Context) error {
	errs := make(chan error, 2)
	go func() {
		errs <- s.broker.Subscribe(ctx, RealtimeSubject, s.handleDelivery)
	}()
	go func() {
		errs <- s.broker.Subscribe(ctx, s.chatEventsTopic(), s.handleChatCreated)
	}()
	return <-errs
}

func (s *InstanceSubscriber) handleDelivery(ctx context.Context, subject string, payload []byte) error {
	var msg domain.DeliveryMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		s.log.Error("realtime Kafka event decode failed", logging.String("topic", subject), logging.Err(err))
		return resilience.Permanent(err)
	}
	if msg.EventID == "" {
		err := fmt.Errorf("realtime event missing event_id: type=%q aggregate_id=%q", msg.Type, msg.AggregateID)
		s.log.Error("realtime Kafka event rejected", logging.String("topic", subject), logging.String("event_type", msg.Type), logging.String("aggregate_id", msg.AggregateID), logging.Err(err))
		return resilience.Permanent(err)
	}
	s.log.Info("realtime Kafka event received",
		logging.String("topic", subject), logging.String("event_id", msg.EventID),
		logging.String("event_type", msg.Type), logging.String("aggregate_id", msg.AggregateID),
		logging.String("delivery_scope", msg.DeliveryScope), logging.String("connection_id", msg.ConnectionID),
		logging.String("user_id", msg.UserID),
	)
	return s.svc.HandleDelivery(ctx, msg)
}

type chatChangedEvent struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	Operation     string          `json:"operation"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	OccurredAt    string          `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

type chatCreatedPayload struct {
	OrderID   string `json:"order_id"`
	BuyerID   string `json:"buyer_id"`
	SellerID  string `json:"seller_id"`
	MessageID string `json:"message_id"`
}

func (s *InstanceSubscriber) handleChatCreated(ctx context.Context, subject string, payload []byte) error {
	var event chatChangedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return resilience.Permanent(err)
	}
	if event.EventType != "chat-service.chat.changed" || !strings.EqualFold(event.Operation, "created") {
		return nil
	}
	var chat chatCreatedPayload
	if err := json.Unmarshal(event.Payload, &chat); err != nil {
		return resilience.Permanent(err)
	}
	// Chat messages use the same CDC topic and operation name as chat-row
	// creation, but their payload is identified by message_id and intentionally
	// has no chat participants. They are projection events, not chat-created
	// notifications; acknowledging them here prevents realtime from sending
	// valid message events to the DLQ and repeatedly restarting its consumer.
	if chat.MessageID != "" {
		return nil
	}
	if chat.OrderID == "" {
		chat.OrderID = event.AggregateID
	}
	if chat.BuyerID == "" || chat.SellerID == "" || chat.OrderID == "" || event.EventID == "" {
		return resilience.Permanent(fmt.Errorf("chat.created CDC event is missing routing fields: event_id=%q order_id=%q buyer_id=%q seller_id=%q", event.EventID, chat.OrderID, chat.BuyerID, chat.SellerID))
	}
	for _, userID := range []string{chat.BuyerID, chat.SellerID} {
		msg := domain.DeliveryMessage{
			EventID:       event.EventID + ":" + userID,
			AggregateType: "chat",
			AggregateID:   chat.OrderID,
			Status:        "completed",
			OccurredAt:    event.OccurredAt,
			UserID:        userID,
			DeliveryScope: "user",
			Type:          "chat.created",
			Payload:       event.Payload,
		}
		if err := s.svc.HandleDelivery(ctx, msg); err != nil {
			s.log.Error("realtime chat.created delivery failed", logging.String("topic", subject), logging.String("event_id", event.EventID), logging.String("user_id", userID), logging.Err(err))
			return err
		}
	}
	return nil
}

func (s *InstanceSubscriber) chatEventsTopic() string {
	return s.chatTopic
}

// Replay rebuilds Redis-backed realtime state from Kafka retention.
func (s *InstanceSubscriber) Replay(ctx context.Context) error {
	return s.broker.Replay(ctx, RealtimeSubject, s.handleReplayDelivery)
}

func (s *InstanceSubscriber) handleReplayDelivery(ctx context.Context, _ string, payload []byte) error {
	var msg domain.DeliveryMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return err
	}
	if msg.EventID == "" {
		err := fmt.Errorf("realtime recovery event missing event_id: type=%q aggregate_id=%q", msg.Type, msg.AggregateID)
		s.log.Error("realtime Kafka recovery event rejected", logging.String("event_type", msg.Type), logging.String("aggregate_id", msg.AggregateID), logging.Err(err))
		return resilience.Permanent(err)
	}
	return s.svc.HandleDelivery(ctx, msg)
}

// Subject returns the shared Kafka topic this subscriber owns.
func (s *InstanceSubscriber) Subject() string { return RealtimeSubject }

const RealtimeSubject = "realtime"
