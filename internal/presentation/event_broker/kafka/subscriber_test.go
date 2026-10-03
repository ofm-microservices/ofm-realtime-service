package kafka

import (
	"context"
	"encoding/json"
	"testing"

	"realtime-service/internal/domain"
)

type subscriberServiceStub struct {
	deliveries []domain.DeliveryMessage
}

func (s *subscriberServiceStub) HandleDelivery(_ context.Context, msg domain.DeliveryMessage) error {
	s.deliveries = append(s.deliveries, msg)
	return nil
}

func (s *subscriberServiceStub) WatchConnection(context.Context, domain.EventRoute, string, string) error {
	return nil
}

func TestHandleChatCreatedIgnoresChatMessageCreation(t *testing.T) {
	stub := &subscriberServiceStub{}
	subscriber := &InstanceSubscriber{svc: stub}
	payload, err := json.Marshal(chatChangedEvent{
		EventID:     "message-event",
		EventType:   "chat-service.chat.changed",
		Operation:   "created",
		AggregateID: "order-1",
		Payload:     json.RawMessage(`{"order_id":"order-1","message_id":"message-1","sender_user_id":"buyer-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := subscriber.handleChatCreated(context.Background(), "migration.chat-service.chat.changed", payload); err != nil {
		t.Fatal(err)
	}
	if len(stub.deliveries) != 0 {
		t.Fatalf("expected no realtime deliveries for a chat message, got %d", len(stub.deliveries))
	}
}

func TestHandleChatCreatedDeliversToParticipants(t *testing.T) {
	stub := &subscriberServiceStub{}
	subscriber := &InstanceSubscriber{svc: stub}
	payload, err := json.Marshal(chatChangedEvent{
		EventID:     "chat-event",
		EventType:   "chat-service.chat.changed",
		Operation:   "created",
		AggregateID: "order-1",
		OccurredAt:  "2026-09-25T00:00:00Z",
		Payload:     json.RawMessage(`{"order_id":"order-1","buyer_id":"buyer-1","seller_id":"seller-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := subscriber.handleChatCreated(context.Background(), "migration.chat-service.chat.changed", payload); err != nil {
		t.Fatal(err)
	}
	if len(stub.deliveries) != 2 {
		t.Fatalf("expected two participant deliveries, got %d", len(stub.deliveries))
	}
	if stub.deliveries[0].Type != "chat.created" || stub.deliveries[1].Type != "chat.created" {
		t.Fatalf("unexpected delivery types: %#v", stub.deliveries)
	}
}
