package domain

import (
	"encoding/json"

	commonrealtime "github.com/ofm-microservices/ofm-common/pkg/realtime"
)

// DeliveryMessage is the Kafka envelope delivered to one realtime-service instance.
type DeliveryMessage struct {
	EventID       string          `json:"event_id,omitempty"`
	OperationID   string          `json:"operation_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	AggregateType string          `json:"aggregate_type,omitempty"`
	AggregateID   string          `json:"aggregate_id,omitempty"`
	Status        string          `json:"status,omitempty"`
	ErrorCode     string          `json:"error_code,omitempty"`
	Retryable     *bool           `json:"retryable,omitempty"`
	OccurredAt    string          `json:"occurred_at,omitempty"`
	TestRunID     string          `json:"test_run_id,omitempty"`
	ConnectionID  string          `json:"connection_id"`
	UserID        string          `json:"user_id"`
	DeliveryScope string          `json:"delivery_scope,omitempty"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
}

// Notification is the stable client-facing envelope delivered over WebSocket.
// Internal Kafka, CDC, recovery, and projection metadata must not be sent to
// clients unless it is intentionally mapped into this contract by a service.
// Notification is the shared client-facing realtime contract.
type Notification = commonrealtime.Notification

// ClientNotificationTypes is the explicit allow-list for events that may
// cross the realtime boundary. Internal migration, CDC, projection, and
// recovery topics are intentionally absent.
var ClientNotificationTypes = map[string]struct{}{
	"order.completed":              {},
	"order.delivered":              {},
	"order.revision_requested":     {},
	"order.disputed":               {},
	"order.dispute_resolved":       {},
	"review.prompt":                {},
	"chat.created":                 {},
	"chat_message_created":         {},
	"chat_message_edited":          {},
	"chat_message_deleted":         {},
	"gig.created":                  {},
	"gig.updated":                  {},
	"gig.published":                {},
	"gig.projection_completed":     {},
	"gig.operation_failed":         {},
	"review.accepted":              {},
	"review.completed":             {},
	"registration.code_sent":       {},
	"registration.completed":       {},
	"registration.failed":          {},
	"order.requirements_completed": {},
	"order.message_completed":      {},
	"file.uploaded":                {},
	"file.processing":              {},
	"file.ready":                   {},
	"file.failed":                  {},
}

// IsClientNotification reports whether an event type is safe for WebSocket
// delivery to a frontend client.
func IsClientNotification(eventType string) bool {
	_, ok := ClientNotificationTypes[eventType]
	return ok
}

// ConnectionReady is sent after a websocket handshake completes.
type ConnectionReady struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connection_id"`
}
