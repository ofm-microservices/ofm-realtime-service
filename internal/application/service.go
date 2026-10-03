package application

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"realtime-service/internal/domain"
)

type service struct {
	manager ConnectionManager
	store   domain.EventStore
	log     logging.Logger
}

// New constructs the realtime service coordinator.
func New(manager ConnectionManager, store domain.EventStore, log logging.Logger) (Service, error) {
	if manager == nil {
		return nil, ErrNilConnectionManager
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	if store == nil {
		return nil, ErrNilEventStore
	}
	return &service{
		manager: manager,
		store:   store,
		log:     log.With(logging.String("module", "application")),
	}, nil
}

// HandleDelivery routes one Kafka delivery envelope to the local websocket registry.
func (s *service) HandleDelivery(ctx context.Context, msg domain.DeliveryMessage) error {
	if !domain.IsClientNotification(msg.Type) {
		s.log.Debug("realtime internal event ignored", logging.String("event_id", msg.EventID), logging.String("type", msg.Type), logging.String("aggregate_id", msg.AggregateID))
		return nil
	}
	notification, err := json.Marshal(domain.Notification{
		EventID: msg.EventID, OperationID: msg.OperationID, CorrelationID: msg.CorrelationID,
		AggregateType: msg.AggregateType, AggregateID: msg.AggregateID, Type: msg.Type,
		Status: msg.Status, ErrorCode: msg.ErrorCode, Retryable: msg.Retryable,
		OccurredAt: msg.OccurredAt, TestRunID: msg.TestRunID, Payload: msg.Payload,
	})
	if err != nil {
		return err
	}
	if strings.EqualFold(msg.DeliveryScope, "registration") {
		sessionID := strings.TrimSpace(msg.AggregateID)
		if sessionID == "" {
			s.log.Warn("realtime registration event has no session route", logging.String("event_id", msg.EventID), logging.String("type", msg.Type))
			return nil
		}
		s.log.Info("realtime route lookup", logging.String("event_id", msg.EventID), logging.String("type", msg.Type), logging.String("route_kind", "session"), logging.String("session_id", sessionID))
		if err := s.store.Append(ctx, domain.EventRoute{Kind: "session", ID: sessionID}, msg.EventID, notification); err != nil {
			s.log.Error("realtime Redis append failed", logging.String("event_id", msg.EventID), logging.String("session_id", sessionID), logging.Err(err))
			return err
		}
		s.log.Info("realtime registration fanout processed",
			logging.String("session_id", sessionID),
			logging.String("type", msg.Type),
			logging.String("operation_id", msg.OperationID),
			logging.String("status", msg.Status),
			logging.String("durable", "true"),
		)
		return nil
	}
	if strings.EqualFold(msg.DeliveryScope, "user") && strings.TrimSpace(msg.UserID) != "" {
		s.log.Info("realtime route lookup", logging.String("event_id", msg.EventID), logging.String("type", msg.Type), logging.String("route_kind", "user"), logging.String("user_id", msg.UserID))
		if err := s.store.Append(ctx, domain.EventRoute{Kind: "user", ID: strings.TrimSpace(msg.UserID)}, msg.EventID, notification); err != nil {
			s.log.Error("realtime Redis append failed", logging.String("event_id", msg.EventID), logging.String("user_id", msg.UserID), logging.Err(err))
			return err
		}
		s.log.Info("realtime user notification buffered", logging.String("type", msg.Type), logging.String("operation_id", msg.OperationID))
		return nil
	}
	if strings.TrimSpace(msg.ConnectionID) != "" {
		s.log.Info("realtime route lookup", logging.String("event_id", msg.EventID), logging.String("type", msg.Type), logging.String("route_kind", "connection"), logging.String("connection_id", msg.ConnectionID))
		if err := s.store.Append(ctx, domain.EventRoute{Kind: "connection", ID: strings.TrimSpace(msg.ConnectionID)}, msg.EventID, notification); err != nil {
			s.log.Error("realtime Redis append failed", logging.String("event_id", msg.EventID), logging.String("connection_id", msg.ConnectionID), logging.Err(err))
			return err
		}
		s.log.Info("realtime connection notification buffered", logging.String("type", msg.Type))
		return nil
	}
	s.log.Warn("realtime delivery ignored", logging.String("event_id", msg.EventID), logging.String("type", msg.Type), logging.String("aggregate_id", msg.AggregateID))
	return nil
}

// WatchConnection replays pending route events and then follows new Redis entries.
func (s *service) WatchConnection(ctx context.Context, route domain.EventRoute, connectionID, lastEventID string) error {
	return s.store.Watch(ctx, route, lastEventID, func(payload []byte) error {
		s.log.Debug("realtime WebSocket send started", logging.String("route_kind", route.Kind), logging.String("route_id", route.ID), logging.String("connection_id", connectionID))
		if err := s.manager.SendToConnection(connectionID, payload); err != nil {
			s.log.Warn("realtime WebSocket send failed", logging.String("route_kind", route.Kind), logging.String("route_id", route.ID), logging.String("connection_id", connectionID), logging.Err(err))
			return err
		}
		s.log.Debug("realtime WebSocket send completed", logging.String("route_kind", route.Kind), logging.String("route_id", route.ID), logging.String("connection_id", connectionID))
		if route.Kind == "session" {
			var notification domain.Notification
			if err := json.Unmarshal(payload, &notification); err == nil && (notification.Status == "completed" || notification.Status == "failed") {
				return ErrRegistrationComplete
			}
		}
		return nil
	})
}

// MarshalConnectionReady encodes the startup-issued connection response.
func MarshalConnectionReady(connectionID string) ([]byte, error) {
	return json.Marshal(domain.ConnectionReady{
		Type:         "connection.ready",
		ConnectionID: connectionID,
	})
}
