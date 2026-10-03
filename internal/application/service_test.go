package application

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"realtime-service/internal/domain"
)

type eventStoreFake struct {
	route   domain.EventRoute
	eventID string
	payload []byte
	watch   []byte
}

func (s *eventStoreFake) Append(_ context.Context, route domain.EventRoute, eventID string, payload []byte) error {
	s.route, s.eventID, s.payload = route, eventID, append([]byte(nil), payload...)
	return nil
}

func (s *eventStoreFake) Watch(_ context.Context, _ domain.EventRoute, _ string, deliver func([]byte) error) error {
	return deliver(s.watch)
}

func (*eventStoreFake) Close() error { return nil }

type connectionFake struct{}

func (*connectionFake) Send([]byte) error      { return nil }
func (*connectionFake) ConnectionID() string   { return "connection-1" }
func (*connectionFake) UserID() string         { return "user-1" }
func (*connectionFake) ConnectedAt() time.Time { return time.Time{} }
func (*connectionFake) Close() error           { return nil }

type managerFake struct{ sent []byte }

func (m *managerFake) Add(domain.Connection)                {}
func (m *managerFake) Remove(string)                        {}
func (m *managerFake) Get(string) (domain.Connection, bool) { return nil, false }
func (m *managerFake) SendToConnection(_ string, payload []byte) error {
	m.sent = append([]byte(nil), payload...)
	return nil
}
func (m *managerFake) SendToUser(string, []byte) error           { return nil }
func (m *managerFake) ActiveUserConnections(string) int          { return 0 }
func (m *managerFake) AddRegistration(string, domain.Connection) {}
func (m *managerFake) SendToRegistration(string, []byte) bool    { return false }
func (m *managerFake) RemoveRegistration(string)                 {}

func testLogger(t *testing.T) logging.Logger {
	t.Helper()
	logger, err := logging.New("realtime-test", "test", "error")
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

func TestHandleDeliveryBuffersRegistrationNotification(t *testing.T) {
	store := &eventStoreFake{}
	svc, err := New(&managerFake{}, store, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	err = svc.HandleDelivery(context.Background(), domain.DeliveryMessage{
		EventID: "019abc00-0000-7000-8000-000000000001", Type: "registration.code_sent",
		DeliveryScope: "registration", AggregateID: "session-1", Payload: json.RawMessage(`{"code":"redacted"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.route != (domain.EventRoute{Kind: "session", ID: "session-1"}) {
		t.Fatalf("unexpected route: %+v", store.route)
	}
	if store.eventID == "" || len(store.payload) == 0 {
		t.Fatal("expected durable notification")
	}
}

func TestWatchConnectionDeliversStoredPayload(t *testing.T) {
	manager := &managerFake{}
	store := &eventStoreFake{watch: []byte(`{"event_id":"event-1"}`)}
	svc, err := New(manager, store, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.WatchConnection(context.Background(), domain.EventRoute{Kind: "user", ID: "user-1"}, "connection-1", ""); err != nil {
		t.Fatal(err)
	}
	if string(manager.sent) != string(store.watch) {
		t.Fatalf("unexpected payload: %s", manager.sent)
	}
}
