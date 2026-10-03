package domain

import "testing"

func TestIsClientNotification(t *testing.T) {
	for _, eventType := range []string{
		"chat_message_created",
		"gig.published",
		"review.accepted",
		"file.ready",
	} {
		if !IsClientNotification(eventType) {
			t.Fatalf("expected %q to be client-visible", eventType)
		}
	}
	for _, eventType := range []string{
		"migration.recovery.completed",
		"migration.recovery.failed",
		"gig.projection.requested",
		"debezium.cdc",
	} {
		if IsClientNotification(eventType) {
			t.Fatalf("expected internal event %q to be blocked", eventType)
		}
	}
}
