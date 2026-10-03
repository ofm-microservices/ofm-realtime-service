# OFM Realtime Service

## Purpose

The Realtime Service consumes user-facing Kafka events and delivers matching notifications over WebSocket connections. It owns connection lifecycle and delivery fan-out, not the business entities that produced the events. Status: active local service.

## Flow and boundaries

A service commits its business change and publishes an event through CDC/Kafka. Realtime consumes the event, resolves the connected user/session, and sends a WebSocket notification. Kafka consumer-group and partition handling must keep required events available; delivery is notification behavior, not the source of truth for business state.

## Configuration

.env.example groups are Kafka brokers/topics/consumer groups, Redis connection and event/session state, WebSocket/HTTP listeners, JWT validation, and OpenTelemetry. Kafka values select event consumption; Redis values select connection/session coordination; JWT values control client authorization.

## Local development

    cp .env.example .env
    go run ./cmd/realtime-service
    go test ./...

## Build and operations

Dockerfile builds ofm/realtime-service:<tag>. ofm-infra deploys the service and provides Kafka, Redis, ingress, and observability. Diagnose Kafka partition lag, consumer assignment, Redis session state, WebSocket lifecycle, and propagated trace/run identifiers.

