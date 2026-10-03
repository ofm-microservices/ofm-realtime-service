FROM golang:1.25.11 AS builder

WORKDIR /src

COPY ofm-common /src/ofm-common
COPY ofm-realtime-service /src/ofm-realtime-service

WORKDIR /src/ofm-realtime-service

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=mod -o /out/realtime-service ./cmd/realtime-service

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app \
    && chown app:app /app \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/realtime-service /app/realtime-service

RUN chown -R app:app /app

USER app:app

EXPOSE 8082

CMD ["/app/realtime-service"]
