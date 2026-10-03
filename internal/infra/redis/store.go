package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/redis/go-redis/v9"
	"realtime-service/config"
	"realtime-service/internal/domain"
)

const retention = 24 * time.Hour

const appendScript = `
local existing = redis.call('GET', KEYS[2])
if existing then return existing end
local id = redis.call('XADD', KEYS[1], '*',
  'event_id', ARGV[1], 'route_kind', ARGV[2], 'route_id', ARGV[3],
  'payload', ARGV[4], 'occurred_at', ARGV[5])
redis.call('SET', KEYS[2], id, 'PX', ARGV[7])
redis.call('ZADD', KEYS[3], ARGV[6], id)
redis.call('EXPIRE', KEYS[3], ARGV[8])
return id
`

// Store persists each realtime route in its own Redis Stream.
type Store struct {
	client *redis.Client
	log    logging.Logger
}

// NewStore constructs the Redis-backed realtime event store.
func NewStore(cfg config.RedisConfig, log logging.Logger) (*Store, error) {
	if log == nil {
		return nil, fmt.Errorf("realtime redis logger is nil")
	}
	return &Store{client: redis.NewClient(&redis.Options{
		Addr:         cfg.Address(),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.PoolSize / 5,
	}), log: log.With(logging.String("module", "redis-event-store"))}, nil
}

// Append atomically inserts a notification, ignoring an already-seen event_id.
func (s *Store) Append(ctx context.Context, route domain.EventRoute, eventID string, payload []byte) error {
	if strings.TrimSpace(route.Kind) == "" || strings.TrimSpace(route.ID) == "" {
		return fmt.Errorf("realtime event route is incomplete")
	}
	if strings.TrimSpace(eventID) == "" {
		return fmt.Errorf("realtime event id is empty")
	}
	now := time.Now().UTC()
	stream := streamKey(route)
	result, err := s.client.Eval(ctx, appendScript, []string{stream, eventKey(eventID), routeKey(route)}, eventID, route.Kind, route.ID, string(payload), now.Format(time.RFC3339Nano), now.UnixMilli(), retention.Milliseconds(), int64(retention/time.Second)).Result()
	if err != nil {
		return fmt.Errorf("append realtime event: %w", err)
	}
	streamID, _ := result.(string)
	s.log.Info("realtime Redis stream append completed", logging.String("event_id", eventID), logging.String("route_kind", route.Kind), logging.String("route_id", route.ID), logging.String("stream_id", streamID))
	cutoff := fmt.Sprintf("%d-0", now.Add(-retention).UnixMilli())
	if err := s.client.XTrimMinID(ctx, stream, cutoff).Err(); err != nil {
		return fmt.Errorf("trim realtime stream: %w", err)
	}
	return nil
}

// Watch replays retained route events and then blocks on the route stream.
func (s *Store) Watch(ctx context.Context, route domain.EventRoute, lastEventID string, deliver func([]byte) error) error {
	if deliver == nil {
		return fmt.Errorf("realtime event delivery callback is nil")
	}
	cursor := "0-0"
	if strings.TrimSpace(lastEventID) != "" {
		if mapped, err := s.client.Get(ctx, eventKey(lastEventID)).Result(); err == nil && mapped != "" {
			cursor = mapped
		}
	}
	stream := streamKey(route)
	boundary, err := s.latestID(ctx, stream)
	if err != nil {
		return err
	}
	ids, err := s.client.ZRange(ctx, routeKey(route), 0, -1).Result()
	if err != nil {
		return fmt.Errorf("read realtime route index: %w", err)
	}
	for _, id := range ids {
		if boundary != "" && compareStreamID(id, boundary) > 0 || compareStreamID(id, cursor) <= 0 {
			continue
		}
		payload, err := s.readExact(ctx, stream, id)
		if err != nil {
			return err
		}
		if payload != nil {
			if err := deliver(payload); err != nil {
				return err
			}
		}
		cursor = id
	}
	if boundary != "" && compareStreamID(cursor, boundary) < 0 {
		cursor = boundary
	}
	for {
		entries, err := s.client.XRead(ctx, &redis.XReadArgs{Streams: []string{stream, cursor}, Count: 100, Block: 5 * time.Second}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return fmt.Errorf("watch realtime stream: %w", err)
		}
		for _, stream := range entries {
			for _, item := range stream.Messages {
				cursor = item.ID
				payload, ok := item.Values["payload"].(string)
				if !ok {
					return fmt.Errorf("realtime stream payload %s is not text", item.ID)
				}
				if err := deliver([]byte(payload)); err != nil {
					return err
				}
			}
		}
	}
}

// Close closes the Redis client owned by the store.
func (s *Store) Close() error { return s.client.Close() }

func (s *Store) latestID(ctx context.Context, stream string) (string, error) {
	items, err := s.client.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	if err != nil && err != redis.Nil {
		return "", fmt.Errorf("read realtime stream boundary: %w", err)
	}
	if len(items) == 0 {
		return "", nil
	}
	return items[0].ID, nil
}

func (s *Store) readExact(ctx context.Context, stream, id string) ([]byte, error) {
	items, err := s.client.XRange(ctx, stream, id, id).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("read realtime event %s: %w", id, err)
	}
	if len(items) == 0 {
		return nil, nil
	}
	payload, ok := items[0].Values["payload"].(string)
	if !ok {
		return nil, fmt.Errorf("realtime event %s payload is not text", id)
	}
	return []byte(payload), nil
}

func routeKey(route domain.EventRoute) string { return "realtime:route:" + route.Kind + ":" + route.ID }

func streamKey(route domain.EventRoute) string {
	return "realtime:stream:" + route.Kind + ":" + route.ID
}

func eventKey(eventID string) string { return "realtime:event:" + eventID }

func compareStreamID(left, right string) int {
	parse := func(value string) (int64, int64) {
		parts := strings.SplitN(value, "-", 2)
		if len(parts) != 2 {
			return 0, 0
		}
		ms, _ := strconv.ParseInt(parts[0], 10, 64)
		seq, _ := strconv.ParseInt(parts[1], 10, 64)
		return ms, seq
	}
	lm, ls := parse(left)
	rm, rs := parse(right)
	if lm < rm || lm == rm && ls < rs {
		return -1
	}
	if lm > rm || lm == rm && ls > rs {
		return 1
	}
	return 0
}
