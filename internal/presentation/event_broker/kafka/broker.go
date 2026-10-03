package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
	"realtime-service/config"
	eb "realtime-service/internal/presentation/event_broker"
)

type broker struct {
	brokers           []string
	group, deadLetter string
	chatEventsTopic   string
	log               logging.Logger
	mu                sync.Mutex
	readers           []*kafka.Reader
}

// NewBroker constructs the Kafka delivery adapter used by realtime-service.
func NewBroker(cfg config.KafkaConfig, log logging.Logger) (eb.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	if log == nil {
		return nil, errors.New("logger is nil")
	}
	group := cfg.GroupID
	return &broker{brokers: cfg.Brokers, group: group, deadLetter: group + ".dead-letter", chatEventsTopic: cfg.ChatEventsTopic, log: log.With(logging.String("module", "kafka-broker"))}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, BatchSize: 100, BatchTimeout: 50 * time.Millisecond}
	defer w.Close()
	kafkaprop.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Value: enveloped, Headers: kafkaHeaders(ctx)})
}

func kafkaHeaders(ctx context.Context) []kafka.Header {
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eb.MessageHandler) error {
	group := b.group + "-" + strings.ReplaceAll(subject, ".", "-")
	go func() {
		if err := (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: group, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			b.log.Error("kafka retry queue stopped", logging.String("topic", resilience.RetryTopic(group)), logging.Err(err))
		}
	}()

	// One kafka-go reader is one stable consumer-group member. Kafka assigns
	// all partitions for this topic to the member and keeps the assignment
	// stable. Creating one reader per partition caused a rebalance for every
	// reader and could leave a partition without fetch progress.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  b.brokers,
		Topic:    subject,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  50 * time.Millisecond,
	})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := b.handleMessage(ctx, subject, handler, msg); err != nil {
			return err
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// Replay reads retained events from the beginning without committing offsets.
func (b *broker) Replay(ctx context.Context, subject string, handler eb.MessageHandler) error {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: subject, MinBytes: 1, MaxBytes: 10e6, StartOffset: kafka.FirstOffset})
	defer r.Close()
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		message, err := r.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		payload, _, unwrapErr := commonevents.Unwrap(message.Value)
		if unwrapErr != nil {
			return unwrapErr
		}
		if err := handler(ctx, subject, payload); err != nil {
			return err
		}
	}
}

func (b *broker) handleMessage(ctx context.Context, subject string, handler eb.MessageHandler, msg kafka.Message) error {
	attempts := retryAttempt(msg.Headers)
	payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
	if unwrapErr != nil {
		return b.deadLetterMessage(ctx, subject, msg, attempts, unwrapErr)
	}
	// Migration bridge output is already a canonical event envelope. Realtime
	// needs its identity and operation fields to turn a created chat row into a
	// user notification, so do not strip that envelope on the Chat CDC topic.
	if subject == b.chatEventsTopic {
		payload = msg.Value
	}
	b.log.Info("kafka event consumed", logging.String("topic", subject), logging.Int("partition", msg.Partition), logging.Int64("offset", msg.Offset), logging.Int("attempt", attempts))
	err := handler(kafkaprop.Context(ctx, msg.Headers), subject, payload)
	if err == nil {
		b.log.Info("kafka event handled", logging.String("topic", subject), logging.Int("partition", msg.Partition), logging.Int64("offset", msg.Offset), logging.Int("attempts", attempts))
		return nil
	}
	var permanent resilience.PermanentError
	if errors.As(err, &permanent) || attempts >= resilience.DefaultRetryPolicy.MaxAttempts {
		return b.deadLetterMessage(ctx, subject, msg, attempts, err)
	}
	group := b.group + "-" + strings.ReplaceAll(subject, ".", "-")
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(group), WriteTimeout: 5 * time.Second}
	queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(ctx, msg, subject, attempts+1, err)
	_ = writer.Close()
	if queueErr != nil {
		return queueErr
	}
	b.log.Warn("kafka event moved to retry queue", logging.String("topic", subject), logging.String("retry_topic", resilience.RetryTopic(group)), logging.Int("attempt", attempts+1), logging.Err(err))
	return nil
}

func retryAttempt(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key == "x-ofm-retry-attempt" {
			if attempt, err := strconv.Atoi(string(header.Value)); err == nil && attempt > 0 {
				return attempt
			}
		}
	}
	return 1
}

func (b *broker) deadLetterMessage(ctx context.Context, subject string, msg kafka.Message, attempts int, cause error) error {
	b.log.Error("kafka event moved to dead-letter", logging.String("topic", subject), logging.Int("partition", msg.Partition), logging.Int64("offset", msg.Offset), logging.Int("attempts", attempts), logging.Err(cause))
	payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", cause), Error: cause.Error(), FailedAt: time.Now().UTC()})
	if marshalErr != nil {
		return marshalErr
	}
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
	cancel()
	_ = writer.Close()
	if dlqErr != nil {
		return dlqErr
	}
	sharedmetrics.IncKafkaDLQ(b.deadLetter)
	return nil
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
