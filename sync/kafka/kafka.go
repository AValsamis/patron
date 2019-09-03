package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/beatlabs/patron/encoding/json"
	"github.com/beatlabs/patron/errors"
	"github.com/beatlabs/patron/trace"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

// Message abstraction of a Kafka message.
type Message struct {
	topic string
	body  []byte
}

// NewMessage creates a new message.
func NewMessage(t string, b []byte) *Message {
	return &Message{topic: t, body: b}
}

// NewJSONMessage creates a new message with a JSON encoded body.
func NewJSONMessage(t string, d interface{}) (*Message, error) {
	b, err := json.Encode(d)
	if err != nil {
		return nil, errors.Wrap(err, "failed to JSON encode")
	}
	return &Message{topic: t, body: b}, nil
}

// Producer interface for Kafka.
type Producer interface {
	Send(ctx context.Context, msg *Message) (int32, int64, error)
	Close() error
}

// SyncProducer defines a async Kafka producer.
type SyncProducer struct {
	cfg  *sarama.Config
	prod sarama.SyncProducer
	tag  opentracing.Tag
}

// NewSyncProducer creates a new sync producer with default configuration.
// Default KafkaVersion is 11.0.0, and can be changed through OptionFunc. Bear
// in mind that in lower versions producing headers are not supported, and we do
// not propagate tracing context.
func NewSyncProducer(brokers []string, oo ...OptionFunc) (*SyncProducer, error) {

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_11_0_0
	cfg.Producer.Return.Successes = true

	sp := SyncProducer{cfg: cfg, tag: opentracing.Tag{Key: "type", Value: "sync"}}

	for _, o := range oo {
		err := o(&sp)
		if err != nil {
			return nil, err
		}
	}

	prod, err := sarama.NewSyncProducer(brokers, sp.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sync producer")
	}
	sp.prod = prod
	return &sp, nil
}

// Send a message to a topic.
func (sp *SyncProducer) Send(ctx context.Context, msg *Message) (partition int32, offset int64, err error) {
	ts, _ := trace.ChildSpan(
		ctx,
		trace.ComponentOpName(trace.KafkaSyncProducerComponent, msg.topic),
		trace.KafkaSyncProducerComponent,
		ext.SpanKindProducer,
		sp.tag,
		opentracing.Tag{Key: "topic", Value: msg.topic},
	)
	pm, err := createProducerMessage(msg, ts)
	if err != nil {
		trace.SpanError(ts)
		return 0, 0, err
	}

	// Producing headers requires at least version 0.11 of kafka. Thus, in lower
	// versions tracing context propagation is not supported
	if pm.Headers != nil && !sp.cfg.Version.IsAtLeast(sarama.V0_11_0_0) {
		pm.Headers = nil
	}

	return sp.prod.SendMessage(pm)
}

// Close gracefully the producer.
func (sp *SyncProducer) Close() error {
	return errors.Wrap(sp.prod.Close(), "failed to close sync producer")
}

func createProducerMessage(msg *Message, sp opentracing.Span) (*sarama.ProducerMessage, error) {
	c := kafkaHeadersCarrier{}
	err := sp.Tracer().Inject(sp.Context(), opentracing.TextMap, &c)
	if err != nil {
		return nil, errors.Wrap(err, "failed to inject tracing headers")
	}
	return &sarama.ProducerMessage{
		Topic:   msg.topic,
		Key:     nil,
		Value:   sarama.ByteEncoder(msg.body),
		Headers: c,
	}, nil
}

type kafkaHeadersCarrier []sarama.RecordHeader

// Set implements Set() of opentracing.TextMapWriter.
func (c *kafkaHeadersCarrier) Set(key, val string) {
	*c = append(*c, sarama.RecordHeader{Key: []byte(key), Value: []byte(val)})
}
