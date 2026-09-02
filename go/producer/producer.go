// Package producer implements reliable Kafka producer patterns in Go.
// Uses the confluent-kafka-go client — the same client used by Confluent Cloud.
package producer

import (
	"encoding/json"
	"fmt"
	"time"
)

// Config holds producer configuration.
type Config struct {
	BootstrapServers string
	Topic            string
	Acks             string // "all" for no data loss
	Idempotent       bool   // exactly-once per partition
	CompressionType  string // snappy, lz4, zstd
}

// DefaultConfig returns production-safe defaults.
func DefaultConfig(servers, topic string) Config {
	return Config{
		BootstrapServers: servers,
		Topic:            topic,
		Acks:             "all",
		Idempotent:       true,
		CompressionType:  "snappy",
	}
}

// Message is a Kafka message with typed key and value.
type Message struct {
	Key     string
	Value   map[string]interface{}
	Headers map[string]string
}

// Serialize converts the value to JSON bytes.
func (m Message) Serialize() ([]byte, error) {
	return json.Marshal(m.Value)
}

// DeliveryReport is returned after a message is confirmed delivered.
type DeliveryReport struct {
	Topic     string
	Partition int32
	Offset    int64
	LatencyMs float64
	Err       error
}

func (d DeliveryReport) Succeeded() bool { return d.Err == nil }

// Producer wraps a Kafka producer with delivery confirmation.
type Producer struct {
	config Config
	sent   []DeliveryReport
}

// New creates a new Producer.
// Production: replace mock with confluent_kafka.NewProducer(configMap)
func New(config Config) *Producer {
	return &Producer{config: config}
}

// Send produces one message and blocks until delivery confirmed.
//
// Production implementation:
//
//	p.producer.Produce(&kafka.Message{
//	    TopicPartition: kafka.TopicPartition{Topic: &config.Topic, Partition: kafka.PartitionAny},
//	    Key:   []byte(msg.Key),
//	    Value: serialized,
//	}, deliveryChan)
//	e := <-deliveryChan
func (p *Producer) Send(msg Message) (DeliveryReport, error) {
	start := time.Now()
	serialized, err := msg.Serialize()
	if err != nil {
		return DeliveryReport{Err: err}, fmt.Errorf("serialize: %w", err)
	}
	_ = serialized // production: pass to confluent producer

	report := DeliveryReport{
		Topic:     p.config.Topic,
		Partition: 0,
		Offset:    int64(len(p.sent)),
		LatencyMs: float64(time.Since(start).Milliseconds()),
	}
	p.sent = append(p.sent, report)
	return report, nil
}

// SendBatch produces all messages before flushing — higher throughput than
// sending one-by-one. Production: call p.producer.Flush(30_000) after loop.
func (p *Producer) SendBatch(msgs []Message) ([]DeliveryReport, error) {
	reports := make([]DeliveryReport, 0, len(msgs))
	for _, msg := range msgs {
		r, err := p.Send(msg)
		if err != nil {
			return reports, err
		}
		reports = append(reports, r)
	}
	return reports, nil
}

// Stats returns delivery statistics.
func (p *Producer) Stats() map[string]interface{} {
	failed := 0
	for _, r := range p.sent {
		if !r.Succeeded() {
			failed++
		}
	}
	return map[string]interface{}{
		"total":    len(p.sent),
		"failed":   failed,
		"topic":    p.config.Topic,
	}
}
