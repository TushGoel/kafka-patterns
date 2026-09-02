package producer_test

import (
	"testing"

	"github.com/TushGoel/kafka-patterns/go/producer"
)

func TestSendReturnsDeliveryReport(t *testing.T) {
	p := producer.New(producer.DefaultConfig("localhost:9092", "test-events"))
	msg := producer.Message{Key: "k1", Value: map[string]interface{}{"event": "order_placed"}}
	report, err := p.Send(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Succeeded() {
		t.Error("expected report to succeed")
	}
	if report.Topic != "test-events" {
		t.Errorf("expected topic test-events, got %s", report.Topic)
	}
}

func TestSendIncrementsOffset(t *testing.T) {
	p := producer.New(producer.DefaultConfig("localhost:9092", "events"))
	r1, _ := p.Send(producer.Message{Key: "k1", Value: map[string]interface{}{"n": 1}})
	r2, _ := p.Send(producer.Message{Key: "k2", Value: map[string]interface{}{"n": 2}})
	if r2.Offset != r1.Offset+1 {
		t.Errorf("expected offset %d, got %d", r1.Offset+1, r2.Offset)
	}
}

func TestSendBatchReturnsAllReports(t *testing.T) {
	p := producer.New(producer.DefaultConfig("localhost:9092", "events"))
	msgs := []producer.Message{
		{Key: "k1", Value: map[string]interface{}{"i": 1}},
		{Key: "k2", Value: map[string]interface{}{"i": 2}},
		{Key: "k3", Value: map[string]interface{}{"i": 3}},
	}
	reports, err := p.SendBatch(msgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reports) != 3 {
		t.Errorf("expected 3 reports, got %d", len(reports))
	}
	for _, r := range reports {
		if !r.Succeeded() {
			t.Error("expected all reports to succeed")
		}
	}
}

func TestDefaultConfigSetsProductionDefaults(t *testing.T) {
	cfg := producer.DefaultConfig("broker:9092", "my-topic")
	if cfg.Acks != "all" {
		t.Errorf("expected acks=all, got %s", cfg.Acks)
	}
	if !cfg.Idempotent {
		t.Error("expected idempotence=true for production safety")
	}
	if cfg.Topic != "my-topic" {
		t.Errorf("expected topic my-topic, got %s", cfg.Topic)
	}
}

func TestMessageSerializesValue(t *testing.T) {
	msg := producer.Message{
		Key:   "order-1",
		Value: map[string]interface{}{"amount": 99.99},
	}
	serialized, err := msg.Serialize()
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}
	if len(serialized) == 0 {
		t.Error("expected non-empty serialized bytes")
	}
}

func TestStatsAfterSend(t *testing.T) {
	p := producer.New(producer.DefaultConfig("localhost:9092", "events"))
	for i := 0; i < 5; i++ {
		p.Send(producer.Message{Key: "k", Value: map[string]interface{}{"i": i}})
	}
	stats := p.Stats()
	if stats["total"] != 5 {
		t.Errorf("expected total=5, got %v", stats["total"])
	}
	if stats["failed"] != 0 {
		t.Errorf("expected failed=0, got %v", stats["failed"])
	}
}
