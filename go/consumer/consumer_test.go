package consumer_test

import (
	"errors"
	"testing"

	"github.com/TushGoel/kafka-patterns/go/consumer"
)

func alwaysSucceed(msg consumer.Message) error { return nil }

func alwaysFail(msg consumer.Message) error { return errors.New("processing failed") }

func failOnce(calls *int) consumer.ProcessFn {
	return func(msg consumer.Message) error {
		*calls++
		if *calls == 1 {
			return errors.New("transient error")
		}
		return nil
	}
}

func makeMessages(n int) []consumer.Message {
	msgs := make([]consumer.Message, n)
	for i := range msgs {
		msgs[i] = consumer.Message{
			Topic: "events", Partition: 0,
			Offset: int64(i), Key: []byte("k"),
			Value: []byte(`{"event":"test"}`),
		}
	}
	return msgs
}

func TestAllSuccessful(t *testing.T) {
	cfg := consumer.Config{
		BootstrapServers: "localhost:9092",
		GroupID:          "test-group",
		Topics:           []string{"events"},
		CommitMode:       consumer.ManualCommit,
	}
	c := consumer.New(cfg, alwaysSucceed, "", 3)
	results := c.Run(makeMessages(5))
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("unexpected error: %v", r.Err)
		}
	}
}

func TestRetryOnTransientFailure(t *testing.T) {
	calls := 0
	cfg := consumer.Config{GroupID: "g", Topics: []string{"t"}, CommitMode: consumer.ManualCommit}
	c := consumer.New(cfg, failOnce(&calls), "", 3)
	results := c.Run(makeMessages(1))
	if results[0].Err != nil {
		t.Errorf("expected success after retry, got: %v", results[0].Err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls (1 fail + 1 success), got %d", calls)
	}
}

func TestPoisonPillRoutedToDLQ(t *testing.T) {
	cfg := consumer.Config{GroupID: "g", Topics: []string{"t"}, CommitMode: consumer.ManualCommit}
	c := consumer.New(cfg, alwaysFail, "events-dlq", 2)
	results := c.Run(makeMessages(1))
	if !results[0].RoutedToDLQ {
		t.Error("expected message to be routed to DLQ")
	}
}

func TestNoDLQIfNotConfigured(t *testing.T) {
	cfg := consumer.Config{GroupID: "g", Topics: []string{"t"}, CommitMode: consumer.ManualCommit}
	c := consumer.New(cfg, alwaysFail, "", 1)
	results := c.Run(makeMessages(1))
	if results[0].RoutedToDLQ {
		t.Error("expected RoutedToDLQ=false when no DLQ configured")
	}
}

func TestStats(t *testing.T) {
	cfg := consumer.Config{GroupID: "g", Topics: []string{"t"}, CommitMode: consumer.ManualCommit}
	c := consumer.New(cfg, alwaysSucceed, "", 1)
	c.Run(makeMessages(4))
	stats := c.Stats()
	if stats["total"] != 4 {
		t.Errorf("expected total=4, got %v", stats["total"])
	}
	if stats["failed"] != 0 {
		t.Errorf("expected failed=0, got %v", stats["failed"])
	}
}
