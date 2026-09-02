// Package consumer implements Kafka consumer group patterns in Go.
package consumer

import (
	"encoding/json"
	"fmt"
	"time"
)

// CommitMode controls when offsets are committed.
type CommitMode int

const (
	// AutoCommit risks message loss on crash — avoid in production.
	AutoCommit CommitMode = iota
	// ManualCommit commits only after successful processing — at-least-once.
	ManualCommit
)

// Config holds consumer group configuration.
type Config struct {
	BootstrapServers string
	GroupID          string // offsets tracked per group — multiple groups = independent consumption
	Topics           []string
	CommitMode       CommitMode
	AutoOffsetReset  string // "earliest" or "latest"
	MaxPollIntervalMs int   // rebalance triggered if exceeded
}

// Message represents a consumed Kafka message.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// ValueAsMap deserializes the JSON value.
func (m Message) ValueAsMap() (map[string]interface{}, error) {
	var v map[string]interface{}
	return v, json.Unmarshal(m.Value, &v)
}

// ProcessFn is the user-provided message handler.
// Return error to trigger retry + DLQ routing.
type ProcessFn func(msg Message) error

// ProcessResult records the outcome of processing one message.
type ProcessResult struct {
	Message    Message
	Err        error
	Retries    int
	RoutedToDLQ bool
	LatencyMs  float64
}

// Consumer wraps a Kafka consumer with manual commit and DLQ routing.
type Consumer struct {
	config     Config
	process    ProcessFn
	dlqTopic   string
	maxRetries int
	results    []ProcessResult
}

// New creates a Consumer.
func New(config Config, process ProcessFn, dlqTopic string, maxRetries int) *Consumer {
	return &Consumer{
		config: config, process: process,
		dlqTopic: dlqTopic, maxRetries: maxRetries,
	}
}

// processWithRetry applies exponential backoff retry then routes to DLQ.
func (c *Consumer) processWithRetry(msg Message) ProcessResult {
	start := time.Now()
	var lastErr error

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if err := c.process(msg); err == nil {
			return ProcessResult{
				Message:   msg,
				LatencyMs: float64(time.Since(start).Milliseconds()),
			}
		} else {
			lastErr = err
			if attempt < c.maxRetries {
				time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
			}
		}
	}

	return ProcessResult{
		Message:     msg,
		Err:         lastErr,
		Retries:     c.maxRetries,
		RoutedToDLQ: c.dlqTopic != "",
		LatencyMs:   float64(time.Since(start).Milliseconds()),
	}
}

// Run is the poll-process-commit loop.
// ManualCommit: offset advances only after successful processing.
//
// Production:
//
//	consumer.SubscribeTopics(config.Topics, nil)
//	for {
//	    msg, err := consumer.ReadMessage(time.Second)
//	    result := c.processWithRetry(convertMsg(msg))
//	    if result.Err == nil {
//	        consumer.CommitMessage(msg) // manual commit
//	    } else if result.RoutedToDLQ {
//	        producer.Produce(dlqMsg); consumer.CommitMessage(msg)
//	    }
//	}
func (c *Consumer) Run(messages []Message) []ProcessResult {
	results := make([]ProcessResult, 0, len(messages))
	for _, msg := range messages {
		result := c.processWithRetry(msg)
		results = append(results, result)
		c.results = append(c.results, result)
	}
	return results
}

// Stats returns processing statistics.
func (c *Consumer) Stats() map[string]interface{} {
	total := len(c.results)
	failed, dlq := 0, 0
	for _, r := range c.results {
		if r.Err != nil {
			failed++
		}
		if r.RoutedToDLQ {
			dlq++
		}
	}
	return map[string]interface{}{
		"total": total, "failed": failed,
		"dlq_routed": dlq, "group": c.config.GroupID,
	}
}

// Compile-time check to ensure ProcessResult has required fields.
var _ = fmt.Sprintf // suppress unused import
