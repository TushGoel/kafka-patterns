"""Tests for OpenTelemetry tracing on producer/consumer patterns.

Uses OpenTelemetry's InMemorySpanExporter — the standard way to assert on
span content in unit tests without a real collector.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from python.patterns.producer import ProducerConfig, Message, ReliableProducer
from python.patterns.consumer import ConsumerConfig, ConsumerGroup, ConsumedMessage

# One TracerProvider + in-memory exporter for the whole test module. The
# OpenTelemetry API only allows the global TracerProvider to be set once per
# process, so we set it here and reset the exporter between tests instead of
# re-creating it per test.
_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture(autouse=True)
def _reset_spans():
    _exporter.clear()
    yield
    _exporter.clear()


def _producer(topic="orders"):
    return ReliableProducer(ProducerConfig(bootstrap_servers="localhost:9092", topic=topic))


def _consumer_group(processor, max_retries=3, dlq_topic=None):
    config = ConsumerConfig(bootstrap_servers="localhost:9092", group_id="test-group", topics=["orders"])
    return ConsumerGroup(config, processor=processor, dlq_topic=dlq_topic, max_retries=max_retries)


# ── Producer spans ────────────────────────────────────────────────────────────

def test_produce_creates_one_span_per_send():
    producer = _producer()
    producer.send(Message(key="order-1", value={"amount": 10}))
    producer.send(Message(key="order-2", value={"amount": 20}))
    assert len(_exporter.get_finished_spans()) == 2


def test_produce_span_has_topic_partition_offset_key():
    producer = _producer(topic="orders")
    report = producer.send(Message(key="order-123", value={"amount": 99.99}))

    spans = _exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "kafka.produce"
    assert span.attributes["messaging.system"] == "kafka"
    assert span.attributes["messaging.destination.name"] == "orders"
    assert span.attributes["messaging.kafka.message_key"] == "order-123"
    assert span.attributes["messaging.kafka.partition"] == report.partition
    assert span.attributes["messaging.kafka.offset"] == report.offset


def test_produce_span_never_includes_message_value():
    """Payload content (e.g. PII) must never end up in span attributes."""
    producer = _producer()
    producer.send(Message(key="order-1", value={"ssn": "123-45-6789", "note": "sensitive"}))

    span = _exporter.get_finished_spans()[0]
    for value in span.attributes.values():
        assert "123-45-6789" not in str(value)
        assert "sensitive" not in str(value)


# ── Consumer spans ────────────────────────────────────────────────────────────

def test_consume_span_has_topic_partition_offset_key():
    group = _consumer_group(processor=lambda m: True)
    msg = ConsumedMessage(topic="orders", partition=2, offset=42, key="order-9", value={"x": 1})
    group._process_with_retry(msg)

    spans = _exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "kafka.consume"
    assert span.attributes["messaging.destination.name"] == "orders"
    assert span.attributes["messaging.kafka.partition"] == 2
    assert span.attributes["messaging.kafka.offset"] == 42
    assert span.attributes["messaging.kafka.message_key"] == "order-9"


def test_consume_span_never_includes_message_value():
    group = _consumer_group(processor=lambda m: True)
    msg = ConsumedMessage(topic="orders", partition=0, offset=1, key="k1", value={"ssn": "123-45-6789"})
    group._process_with_retry(msg)

    span = _exporter.get_finished_spans()[0]
    for value in span.attributes.values():
        assert "123-45-6789" not in str(value)


def test_consume_span_records_retry_count_on_success():
    calls = [0]

    def flaky(m):
        calls[0] += 1
        if calls[0] < 2:
            raise ValueError("transient")
        return True

    group = _consumer_group(processor=flaky, max_retries=3)
    msg = ConsumedMessage(topic="orders", partition=0, offset=1, key="k1", value={})
    group._process_with_retry(msg)

    span = _exporter.get_finished_spans()[0]
    assert span.attributes["messaging.kafka.retry_count"] == 1


def test_consume_span_error_status_after_dlq_routing():
    def always_fail(m):
        raise RuntimeError("poison pill")

    group = _consumer_group(processor=always_fail, max_retries=2, dlq_topic="orders-dlq")
    msg = ConsumedMessage(topic="orders", partition=0, offset=1, key="k1", value={})
    group._process_with_retry(msg)

    span = _exporter.get_finished_spans()[0]
    assert span.status.status_code == trace.StatusCode.ERROR
    assert span.attributes["messaging.kafka.routed_to_dlq"] is True
