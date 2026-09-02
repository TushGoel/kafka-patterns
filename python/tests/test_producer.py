"""Tests for Kafka producer patterns."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from python.patterns.producer import ProducerConfig, Message, ReliableProducer


def _producer():
    config = ProducerConfig(bootstrap_servers="localhost:9092", topic="test-events")
    return ReliableProducer(config)


def test_send_returns_delivery_report():
    p = _producer()
    msg = Message(key="k1", value={"event": "order_placed", "id": 1})
    report = p.send(msg)
    assert report.succeeded
    assert report.topic == "test-events"
    assert report.offset == 0


def test_send_increments_offset():
    p = _producer()
    r1 = p.send(Message(key="k1", value={"n": 1}))
    r2 = p.send(Message(key="k2", value={"n": 2}))
    assert r2.offset == r1.offset + 1


def test_send_batch_returns_all_reports():
    p = _producer()
    msgs = [Message(key=f"k{i}", value={"i": i}) for i in range(5)]
    reports = p.send_batch(msgs)
    assert len(reports) == 5
    assert all(r.succeeded for r in reports)


def test_delivery_callback_called():
    callbacks = []
    config = ProducerConfig(bootstrap_servers="localhost:9092", topic="events")
    p = ReliableProducer(config, delivery_callback=callbacks.append)
    p.send(Message(key="k", value={"x": 1}))
    assert len(callbacks) == 1
    assert callbacks[0].succeeded


def test_stats_counts_sent():
    p = _producer()
    for i in range(3):
        p.send(Message(key=str(i), value={"i": i}))
    stats = p.stats()
    assert stats["total"] == 3
    assert stats["succeeded"] == 3
    assert stats["failed"] == 0


def test_message_serializes_to_json():
    msg = Message(key="order-1", value={"amount": 99.99, "currency": "USD"})
    serialized = msg.serialize()
    assert b"amount" in serialized
    assert b"USD" in serialized


def test_message_key_bytes():
    msg = Message(key="partition-key-123", value={})
    assert msg.key_bytes() == b"partition-key-123"
