"""Tests for Kafka consumer group patterns."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from python.patterns.consumer import ConsumerConfig, ConsumerGroup, ConsumedMessage, CommitMode


def _config():
    return ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topics=["events"],
    )


def _message(offset=0):
    return ConsumedMessage(
        topic="events", partition=0, offset=offset,
        key="k1", value={"event": "test", "id": offset},
    )


def test_successful_processing():
    processed = []
    group = ConsumerGroup(_config(), processor=lambda m: processed.append(m) or True)
    group.run()
    # No messages injected in mock — just verifying run doesn't raise
    assert group.stats()["total"] == 0


def test_stats_structure():
    group = ConsumerGroup(_config(), processor=lambda m: True)
    stats = group.stats()
    assert "total" in stats
    assert "succeeded" in stats
    assert "failed" in stats
    assert "dlq_routed" in stats


def test_process_with_retry_succeeds_on_first():
    calls = [0]
    def proc(m):
        calls[0] += 1
        return True
    group = ConsumerGroup(_config(), processor=proc)
    result = group._process_with_retry(_message(0))
    assert result.success
    assert calls[0] == 1


def test_process_with_retry_retries_on_failure():
    calls = [0]
    def proc(m):
        calls[0] += 1
        if calls[0] < 3:
            raise ValueError("transient error")
        return True
    group = ConsumerGroup(_config(), processor=proc, max_retries=3)
    result = group._process_with_retry(_message(0))
    assert result.success
    assert calls[0] == 3


def test_process_routes_to_dlq_after_exhaustion():
    def always_fail(m):
        raise RuntimeError("poison pill")
    group = ConsumerGroup(
        _config(), processor=always_fail,
        dlq_topic="events-dlq", max_retries=2
    )
    result = group._process_with_retry(_message(0))
    assert not result.success
    assert result.routed_to_dlq


def test_process_no_dlq_configured():
    def always_fail(m):
        raise RuntimeError("error")
    group = ConsumerGroup(_config(), processor=always_fail, max_retries=1)
    result = group._process_with_retry(_message(0))
    assert not result.success
    assert not result.routed_to_dlq


def test_consumed_message_from_bytes():
    msg = ConsumedMessage.from_bytes(
        topic="test", partition=0, offset=5,
        key=b"order-1", value=b'{"amount": 100}',
    )
    assert msg.key == "order-1"
    assert msg.value["amount"] == 100
    assert msg.offset == 5
