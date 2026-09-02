"""
Kafka consumer group patterns.

Key production concerns:
  - Manual offset commit (not auto) — control exactly-once processing
  - At-least-once vs exactly-once semantics
  - Graceful shutdown without losing unprocessed messages
  - Consumer group rebalancing
  - Dead letter queue routing for failed messages
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class CommitMode(str, Enum):
    AUTO = "auto"          # risk: message lost if crash before processing
    MANUAL = "manual"      # safe: commit only after successful processing
    TRANSACTIONAL = "tx"   # exactly-once: atomic process + commit


@dataclass
class ConsumerConfig:
    bootstrap_servers: str
    group_id: str              # consumer group — offsets tracked per group
    topics: list[str]
    commit_mode: CommitMode = CommitMode.MANUAL
    auto_offset_reset: str = "earliest"   # start from beginning on new group
    max_poll_interval_ms: int = 300_000   # max time between polls before rebalance
    session_timeout_ms: int = 45_000


@dataclass
class ConsumedMessage:
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, topic: str, partition: int, offset: int,
                   key: Optional[bytes], value: bytes) -> "ConsumedMessage":
        return cls(
            topic=topic, partition=partition, offset=offset,
            key=key.decode("utf-8") if key else None,
            value=json.loads(value),
        )


@dataclass
class ProcessResult:
    message: ConsumedMessage
    success: bool
    error: Optional[str] = None
    routed_to_dlq: bool = False


class ConsumerGroup:
    """
    Kafka consumer group with manual offset commit and DLQ routing.

    Manual commit ensures: if processing fails, offset is NOT committed,
    so the message is reprocessed on next poll. This gives at-least-once
    semantics — combine with idempotent processing for exactly-once.

    Dead letter queue: messages that fail after max_retries are routed
    to a DLQ topic instead of blocking the partition indefinitely.

    Usage:
        def process(msg: ConsumedMessage) -> bool:
            return handle_event(msg.value)

        consumer = ConsumerGroup(config, processor=process)
        consumer.run(max_messages=1000)
    """

    def __init__(self, config: ConsumerConfig,
                 processor: Callable[[ConsumedMessage], bool],
                 dlq_topic: Optional[str] = None,
                 max_retries: int = 3) -> None:
        self.config = config
        self._processor = processor
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries
        self._processed: list[ProcessResult] = []
        self._running = False

    def _process_with_retry(self, msg: ConsumedMessage) -> ProcessResult:
        """Process with exponential backoff retry. Route to DLQ after exhaustion."""
        import time
        last_error = None
        for attempt in range(1, self._max_retries + 1):
            try:
                success = self._processor(msg)
                if success:
                    return ProcessResult(message=msg, success=True)
            except Exception as e:
                last_error = str(e)
                if attempt < self._max_retries:
                    time.sleep(0.1 * (2 ** attempt))  # exponential backoff

        # Route to DLQ — don't block the partition
        return ProcessResult(
            message=msg, success=False,
            error=last_error, routed_to_dlq=bool(self._dlq_topic),
        )

    def run(self, max_messages: int = -1) -> list[ProcessResult]:
        """
        Poll → process → commit loop.
        Manual commit: offset advanced only after successful processing.
        """
        # Production:
        # consumer = confluent_kafka.Consumer({
        #     "bootstrap.servers": self.config.bootstrap_servers,
        #     "group.id": self.config.group_id,
        #     "enable.auto.commit": self.config.commit_mode == CommitMode.AUTO,
        #     "auto.offset.reset": self.config.auto_offset_reset,
        # })
        # consumer.subscribe(self.config.topics)
        # while self._running:
        #     msg = consumer.poll(timeout=1.0)
        #     if msg is None or msg.error(): continue
        #     result = self._process_with_retry(ConsumedMessage.from_bytes(...))
        #     if result.success and self.config.commit_mode == CommitMode.MANUAL:
        #         consumer.commit(msg)  # only commit after processing

        return self._processed

    def stop(self) -> None:
        """Graceful shutdown — finish current message before stopping."""
        self._running = False

    def stats(self) -> dict:
        total = len(self._processed)
        dlq = sum(1 for r in self._processed if r.routed_to_dlq)
        return {
            "total": total,
            "succeeded": sum(1 for r in self._processed if r.success),
            "failed": sum(1 for r in self._processed if not r.success),
            "dlq_routed": dlq,
        }
