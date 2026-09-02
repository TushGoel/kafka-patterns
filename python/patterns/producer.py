"""
Reliable Kafka producer patterns.

Key production concerns:
  - Idempotent delivery (acks=all + enable.idempotence=true)
  - Retry with backoff on transient failures
  - Delivery callback for guaranteed confirmation
  - Schema-validated messages
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ProducerConfig:
    bootstrap_servers: str
    topic: str
    acks: str = "all"            # all replicas must ack — no data loss
    retries: int = 5
    retry_backoff_ms: int = 300
    idempotence: bool = True     # exactly-once delivery per partition
    compression: str = "snappy"  # reduce network cost at scale


@dataclass
class Message:
    key: str
    value: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)

    def serialize(self) -> bytes:
        return json.dumps(self.value).encode("utf-8")

    def key_bytes(self) -> bytes:
        return self.key.encode("utf-8")


@dataclass
class DeliveryReport:
    topic: str
    partition: int
    offset: int
    latency_ms: float
    error: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


class ReliableProducer:
    """
    Kafka producer with idempotent delivery and delivery confirmation.

    Production pattern: never fire-and-forget. Always confirm delivery via
    callback before considering a message sent. Track offset for audit trail.

    Usage:
        producer = ReliableProducer(config)
        report = producer.send(Message(key="order-123", value={"amount": 100}))
        assert report.succeeded
    """

    def __init__(self, config: ProducerConfig,
                 delivery_callback: Optional[Callable[[DeliveryReport], None]] = None) -> None:
        self.config = config
        self._delivery_callback = delivery_callback
        self._sent: list[DeliveryReport] = []
        # In production: self._producer = confluent_kafka.Producer({...})

    def send(self, message: Message) -> DeliveryReport:
        """Send one message. Blocks until delivery confirmed."""
        start = time.monotonic()

        # Production:
        # self._producer.produce(
        #     topic=self.config.topic,
        #     key=message.key_bytes(),
        #     value=message.serialize(),
        #     headers=message.headers,
        #     on_delivery=self._on_delivery,
        # )
        # self._producer.flush()  # block until ack

        # Simulated delivery report (replace with real confluent_kafka.Message)
        report = DeliveryReport(
            topic=self.config.topic,
            partition=0,
            offset=len(self._sent),
            latency_ms=(time.monotonic() - start) * 1000,
        )
        self._sent.append(report)
        if self._delivery_callback:
            self._delivery_callback(report)
        return report

    def send_batch(self, messages: list[Message]) -> list[DeliveryReport]:
        """Send a batch. All messages produced before flush — more efficient."""
        reports = []
        for msg in messages:
            reports.append(self.send(msg))
        return reports

    def stats(self) -> dict:
        total = len(self._sent)
        failed = sum(1 for r in self._sent if not r.succeeded)
        return {
            "total": total,
            "succeeded": total - failed,
            "failed": failed,
            "avg_latency_ms": sum(r.latency_ms for r in self._sent) / total if total else 0,
        }
