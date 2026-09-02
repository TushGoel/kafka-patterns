"""
LLM inference event streaming.

Every LLM call in production generates telemetry: model, provider, tokens,
latency, cost, caller. At scale — millions of calls/day — this data must be
streamed, not batch-loaded. Kafka is the backbone.

This pattern is used at companies running LLM infrastructure at scale:
  - Stream every model invocation for real-time cost tracking
  - Feed latency events to SLO monitors (p95 exceeded → page)
  - Replay events for debugging failed agent runs
  - Power feedback loops: high-latency calls → re-route to faster provider

Schema is registered in Schema Registry — breaking changes rejected at produce
time, not discovered at 3am when a consumer crashes.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from .schema_registry import SchemaRegistry, CompatibilityMode
from .producer import ProducerConfig, Message, ReliableProducer

# ── LLM invocation event schema ───────────────────────────────────────────────

LLM_INVOCATION_SCHEMA = {
    "type": "object",
    "required": ["event_id", "model", "provider", "input_tokens",
                 "output_tokens", "latency_ms", "caller_id", "timestamp_ms"],
    "properties": {
        "event_id":      {"type": "string"},   # UUID per invocation
        "model":         {"type": "string"},   # claude-3-5-sonnet, gpt-4o, etc.
        "provider":      {"type": "string"},   # bedrock, openai, anthropic
        "input_tokens":  {"type": "integer"},
        "output_tokens": {"type": "integer"},
        "latency_ms":    {"type": "number"},
        "cost_usd":      {"type": "number"},
        "caller_id":     {"type": "string"},   # agent, service, or user ID
        "success":       {"type": "boolean"},
        "error":         {"type": "string"},   # optional
        "timestamp_ms":  {"type": "integer"},  # unix ms
        "trace_id":      {"type": "string"},   # links to agent execution span
    },
}

LLM_TOPIC = "llm-invocations"
SUBJECT = f"{LLM_TOPIC}-value"


@dataclass
class LLMInvocationEvent:
    """Structured event for one LLM API call."""
    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    caller_id: str
    cost_usd: float = 0.0
    success: bool = True
    error: Optional[str] = None
    trace_id: Optional[str] = None
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "model": self.model,
            "provider": self.provider,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "latency_ms": self.latency_ms,
            "cost_usd": self.cost_usd,
            "caller_id": self.caller_id,
            "success": self.success,
            "error": self.error or "",
            "timestamp_ms": self.timestamp_ms,
            "trace_id": self.trace_id or "",
        }


class LLMEventStream:
    """
    Streams LLM invocation telemetry to Kafka.

    Used by LLM gateways to publish every model call — enabling real-time
    cost dashboards, latency SLOs, and per-caller spend enforcement
    downstream.

    Usage:
        stream = LLMEventStream(bootstrap_servers="localhost:9092")

        # After every LLM call
        event = LLMInvocationEvent(
            model="claude-3-5-sonnet",
            provider="bedrock",
            input_tokens=850,
            output_tokens=320,
            latency_ms=1240,
            caller_id="oncall-triage-agent",
            cost_usd=0.0035,
            trace_id="span-abc123",
        )
        stream.publish(event)
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self._registry = SchemaRegistry()
        self._registry.register(SUBJECT, LLM_INVOCATION_SCHEMA, CompatibilityMode.BACKWARD)

        config = ProducerConfig(
            bootstrap_servers=bootstrap_servers,
            topic=LLM_TOPIC,
            acks="all",
            idempotence=True,
        )
        self._producer = ReliableProducer(config)
        self._published: list[LLMInvocationEvent] = []

    def publish(self, event: LLMInvocationEvent) -> bool:
        """Publish one LLM invocation event. Returns True on success."""
        result = self._registry.validate(SUBJECT, event.to_dict())
        if not result.valid:
            raise ValueError(f"Schema validation failed: {result.errors}")

        report = self._producer.send(Message(
            key=event.caller_id,      # partition by caller — related events co-located
            value=event.to_dict(),
        ))
        if report.succeeded:
            self._published.append(event)
        return report.succeeded

    def publish_batch(self, events: list[LLMInvocationEvent]) -> int:
        """Publish a batch. Returns count of successfully published events."""
        return sum(1 for e in events if self.publish(e))

    def stats(self) -> dict:
        if not self._published:
            return {"total": 0}
        total_cost = sum(e.cost_usd for e in self._published)
        total_tokens = sum(e.total_tokens for e in self._published)
        avg_latency = sum(e.latency_ms for e in self._published) / len(self._published)
        return {
            "total_events": len(self._published),
            "total_cost_usd": round(total_cost, 4),
            "total_tokens": total_tokens,
            "avg_latency_ms": round(avg_latency, 1),
            "models": list({e.model for e in self._published}),
        }
