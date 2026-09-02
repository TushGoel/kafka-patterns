"""
Real-time LLM inference anomaly detection.

Consumes the llm-invocations Kafka topic and detects:
  - Latency SLO breaches (p95 > threshold)
  - Error rate spikes per model/provider
  - Cost anomalies per caller
  - Provider degradation (one provider suddenly slow → route away)

This is the feedback loop that makes LLM gateways self-healing:
  stream event → detect anomaly → trigger rerouting or paging.

At Netflix/Anthropic scale: millions of LLM calls/day, each generating
a telemetry event. This consumer processes them in real-time so latency
degradation triggers alerts in seconds, not the next daily report.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

from .llm_event_stream import LLMInvocationEvent


@dataclass
class ProviderStats:
    provider: str
    model: str
    latencies: deque = field(default_factory=lambda: deque(maxlen=1000))
    errors: int = 0
    total: int = 0
    total_cost: float = 0.0

    def record(self, event: LLMInvocationEvent) -> None:
        self.latencies.append(event.latency_ms)
        self.total += 1
        self.total_cost += event.cost_usd
        if not event.success:
            self.errors += 1

    @property
    def error_rate(self) -> float:
        return self.errors / self.total if self.total else 0.0

    @property
    def p95_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        sorted_l = sorted(self.latencies)
        idx = max(0, math.ceil(len(sorted_l) * 0.95) - 1)
        return sorted_l[idx]

    @property
    def avg_latency_ms(self) -> float:
        return sum(self.latencies) / len(self.latencies) if self.latencies else 0.0


@dataclass
class Anomaly:
    kind: str          # "LATENCY_SLO", "ERROR_SPIKE", "COST_ANOMALY"
    severity: str      # "WARN", "PAGE"
    provider: str
    model: str
    metric: float
    threshold: float
    message: str

    def __str__(self) -> str:
        return f"[{self.severity}] {self.kind} {self.provider}/{self.model}: {self.message}"


class InferenceMonitor:
    """
    Real-time anomaly detection on the LLM invocation event stream.

    Usage:
        monitor = InferenceMonitor(latency_slo_ms=2000, error_rate_threshold=0.05)

        # In consumer loop
        for event in consume_llm_invocations():
            anomalies = monitor.process(event)
            for a in anomalies:
                if a.severity == "PAGE":
                    alert_oncall(str(a))
    """

    def __init__(
        self,
        latency_slo_ms: float = 2000.0,     # p95 SLO
        error_rate_threshold: float = 0.05,  # 5% error rate
        cost_per_caller_limit: float = 10.0, # $10/hour per caller
    ) -> None:
        self.latency_slo_ms = latency_slo_ms
        self.error_rate_threshold = error_rate_threshold
        self.cost_per_caller_limit = cost_per_caller_limit

        self._stats: dict[str, ProviderStats] = {}
        self._caller_costs: dict[str, float] = defaultdict(float)
        self._anomalies: list[Anomaly] = []

    def _key(self, event: LLMInvocationEvent) -> str:
        return f"{event.provider}/{event.model}"

    def process(self, event: LLMInvocationEvent) -> list[Anomaly]:
        """Process one event. Returns any anomalies detected."""
        key = self._key(event)
        if key not in self._stats:
            self._stats[key] = ProviderStats(provider=event.provider, model=event.model)

        stats = self._stats[key]
        stats.record(event)
        self._caller_costs[event.caller_id] += event.cost_usd

        anomalies = []

        # 1. Latency SLO breach
        if stats.total >= 10 and stats.p95_latency_ms > self.latency_slo_ms:
            severity = "PAGE" if stats.p95_latency_ms > self.latency_slo_ms * 2 else "WARN"
            a = Anomaly(
                kind="LATENCY_SLO",
                severity=severity,
                provider=event.provider,
                model=event.model,
                metric=stats.p95_latency_ms,
                threshold=self.latency_slo_ms,
                message=f"p95={stats.p95_latency_ms:.0f}ms exceeds SLO {self.latency_slo_ms:.0f}ms",
            )
            anomalies.append(a)
            self._anomalies.append(a)

        # 2. Error rate spike
        if stats.total >= 20 and stats.error_rate > self.error_rate_threshold:
            severity = "PAGE" if stats.error_rate > 0.20 else "WARN"
            a = Anomaly(
                kind="ERROR_SPIKE",
                severity=severity,
                provider=event.provider,
                model=event.model,
                metric=stats.error_rate,
                threshold=self.error_rate_threshold,
                message=f"error_rate={stats.error_rate:.1%} exceeds {self.error_rate_threshold:.1%}",
            )
            anomalies.append(a)
            self._anomalies.append(a)

        # 3. Per-caller cost anomaly
        caller_cost = self._caller_costs[event.caller_id]
        if caller_cost > self.cost_per_caller_limit:
            a = Anomaly(
                kind="COST_ANOMALY",
                severity="WARN",
                provider=event.provider,
                model=event.model,
                metric=caller_cost,
                threshold=self.cost_per_caller_limit,
                message=f"caller={event.caller_id} cost=${caller_cost:.2f} exceeds limit ${self.cost_per_caller_limit:.2f}",
            )
            anomalies.append(a)
            self._anomalies.append(a)

        return anomalies

    def slowest_provider(self) -> Optional[str]:
        """Returns the provider/model with highest p95 latency — reroute candidate."""
        if not self._stats:
            return None
        return max(self._stats, key=lambda k: self._stats[k].p95_latency_ms)

    def summary(self) -> dict:
        return {
            "providers_tracked": len(self._stats),
            "total_anomalies": len(self._anomalies),
            "page_alerts": sum(1 for a in self._anomalies if a.severity == "PAGE"),
            "stats": {
                k: {
                    "p95_ms": round(v.p95_latency_ms, 1),
                    "error_rate": round(v.error_rate, 3),
                    "total": v.total,
                }
                for k, v in self._stats.items()
            },
        }
