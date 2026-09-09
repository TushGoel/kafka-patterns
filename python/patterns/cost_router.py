"""
Cost- and latency-aware provider routing.

Consumes the same telemetry stream inference_monitor.py already tracks —
cost, latency, and success/failure per LLM invocation — and makes a routing
decision: once a provider's p95 latency or cost-per-1k-tokens breaches a
configured SLO, subsequent requests should go to a fallback provider.
Once the primary recovers, traffic routes back.

Why hysteresis matters: evaluating on a single noisy sample causes routing
to flap back and forth on every message, which is worse than staying on a
degraded provider — each switch has its own cost (cold caches, different
tokenizer, inconsistent latency profile while warming up). This pattern
requires several consecutive breaching (or recovering) samples before
switching, plus a minimum cooldown between switches.

This is the decision half of the feedback loop inference_monitor.py's
`slowest_provider()` hints at: detect degradation, then actually act on it.
"""
from __future__ import annotations

import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

from .llm_event_stream import LLMInvocationEvent


@dataclass
class RoutingSLO:
    p95_latency_ms: float = 2000.0        # p95 latency SLO per provider
    cost_per_1k_tokens_usd: float = 5.0   # cost SLO per 1k tokens
    window_size: int = 20                 # rolling window for p95/cost calc
    min_samples: int = 5                  # samples required before evaluating SLO
    breach_streak_to_reroute: int = 3     # consecutive breaches before switching away
    recovery_streak_to_restore: int = 5   # consecutive healthy samples before switching back
    cooldown_seconds: float = 30.0        # minimum time between route switches


@dataclass
class _Streaks:
    breach: int = 0
    ok: int = 0


@dataclass
class RouteChange:
    at: float
    from_provider: str
    to_provider: str
    reason: str

    def __str__(self) -> str:
        return f"{self.from_provider} -> {self.to_provider}: {self.reason}"


class CostRouter:
    """
    Routes LLM requests away from a provider breaching its cost/latency SLO,
    and back once it recovers — with hysteresis to avoid flapping.

    Usage:
        router = CostRouter(primary="bedrock", fallback="openai")

        for event in consume_llm_invocations():
            active = router.process(event)
            # `active` is the provider the *next* request should target.
            # Route a small slice of canary traffic to the non-active
            # provider and feed those events in too, so recovery can be
            # detected without fully committing traffic back first.
    """

    def __init__(self, primary: str, fallback: str,
                 slo: Optional[RoutingSLO] = None) -> None:
        if primary == fallback:
            raise ValueError("primary and fallback provider must differ")
        self.primary = primary
        self.fallback = fallback
        self.slo = slo or RoutingSLO()

        self._active = primary
        self._latencies: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.slo.window_size))
        self._costs_per_1k: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.slo.window_size))
        self._streaks: dict[str, _Streaks] = defaultdict(_Streaks)
        self._last_switch_at: Optional[float] = None
        self._history: list[RouteChange] = []

    @property
    def active_provider(self) -> str:
        return self._active

    def _record(self, event: LLMInvocationEvent) -> None:
        self._latencies[event.provider].append(event.latency_ms)
        total_tokens = event.total_tokens
        cost_per_1k = (event.cost_usd / total_tokens) * 1000 if total_tokens else 0.0
        self._costs_per_1k[event.provider].append(cost_per_1k)

    @staticmethod
    def _p95(values: deque) -> float:
        if not values:
            return 0.0
        sorted_v = sorted(values)
        idx = max(0, math.ceil(len(sorted_v) * 0.95) - 1)
        return sorted_v[idx]

    def _breaches_slo(self, provider: str) -> bool:
        latencies = self._latencies[provider]
        if len(latencies) < min(self.slo.min_samples, self.slo.window_size):
            return False  # not enough samples to trust the signal yet
        p95_latency = self._p95(latencies)
        costs = self._costs_per_1k[provider]
        avg_cost = sum(costs) / len(costs) if costs else 0.0
        return (p95_latency > self.slo.p95_latency_ms
                or avg_cost > self.slo.cost_per_1k_tokens_usd)

    def _in_cooldown(self, now: float) -> bool:
        if self._last_switch_at is None:
            return False
        return (now - self._last_switch_at) < self.slo.cooldown_seconds

    def _switch_to(self, provider: str, now: float, reason: str) -> None:
        change = RouteChange(at=now, from_provider=self._active,
                              to_provider=provider, reason=reason)
        self._history.append(change)
        self._active = provider
        self._last_switch_at = now

    def process(self, event: LLMInvocationEvent, now: Optional[float] = None) -> str:
        """
        Record one telemetry event and return the provider that subsequent
        requests should target.

        `now` is injectable for deterministic tests; defaults to wall clock.
        """
        now = time.monotonic() if now is None else now
        self._record(event)

        provider = event.provider
        streak = self._streaks[provider]
        if self._breaches_slo(provider):
            streak.breach += 1
            streak.ok = 0
        else:
            streak.ok += 1
            streak.breach = 0

        if (provider == self._active == self.primary
                and streak.breach >= self.slo.breach_streak_to_reroute
                and not self._in_cooldown(now)):
            self._switch_to(
                self.fallback, now,
                reason=f"{provider} breached SLO {streak.breach} times in a row",
            )
        elif (self._active == self.fallback and provider == self.primary
                and streak.ok >= self.slo.recovery_streak_to_restore
                and not self._in_cooldown(now)):
            self._switch_to(
                self.primary, now,
                reason=f"{provider} recovered {streak.ok} samples in a row",
            )

        return self._active

    def history(self) -> list[RouteChange]:
        return list(self._history)

    def summary(self) -> dict:
        return {
            "active_provider": self._active,
            "primary": self.primary,
            "fallback": self.fallback,
            "switch_count": len(self._history),
            "last_switch_reason": self._history[-1].reason if self._history else None,
        }
