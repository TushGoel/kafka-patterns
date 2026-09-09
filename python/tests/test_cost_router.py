"""Tests for cost/latency-aware provider routing with hysteresis."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import pytest

from python.patterns.llm_event_stream import LLMInvocationEvent
from python.patterns.cost_router import CostRouter, RoutingSLO


def _event(provider, latency_ms=500.0, cost=0.003, input_tokens=500, output_tokens=200):
    return LLMInvocationEvent(
        model="test-model", provider=provider,
        input_tokens=input_tokens, output_tokens=output_tokens,
        latency_ms=latency_ms, caller_id="agent-1", cost_usd=cost,
    )


def test_rejects_identical_primary_and_fallback():
    with pytest.raises(ValueError):
        CostRouter(primary="bedrock", fallback="bedrock")


def test_stays_on_primary_within_slo():
    router = CostRouter("bedrock", "openai", slo=RoutingSLO(min_samples=3, p95_latency_ms=2000))
    active = "bedrock"
    for _ in range(10):
        active = router.process(_event("bedrock", latency_ms=500))
    assert active == "bedrock"
    assert router.active_provider == "bedrock"
    assert router.history() == []


def test_single_breach_does_not_reroute():
    """One bad sample shouldn't flip routing — that's the whole point of hysteresis."""
    slo = RoutingSLO(min_samples=1, window_size=1, breach_streak_to_reroute=3, p95_latency_ms=1000)
    router = CostRouter("bedrock", "openai", slo=slo)
    active = router.process(_event("bedrock", latency_ms=5000), now=0.0)
    assert active == "bedrock"


def test_breach_streak_triggers_reroute_to_fallback():
    slo = RoutingSLO(min_samples=1, window_size=1, breach_streak_to_reroute=3, p95_latency_ms=1000)
    router = CostRouter("bedrock", "openai", slo=slo)

    active = "bedrock"
    for i in range(3):
        active = router.process(_event("bedrock", latency_ms=5000), now=float(i))

    assert active == "openai"
    assert router.active_provider == "openai"
    assert len(router.history()) == 1
    assert router.history()[0].from_provider == "bedrock"
    assert router.history()[0].to_provider == "openai"


def test_cost_slo_breach_also_triggers_reroute():
    """Latency can be fine while cost-per-token is the thing that breached."""
    slo = RoutingSLO(min_samples=1, window_size=1, breach_streak_to_reroute=2,
                      p95_latency_ms=999_999, cost_per_1k_tokens_usd=1.0)
    router = CostRouter("bedrock", "openai", slo=slo)

    active = "bedrock"
    for i in range(2):
        # cost_usd=5.0 over 1000 tokens => $5.00 / 1k tokens, way above the $1.00 SLO
        active = router.process(
            _event("bedrock", latency_ms=100, cost=5.0, input_tokens=800, output_tokens=200),
            now=float(i),
        )

    assert active == "openai"


def test_recovery_suppressed_during_cooldown_then_switches_back():
    slo = RoutingSLO(
        min_samples=1, window_size=1,
        breach_streak_to_reroute=2, recovery_streak_to_restore=2,
        cooldown_seconds=10.0, p95_latency_ms=1000,
    )
    router = CostRouter("bedrock", "openai", slo=slo)

    # Drive bedrock into breach — switches to fallback at t=1.0.
    router.process(_event("bedrock", latency_ms=3000), now=0.0)
    active = router.process(_event("bedrock", latency_ms=3000), now=1.0)
    assert active == "openai"

    # Canary traffic to bedrock now looks healthy (2 consecutive good samples),
    # satisfying the recovery streak — but we're still inside the 10s cooldown
    # window since the last switch (t=1.0), so routing must NOT flip back yet.
    router.process(_event("bedrock", latency_ms=200), now=2.0)
    active = router.process(_event("bedrock", latency_ms=200), now=3.0)
    assert active == "openai", "should stay on fallback while inside cooldown"

    # Once the cooldown has elapsed, the next healthy sample restores primary.
    active = router.process(_event("bedrock", latency_ms=200), now=15.0)
    assert active == "bedrock"
    assert router.history()[-1].to_provider == "bedrock"
    assert "recovered" in router.history()[-1].reason


def test_history_and_summary_reflect_switches():
    slo = RoutingSLO(min_samples=1, window_size=1, breach_streak_to_reroute=1, p95_latency_ms=1000)
    router = CostRouter("bedrock", "openai", slo=slo)
    router.process(_event("bedrock", latency_ms=3000), now=0.0)

    summary = router.summary()
    assert summary["active_provider"] == "openai"
    assert summary["primary"] == "bedrock"
    assert summary["fallback"] == "openai"
    assert summary["switch_count"] == 1
    assert "breached SLO" in summary["last_switch_reason"]


def test_summary_before_any_switch():
    router = CostRouter("bedrock", "openai")
    summary = router.summary()
    assert summary["active_provider"] == "bedrock"
    assert summary["switch_count"] == 0
    assert summary["last_switch_reason"] is None
