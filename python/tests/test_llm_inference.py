"""Tests for LLM inference event streaming and anomaly detection."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from python.patterns.llm_event_stream import LLMInvocationEvent, LLMEventStream
from python.patterns.inference_monitor import InferenceMonitor


def _event(model="claude-3-5-sonnet", provider="bedrock",
           latency_ms=500.0, success=True, cost=0.003,
           caller="agent-1", input_tokens=500, output_tokens=200):
    return LLMInvocationEvent(
        model=model, provider=provider,
        input_tokens=input_tokens, output_tokens=output_tokens,
        latency_ms=latency_ms, caller_id=caller,
        cost_usd=cost, success=success,
    )


# ── LLM Event Stream ──────────────────────────────────────────────────────────

def test_publish_valid_event():
    stream = LLMEventStream()
    assert stream.publish(_event())


def test_publish_batch_returns_count():
    stream = LLMEventStream()
    events = [_event(latency_ms=float(i * 100)) for i in range(1, 6)]
    count = stream.publish_batch(events)
    assert count == 5


def test_stats_tracks_cost_and_tokens():
    stream = LLMEventStream()
    stream.publish(_event(cost=0.005, input_tokens=1000, output_tokens=400))
    stream.publish(_event(cost=0.003, input_tokens=500, output_tokens=200))
    stats = stream.stats()
    assert stats["total_events"] == 2
    assert abs(stats["total_cost_usd"] - 0.008) < 0.0001
    assert stats["total_tokens"] == 2100


def test_event_schema_requires_fields():
    stream = LLMEventStream()
    event = _event()
    d = event.to_dict()
    for field in ["event_id", "model", "provider", "latency_ms", "caller_id"]:
        assert field in d


# ── Inference Monitor ─────────────────────────────────────────────────────────

def test_no_anomaly_within_slo():
    monitor = InferenceMonitor(latency_slo_ms=2000)
    for _ in range(15):
        anomalies = monitor.process(_event(latency_ms=500))
    assert not any(a.kind == "LATENCY_SLO" for a in monitor._anomalies)


def test_detects_latency_slo_breach():
    monitor = InferenceMonitor(latency_slo_ms=1000)
    for _ in range(15):
        monitor.process(_event(latency_ms=3000))
    assert any(a.kind == "LATENCY_SLO" for a in monitor._anomalies)


def test_page_severity_for_extreme_latency():
    monitor = InferenceMonitor(latency_slo_ms=1000)
    for _ in range(15):
        monitor.process(_event(latency_ms=5000))
    pages = [a for a in monitor._anomalies if a.severity == "PAGE"]
    assert len(pages) > 0


def test_detects_error_rate_spike():
    monitor = InferenceMonitor(error_rate_threshold=0.05)
    for _ in range(25):
        monitor.process(_event(success=False))
    assert any(a.kind == "ERROR_SPIKE" for a in monitor._anomalies)


def test_detects_cost_anomaly():
    monitor = InferenceMonitor(cost_per_caller_limit=1.0)
    for _ in range(5):
        monitor.process(_event(cost=0.30, caller="expensive-agent"))
    assert any(a.kind == "COST_ANOMALY" for a in monitor._anomalies)


def test_slowest_provider_identified():
    monitor = InferenceMonitor()
    for _ in range(5):
        monitor.process(_event(provider="bedrock", model="claude", latency_ms=200))
        monitor.process(_event(provider="openai", model="gpt-4o", latency_ms=2000))
    slowest = monitor.slowest_provider()
    assert "openai" in slowest


def test_summary_structure():
    monitor = InferenceMonitor()
    monitor.process(_event())
    summary = monitor.summary()
    assert "providers_tracked" in summary
    assert "total_anomalies" in summary
    assert "stats" in summary
