"""Tests for consumer lag monitoring."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from python.patterns.consumer_lag import PartitionLag, LagMonitor


def _partition(latest, committed, partition=0):
    return {"topic": "events", "partition": partition,
            "latest_offset": latest, "committed_offset": committed}


def test_zero_lag_is_caught_up():
    p = PartitionLag("events", 0, 1000, 1000, "group-1")
    assert p.lag == 0
    assert p.status == "CAUGHT_UP"


def test_small_lag_is_nominal():
    p = PartitionLag("events", 0, 1000, 500, "group-1")
    assert p.lag == 500
    assert p.status == "NOMINAL"


def test_large_lag_is_critical():
    p = PartitionLag("events", 0, 100_000, 0, "group-1")
    assert p.lag == 100_000
    assert p.status == "CRITICAL"


def test_monitor_builds_report():
    monitor = LagMonitor()
    report = monitor.check("orders-group", [
        _partition(1000, 990, 0),
        _partition(2000, 1900, 1),
    ])
    assert report.total_lag == 110
    assert len(report.partitions) == 2


def test_report_is_healthy_when_nominal():
    monitor = LagMonitor()
    report = monitor.check("g", [_partition(100, 95)])
    assert report.is_healthy


def test_report_detects_critical_partitions():
    monitor = LagMonitor()
    report = monitor.check("g", [
        _partition(100_000, 0, 0),
        _partition(100, 99, 1),
    ])
    assert not report.is_healthy
    assert len(report.critical_partitions()) == 1


def test_lag_trend_detects_growing():
    monitor = LagMonitor()
    for lag in [100, 500, 2000]:
        monitor.check("g", [_partition(lag, 0)])
    assert monitor.is_lag_growing("g")


def test_lag_trend_stable_not_growing():
    monitor = LagMonitor()
    for lag in [500, 400, 300]:
        monitor.check("g", [_partition(lag, 0)])
    assert not monitor.is_lag_growing("g")
