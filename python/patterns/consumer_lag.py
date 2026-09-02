"""
Consumer lag monitoring.

Consumer lag = (latest offset in partition) - (committed consumer offset).

Production concern: lag > threshold means consumers can't keep up with
producers — a leading indicator of processing pipeline degradation before
it becomes a customer-visible incident.

At Confluent/WarpStream scale: monitor lag per consumer group, per partition.
Alert when lag exceeds SLO thresholds. Correlate with processing latency.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PartitionLag:
    topic: str
    partition: int
    latest_offset: int          # broker's latest offset (high watermark)
    committed_offset: int       # consumer group's last committed offset
    consumer_group: str

    @property
    def lag(self) -> int:
        """Messages not yet consumed by this group."""
        return max(0, self.latest_offset - self.committed_offset)

    @property
    def status(self) -> str:
        if self.lag == 0:
            return "CAUGHT_UP"
        if self.lag < 1000:
            return "NOMINAL"
        if self.lag < 10_000:
            return "WARNING"
        return "CRITICAL"

    def __str__(self) -> str:
        return (
            f"{self.consumer_group}/{self.topic}[{self.partition}] "
            f"lag={self.lag:,} ({self.status})"
        )


@dataclass
class ConsumerLagReport:
    group_id: str
    partitions: list[PartitionLag] = field(default_factory=list)

    @property
    def total_lag(self) -> int:
        return sum(p.lag for p in self.partitions)

    @property
    def max_partition_lag(self) -> int:
        return max((p.lag for p in self.partitions), default=0)

    @property
    def is_healthy(self) -> bool:
        return all(p.status in ("CAUGHT_UP", "NOMINAL") for p in self.partitions)

    def critical_partitions(self) -> list[PartitionLag]:
        return [p for p in self.partitions if p.status == "CRITICAL"]

    def summary(self) -> str:
        status = "✅ HEALTHY" if self.is_healthy else "🚨 DEGRADED"
        return (
            f"{status} {self.group_id} | "
            f"total_lag={self.total_lag:,} | "
            f"max_partition_lag={self.max_partition_lag:,} | "
            f"partitions={len(self.partitions)}"
        )


class LagMonitor:
    """
    Monitors consumer lag across groups and partitions.

    Production integration:
        # confluent_kafka AdminClient
        consumer = Consumer({...})
        partitions = consumer.assignment()
        watermarks = consumer.get_watermark_offsets(partition)
        committed = consumer.committed(partitions)
        lag = watermarks[1] - committed[0].offset  # high_watermark - committed

    Usage:
        monitor = LagMonitor(alert_threshold=10_000)
        report = monitor.check("payments-group", partitions_data)
        if not report.is_healthy:
            alert_oncall(report.summary())
    """

    def __init__(self, alert_threshold: int = 10_000) -> None:
        self.alert_threshold = alert_threshold
        self._history: list[ConsumerLagReport] = []

    def check(self, group_id: str,
              partition_data: list[dict]) -> ConsumerLagReport:
        """
        Build a lag report from partition offset data.

        Args:
            group_id: Consumer group ID
            partition_data: List of dicts with topic, partition,
                           latest_offset, committed_offset
        """
        partitions = [
            PartitionLag(
                topic=d["topic"],
                partition=d["partition"],
                latest_offset=d["latest_offset"],
                committed_offset=d["committed_offset"],
                consumer_group=group_id,
            )
            for d in partition_data
        ]
        report = ConsumerLagReport(group_id=group_id, partitions=partitions)
        self._history.append(report)
        return report

    def trend(self, group_id: str, last_n: int = 5) -> list[int]:
        """Return total lag over last N checks — detect growing lag."""
        relevant = [r for r in self._history if r.group_id == group_id]
        return [r.total_lag for r in relevant[-last_n:]]

    def is_lag_growing(self, group_id: str) -> bool:
        """True if lag has been increasing monotonically — signals pipeline issue."""
        trend = self.trend(group_id)
        if len(trend) < 3:
            return False
        return all(trend[i] < trend[i + 1] for i in range(len(trend) - 1))
