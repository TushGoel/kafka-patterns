# kafka-patterns

![CI](https://github.com/TushGoel/kafka-patterns/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Go](https://img.shields.io/badge/go-1.21%2B-blue)
![Tests](https://img.shields.io/badge/tests-64%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

Production Kafka patterns in **Python and Go** — from reliable producer to LLM inference telemetry streaming and real-time anomaly detection.

Built against patterns used in Confluent Cloud, WarpStream, and AI infrastructure platforms that stream millions of model invocation events daily.

---

## The Problem → Solution → Impact

| | |
|---|---|
| **Problem** | At-least-once delivery creates duplicates. Auto-commit loses messages on crash. No DLQ means one bad message blocks an entire partition indefinitely. |
| **Solution** | Three composable patterns: idempotent producer with delivery confirmation, consumer group with manual offset commit + retry, and transactional exactly-once for critical paths. |
| **Impact** | Zero message loss, no duplicate side effects, poison pill messages routed to DLQ instead of blocking consumption. |

---

## System Design

```mermaid
graph TD
    A[Event Source] --> B[ReliableProducer\nidempotent · acks=all\ndelivery confirmation]
    B --> C[(Kafka Topic\npartitioned · replicated)]

    C --> D[ConsumerGroup\nmanual offset commit\nretry + backoff]
    D -->|success| E[✅ Commit offset\nmessage processed]
    D -->|fail after retries| F[DLQ Topic\npoison pill isolated]

    subgraph Exactly_Once
        G[ExactlyOnceProcessor\ntransactional producer\natomic process + commit]
    end

    C --> G --> H[(Output Topic\nno duplicates guaranteed)]
```

---

## Patterns

### 1. Reliable Producer (Python + Go)

```python
from python.patterns.producer import ProducerConfig, Message, ReliableProducer

config = ProducerConfig(
    bootstrap_servers="localhost:9092",
    topic="order-events",
    acks="all",           # all replicas must ack — no data loss
    idempotence=True,     # exactly-once per partition
)
producer = ReliableProducer(config)

report = producer.send(Message(
    key="order-123",
    value={"type": "order_placed", "amount": 99.99},
))
assert report.succeeded   # confirmed by broker, not fire-and-forget
print(f"Delivered to partition {report.partition} offset {report.offset}")
```

**Why `acks=all`:** With `acks=1` (default), the leader acknowledges before replication. If the leader fails before replication, data is lost. `acks=all` waits for all in-sync replicas — zero data loss.

---

### 2. Consumer Group with Manual Commit (Python + Go)

```python
from python.patterns.consumer import ConsumerConfig, ConsumerGroup, CommitMode

def process_order(msg):
    # Your business logic here
    return save_to_database(msg.value)  # True on success

group = ConsumerGroup(
    ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="order-processor",
        topics=["order-events"],
        commit_mode=CommitMode.MANUAL,   # commit only after processing
    ),
    processor=process_order,
    dlq_topic="order-events-dlq",       # failed messages go here, not stuck
    max_retries=3,
)
group.run()
print(group.stats())
# {'total': 1000, 'succeeded': 998, 'failed': 2, 'dlq_routed': 2}
```

**Why manual commit:** Auto-commit advances the offset on a timer regardless of whether processing succeeded. With manual commit, if your process crashes mid-message, the offset stays at the failed message — it's reprocessed on restart. No data loss.

---

### 4. LLM Inference Event Streaming

```python
from python.patterns.llm_event_stream import LLMInvocationEvent, LLMEventStream
from python.patterns.inference_monitor import InferenceMonitor

stream = LLMEventStream(bootstrap_servers="localhost:9092")
monitor = InferenceMonitor(latency_slo_ms=2000, error_rate_threshold=0.05)

# After every LLM API call — stream the telemetry event
event = LLMInvocationEvent(
    model="claude-3-5-sonnet", provider="bedrock",
    input_tokens=850, output_tokens=320,
    latency_ms=1240, caller_id="oncall-triage-agent",
    cost_usd=0.0035, trace_id="span-abc123",
)
stream.publish(event)

# Consumer-side: detect anomalies in real time
anomalies = monitor.process(event)
for a in anomalies:
    if a.severity == "PAGE":
        alert_oncall(str(a))
# [PAGE] LATENCY_SLO bedrock/claude-3-5-sonnet: p95=4200ms exceeds SLO 2000ms

# Cost visibility per caller
print(stream.stats())
# {'total_events': 247, 'total_cost_usd': 0.8645, 'avg_latency_ms': 1340.2}

# Identify degraded provider for rerouting
slow = monitor.slowest_provider()
# "openai/gpt-4o" — route away until recovered
```

**Why stream LLM telemetry:** At millions of calls/day, batch analytics are too slow for incident response. Streaming every invocation enables p95 latency SLOs (page before users notice), per-caller cost enforcement, and provider health monitoring — all in real time.

---

### 3. Exactly-Once with Transactions (Python)

```python
from python.patterns.exactly_once import TransactionalConfig, ExactlyOnceProcessor

processor = ExactlyOnceProcessor(
    TransactionalConfig(
        bootstrap_servers="localhost:9092",
        transactional_id="payment-processor-1",  # unique per instance
        input_topic="payments-raw",
        output_topic="payments-processed",
        consumer_group="payment-group",
    ),
    transform=lambda msg: {**msg, "processed": True, "fee": msg["amount"] * 0.02},
)
count = processor.process_batch(messages)
# Output messages are invisible until transaction commits — no partial writes
```

**When to use:** Payment processing, inventory updates, anything where duplicate processing causes real-world harm. Cost: ~25% throughput reduction. Use only when idempotent consumers aren't possible.

---

### 5. Distributed Tracing (OpenTelemetry)

| | |
|---|---|
| **Problem** | A message that fails somewhere between produce and consume is hard to debug — logs are scattered across services with no shared identifier, and there's no way to see partition/offset/retry history for one specific message without grepping multiple hosts. |
| **Solution** | Wrap every produce and consume-with-retry call in an OpenTelemetry span carrying `messaging.system`, `messaging.destination.name` (topic), `messaging.kafka.partition`, `messaging.kafka.offset`, and `messaging.kafka.message_key`. The message **value** is never added as a span attribute — payloads may carry PII, and tracing backends are not an appropriate place to persist them. Failed consumes (including DLQ routing) set an `ERROR` span status so trace-based alerting can pick them up. |
| **Impact** | One trace ID follows a message from produce through every retry to final commit or DLQ routing — no more correlating logs by timestamp across services. Span data feeds directly into existing tracing backends (Jaeger, Tempo, X-Ray, etc.) with zero payload exposure. |

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Wire a real collector once, at process start
provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint="localhost:4317")))
trace.set_tracer_provider(provider)

# producer.py / consumer.py already open spans internally — nothing else to do
producer.send(Message(key="order-123", value={"amount": 99.99}))
# → span "kafka.produce": topic=order-events partition=0 offset=42 message_key=order-123
```

**Why never trace the value:** Span attributes and events are exported to a tracing backend and often retained longer than the message itself. Tracing `message.key` and offsets is enough to locate and replay a specific message from the topic if deeper inspection is needed — there's no reason to duplicate PII-bearing payload content into a system with different (usually weaker) access controls than Kafka itself.

Tests use OpenTelemetry's `InMemorySpanExporter` (see `python/tests/test_tracing.py`) — the standard way to assert on span attributes and status in unit tests without a real collector.

---

### 6. Consumer Autoscaling on Kafka Lag (Kubernetes)

| | |
|---|---|
| **Problem** | CPU-based autoscaling is the wrong signal for a Kafka consumer — a slow downstream dependency (e.g. a database write) causes lag to build while the consumer sits idle waiting on I/O, so CPU utilization stays flat even as the backlog grows unbounded. |
| **Solution** | A `Deployment` + `HorizontalPodAutoscaler` in `k8s/` where the HPA scales on an **external metric** (`kafka_consumergroup_group_lag`) instead of CPU/memory — the same signal `python/patterns/consumer_lag.py` uses to classify partition health. The scale-out target (`averageValue: 5000`) sits below that pattern's `CRITICAL` threshold (lag ≥ 10,000) so capacity is added before a partition is actually degraded; scale-down is deliberately slower to avoid flapping through repeated consumer-group rebalances. |
| **Impact** | Replica count tracks actual backlog instead of a proxy metric — the consumer group scales out under real load and scales back in once lag clears, without the manual toil of watching a lag dashboard and adjusting replicas by hand. |

```
k8s/
├── consumer-deployment.yaml   # Deployment for the ConsumerGroup pattern; readiness/liveness probes, graceful shutdown
├── consumer-service.yaml      # ClusterIP Service exposing a /metrics port for Prometheus scraping
└── consumer-hpa.yaml          # HPA scaling on external metric kafka_consumergroup_group_lag
```

**Metrics-adapter wiring (not included — out of scope for a demo repo):** the Kubernetes External Metrics API doesn't know about Kafka on its own. A real cluster needs a lag exporter (e.g. `kafka-lag-exporter`) scraping committed offset vs. high watermark per consumer group, Prometheus scraping that exporter, and `prometheus-adapter` (or a KEDA `ScaledObject` as an alternative to a raw HPA) mapping the resulting series into `external.metrics.k8s.io`. `k8s/consumer-hpa.yaml` documents this chain inline as a comment next to the metric it expects.

Validate manifests with `python3 -c "import yaml; yaml.safe_load(open(f))"` per file, or `kubectl apply --dry-run=client -f k8s/` if you have a cluster context configured.

---

## Project Structure

```
kafka-patterns/
├── python/
│   ├── patterns/
│   │   ├── producer.py          # Reliable producer, idempotent delivery
│   │   ├── consumer.py          # Consumer group, manual commit, DLQ routing
│   │   ├── exactly_once.py      # Transactional exactly-once processing
│   │   ├── consumer_lag.py      # Partition lag monitoring, trend detection
│   │   ├── schema_registry.py   # Schema registration, wire format, compatibility
│   │   ├── llm_event_stream.py  # LLM invocation telemetry streaming
│   │   └── inference_monitor.py # Real-time anomaly detection, SLO enforcement
│   └── tests/
│       ├── test_producer.py     # 7 tests
│       ├── test_consumer.py     # 7 tests
│       ├── test_consumer_lag.py # 12 tests
│       ├── test_schema_registry.py # 9 tests
│       ├── test_llm_inference.py   # 11 tests
│       └── test_tracing.py         # 7 tests — OpenTelemetry span assertions
├── go/
│   ├── producer/
│   │   ├── producer.go
│   │   └── producer_test.go     # 6 tests
│   └── consumer/
│       ├── consumer.go
│       └── consumer_test.go     # 5 tests
├── k8s/
│   ├── consumer-deployment.yaml # Deployment for the consumer pattern
│   ├── consumer-service.yaml    # Service exposing /metrics for Prometheus
│   └── consumer-hpa.yaml        # HPA scaling on Kafka consumer lag
└── go.mod
```

---

## Design Decisions & Trade-offs

**Manual commit over auto-commit:**
Auto-commit risks message loss — offset advances before processing completes. Manual commit ensures offset advances only after successful processing. The tradeoff: you must handle duplicate delivery (at-least-once) in your processing logic.

**DLQ over infinite retry:**
Without a DLQ, one malformed "poison pill" message blocks an entire partition indefinitely — all subsequent messages wait behind it. DLQ routes the bad message to a separate topic for inspection, unblocking the partition immediately.

**Idempotent producer by default:**
Idempotence (enable.idempotence=true) ensures the broker deduplicates retried produce requests. Without it, a network timeout causes the producer to retry, potentially writing the message twice. Cost: negligible. Benefit: no duplicates from producer retries.

**Transactional ID uniqueness:**
Each transactional producer instance must have a unique transactional ID. Reusing the same ID across instances causes a fence — the old instance is killed when the new one initializes. This prevents split-brain in exactly-once scenarios.

---

## Running Tests

```bash
pip install pytest
pytest python/tests/ -v
```

---

## Part of the Agentic Infrastructure Stack

| Repo | What It Is |
|------|-----------|
| **[workflow-orchestration-patterns](https://github.com/TushGoel/workflow-orchestration-patterns)** | Step Functions + SQS (Kafka-equivalent) orchestration |
| **[platform-observability](https://github.com/TushGoel/platform-observability)** | SLOs for event pipeline reliability |
| **[kafka-patterns](https://github.com/TushGoel/kafka-patterns)** | ← You are here: Kafka producer/consumer patterns |

---

## License

MIT
