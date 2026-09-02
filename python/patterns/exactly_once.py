"""
Exactly-once semantics with Kafka transactions.

The problem: at-least-once means duplicates. Idempotent consumers help but
aren't always possible. Kafka transactions (transactional producer + read-
process-write atomic commit) give true exactly-once across producer + consumer.

When to use: financial transactions, inventory updates, any case where
duplicate processing causes real-world harm.

Cost: 20-30% throughput reduction vs at-least-once. Use only when needed.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class TransactionalConfig:
    bootstrap_servers: str
    transactional_id: str     # unique per producer instance — enables exactly-once
    input_topic: str
    output_topic: str
    consumer_group: str


class ExactlyOnceProcessor:
    """
    Read-process-write with atomic Kafka transaction.

    Pattern:
      1. Begin transaction
      2. Read from input topic
      3. Process
      4. Produce to output topic (within transaction)
      5. Commit offsets within transaction (atomic with produce)
      6. Commit transaction

    If any step fails → abort → message reprocessed from last committed offset.
    The output message is NEVER visible to consumers until transaction commits.

    Usage:
        processor = ExactlyOnceProcessor(config, transform=my_transform)
        processor.process_batch(messages)
    """

    def __init__(self, config: TransactionalConfig,
                 transform: Callable[[dict], dict]) -> None:
        self.config = config
        self._transform = transform

        # Production init:
        # self._producer = confluent_kafka.Producer({
        #     "bootstrap.servers": config.bootstrap_servers,
        #     "transactional.id": config.transactional_id,
        #     "enable.idempotence": True,
        # })
        # self._producer.init_transactions()

    def process_batch(self, messages: list[dict[str, Any]]) -> int:
        """
        Process a batch atomically. Returns count of successfully processed messages.
        All-or-nothing: if any message fails, entire batch is retried.
        """
        # Production:
        # self._producer.begin_transaction()
        # try:
        #     for msg in messages:
        #         transformed = self._transform(msg)
        #         self._producer.produce(self.config.output_topic, value=transformed)
        #
        #     # Commit offsets atomically with transaction
        #     self._producer.send_offsets_to_transaction(
        #         offsets, self.config.consumer_group
        #     )
        #     self._producer.commit_transaction()
        #     return len(messages)
        # except Exception:
        #     self._producer.abort_transaction()
        #     raise

        # Simulated
        return len([self._transform(m) for m in messages])
