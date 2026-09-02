"""
Schema Registry integration — Confluent's core differentiator.

The problem: in a Kafka ecosystem with many producers and consumers,
schema mismatches cause silent data corruption or consumer crashes.
Schema Registry enforces contracts between producers and consumers.

Schema evolution rules (Confluent defaults):
  BACKWARD: new schema can read old messages (add optional fields)
  FORWARD:  old schema can read new messages (remove fields)
  FULL:     both backward + forward compatible

Why this matters at Confluent/WarpStream:
  Without schema governance, a producer change can break all downstream
  consumers — no coordination needed. With Schema Registry, breaking
  changes are rejected at produce time, not discovered at consume time.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class CompatibilityMode(str, Enum):
    BACKWARD = "BACKWARD"    # new schema reads old data
    FORWARD = "FORWARD"      # old schema reads new data
    FULL = "FULL"            # both directions
    NONE = "NONE"            # no compatibility checks


@dataclass
class SchemaVersion:
    subject: str             # typically "{topic}-value" or "{topic}-key"
    version: int
    schema_id: int
    schema: dict             # JSON Schema or Avro schema definition
    compatibility: CompatibilityMode = CompatibilityMode.BACKWARD


@dataclass
class ValidationResult:
    valid: bool
    schema_id: Optional[int] = None
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.valid:
            return f"✅ Valid (schema_id={self.schema_id})"
        return f"❌ Invalid: {'; '.join(self.errors)}"


class SchemaRegistry:
    """
    In-memory Schema Registry — same contract as Confluent Schema Registry.

    Production: replace with confluent_kafka.schema_registry.SchemaRegistryClient
    URL: http://schema-registry:8081

    Usage:
        registry = SchemaRegistry()
        registry.register("orders-value", order_schema)

        # Validate before producing
        result = registry.validate("orders-value", my_message)
        if not result.valid:
            raise SchemaViolation(result.errors)

        # Serialize with schema ID embedded (wire format)
        payload = registry.serialize("orders-value", my_message)
    """

    def __init__(self) -> None:
        self._schemas: dict[str, list[SchemaVersion]] = {}
        self._id_counter = 1

    def register(self, subject: str, schema: dict,
                 compatibility: CompatibilityMode = CompatibilityMode.BACKWARD) -> int:
        """Register a new schema version. Returns schema ID."""
        if subject not in self._schemas:
            self._schemas[subject] = []

        schema_id = self._id_counter
        self._id_counter += 1
        version = len(self._schemas[subject]) + 1

        self._schemas[subject].append(SchemaVersion(
            subject=subject,
            version=version,
            schema_id=schema_id,
            schema=schema,
            compatibility=compatibility,
        ))
        return schema_id

    def latest(self, subject: str) -> Optional[SchemaVersion]:
        """Get the latest registered schema for a subject."""
        versions = self._schemas.get(subject, [])
        return versions[-1] if versions else None

    def validate(self, subject: str, message: dict[str, Any]) -> ValidationResult:
        """
        Validate a message against the latest registered schema.
        Checks required fields and type compatibility.

        Production: confluent_kafka.schema_registry validates Avro/Protobuf
        schemas automatically during serialization.
        """
        schema_version = self.latest(subject)
        if not schema_version:
            return ValidationResult(valid=False, errors=[f"No schema registered for {subject}"])

        schema = schema_version.schema
        errors = []

        # Check required fields
        required = schema.get("required", [])
        for field_name in required:
            if field_name not in message:
                errors.append(f"Missing required field: '{field_name}'")

        # Check field types
        properties = schema.get("properties", {})
        for field_name, field_schema in properties.items():
            if field_name in message:
                expected_type = field_schema.get("type")
                actual_value = message[field_name]
                if not _type_matches(actual_value, expected_type):
                    errors.append(
                        f"Field '{field_name}': expected {expected_type}, "
                        f"got {type(actual_value).__name__}"
                    )

        return ValidationResult(
            valid=len(errors) == 0,
            schema_id=schema_version.schema_id,
            errors=errors,
        )

    def serialize(self, subject: str, message: dict[str, Any]) -> bytes:
        """
        Serialize message with Confluent wire format:
        [magic byte (0x00)] [4-byte schema ID] [Avro/JSON payload]

        Consumers use the schema ID to fetch the schema and deserialize.
        Production: use AvroSerializer or JSONSchemaSerializer from
        confluent_kafka.schema_registry.avro / .json_schema
        """
        schema_version = self.latest(subject)
        if not schema_version:
            raise ValueError(f"No schema registered for {subject}")

        payload = json.dumps(message).encode("utf-8")
        schema_id_bytes = schema_version.schema_id.to_bytes(4, byteorder="big")
        return b"\x00" + schema_id_bytes + payload

    def deserialize(self, data: bytes) -> tuple[int, dict]:
        """Deserialize Confluent wire format. Returns (schema_id, message)."""
        if data[0] != 0:
            raise ValueError("Not Confluent wire format (expected magic byte 0x00)")
        schema_id = int.from_bytes(data[1:5], byteorder="big")
        message = json.loads(data[5:])
        return schema_id, message

    def check_compatibility(self, subject: str, new_schema: dict) -> bool:
        """
        Check if new_schema is compatible with the latest registered version.

        BACKWARD compatibility: new schema can read messages written with old schema.
        Rule: may only ADD optional fields or REMOVE fields.
        """
        current = self.latest(subject)
        if not current:
            return True  # first version — always compatible

        mode = current.compatibility
        if mode == CompatibilityMode.NONE:
            return True

        current_fields = set(current.schema.get("properties", {}).keys())
        new_fields = set(new_schema.get("properties", {}).keys())
        new_required = set(new_schema.get("required", []))

        if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL):
            # New required fields break backward compatibility
            added_required = (new_fields - current_fields) & new_required
            if added_required:
                return False

        return True


def _type_matches(value: Any, expected: Optional[str]) -> bool:
    if expected is None:
        return True
    type_map = {"string": str, "integer": int, "number": (int, float),
                "boolean": bool, "array": list, "object": dict}
    expected_types = type_map.get(expected)
    if expected_types is None:
        return True
    return isinstance(value, expected_types)
