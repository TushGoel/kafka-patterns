"""Tests for Schema Registry integration."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from python.patterns.schema_registry import SchemaRegistry, CompatibilityMode

ORDER_SCHEMA = {
    "type": "object",
    "required": ["order_id", "amount"],
    "properties": {
        "order_id": {"type": "string"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
    },
}


def _registry():
    r = SchemaRegistry()
    r.register("orders-value", ORDER_SCHEMA)
    return r


def test_register_returns_schema_id():
    r = SchemaRegistry()
    sid = r.register("orders-value", ORDER_SCHEMA)
    assert sid > 0


def test_validate_valid_message():
    r = _registry()
    result = r.validate("orders-value", {"order_id": "o-1", "amount": 99.99})
    assert result.valid


def test_validate_missing_required_field():
    r = _registry()
    result = r.validate("orders-value", {"amount": 50.0})  # missing order_id
    assert not result.valid
    assert any("order_id" in e for e in result.errors)


def test_validate_wrong_type():
    r = _registry()
    result = r.validate("orders-value", {"order_id": 123, "amount": 50.0})  # id should be string
    assert not result.valid


def test_validate_unknown_subject():
    r = SchemaRegistry()
    result = r.validate("nonexistent-value", {"x": 1})
    assert not result.valid
    assert "No schema" in result.errors[0]


def test_serialize_uses_wire_format():
    r = _registry()
    payload = r.serialize("orders-value", {"order_id": "o-1", "amount": 10.0})
    assert payload[0] == 0x00  # magic byte
    assert len(payload) > 5   # schema ID + data


def test_deserialize_roundtrip():
    r = _registry()
    msg = {"order_id": "o-1", "amount": 99.0, "currency": "USD"}
    payload = r.serialize("orders-value", msg)
    schema_id, recovered = r.deserialize(payload)
    assert recovered["order_id"] == "o-1"
    assert schema_id > 0


def test_backward_compat_allows_adding_optional_field():
    r = SchemaRegistry()
    r.register("t-value", ORDER_SCHEMA, CompatibilityMode.BACKWARD)
    new_schema = {**ORDER_SCHEMA, "properties": {
        **ORDER_SCHEMA["properties"],
        "notes": {"type": "string"},  # new optional field — OK
    }}
    assert r.check_compatibility("t-value", new_schema)


def test_backward_compat_rejects_new_required_field():
    r = SchemaRegistry()
    r.register("t-value", ORDER_SCHEMA, CompatibilityMode.BACKWARD)
    new_schema = {
        **ORDER_SCHEMA,
        "required": ["order_id", "amount", "region"],  # new required field — BREAKS
        "properties": {**ORDER_SCHEMA["properties"], "region": {"type": "string"}},
    }
    assert not r.check_compatibility("t-value", new_schema)
