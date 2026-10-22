"""Strict structured-output JSON schemas generated from the proto contracts.

A model-facing schema for a value the agent returns is derived from the same
message descriptor Go loads, so the keys the prompt asks for and the keys the
loader accepts cannot drift. Constraints come from the proto's protovalidate
field rules (``min_len``, ``in``, ``min_items``); everything else is checked by
the full ``validate`` after the reply is parsed. A strict provider schema lists
every property as required and forbids extras.

Fields the agent must not supply (worker-owned accounting, controller-owned
state) are named in ``omit`` by message full name, so each exclusion is visible
at the call site. Deprecated fields are never requested.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from google.protobuf.descriptor import FieldDescriptor

from sdo.contracts.proto import validate_pb2

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

_TIMESTAMP = "google.protobuf.Timestamp"
_SCALARS: dict[int, dict[str, Any]] = {
    FieldDescriptor.TYPE_STRING: {"type": "string"},
    FieldDescriptor.TYPE_BOOL: {"type": "boolean"},
    FieldDescriptor.TYPE_INT32: {"type": "integer"},
    FieldDescriptor.TYPE_INT64: {"type": "integer"},
    FieldDescriptor.TYPE_UINT32: {"type": "integer"},
    FieldDescriptor.TYPE_UINT64: {"type": "integer"},
    FieldDescriptor.TYPE_DOUBLE: {"type": "number"},
}


def model_output_schema(descriptor: Any, *, omit: Mapping[str, Collection[str]] | None = None) -> dict[str, Any]:
    """The strict JSON schema of *descriptor*, without the fields named in *omit*.

    Raises:
        ValueError: *omit* names a field the message does not have, or a requested
            field has no model-output form (map, optional, enum, bytes, well-known type).
    """

    omitted = {name: frozenset(fields) for name, fields in (omit or {}).items()}
    seen: set[str] = set()
    schema = _message_schema(descriptor, omitted, seen)
    unknown = set(omitted) - seen
    if unknown:
        raise ValueError(f"omit names messages not in the schema: {sorted(unknown)}")
    return schema


def _message_schema(descriptor: Any, omit: Mapping[str, frozenset[str]], seen: set[str]) -> dict[str, Any]:
    seen.add(descriptor.full_name)
    omitted = omit.get(descriptor.full_name, frozenset())
    missing = omitted - set(descriptor.fields_by_name)
    if missing:
        raise ValueError(f"{descriptor.full_name} has no fields {sorted(missing)} to omit")
    properties = {
        field.name: _field_schema(field, omit, seen)
        for field in descriptor.fields
        if field.name not in omitted and not field.GetOptions().deprecated
    }
    return {"type": "object", "properties": properties, "required": list(properties), "additionalProperties": False}


def _field_schema(field: Any, omit: Mapping[str, frozenset[str]], seen: set[str]) -> dict[str, Any]:
    where = f"{field.containing_type.full_name}.{field.name}"
    if field.message_type is not None and field.message_type.GetOptions().map_entry:
        raise ValueError(f"{where} is a map; omit it or give the model an explicit list message")
    if field.has_presence and field.containing_oneof is None and field.message_type is None:
        raise ValueError(f"{where} is optional; omit it or make it a plain field")
    rules = field.GetOptions().Extensions[validate_pb2.field]
    if field.message_type is not None:
        if field.message_type.full_name == _TIMESTAMP:
            item: dict[str, Any] = {"type": "string", "format": "date-time"}
        elif field.message_type.full_name.startswith("google.protobuf."):
            raise ValueError(f"{where} is a well-known type with no model-output form")
        else:
            item = _message_schema(field.message_type, omit, seen)
    elif field.type in _SCALARS:
        item = dict(_SCALARS[field.type])
    else:
        raise ValueError(f"{where} has a type with no model-output form")
    if field.is_repeated:
        element = rules.repeated.items.string if field.type == FieldDescriptor.TYPE_STRING else None
        if element is not None:
            _apply_string_rules(item, element)
        array: dict[str, Any] = {"type": "array", "items": item}
        if rules.repeated.min_items:
            array["minItems"] = rules.repeated.min_items
        return array
    if field.type == FieldDescriptor.TYPE_STRING:
        _apply_string_rules(item, rules.string)
    return item


def _apply_string_rules(item: dict[str, Any], rules: Any) -> None:
    if rules.min_len:
        item["minLength"] = rules.min_len
    allowed = list(getattr(rules, "in"))
    if allowed:
        item["enum"] = allowed
