"""Normalize public JSON selection inputs before the generated wire contract."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from google.protobuf.descriptor import Descriptor, FieldDescriptor

if TYPE_CHECKING:
    from google.protobuf.message import Message


def normalize_selection_set(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("selection_set: expected object")
    if value.get("version") != 1:
        raise ValueError("selection_set.version: expected integer 1")
    options = value.get("options")
    if not isinstance(options, list) or not 1 <= len(options) <= 32:
        raise ValueError("selection_set.options: expected array of 1..32 option objects")
    for index, option in enumerate(options):
        path = f"selection_set.options[{index}]"
        if not isinstance(option, dict):
            raise ValueError(f"{path}: expected option object")
        if not isinstance(option.get("option_id"), str) or not option["option_id"]:
            raise ValueError(f"{path}.option_id: expected nonempty string")
        if "parameters" not in option:
            continue
        parameters = option["parameters"]
        if not isinstance(parameters, dict):
            raise ValueError(f"{path}.parameters: expected object mapping names to JSON values")
        for name, parameter in parameters.items():
            if parameter is None:
                raise ValueError(f"{path}.parameters.{name}: expected string, number, boolean, object or array")
            if not isinstance(parameter, str):
                parameters[name] = json.dumps(parameter, separators=(",", ":"), sort_keys=True, allow_nan=False)
    return value


def validate_message_shape(value: Any, message: Message, path: str) -> None:
    # UPB and pure-Python descriptors implement the same public descriptor API.
    validate_shape(value, cast("Descriptor", message.DESCRIPTOR), path)


def validate_shape(value: Any, descriptor: Descriptor, path: str) -> None:
    """Name a bad field and its shape without echoing potentially private values."""
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected object ({descriptor.name})")
    fields = {field.json_name: field for field in descriptor.fields}
    fields.update(descriptor.fields_by_name)
    seen: set[int] = set()
    groups: set[str] = set()
    for name, item in value.items():
        field = fields.get(name)
        location = f"{path}.{name}"
        if field is None:
            raise ValueError(f"{location}: unknown field; expected fields of {descriptor.name}")
        if field.number in seen:
            raise ValueError(f"{location}: duplicate field")
        seen.add(field.number)
        if field.containing_oneof:
            group = field.containing_oneof.name
            if group in groups:
                raise ValueError(f"{location}: expected one field from {group}")
            groups.add(group)
        if field.message_type and field.message_type.GetOptions().map_entry:
            if not isinstance(item, dict):
                raise ValueError(f"{location}: expected object (map)")
            for key, entry in item.items():
                _singular_shape(entry, field.message_type.fields_by_name["value"], f"{location}.{key}")
        elif field.is_repeated:
            if not isinstance(item, list):
                raise ValueError(f"{location}: expected array")
            for index, entry in enumerate(item):
                _singular_shape(entry, field, f"{location}[{index}]")
        else:
            _singular_shape(item, field, location)


def _singular_shape(value: Any, field: FieldDescriptor, path: str) -> None:
    if field.message_type:
        name = field.message_type.full_name
        if name == "google.protobuf.Value":
            return
        if name in {"google.protobuf.Duration", "google.protobuf.Timestamp"}:
            if not isinstance(value, str):
                raise ValueError(f"{path}: expected {field.message_type.name} string")
            return
        if name == "google.protobuf.ListValue":
            if not isinstance(value, list):
                raise ValueError(f"{path}: expected array")
            return
        if name == "google.protobuf.Struct":
            if not isinstance(value, dict):
                raise ValueError(f"{path}: expected object")
            return
        validate_shape(value, field.message_type, path)
        return
    if field.type in (FieldDescriptor.TYPE_STRING, FieldDescriptor.TYPE_BYTES):
        expected, valid = "string", isinstance(value, str)
    elif field.type == FieldDescriptor.TYPE_BOOL:
        expected, valid = "boolean", isinstance(value, bool)
    elif field.type == FieldDescriptor.TYPE_ENUM:
        expected, valid = "enum name or integer", isinstance(value, (str, int)) and not isinstance(value, bool)
    elif field.type in (FieldDescriptor.TYPE_FLOAT, FieldDescriptor.TYPE_DOUBLE):
        expected = "number"
        valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    else:
        expected = "integer"
        valid = False
        if isinstance(value, (int, float, str)) and not isinstance(value, bool):
            try:
                number = int(value)
                valid = isinstance(value, str) or number == value
                unsigned = field.type in {
                    FieldDescriptor.TYPE_UINT32,
                    FieldDescriptor.TYPE_UINT64,
                    FieldDescriptor.TYPE_FIXED32,
                    FieldDescriptor.TYPE_FIXED64,
                }
                wide = field.type in {
                    FieldDescriptor.TYPE_INT64,
                    FieldDescriptor.TYPE_UINT64,
                    FieldDescriptor.TYPE_SINT64,
                    FieldDescriptor.TYPE_FIXED64,
                    FieldDescriptor.TYPE_SFIXED64,
                }
                bits = 64 if wide else 32
                low, high = (0, 2**bits - 1) if unsigned else (-(2 ** (bits - 1)), 2 ** (bits - 1) - 1)
                valid = valid and low <= number <= high
            except (ValueError, OverflowError):
                pass
    if not valid:
        raise ValueError(f"{path}: expected {expected}")
