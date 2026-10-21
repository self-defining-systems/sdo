"""Helpers for tests that build and vary generated contract messages."""

from __future__ import annotations

from typing import Any, TypeVar

from google.protobuf.message import Message

M = TypeVar("M", bound=Message)


def evolve(message: M, **updates: Any) -> M:
    """A copy of ``message`` with ``updates`` applied; ``None`` clears a field.

    Repeated fields are replaced by the given sequence and message fields are copied from the given message.
    """

    clone = type(message)()
    clone.CopyFrom(message)
    for name, value in updates.items():
        if value is None:
            clone.ClearField(name)
            continue
        current = getattr(clone, name)
        if hasattr(current, "extend"):
            del current[:]
            current.extend(value)
        elif isinstance(current, Message):
            current.CopyFrom(value)
        else:
            setattr(clone, name, value)
    return clone


def from_dict(message_type: type[M], value: Any) -> M:
    """Build a generated message from JSON-shaped test evidence."""
    from datetime import datetime, timedelta
    from enum import Enum

    from google.protobuf import json_format

    def normalize(item: Any) -> Any:
        if isinstance(item, Message):
            return json_format.MessageToDict(item, preserving_proto_field_name=True)
        if isinstance(item, datetime):
            return item.isoformat()
        if isinstance(item, timedelta):
            return f"{item.total_seconds():g}s"
        if isinstance(item, Enum):
            return item.value
        if isinstance(item, dict):
            return {key: normalize(child) for key, child in item.items() if child is not None}
        if isinstance(item, (list, tuple)):
            return [normalize(child) for child in item]
        return item

    return json_format.ParseDict(normalize(value), message_type())
