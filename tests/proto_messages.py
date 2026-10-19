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
