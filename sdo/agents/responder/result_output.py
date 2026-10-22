"""The responder's model-output edge: parse its structured reply into the generated ``IncidentResult``.

The responder asks the model for a JSON document shaped by ``codex._incident_result_schema``
(a thin hand-pinned view of ``IncidentResult``; a parity test checks every property against
the proto descriptor). This module turns that reply, plus the turn's token accounting, into
the canonical generated message, fills the deprecated usage alias, and enforces the proto's
protovalidate rules (which own every invariant, including timestamp ordering and unique
repair action IDs).
"""

from __future__ import annotations

from typing import Any

from google.protobuf import json_format

from sdo.contracts import SCHEMA_VERSION, IncidentResult, validate


def parse_incident_result(payload: dict[str, Any], usage: dict[str, Any]) -> IncidentResult:
    """Build and validate the result from the model's reply and the turn's usage record."""

    document = {"schema_version": SCHEMA_VERSION, **payload, "usage": usage}
    try:
        result = json_format.ParseDict(document, IncidentResult())
    except json_format.ParseError as exc:
        raise ValueError(str(exc)) from exc
    validate_incident_result(result)
    return result


def validate_incident_result(result: IncidentResult) -> None:
    """Normalize the usage alias in place, then enforce the proto's rules."""

    normalize_usage(result.usage)
    validate(result)


def normalize_usage(usage: Any) -> None:
    """Set the deprecated ``cached_input_tokens`` alias from cache reads; the proto's rules check the rest."""

    if usage.HasField("cache_read_input_tokens") and usage.cached_input_tokens == 0:
        usage.cached_input_tokens = usage.cache_read_input_tokens
