"""The responder's model-output edge: parse its structured reply into the generated ``IncidentResult``.

The responder asks the model for a JSON document shaped by ``codex._incident_result_schema``
(an ownership-filtered view of ``IncidentResult``). This module turns that reply, plus the turn's token accounting, into
the canonical generated message, fills the deprecated usage alias, and enforces the proto's
protovalidate rules (which own every invariant, including timestamp ordering and unique
repair action IDs).
"""

from __future__ import annotations

from typing import Any

import protovalidate
from google.protobuf import json_format

from sdo.contracts import SCHEMA_VERSION, AgentChange, IncidentFinding, IncidentResult, validate, worker_result_payload


class IncidentResultValidationError(ValueError):
    """The model reply violates the worker incident-result contract."""


def parse_incident_result(
    payload: dict[str, Any], usage: dict[str, Any], *, incident_id: str | None = None
) -> IncidentResult:
    """Build and validate the result from the model's reply and the turn's usage record."""

    document = {
        "schema_version": SCHEMA_VERSION,
        **worker_result_payload(payload, IncidentResult.DESCRIPTOR),
        "usage": usage,
    }
    notes = _drop_unparseable_entries(document)
    try:
        result = json_format.ParseDict(document, IncidentResult())
        notes += _normalize_advisory_fields(result)
        _record_notes(result, notes)
        validate_incident_result(result)
        if incident_id is not None and result.incident_id != incident_id:
            raise ValueError("result incident_id does not match request")
    except (json_format.ParseError, ValueError) as exc:
        raise IncidentResultValidationError(str(exc)) from exc
    return result


def validate_incident_result(result: IncidentResult) -> None:
    """Normalize the usage alias in place, then enforce the proto's rules."""

    normalize_usage(result.usage)
    validate(result)


def normalize_usage(usage: Any) -> None:
    """Set the deprecated ``cached_input_tokens`` alias from cache reads; the proto's rules check the rest."""

    if usage.HasField("cache_read_input_tokens") and usage.cached_input_tokens == 0:
        usage.cached_input_tokens = usage.cache_read_input_tokens


# Fields the agent writes as advisory evidence: they are stored and shown, and never decide closure, escalation or
# learning. A violation inside one is repaired (truncated, or the entry dropped) and noted; a violation anywhere
# else (status, ids, timing, repair receipts) still fails the result.
_ADVISORY_ENTRY_TYPES = {"findings": IncidentFinding, "agent_changes": AgentChange}
_ADVISORY_FIELDS = frozenset(
    {"findings", "agent_changes", "investigation_report", "repair_changes", "proposed_artifact_ids"}
)
_TRUNCATION_MARK = "..."
_MAX_NORMALIZE_PASSES = 8


def _drop_unparseable_entries(document: dict[str, Any]) -> list[str]:
    """Drop advisory entries that are not even parseable (a bad timestamp or enum) before the strict parse."""

    notes: list[str] = []
    for field, entry_type in _ADVISORY_ENTRY_TYPES.items():
        entries = document.get(field)
        if not isinstance(entries, list):
            continue
        kept = []
        for index, entry in enumerate(entries):
            try:
                json_format.ParseDict(entry, entry_type())
            except (json_format.ParseError, TypeError, AttributeError):
                notes.append(f"{field}[{index}] dropped: unparseable")
                continue
            kept.append(entry)
        document[field] = kept
    return notes


def _normalize_advisory_fields(result: IncidentResult) -> list[str]:
    """Repair violations confined to advisory fields; leave any other violation for the strict validation."""

    notes: list[str] = []
    for _ in range(_MAX_NORMALIZE_PASSES):
        try:
            protovalidate.validate(result)
            return notes
        except protovalidate.ValidationError as exc:
            violations = list(exc.violations)
        deletions: set[tuple[str, int]] = set()
        changed = False
        for violation in violations:
            field = violation.proto.field
            elements = list(field.elements) if field is not None else []
            if not elements or elements[0].field_name not in _ADVISORY_FIELDS:
                return notes
            changed |= _repair(result, elements, violation.proto.rule_id, deletions, notes)
        for field, index in sorted(deletions, key=lambda item: (item[0], -item[1])):
            del getattr(result, field)[index]
        if not changed and not deletions:
            return notes
    return notes


def _repair(
    result: IncidentResult, elements: list[Any], rule_id: str, deletions: set[tuple[str, int]], notes: list[str]
) -> bool:
    top = elements[0]
    target: Any = getattr(result, top.field_name)
    index = _index(top)
    if len(elements) == 1 and index is None:
        return _repair_leaf(result, top.field_name, rule_id, notes)
    if len(elements) == 1 and index is not None:
        return _repair_item(target, top.field_name, index, rule_id, deletions, notes)
    if index is None or len(elements) != 2 or top.field_name not in _ADVISORY_ENTRY_TYPES:
        return False
    entry = target[index]
    leaf = elements[1]
    path = f"{top.field_name}[{index}].{leaf.field_name}"
    value = getattr(entry, leaf.field_name)
    if rule_id == "string.max_len" and isinstance(value, str):
        setattr(entry, leaf.field_name, _truncate(value, _max_len(entry, leaf.field_name)))
        notes.append(f"{path} truncated")
        return True
    deletions.add((top.field_name, index))
    notes.append(f"{top.field_name}[{index}] dropped: {leaf.field_name} invalid")
    return True


def _index(element: Any) -> int | None:
    subscript = element.subscript
    return int(subscript.value) if subscript is not None and subscript.field == "index" else None


def _repair_leaf(result: IncidentResult, field: str, rule_id: str, notes: list[str]) -> bool:
    value = getattr(result, field)
    if rule_id == "string.max_len" and isinstance(value, str):
        setattr(result, field, _truncate(value, _max_len(result, field)))
        notes.append(f"{field} truncated")
        return True
    if rule_id == "repeated.max_items":
        limit = _max_items(result, field)
        del value[limit:]
        notes.append(f"{field} capped at {limit} entries")
        return True
    return False


def _repair_item(
    target: Any, field: str, index: int, rule_id: str, deletions: set[tuple[str, int]], notes: list[str]
) -> bool:
    deletions.add((field, index))
    notes.append(f"{field}[{index}] dropped: {rule_id}")
    return True


def _rule(message: Any, field: str) -> Any:
    from buf.validate import validate_pb2

    options = message.DESCRIPTOR.fields_by_name[field].GetOptions()
    return options.Extensions[validate_pb2.field]


def _max_len(message: Any, field: str) -> int:
    return int(_rule(message, field).string.max_len)


def _max_items(message: Any, field: str) -> int:
    return int(_rule(message, field).repeated.max_items)


def _truncate(value: str, limit: int) -> str:
    return value[: limit - len(_TRUNCATION_MARK)] + _TRUNCATION_MARK


def _record_notes(result: IncidentResult, notes: list[str]) -> None:
    """Keep what was repaired in the outcome as one finding, so a later reader sees the result was normalized."""

    if not notes:
        return
    statement = _truncate("Result normalized by SDO: " + "; ".join(notes), 300)
    note = IncidentFinding(kind="observation", statement=statement, seen_in="result validation")
    limit = _max_items(result, "findings")
    del result.findings[limit - 1 :]
    result.findings.append(note)
