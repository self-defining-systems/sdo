"""The responder's model-output edge: parse its structured reply into the generated ``IncidentResult``.

The responder asks the model for a JSON document shaped by ``codex._incident_result_schema``
(a thin hand-pinned view of ``IncidentResult``; a parity test checks every property against
the proto descriptor). This module turns that reply, plus the turn's token accounting, into
the canonical generated message and applies the invariants a generated message cannot carry:
the usage cache-read alias normalization, timestamp ordering, and unique repair action IDs.
"""

from __future__ import annotations

from typing import Any

from google.protobuf import json_format

from sdo.contracts import IncidentResult, timestamp_datetime

SCHEMA_VERSION = "sdo.dev/v1alpha1"
INCIDENT_STATUSES = ("completed", "failed", "cancelled")
#: Live evidence kinds a root cause may cite. Static artifacts (manifests,
#: scripts, ConfigMap bodies, source files, architecture notes) show what
#: could go wrong, not what did, so they are only ``static_context``.
ROOT_CAUSE_EVIDENCE_KINDS = ("detector-finding", "synthetic-traffic", "state-change", "live-observation")


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
    """Check the invariants of ``result`` and normalize its usage alias in place."""

    if result.schema_version != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION!r}")
    if not result.incident_id:
        raise ValueError("incident_id is required")
    if result.status not in INCIDENT_STATUSES:
        raise ValueError(f"status must be one of {', '.join(INCIDENT_STATUSES)}")
    for cause in result.confirmed_root_causes:
        if not cause.summary or not cause.resources:
            raise ValueError("a confirmed root cause needs a summary and at least one resource")
        for evidence in cause.evidence:
            if evidence.kind not in ROOT_CAUSE_EVIDENCE_KINDS:
                raise ValueError(f"root cause evidence kind must be one of {', '.join(ROOT_CAUSE_EVIDENCE_KINDS)}")
            if not evidence.source or not evidence.observation:
                raise ValueError("root cause evidence needs a source and an observation")
    action_ids = [action.action_id for action in result.repair_actions]
    if len(action_ids) != len(set(action_ids)):
        raise ValueError("repair action IDs must be unique")
    for action in result.repair_actions:
        if timestamp_datetime(action.completed_at) < timestamp_datetime(action.started_at):
            raise ValueError("completed_at must not be before started_at")
    if not result.HasField("timing"):
        raise ValueError("timing is required")
    if timestamp_datetime(result.timing.completed_at) < timestamp_datetime(result.timing.started_at):
        raise ValueError("completed_at must not be before started_at")
    if not result.HasField("usage"):
        raise ValueError("usage is required")
    normalize_usage(result.usage)


def normalize_usage(usage: Any) -> None:
    """Apply agentshim's cache accounting rules, setting the deprecated alias from cache reads."""

    if usage.reasoning_output_tokens > usage.output_tokens:
        raise ValueError("reasoning_output_tokens must not exceed output_tokens")
    if usage.HasField("cache_write_1h_input_tokens") and (
        usage.cache_write_1h_input_tokens > usage.cache_write_input_tokens
    ):
        raise ValueError("cache_write_1h_input_tokens must not exceed cache_write_input_tokens")
    if not usage.HasField("cache_read_input_tokens"):
        # A record without cache_read_input_tokens predates the split (agentshim < 0.7): its
        # cached_input_tokens counted Claude cache writes too.
        if usage.cached_input_tokens > usage.input_tokens:
            raise ValueError("cached_input_tokens must not exceed input_tokens")
        if usage.cache_write_input_tokens > usage.cached_input_tokens:
            raise ValueError("cache_write_input_tokens must not exceed cached_input_tokens")
        return
    read = usage.cache_read_input_tokens
    if usage.cached_input_tokens == 0 and read:
        usage.cached_input_tokens = read
    if usage.cached_input_tokens != read:
        raise ValueError("cached_input_tokens is an alias of cache_read_input_tokens and must equal it")
    if read + usage.cache_write_input_tokens > usage.input_tokens:
        raise ValueError("cache reads plus cache writes must not exceed input_tokens")
    uncached = usage.input_tokens - read - usage.cache_write_input_tokens
    if usage.HasField("uncached_input_tokens") and usage.uncached_input_tokens != uncached:
        raise ValueError("uncached_input_tokens must equal input minus cache reads and writes")
