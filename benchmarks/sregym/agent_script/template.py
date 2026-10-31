"""Closed task references, typed whole-value substitution, and injected step clock."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

from sdo.contracts import (
    ArchitectureRequest,
    HealthJudgeRequest,
    IncidentRequest,
    ReflectionRequest,
    SourceDeploymentRequest,
)

TOKEN = re.compile(r"\{\{([a-z][^{}]*)\}\}")
RESOURCE = r"(?:api_version|kind|namespace|name)"
BINDINGS = rf"parameter_bindings(?:\.[a-zA-Z_][a-zA-Z0-9_]*(?:\.{RESOURCE})?)?"
FINDING = (
    rf"(?:detector_id|rule_id|status|severity|summary|evidence|fingerprint|primary_resource(?:\.{RESOURCE})?|"
    rf"{BINDINGS}|playbook_ids(?:\.[0-9]+)?)"
)
NESTED = re.compile(
    rf"request\.(?:findings\.[0-9]+(?:\.{FINDING})?|surfaced_playbooks\.[0-9]+(?:\.(?:playbook_id|{BINDINGS}))?)"
)
SCALARS = {
    field.name
    for cls in (ArchitectureRequest, HealthJudgeRequest, IncidentRequest, ReflectionRequest, SourceDeploymentRequest)
    for field in cls.DESCRIPTOR.fields
    if field.message_type is None and not field.is_repeated
}


def validate_references(value: Any) -> None:
    if isinstance(value, str):
        for match in TOKEN.finditer(value):
            name = match[1]
            if not (
                name in {"now", "turn.started_at", "checkout", "incident_id", "objective_digest", "request"}
                or re.fullmatch(r"steps\.[0-9]+\.(?:started_at|completed_at|output(?:\.[a-zA-Z0-9_]+)*)", name)
                or NESTED.fullmatch(name)
                or name.startswith("request.")
                and name[8:] in SCALARS
            ):
                raise ValueError(f"unknown script reference: {name}")
        if re.search(r"\{\{[a-z]", TOKEN.sub("", value)):
            raise ValueError("malformed script reference")
    elif isinstance(value, list):
        for item in value:
            validate_references(item)
    elif isinstance(value, dict):
        for item in value.values():
            validate_references(item)


def validate_step_references(value: Any, *, limit: int) -> None:
    if isinstance(value, str):
        for match in TOKEN.finditer(value):
            capture = re.fullmatch(r"steps\.([0-9]+)\.(?:started_at|completed_at|output(?:\.[a-zA-Z0-9_]+)*)", match[1])
            if capture and int(capture[1]) >= limit:
                raise ValueError(f"unavailable step reference: {match[1]}")
    elif isinstance(value, (list, dict)):
        for item in value.values() if isinstance(value, dict) else value:
            validate_step_references(item, limit=limit)


def expand(value: Any, values: dict[str, Any]) -> Any:
    if isinstance(value, str):

        def resolve(name: str) -> Any:
            if name not in values:
                raise ValueError(f"unavailable script placeholder: {name}")
            return values[name]

        whole = TOKEN.fullmatch(value)
        if whole:
            return resolve(whole[1])

        def replace(match: re.Match[str]) -> str:
            item = resolve(match[1])
            if isinstance(item, (dict, list)):
                raise ValueError(f"structured reference requires a whole value: {match[1]}")
            return str(item)

        return TOKEN.sub(replace, value)
    if isinstance(value, list):
        return [expand(item, values) for item in value]
    if isinstance(value, dict):
        return {key: expand(item, values) for key, item in value.items()}
    return value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class TemplateContext:
    def __init__(
        self,
        request: dict[str, Any],
        checkout: Path,
        *,
        clock: Callable[[], str] = utc_now,
        incident: str | None = None,
    ) -> None:
        self.clock = clock
        self.values: dict[str, Any] = {"checkout": str(checkout), "turn.started_at": clock()}

        def normalize(value: Any, *, mapping: bool = False) -> Any:
            if isinstance(value, dict):
                return {
                    (key if mapping else re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()): normalize(
                        item, mapping=key in {"parameterBindings", "parameter_bindings", "metadata"}
                    )
                    for key, item in value.items()
                }
            if isinstance(value, list):
                return [normalize(item) for item in value]
            return value

        # Binding names are user keys, not protobuf fields; preserve their spelling.
        def flatten(prefix: str, value: Any) -> None:
            self.values[prefix] = value
            if isinstance(value, dict):
                for key, item in value.items():
                    flatten(f"{prefix}.{key}", item)
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    flatten(f"{prefix}.{index}", item)

        normalized = normalize(request)
        flatten("request", normalized)
        incident = normalized.get("incident_id", incident)
        if incident is not None:
            self.values["incident_id"] = incident
        objective = normalized.get("health_objective")
        if objective is not None:
            self.values["objective_digest"] = hashlib.sha256(objective.strip().encode()).hexdigest()

    def expand(self, value: Any, *, now: str) -> Any:
        return expand(value, {**self.values, "now": now})
