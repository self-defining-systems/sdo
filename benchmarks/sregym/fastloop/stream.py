"""Ordered incident streams for one warm application deployment."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.faults.description import CapabilityMode


@dataclass(frozen=True)
class StreamConfig:
    incidents: tuple[tuple[str, ...], ...]
    diagnosis_grading: str = "off"
    judge_model_id: str = "codex-gpt-6-luna"
    capability_mode: CapabilityMode = "strict"
    permissive_responder: bool = False
    responder_namespaces: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.capability_mode not in ("strict", "permissive"):
            raise ValueError("capability_mode must be strict or permissive")
        if type(self.permissive_responder) is not bool:
            raise TypeError("permissive_responder must be a boolean")
        if any(not isinstance(ns, str) or not ns for ns in self.responder_namespaces):
            raise ValueError("responder_namespaces must contain namespace names")
        if self.responder_namespaces and not self.permissive_responder:
            raise ValueError("responder_namespaces requires permissive_responder")


def load_stream_config(path: Path) -> StreamConfig:
    """Read incidents and optional run settings from a JSON stream."""

    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        mode = data.get("diagnosis_grading", "off")
        model = data.get("judge_model_id", "codex-gpt-6-luna")
        entries = data.get("incidents")
    else:
        mode, model, entries = "off", "codex-gpt-6-luna", data
    if mode not in ("off", "deferred", "inline"):
        raise ValueError("diagnosis_grading must be off, deferred, or inline")
    if not isinstance(model, str) or not model.strip():
        raise ValueError("judge_model_id must be a non-empty string")
    if not isinstance(entries, list) or not entries:
        raise ValueError("stream must contain a non-empty JSON array")
    incidents: list[tuple[str, ...]] = []
    for index, entry in enumerate(entries):
        ids = [entry] if isinstance(entry, str) else entry
        if (
            not isinstance(ids, list)
            or not ids
            or any(not isinstance(item, str) or not item.strip() for item in ids)
            or len(ids) != len(set(ids))
        ):
            raise ValueError(f"stream incident {index} must contain distinct non-empty problem IDs")
        incidents.append(tuple(ids))
    options = (
        {key: data[key] for key in ("capability_mode", "permissive_responder", "responder_namespaces") if key in data}
        if isinstance(data, dict)
        else {}
    )
    if "responder_namespaces" in options:
        if not isinstance(options["responder_namespaces"], list):
            raise TypeError("responder_namespaces must be an array")
        options["responder_namespaces"] = tuple(options["responder_namespaces"])
    return StreamConfig(tuple(incidents), mode, model, **options)


def load_stream(path: Path) -> tuple[tuple[str, ...], ...]:
    return load_stream_config(path).incidents
