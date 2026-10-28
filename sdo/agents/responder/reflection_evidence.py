"""Bounded, outcome-bound browsing of full immutable controller captures."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, get_args

from pydantic import BaseModel, Field

from sdo.agents.responder.reflection_authoring import OpenPhase

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

    from sdo.contracts import OperationalMemorySnapshot

CapturePhase = OpenPhase | Literal["fixed"]
CaptureSection = Literal[
    "configMaps",
    "services",
    "pods",
    "deployments",
    "statefulSets",
    "replicaSets",
    "horizontalPodAutoscalers",
    "podDisruptionBudgets",
    "endpoints",
    "endpointSlices",
    "networkPolicies",
    "events",
    "recordings",
]
_MAX_CHARS = 12_000


class ResourceSummary(BaseModel):
    count: int
    names: list[str]
    omitted: int


class CaptureSummary(BaseModel):
    file: str
    sha256: str
    bytes: int
    namespace: str
    resources: dict[str, ResourceSummary]
    recorded_methods: dict[str, int] = Field(default_factory=dict)


class CaptureInventory(BaseModel):
    incident_id: str
    accepted_commit: str
    captures: list[CaptureSummary]


class RecordingEntry(BaseModel):
    index: int
    phase_index: int
    phase: str
    method: str
    bound_object: dict[str, str] | None = None
    error: str
    error_omitted_characters: int


class RecordingIndex(BaseModel):
    incident_id: str
    accepted_commit: str
    sha256: str
    items: list[RecordingEntry]
    total: int
    next_offset: int | None = None


class CapturePage(BaseModel):
    incident_id: str
    accepted_commit: str
    sha256: str
    items: list[Any] = Field(default_factory=list)
    total: int
    next_offset: int | None = None
    oversized_indices: list[int] = Field(default_factory=list)


def capture_inventory(captures: Path) -> dict[str, Any]:
    summaries = []
    filenames = ["fixed.json"]
    for phase in get_args(OpenPhase):
        filenames.extend((f"open@{phase}.json", f"open@{phase}-recordings.json"))
    for filename in sorted(filenames):
        file = captures / filename
        if not file.is_file():
            continue
        raw = file.read_bytes()
        snapshot = json.loads(raw)
        resources = {}
        if isinstance(snapshot, dict):
            for kind, items in snapshot.items():
                if isinstance(items, list):
                    names = [str(item.get("metadata", {}).get("name", ""))[:80] for item in items[:8]]
                    resources[kind] = ResourceSummary(count=len(items), names=names, omitted=max(0, len(items) - 8))
        elif isinstance(snapshot, list):
            resources["recordings"] = ResourceSummary(count=len(snapshot), names=[], omitted=len(snapshot))
        summaries.append(
            CaptureSummary(
                file=file.name,
                sha256=hashlib.sha256(raw).hexdigest(),
                bytes=len(raw),
                namespace=snapshot.get("namespace", "") if isinstance(snapshot, dict) else "",
                resources=resources,
                recorded_methods=dict(sorted(Counter(call["method"] for call in snapshot).items()))
                if isinstance(snapshot, list)
                else {},
            )
        )
    return CaptureInventory(
        incident_id=os.environ.get("SDO_INCIDENT_ID", ""),
        accepted_commit=os.environ.get("SDO_BASE_COMMIT", ""),
        captures=summaries,
    ).model_dump()


def recording_index(
    captures: Path,
    *,
    open_phase: OpenPhase = "detect",
    method: str | None = None,
    phase: str | None = None,
    offset: int = 0,
    limit: int = 64,
) -> dict[str, Any]:
    """Index metadata across the entire recording; never load result bodies into context."""
    if offset < 0 or not 1 <= limit <= 100:
        raise ValueError("recording pages require a nonnegative offset and limit between 1 and 100")
    raw = (captures / f"open@{open_phase}-recordings.json").read_bytes()
    calls = []
    phase_counts: dict[str, int] = {}
    for index, call in enumerate(json.loads(raw)):
        recorded_phase = call.get("phase") or "open"
        phase_index = phase_counts.get(recorded_phase, 0)
        phase_counts[recorded_phase] = phase_index + 1
        if (method is None or call["method"] == method) and (phase is None or recorded_phase == phase):
            calls.append((index, phase_index, call))
    entries = []
    for index, phase_index, call in calls[offset : offset + limit]:
        arguments = call.get("arguments") or []
        ref = arguments[0] if arguments and isinstance(arguments[0], dict) else None
        error = call.get("error") or ""
        entries.append(
            RecordingEntry(
                index=index,
                phase_index=phase_index,
                phase=call.get("phase") or "open",
                method=call["method"],
                bound_object={key: str(ref[key]) for key in ("api_version", "kind", "namespace", "name") if key in ref}
                if ref is not None
                else None,
                error=error[:256],
                error_omitted_characters=max(0, len(error) - 256),
            )
        )
    stop = offset + len(entries)
    return RecordingIndex(
        incident_id=os.environ.get("SDO_INCIDENT_ID", ""),
        accepted_commit=os.environ.get("SDO_BASE_COMMIT", ""),
        sha256=hashlib.sha256(raw).hexdigest(),
        items=entries,
        total=len(calls),
        next_offset=stop if stop < len(calls) else None,
    ).model_dump()


def _field(item: Any, field: str) -> Any:
    value = item
    for part in field.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list) and part.isdecimal() and int(part) < len(value):
            value = value[int(part)]
        else:
            return None
    return value


def read_capture(
    captures: Path,
    phase: CapturePhase,
    section: CaptureSection,
    *,
    name: str | None = None,
    namespace: str | None = None,
    fields: list[str] | None = None,
    open_phase: OpenPhase = "detect",
    offset: int = 0,
    limit: int = 8,
) -> dict[str, Any]:
    if not 1 <= limit <= 20 or offset < 0:
        raise ValueError("capture pages require a nonnegative offset and limit between 1 and 20")
    filename = (
        f"open@{open_phase if phase == 'fixed' else phase}-recordings.json"
        if section == "recordings"
        else ("fixed.json" if phase == "fixed" else f"open@{phase}.json")
    )
    raw = (captures / filename).read_bytes()
    snapshot = json.loads(raw)
    items = snapshot if section == "recordings" else snapshot.get(section) or []
    if section == "recordings":
        items = [item for item in items if (item.get("phase") or "open") == ("fixed" if phase == "fixed" else "open")]
    if name is not None:
        items = [item for item in items if item.get("metadata", {}).get("name") == name]
    if namespace is not None:
        items = [item for item in items if item.get("metadata", {}).get("namespace") == namespace]
    page = CapturePage(
        incident_id=os.environ.get("SDO_INCIDENT_ID", ""),
        accepted_commit=os.environ.get("SDO_BASE_COMMIT", ""),
        sha256=hashlib.sha256(raw).hexdigest(),
        total=len(items),
    )
    budget = 0
    stop = min(len(items), offset + limit)
    for index in range(offset, stop):
        value = {field: _field(items[index], field) for field in fields} if fields else items[index]
        size = len(json.dumps(value))
        if size > _MAX_CHARS:
            page.oversized_indices.append(index)
            continue
        if budget + size > _MAX_CHARS:
            stop = index
            break
        page.items.append(value)
        budget += size
    if stop < len(items):
        page.next_offset = stop
    return page.model_dump()


def playbook_description(markdown: str) -> str:
    """Prefer authored one-line metadata; index old accepted documents too."""
    import yaml

    if markdown.startswith("---\n"):
        sections = markdown.split("---\n", 2)
        if len(sections) == 3:
            metadata = yaml.safe_load(sections[1])
            if isinstance(metadata, dict) and isinstance(metadata.get("description"), str):
                return " ".join(metadata["description"].split())
            markdown = sections[2]
    return next(
        (line.lstrip("# ").strip() for line in markdown.splitlines() if line.startswith("#")),
        "Description absent; open README.",
    )


def memory_view(snapshot: OperationalMemorySnapshot, *, linked_ids: set[str] | None = None) -> dict[str, Any]:
    """Every logical ID and description, incident-linked first; bodies stay in files."""
    linked = linked_ids or set()
    return {
        "accepted_commit": snapshot.accepted_commit,
        "detectors": [
            {
                "id": item.id,
                "description": " ".join(item.description.split()),
                "playbook_ids": list(item.possible_playbook_ids),
            }
            for item in sorted(snapshot.detectors, key=lambda item: (item.id not in linked, item.id))
        ],
        "playbooks": [
            {"id": item.id, "description": playbook_description(item.markdown)}
            for item in sorted(snapshot.playbooks, key=lambda item: (item.id not in linked, item.id))
        ],
        "full_text": "Open only relevant accepted files in the proposal checkout.",
    }


def install_evidence_tools(server: FastMCP) -> None:
    def directory() -> Path:
        return Path(os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus"))

    @server.tool()
    def evidence_inventory() -> CaptureInventory:
        """List capture digests, counts and a few resource names, bound to the accepted outcome.

        Recorded method counts cover the whole file. Use recordings_index to locate
        primitive calls without reading result payloads. Full immutable evidence
        remains available through capture_fields and to
        independent validation. Read relevant fields rather than entire captures.
        """
        return CaptureInventory.model_validate(capture_inventory(directory()))

    @server.tool()
    def capture_fields(
        phase: CapturePhase,
        section: CaptureSection,
        name: str | None = None,
        namespace: str | None = None,
        fields: list[str] | None = None,
        open_phase: OpenPhase = "detect",
        offset: int = 0,
        limit: int = 8,
    ) -> CapturePage:
        """Read captured resource fields or primitive recordings on demand.

        Fields are dotted paths; list indices are numeric (spec.containers.0.image).
        Null collections are empty. Results are paginated and bounded to 12000
        characters; oversized_indices identifies items needing narrower fields.
        Missing fields are null, never evidence that a primitive succeeded.
        """
        return CapturePage.model_validate(
            read_capture(
                directory(),
                phase,
                section,
                name=name,
                namespace=namespace,
                fields=fields,
                open_phase=open_phase,
                offset=offset,
                limit=limit,
            )
        )

    @server.tool()
    def recordings_index(
        open_phase: OpenPhase = "detect",
        method: str | None = None,
        phase: str | None = None,
        offset: int = 0,
        limit: int = 64,
    ) -> RecordingIndex:
        """Index primitive calls by original position, phase, method, bound object and error.

        Filters search the entire recording. next_offset marks incomplete pages;
        a truncated page does not establish that a primitive was unrecorded.
        Empty recorded phases mean open. phase_index locates the call within that
        phase's capture_fields page. Error excerpts report omitted characters.
        open_phase selects the protected recording file, including its fixed observations.
        Result payloads remain available through capture_fields.
        """
        return RecordingIndex.model_validate(
            recording_index(directory(), open_phase=open_phase, method=method, phase=phase, offset=offset, limit=limit)
        )
