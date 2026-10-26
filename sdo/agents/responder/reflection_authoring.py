"""Version-matched authoring references and observed replay fixture materialization.

These are proposal-checkout helpers. The Go broker still owns acceptance and
independently compiles programs and executes mitigation replay on the fixture.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.responder import tool_output
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.tool_output import MAX_TOOL_REPLY_BYTES

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

OpenPhase = Literal["detect", "pre-apply"]
ReferenceTopic = Literal["programs", "sdk", "rules"]


class ResourceBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str
    namespace: str = ""
    name: str
    api_version: str = "v1"


class SdkGap(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fault_class: str = Field(min_length=1)
    primitive: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    scratch_script: str = Field(min_length=1)
    resources: list[ResourceBinding] = Field(default_factory=list)


class SessionObservations(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_json: str
    recordings_json: str = "[]"
    accepted_json: str | None = None


_SECTION_START = re.compile(r"^(?:#{1,3} |type |func |package )")


def authoring_reference(topic: ReferenceTopic = "programs", section: str = "") -> str:
    """Read shipped authoring references: a section index, or one section by heading or declaration.

    The full topic text is saved once to the file named in the reply; search
    that file instead of re-reading the reference. SDK includes detector
    source-diff access before playbook selection.
    """
    text = _reference_text(topic)
    path = _reference_file(topic, text)
    blocks = _sections(text)
    header = f"Reference topic {topic}: {len(text.encode('utf-8'))} bytes.\nFull text: {path}\n"
    if not section.strip():
        return header + "Sections:\n" + _index(blocks)
    wanted = section.strip().lower()
    matches = [block for block in blocks if wanted in block[0].lower()]
    if not matches:
        return header + f"No section matches {section!r}; search the full text or pick from:\n" + _index(blocks)
    reply = header
    for shown, block in enumerate(matches):
        part = "\n".join(block) + "\n\n"
        if len((reply + part).encode("utf-8")) > MAX_TOOL_REPLY_BYTES - 200:
            return reply + f"{len(matches) - shown} more matching sections are in the full text."
        reply += part
    return reply


def _reference_text(topic: ReferenceTopic) -> str:
    if topic == "sdk":
        paths = [
            SKILLS_ROOT / "sdo-playbook-authoring/references/program-sdk.md",
            SKILLS_ROOT / "sdo-playbook-authoring/references/kubernetes-client.md",
            SKILLS_ROOT / "sdo-detector-authoring/references/sdk.md",
        ]
    elif topic == "rules":
        paths = [
            SKILLS_ROOT / name / "references/rules.md"
            for name in (
                "sdo-memory",
                "sdo-detector-authoring",
                "sdo-playbook-authoring",
            )
        ]
    else:
        examples = Path(
            os.environ.get("SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR")
            or str(SKILLS_ROOT.parents[1] / "examples/playbook-programs")
        )
        paths = [SKILLS_ROOT / "sdo-playbook-authoring/references/programs.md"]
        paths.extend(
            examples / "playbooks/service-targetport-mismatch" / name
            for name in (
                "diagnose.go",
                "mitigate.go",
            )
        )
    return "\n\n".join(f"## {path.name}\n{path.read_text()}" for path in paths)


def _reference_file(topic: ReferenceTopic, text: str) -> Path:
    """Write each reference version once per session so repeated reads reuse one file."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    path = tool_output.output_directory(root=tool_output.session_output_root()) / f"reference-{topic}-{digest}.md"
    if not path.exists():
        temporary = path.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(text)
        temporary.replace(path)
    return path


def _sections(text: str) -> list[list[str]]:
    blocks: list[list[str]] = []
    for line in text.splitlines():
        if _SECTION_START.match(line) or not blocks:
            blocks.append([line])
        else:
            blocks[-1].append(line)
    return blocks


def _index(blocks: list[list[str]]) -> str:
    """Headings in full, Go declarations by name, grouped under their file."""
    lines: list[str] = []
    names: list[str] = []
    for block in blocks:
        title = block[0]
        if title.startswith("#"):
            if names:
                lines.append("  " + ", ".join(names))
                names = []
            lines.append(title)
        elif declared := re.match(r"(?:type|func|package) \w+", title):
            names.append(declared.group())
    if names:
        lines.append("  " + ", ".join(names))
    return "\n".join(lines)


def prepare_corpus(
    root: Path,
    captures: Path,
    incident_id: str,
    case_id: str,
    open_phase: OpenPhase,
    program_ids: list[str],
    bindings: dict[str, ResourceBinding],
    peer_bindings: dict[str, ResourceBinding],
) -> dict[str, str]:
    """Copy optional controller evidence and retain its capture attribution."""
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", case_id) or not incident_id:
        raise ValueError("corpus requires a logical case ID and a provisioned incident")
    if open_phase not in get_args(OpenPhase):
        raise ValueError("unknown open capture phase")
    target = root / ".sdo/corpus" / case_id
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError("corpus must stay inside the proposal checkout")
    # Read everything before creating the case, so absent evidence leaves no partial case.
    opened = (captures / f"open@{open_phase}.json").read_bytes()
    fixed = (captures / "fixed.json").read_bytes()
    recordings = (captures / f"open@{open_phase}-recordings.json").read_bytes()
    labels = {
        "root_cause_program_ids": program_ids,
        "bindings": {key: value.model_dump() for key, value in bindings.items()},
        "peer_bindings": {key: value.model_dump() for key, value in peer_bindings.items()},
        "provenance": {
            "incident_id": incident_id,
            "open_at": open_phase,
            "open_sha256": hashlib.sha256(opened).hexdigest(),
            "fixed_sha256": hashlib.sha256(fixed).hexdigest(),
        },
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir()
    for name, data in {
        "open.json": opened,
        "fixed.json": fixed,
        "healthy.json": fixed,
        "recordings.json": recordings,
        "labels.json": json.dumps(labels).encode(),
    }.items():
        (target / name).write_bytes(data)
    return {"case_id": case_id, "open_phase": open_phase}


def prepare_session_corpus(
    root: Path,
    incident_id: str,
    case_id: str,
    program_ids: list[str],
    bindings: dict[str, ResourceBinding],
    observations: SessionObservations,
) -> dict[str, str]:
    """Materialize observed session inputs without requiring a controller capture."""
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", case_id) or not incident_id:
        raise ValueError("corpus requires a logical case ID and a provisioned incident")
    if not isinstance(json.loads(observations.snapshot_json), dict):
        raise ValueError("session snapshot must be a JSON object")
    if not isinstance(json.loads(observations.recordings_json), list):
        raise ValueError("session recordings must be a JSON array")
    if observations.accepted_json is not None:
        json.loads(observations.accepted_json)
    target = root / ".sdo/corpus" / case_id
    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir()
    # The legacy context files retain the observed bytes. Only executing the
    # mitigation supplies a repaired post-state; these files prove no repair.
    for name in ("open.json", "fixed.json", "healthy.json"):
        (target / name).write_text(observations.snapshot_json)
    (target / "recordings.json").write_text(observations.recordings_json)
    (target / "labels.json").write_text(
        json.dumps(
            {
                "root_cause_program_ids": program_ids,
                "bindings": {key: value.model_dump() for key, value in bindings.items()},
                "peer_bindings": {},
                "provenance": {"incident_id": incident_id},
            }
        )
    )
    if observations.accepted_json is not None:
        (target / "accepted.json").write_text(observations.accepted_json)
    return {"case_id": case_id}


def install_authoring_tools(server: FastMCP, root: Path) -> None:
    server.add_tool(authoring_reference)

    @server.tool()
    def create_corpus(
        case_id: str,
        program_ids: list[str],
        bindings: dict[str, ResourceBinding],
        observations: SessionObservations | None = None,
        open_phase: OpenPhase = "detect",
        peer_bindings: dict[str, ResourceBinding] | None = None,
    ) -> dict[str, str]:
        """Create a positive replay fixture from recorded session observations or optional captures.

        Pass observations to use the snapshot and primitive replies already in
        the responder session; include accepted_json for source-reading plans.
        Missing controller captures do not block this mode. Without observations,
        copy the selected available capture and peer context. In both modes the
        save gate executes mitigation and requires diagnosis to become NoMatch.
        """
        if observations is not None:
            return prepare_session_corpus(
                root, os.environ.get("SDO_INCIDENT_ID", ""), case_id, program_ids, bindings, observations
            )
        return prepare_corpus(
            root,
            Path(os.environ.get("SDO_CAPTURED_CORPUS_DIR", "/sdo/session/corpus")),
            os.environ.get("SDO_INCIDENT_ID", ""),
            case_id,
            open_phase,
            program_ids,
            bindings,
            peer_bindings or {},
        )
