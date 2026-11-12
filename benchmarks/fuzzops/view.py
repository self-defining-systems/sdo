"""SDO's operator view for the fuzzops-bench dashboard: ``operator/view.json`` and ``operator/view/transcripts/``.

The ``collect`` hook calls ``write(dest)`` last; ``python -m benchmarks.fuzzops.view RUN_DIR [...]`` regenerates the
two outputs of existing run directories. Both read only the collected copy under ``dest`` (``evidence/``,
``memory/.sdo/``, ``controller-state.json``) and the run's ``run.json`` / ``record.json`` beside it; they never read
the cluster or a Secret and write nothing but ``dest/view.json`` and ``dest/view/``.

``view.json`` is ``fuzzops.view/v1`` (fuzzops repository, ``docs/bench.md``): a scalar ``summary``, ``hide`` globs,
timeline ``events``, and ``panels`` (tables and transcripts). The dataclasses below hold the contract's limits, so
anything written is valid. Transcripts are normalized ``fuzzops.transcript/v1`` JSONL built from Codex
``rollout-*.jsonl`` files (``response_item`` entries only) and redacted (``redact``) before they are written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops import sink, table

if TYPE_CHECKING:
    from collections.abc import Iterable

SCHEMA = "fuzzops.view/v1"
TRANSCRIPT_SCHEMA = "fuzzops.transcript/v1"
VIEW_FILE = "view.json"
VIEW_DIR = "view"
TRANSCRIPT_DIR = f"{VIEW_DIR}/transcripts"

MAX_SUMMARY = 20
MAX_SUMMARY_VALUE = 200
MAX_HIDE = 50
MAX_EVENTS = 2000
MAX_LANES = 6
MAX_LANE = 24
MAX_EVENT_TEXT = 500
MAX_PANELS = 50
MAX_COLUMNS = 20
MAX_ROWS = 5000
MAX_ENTRIES = 20000
OUTPUT_LIMIT = 4000
ENTRY_LIMIT = 20000
SUMMARY_KEY = re.compile(r"^[a-z0-9_]{1,40}$")
ROLES = frozenset({"user", "assistant", "reasoning", "tool", "output", "note"})

HIDE = ["evidence/**/rollout-*.jsonl", "evidence/**/model_io/**", "controller.log"]

REDACTED = "[redacted]"

LANES = ("detect", "stage", "act", "close", "learn")


class ViewError(ValueError):
    """A view that breaks the ``fuzzops.view/v1`` contract."""


# ---- redaction -----------------------------------------------------------------------------------------------

_KEY = (
    r"[A-Za-z0-9_.-]*(?:token(?!s\b|s[_-]|_count|_usage|_limit|_budget)|secret|password|passwd|api[_-]?key|apikey"
    r"|authorization|cookie|client[_-]key[_-]data|private[_-]?key)[A-Za-z0-9_.-]*"
)
_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]+-----.*?(?:-----END [A-Z0-9 ]+-----|\Z)", re.DOTALL)
_JWT = re.compile(r"\beyJ[\w-]{5,}\.eyJ[\w-]{5,}\.[\w-]*")
_PREFIXED = re.compile(
    r"\bsk-[A-Za-z0-9_-]{16,}|\bghp_[A-Za-z0-9]{20,}|\bgithub_pat_[A-Za-z0-9_]{20,}|\bxox[abp]-[A-Za-z0-9-]{10,}"
    r"|\bAKIA[0-9A-Z]{16}\b"
)
# "key": "value" in plain or backslash-escaped JSON (a JSON document inside a JSON string).
_QUOTED = re.compile(
    r"(\\?[\"']" + _KEY + r"\\?[\"']\s*:\s*\\?[\"'])((?:[^\"'\\]|\\[^\"'])*)(?=\\?[\"'])", re.IGNORECASE
)
# Unquoted pairs: a colon or an equals sign between key and value.
_PLAIN = re.compile(
    r"(?<![\w-])(" + _KEY + r"[ \t]*[:=][ \t]*)(?!\[redacted\])((?:Bearer|Basic)[ \t]+\S+|[^\s\"',;&]+)", re.IGNORECASE
)
_SCHEME = re.compile(r"\b(Bearer|Basic)[ \t]+([A-Za-z0-9._~+/=-]{8,})")
_BASE64_FIELD = re.compile(
    r"((?:[\w.-]*-data|data|secret)[ \t]*[:=][ \t]*[\"']?)[A-Za-z0-9+/]{40,}={0,2}", re.IGNORECASE
)


def _scheme(match: re.Match[str]) -> str:
    token = match.group(2)
    if re.fullmatch(r"[A-Za-z][a-z]*", token):  # an ordinary word after "Bearer"/"Basic"
        return match.group(0)
    return f"{match.group(1)} {REDACTED}"


def _plain(match: re.Match[str]) -> str:
    value = match.group(2)
    if re.fullmatch(r"\d{1,9}", value):  # a count or limit, not a credential
        return match.group(0)
    return match.group(1) + REDACTED


def redact(text: str, identities: Iterable[str] = ()) -> str:
    """``text`` without credentials: secret-named key values, bearer/basic credentials, JWTs, well-known API keys,
    PEM blocks, long base64 values in data/secret fields and the given account identifiers."""
    for ident in sorted({i for i in identities if len(i) >= 4}, key=len, reverse=True):
        text = text.replace(ident, REDACTED)
    text = _PEM.sub(REDACTED, text)
    text = _JWT.sub(REDACTED, text)
    text = _PREFIXED.sub(REDACTED, text)
    text = _QUOTED.sub(lambda m: m.group(1) + REDACTED, text)
    text = _PLAIN.sub(_plain, text)
    text = _SCHEME.sub(_scheme, text)
    return _BASE64_FIELD.sub(lambda m: m.group(1) + REDACTED, text)


# ---- the contract ----------------------------------------------------------------------------------------------


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "\u2026"


def _rel_ok(path: str) -> bool:
    pure = PurePosixPath(path)
    return bool(path) and not pure.is_absolute() and ".." not in pure.parts and "\\" not in path


def _check_file(path: str, what: str) -> None:
    if not isinstance(path, str) or not _rel_ok(path):
        raise ViewError(f"{what}: {path!r} is not a relative path without '..'")


def _scalar(value: Any) -> bool:
    return value is None or isinstance(value, str | bool) or (isinstance(value, int | float) and math.isfinite(value))


@dataclass(frozen=True)
class FileCell:
    """A table cell linking a collected file."""

    text: str
    file: str

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            raise ViewError("cell text must be a string")
        _check_file(self.file, "cell file")

    def to_json(self) -> dict[str, str]:
        return {"text": self.text, "file": self.file}


Cell = str | int | float | bool | FileCell | None


@dataclass(frozen=True)
class Event:
    at: str
    lane: str
    text: str
    end: str | None = None
    component: str | None = None
    file: str | None = None

    def __post_init__(self) -> None:
        if parse(self.at) is None or (self.end is not None and parse(self.end) is None):
            raise ViewError(f"event times must be RFC 3339, got {self.at!r}/{self.end!r}")
        if not 0 < len(self.lane) <= MAX_LANE:
            raise ViewError(f"event lane {self.lane!r} must be 1..{MAX_LANE} characters")
        if len(self.text) > MAX_EVENT_TEXT:
            raise ViewError(f"event text is longer than {MAX_EVENT_TEXT} characters")
        if self.file is not None:
            _check_file(self.file, "event file")

    def to_json(self) -> dict[str, str]:
        out = {"at": self.at, "lane": self.lane, "text": self.text}
        for name in ("end", "component", "file"):
            if getattr(self, name):
                out[name] = getattr(self, name)
        return out


@dataclass(frozen=True)
class TablePanel:
    title: str
    columns: list[str]
    rows: list[list[Cell]]

    def __post_init__(self) -> None:
        if not 0 < len(self.columns) <= MAX_COLUMNS:
            raise ViewError(f"table {self.title!r} needs 1..{MAX_COLUMNS} columns")
        if len(self.rows) > MAX_ROWS:
            raise ViewError(f"table {self.title!r} has more than {MAX_ROWS} rows")
        for row in self.rows:
            if len(row) != len(self.columns):
                raise ViewError(f"table {self.title!r}: a row has {len(row)} cells for {len(self.columns)} columns")
            if not all(isinstance(c, FileCell) or _scalar(c) for c in row):
                raise ViewError(f"table {self.title!r}: a cell is not a scalar or a file link")

    def files(self) -> list[str]:
        return [c.file for row in self.rows for c in row if isinstance(c, FileCell)]

    def to_json(self) -> dict[str, Any]:
        rows = [[c.to_json() if isinstance(c, FileCell) else c for c in row] for row in self.rows]
        return {"type": "table", "title": self.title, "columns": self.columns, "rows": rows}


@dataclass(frozen=True)
class TranscriptPanel:
    title: str
    file: str

    def __post_init__(self) -> None:
        _check_file(self.file, "transcript file")

    def files(self) -> list[str]:
        return [self.file]

    def to_json(self) -> dict[str, str]:
        return {"type": "transcript", "title": self.title, "file": self.file}


Panel = TablePanel | TranscriptPanel


@dataclass(frozen=True)
class View:
    summary: dict[str, str | int | float | bool] = field(default_factory=dict)
    hide: list[str] = field(default_factory=lambda: list(HIDE))
    events: list[Event] = field(default_factory=list)
    panels: list[Panel] = field(default_factory=list)

    def __post_init__(self) -> None:
        if len(self.summary) > MAX_SUMMARY:
            raise ViewError(f"summary has more than {MAX_SUMMARY} keys")
        for key, value in self.summary.items():
            if not SUMMARY_KEY.match(key):
                raise ViewError(f"summary key {key!r} does not match [a-z0-9_]{{1,40}}")
            if isinstance(value, str):
                if len(value) > MAX_SUMMARY_VALUE:
                    raise ViewError(f"summary {key!r} is longer than {MAX_SUMMARY_VALUE} characters")
            elif not (isinstance(value, bool | int | float) and _scalar(value)):
                raise ViewError(f"summary {key!r} must be a string, number or bool")
        if len(self.hide) > MAX_HIDE:
            raise ViewError(f"hide has more than {MAX_HIDE} globs")
        if len(self.events) > MAX_EVENTS:
            raise ViewError(f"more than {MAX_EVENTS} events")
        if len({e.lane for e in self.events}) > MAX_LANES:
            raise ViewError(f"events use more than {MAX_LANES} lanes")
        if len(self.panels) > MAX_PANELS:
            raise ViewError(f"more than {MAX_PANELS} panels")

    def files(self) -> list[str]:
        return [e.file for e in self.events if e.file] + [f for p in self.panels for f in p.files()]

    def check_files(self, dest: Path) -> None:
        """Every referenced file must exist below ``dest``."""
        for rel in self.files():
            if not (dest / rel).is_file():
                raise ViewError(f"{rel} does not exist")

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "summary": self.summary,
            "hide": self.hide,
            "events": [e.to_json() for e in self.events],
            "panels": [p.to_json() for p in self.panels],
        }


@dataclass(frozen=True)
class Entry:
    """One transcript line."""

    role: str
    text: str
    at: str | None = None
    title: str | None = None

    def __post_init__(self) -> None:
        if self.role not in ROLES:
            raise ViewError(f"transcript role {self.role!r} is not one of {sorted(ROLES)}")

    def to_json(self) -> dict[str, str]:
        out = {"role": self.role, "text": self.text}
        if self.at:
            out = {"at": self.at, **out}
        if self.title:
            out["title"] = self.title
        return out


# ---- times -----------------------------------------------------------------------------------------------------


def parse(value: Any) -> datetime | None:
    return sink.parse_time(value)


def stamp(value: Any) -> str | None:
    """``value`` as an RFC 3339 UTC time (microseconds), or None."""
    parsed = parse(value)
    if parsed is None:
        return None
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


# ---- reading the collected copy ------------------------------------------------------------------------------------


def _snake(value: Any) -> Any:
    """ProtoJSON (camelCase) keys as proto field names, recursively."""
    if isinstance(value, dict):
        return {
            re.sub(r"(?<=[a-z0-9])([A-Z])", lambda m: "_" + m.group(1).lower(), str(k)): _snake(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_snake(v) for v in value]
    return value


def _read(path: Path) -> dict[str, Any]:
    value = sink._read_json(path)
    return _snake(value) if value else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


@dataclass
class Incident:
    id: str
    #: ``evidence/incidents/<hash>`` (relative to dest), "" when the copy has no directory for it.
    dir: str
    ledger: dict[str, Any]
    outcome: dict[str, Any]

    @property
    def short(self) -> str:
        return (self.dir.rsplit("/", 1)[-1] or sink_hash(self.id))[:12]

    @property
    def ledger_file(self) -> str:
        return f"{self.dir}/{sink.LEDGER_FILE}" if self.dir and self.ledger else ""

    @property
    def closure(self) -> dict[str, Any]:
        return _dict(self.ledger.get("closure"))

    @property
    def stamps(self) -> dict[str, Any]:
        return _dict(self.outcome.get("timestamps"))

    def time(self, name: str) -> str | None:
        """An outcome timestamp, else the matching closure time."""
        return stamp(self.stamps.get(name)) or stamp(self.closure.get(name))


def sink_hash(incident_id: str) -> str:
    return hashlib.sha256(incident_id.encode()).hexdigest()


def _incidents(dest: Path) -> list[Incident]:
    """Every incident of the collected copy: ledgers under ``evidence/incidents`` joined with memory outcomes."""
    memory = {
        str(doc.get("incident_id")): doc
        for p in sorted((dest / "memory" / ".sdo" / "outcomes").glob("*.json"))
        for doc in [_read(p)]
        if doc.get("incident_id")
    }
    found: dict[str, Incident] = {}
    for directory in sink.incident_dirs(dest / "evidence"):
        ledger = _read(directory / sink.LEDGER_FILE)
        identity = _read(directory / sink.IDENTITY_FILE)
        incident_id = str(ledger.get("incident_id") or identity.get("incident_id") or "")
        if not incident_id:
            continue
        outcome = _dict(ledger.get("outcome_document")) or memory.get(incident_id, {})
        found[incident_id] = Incident(incident_id, f"evidence/{sink.INCIDENTS}/{directory.name}", ledger, outcome)
    for incident_id, doc in memory.items():
        found.setdefault(incident_id, Incident(incident_id, "", {}, doc))
    return sorted(found.values(), key=lambda i: (i.time("detected_at") or "", i.id))


def _bench_run(dest: Path) -> table.BenchRun | None:
    run_dir = dest.parent
    info = table._read(run_dir / "run.json")
    record = table._read(run_dir / "record.json")
    if info is None and record is None:
        return None
    return table.BenchRun(run_dir, info or {}, record)


def _select(dest: Path, incidents: list[Incident]) -> tuple[list[Incident], datetime | None]:
    """The incidents of this run, and the inject time. Without a run record, every incident of the copy."""
    run = _bench_run(dest)
    if run is None or run.inject is None or run.end is None:
        return incidents, run.inject if run is not None else None
    wanted = {str(o.get("incident_id")) for o in run.pick([i.outcome for i in incidents])}
    return [i for i in incidents if i.id in wanted], run.inject


# ---- events --------------------------------------------------------------------------------------------------------


def _component(resource: Any) -> str | None:
    """``Kind/name`` of a resource ref (``{kind, name}``) or of ``Kind/namespace/name``."""
    if isinstance(resource, dict):
        kind, name = resource.get("kind"), resource.get("name")
        return f"{kind}/{name}" if kind and name else None
    if isinstance(resource, str) and "/" in resource:
        parts = resource.split("/")
        return f"{parts[0]}/{parts[-1]}"
    return None


def _findings(incident: Incident) -> list[dict[str, Any]]:
    request = _dict(incident.closure.get("request"))
    return [_dict(f) for f in _list(incident.outcome.get("findings")) or _list(request.get("findings"))]


def _finding_component(fingerprint: str, findings: list[dict[str, Any]]) -> str | None:
    for finding in findings:
        if finding.get("fingerprint") == fingerprint:
            return _component(finding.get("primary_resource"))
    tail = fingerprint.split("/")[-2:]
    return "/".join(tail) if len(tail) == 2 and tail[0][:1].isupper() else None


def _exit_reason(stage: dict[str, Any]) -> str:
    reason = _dict(stage.get("recovery_wait")).get("exit_reason")
    return str(reason).removeprefix("EXIT_REASON_").lower() if reason else "unspecified"


def incident_events(incident: Incident) -> list[Event]:
    events: list[Event] = []
    link = incident.ledger_file or None

    def add(
        at: Any, lane: str, text: str, end: Any = None, component: str | None = None, file: str | None = None
    ) -> None:
        when = stamp(at)
        if when is None:
            return
        finish = stamp(end)
        if finish is not None and finish <= when:
            finish = None
        events.append(Event(when, lane, _clip(text, MAX_EVENT_TEXT), finish, component, file))

    findings = _findings(incident)
    first = findings[0] if findings else {}
    add(
        incident.time("detected_at"),
        "detect",
        f"incident opened: {first.get('summary') or incident.id}",
        component=_component(first.get("primary_resource")),
        file=link,
    )
    seen: set[str] = set()
    timeline = sorted(
        (_dict(t) for t in _list(incident.closure.get("detector_timeline"))),
        key=lambda t: stamp(t.get("first_activated_at")) or "",
    )
    for entry in timeline:
        detector = str(entry.get("detector_id") or "")
        if detector in seen:
            continue
        seen.add(detector)
        rule = f" ({entry['rule_id']})" if entry.get("rule_id") else ""
        add(
            entry.get("first_activated_at"),
            "detect",
            f"detector {detector} active{rule}",
            component=_finding_component(str(entry.get("fingerprint") or ""), findings),
        )
    if not timeline:
        for finding in findings:
            detector = str(finding.get("detector_id") or "")
            if detector and detector not in seen:
                seen.add(detector)
                add(
                    incident.time("detected_at"),
                    "detect",
                    f"detector {detector}: {finding.get('summary') or finding.get('rule_id') or 'finding'}",
                    component=_component(finding.get("primary_resource")),
                )
    add(incident.time("dispatched_at"), "stage", "responder dispatched")
    for stage in _list(incident.outcome.get("prior_stages")):
        stage = _dict(stage)
        applied = "mitigations applied" if stage.get("mitigations") else "no mitigation applied"
        add(
            stage.get("started_at"),
            "stage",
            f"{stage.get('stage') or 'stage'} stage: {applied}, exit {_exit_reason(stage)}",
        )
    for action in _list(incident.outcome.get("repair_actions")):
        action = _dict(action)
        resources = _list(action.get("resources"))
        target = str(action.get("target") or "")
        component = _component(resources[0]) if resources else _component(target)
        mark = "" if action.get("success", True) else " (failed)"
        add(
            action.get("started_at"),
            "act",
            f"{action.get('kind') or 'action'} {target}: {action.get('summary') or ''}{mark}".strip(),
            end=action.get("completed_at"),
            component=component,
            file=link,
        )
    add(incident.time("verified_at"), "close", "recovery verified")
    phases = _dict(incident.ledger.get("closure_phase_timestamps"))
    add(phases.get("closure_ack") or incident.ledger.get("acknowledged_at"), "close", "closure acknowledged")
    add(
        phases.get("reflection_dispatch") or incident.ledger.get("reflection_dispatch_at"),
        "learn",
        "reflection dispatched",
    )
    decision = _dict(incident.ledger.get("reflection_decision")).get("learning_decision")
    add(phases.get("reflection_committed"), "learn", f"reflection committed ({decision or 'no decision'})", file=link)
    return events


# ---- learned memory ------------------------------------------------------------------------------------------------

_PLAYBOOK_PATH = re.compile(r"^\.sdo/playbooks/([^/]+)/")
_DETECTOR_PATH = re.compile(r"^\.sdo/diagnostics/detectors/[^/]+/([^/]+)/")


@dataclass(frozen=True)
class Learned:
    kind: str
    id: str
    file: str = ""


def _memory_file(dest: Path, prefix: str) -> str:
    """A representative collected file below ``memory/<prefix>`` ("" when the copy has none): README first."""
    root = dest / "memory" / prefix
    if (root / "README.md").is_file():
        return f"memory/{prefix}/README.md"
    files = sorted(p for p in root.rglob("*") if p.is_file()) if root.is_dir() else []
    return files[0].relative_to(dest).as_posix() if files else ""


def learned_items(dest: Path, incidents: list[Incident]) -> list[Learned]:
    """Playbooks, detectors and other files the reflections of ``incidents`` named, once each."""
    playbooks: dict[str, None] = {}
    detectors: dict[str, None] = {}
    files: dict[str, None] = {}
    for incident in incidents:
        ledger = incident.ledger
        decisions = [
            _dict(ledger.get("reflection_decision")),
            *(_dict(r) for r in _list(ledger.get("reflection_results"))),
        ]
        for decision in decisions:
            for pid in _list(decision.get("playbook_ids")):
                playbooks.setdefault(str(pid), None)
        for raw in _list(ledger.get("reflection_paths")):
            path = str(raw)
            if m := _PLAYBOOK_PATH.match(path):
                playbooks.setdefault(m.group(1), None)
            elif m := _DETECTOR_PATH.match(path):
                detectors.setdefault(m.group(1), None)
            else:
                files.setdefault(path, None)
    items = [Learned("playbook", pid, _memory_file(dest, f".sdo/playbooks/{pid}")) for pid in playbooks]
    items += [Learned("detector", did) for did in detectors]
    items += [Learned("file", path, _memory_file(dest, path)) for path in files if (dest / "memory" / path).is_file()]
    items += [Learned("file", path) for path in files if not (dest / "memory" / path).is_file()]
    return items


# ---- transcripts -----------------------------------------------------------------------------------------------------


def _js_string(src: str, start: int) -> tuple[str, int] | None:
    """The JavaScript string literal starting at ``src[start]`` (a quote); returns its value and end index."""
    quote = src[start]
    out: list[str] = []
    i = start + 1
    simple = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "v": "\v", "0": "\0"}
    while i < len(src):
        c = src[i]
        if c == quote:
            return "".join(out), i + 1
        if c == "\\" and i + 1 < len(src):
            n = src[i + 1]
            if n in "ux":
                width = 4 if n == "u" else 2
                digits = src[i + 2 : i + 2 + width]
                if re.fullmatch(r"[0-9a-fA-F]+", digits) and len(digits) == width:
                    out.append(chr(int(digits, 16)))
                    i += 2 + width
                    continue
            out.append(simple.get(n, n))
            i += 2
            continue
        out.append(c)
        i += 1
    return None


_CMD = re.compile(r"""\bcmd\s*:\s*(["'`])""")


def exec_commands(js: str) -> list[str]:
    """The ``cmd`` string of every ``tools.exec_command({cmd: "..."})`` in a JS snippet."""
    out = []
    for call in re.finditer(r"tools\.exec_command\s*\(", js):
        match = _CMD.search(js, call.end())
        if match is None:
            continue
        parsed = _js_string(js, match.end() - 1)
        if parsed is not None:
            out.append(parsed[0])
    return out


def _parts_text(output: Any) -> str:
    if isinstance(output, str):
        return output
    texts = []
    for part in _list(output):
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            texts.append(part["text"])
        elif isinstance(part, str):
            texts.append(part)
    return "\n".join(texts)


def _truncate_output(text: str) -> str:
    if len(text) <= OUTPUT_LIMIT:
        return text
    return f"{text[:OUTPUT_LIMIT]}\n[output truncated: {len(text) - OUTPUT_LIMIT} more characters]"


@dataclass
class Turn:
    entries: list[Entry] = field(default_factory=list)
    prompt: str = ""
    last_message: str = ""


@dataclass
class Rollout:
    turns: list[Turn]
    identities: set[str]


def _is_reflection(turn: Turn) -> bool:
    return bool(re.search(r"reflect", turn.prompt[:400], re.IGNORECASE)) or '"learning_decision"' in turn.last_message


def parse_rollout(path: Path) -> Rollout:
    """A Codex rollout as turns (split at ``task_started``) of normalized entries from ``response_item`` lines."""
    turns: list[Turn] = []
    identities: set[str] = set()
    seen: set[str] = set()
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return Rollout([], identities)
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        payload = _dict(row.get("payload"))
        kind = row.get("type")
        if kind == "session_meta":
            identities.update(
                str(payload[k]) for k in ("creator_user_id", "creator_account_id") if isinstance(payload.get(k), str)
            )
        elif kind == "event_msg" and payload.get("type") == "task_started":
            turns.append(Turn())
        elif kind == "event_msg" and payload.get("type") == "task_complete":
            if turns and isinstance(payload.get("last_agent_message"), str):
                turns[-1].last_message = payload["last_agent_message"]
        elif kind == "response_item":
            item_id = payload.get("id")
            if isinstance(item_id, str):
                if item_id in seen:
                    continue
                seen.add(item_id)
            if not turns:
                turns.append(Turn())
            _add_item(turns[-1], payload, stamp(row.get("timestamp")))
    return Rollout([t for t in turns if t.entries or t.prompt], identities)


def _add_item(turn: Turn, item: dict[str, Any], at: str | None) -> None:
    kind = item.get("type")
    entries = turn.entries
    if kind == "message":
        role = item.get("role")
        text = _parts_text(item.get("content"))
        if role == "user" and text and not text.lstrip().startswith("<environment_context>"):
            turn.prompt += text + "\n"
            entries.append(Entry("user", text, at))
        elif role == "assistant" and text:
            entries.append(Entry("assistant", text, at))
    elif kind == "custom_tool_call":
        name = str(item.get("name") or "tool")
        source = str(item.get("input") or "")
        commands = exec_commands(source) if name == "exec" else []
        if commands:
            entries += [Entry("tool", cmd, at, "exec_command") for cmd in commands]
        else:
            entries.append(Entry("tool", source, at, name))
    elif kind == "function_call":
        entries.append(Entry("tool", str(item.get("arguments") or ""), at, str(item.get("name") or "function")))
    elif kind in ("custom_tool_call_output", "function_call_output"):
        entries.append(Entry("output", _truncate_output(_parts_text(item.get("output"))), at))
    elif kind == "reasoning":
        text = "\n".join(
            str(s.get("text")) for s in _list(item.get("summary")) if isinstance(s, dict) and s.get("text")
        )
        entries.append(Entry("reasoning", text, at) if text else Entry("note", "reasoning (not summarized)", at))


def transcript_lines(entries: list[Entry], title: str, source: str, identities: Iterable[str]) -> list[str]:
    """Header plus entries as JSONL lines, every text redacted and clipped."""
    ids = set(identities)
    lines = [json.dumps({"schema": TRANSCRIPT_SCHEMA, "title": title, "source": source})]
    for entry in entries[:MAX_ENTRIES]:
        out = Entry(
            entry.role,
            _clip(redact(entry.text, ids), ENTRY_LIMIT),
            entry.at,
            redact(entry.title, ids) if entry.title else None,
        )
        lines.append(json.dumps(out.to_json(), ensure_ascii=False))
    return lines


def _write_transcripts(dest: Path, incidents: list[Incident], identities: set[str]) -> list[TranscriptPanel]:
    root = dest / TRANSCRIPT_DIR
    panels: list[TranscriptPanel] = []
    for incident in incidents:
        if not incident.dir:
            continue
        for index, rollout_path in enumerate(sorted((dest / incident.dir).rglob(sink.ROLLOUT_GLOB))):
            if not rollout_path.is_file():
                continue
            rollout = parse_rollout(rollout_path)
            groups: dict[str, list[Entry]] = {"responder": [], "reflection": []}
            for position, turn in enumerate(rollout.turns):
                role = "reflection" if _is_reflection(turn) and (position > 0 or turn.prompt) else "responder"
                groups[role] += turn.entries
            suffix = "" if index == 0 else f"-{index + 1}"
            source = rollout_path.relative_to(dest).as_posix()
            for role, entries in groups.items():
                if not entries:
                    continue
                name = f"{incident.short}-{role}{suffix}.jsonl"
                title = f"{incident.short} {role}"
                root.mkdir(parents=True, exist_ok=True)
                lines = transcript_lines(entries, title, source, identities | rollout.identities)
                (root / name).write_text("\n".join(lines) + "\n")
                panels.append(
                    TranscriptPanel(title + (f" ({index + 1})" if suffix else ""), f"{TRANSCRIPT_DIR}/{name}")
                )
    return panels


def _identities(dest: Path, incidents: list[Incident]) -> set[str]:
    ids: set[str] = set()
    for incident in incidents:
        if incident.dir:
            for p in (dest / incident.dir).rglob(sink.ROLLOUT_GLOB):
                ids |= parse_rollout(p).identities
    return ids


# ---- tables and summary ------------------------------------------------------------


def _after(at: str | None, inject: datetime | None) -> Cell:
    if at is None:
        return None
    if inject is None:
        return at
    return round((parse(at) - inject).total_seconds(), 1)  # type: ignore[operator]


def _reflection_tokens(incident: Incident) -> int:
    return _int(_dict(_dict(incident.ledger.get("reflection_decision")).get("usage")).get("input_tokens"))


def _auto_applied_cell(incident: Incident) -> str:
    verdict = table.auto_applied(incident.outcome)
    ids = sorted(
        {
            str(p)
            for plan in _list(incident.outcome.get("plan_applications"))
            if isinstance(plan, dict) and plan.get("applied") and not plan.get("dry_run")
            for p in _list(plan.get("program_ids"))
        }
    )
    return f"{verdict} ({', '.join(ids)})" if ids else verdict


def _causes(incident: Incident) -> tuple[str, str]:
    causes = [_dict(c) for c in _list(incident.outcome.get("confirmed_root_causes"))]
    return (
        ",".join(sorted({str(c.get("cause_class") or "") for c in causes} - {""})),
        ",".join(sorted({str(c.get("reach") or "") for c in causes} - {""})),
    )


def incidents_table(incidents: list[Incident], inject: datetime | None) -> TablePanel:
    unit = " s" if inject is not None else ""
    columns = [
        "incident",
        f"detected{unit}",
        f"verified{unit}",
        f"closed{unit}",
        "outcome",
        "resolution path",
        "cause class",
        "reach",
        "auto-applied",
        "responder tokens",
        "reflection tokens",
        "ledger",
    ]
    rows: list[list[Cell]] = []
    for i in incidents:
        cause, reach = _causes(i)
        ledger: Cell = FileCell("ledger.json", i.ledger_file) if i.ledger_file else None
        rows.append(
            [
                i.id,
                _after(i.time("detected_at"), inject),
                _after(i.time("verified_at"), inject),
                _after(stamp(i.stamps.get("completed_at")), inject),
                str(i.outcome.get("classification") or ""),
                str(i.outcome.get("resolution_path") or ""),
                cause,
                reach,
                _auto_applied_cell(i),
                _int(_dict(i.outcome.get("usage")).get("input_tokens")),
                _reflection_tokens(i),
                ledger,
            ]
        )
    return TablePanel("Incidents", columns, rows[:MAX_ROWS])


def actions_table(incidents: list[Incident], inject: datetime | None) -> TablePanel:
    unit = " s" if inject is not None else ""
    rows: list[list[Cell]] = []
    for i in incidents:
        for action in _list(i.outcome.get("repair_actions")):
            action = _dict(action)
            at = stamp(action.get("started_at"))
            rows.append(
                [
                    _after(at, inject),
                    str(action.get("kind") or ""),
                    str(action.get("target") or ""),
                    bool(action.get("success", False)),
                    str(action.get("summary") or ""),
                ]
            )
    return TablePanel("Repair actions", [f"time{unit}", "kind", "target", "ok", "summary"], rows[:MAX_ROWS])


def learned_table(dest: Path, incidents: list[Incident]) -> TablePanel:
    rows: list[list[Cell]] = []
    for item in learned_items(dest, incidents):
        cell: Cell = FileCell(item.file.rsplit("/", 1)[-1], item.file) if item.file else None
        rows.append([item.kind, item.id, cell])
    return TablePanel("Learned in this run", ["kind", "id", "file"], rows[:MAX_ROWS])


def summarize(incidents: list[Incident], learned: list[Learned]) -> dict[str, str | int | float | bool]:
    row = table.Row(experiment="", run="", fault="", sequence=0, position=0, seed=0)
    docs = []
    for i in incidents:
        decision = _dict(i.ledger.get("reflection_decision"))
        docs.append(
            {
                **i.outcome,
                "_reflection_usage": _dict(decision.get("usage")),
                "_reflection_decision": decision.get("learning_decision", ""),
            }
        )
    table.fill_from_outcomes(row, docs, None)
    summary: dict[str, str | int | float | bool] = {"incidents": len(incidents)}
    summary["outcome"] = row.classification or "n/a"
    summary["cause_class"] = row.cause_class or "n/a"
    summary["reach"] = row.sdo_reach or "n/a"
    summary["auto_applied"] = row.auto_applied or "n/a"
    summary["responder_tokens"] = row.tokens_in
    summary["reflection"] = row.reflection or "n/a"
    summary["learned"] = sum(1 for item in learned if item.kind in ("playbook", "detector"))
    return {k: _clip(v, MAX_SUMMARY_VALUE) if isinstance(v, str) else v for k, v in summary.items()}


# ---- build and write ------------------------------------------------------------


def _scrub(view: View, identities: set[str]) -> View:
    """The view with every string redacted (cells, event text, summary values)."""

    def r(text: str) -> str:
        return redact(text, identities)

    def cell(c: Cell) -> Cell:
        if isinstance(c, FileCell):
            return FileCell(r(c.text), c.file)
        return r(c) if isinstance(c, str) else c

    panels: list[Panel] = []
    for p in view.panels:
        if isinstance(p, TablePanel):
            panels.append(TablePanel(r(p.title), p.columns, [[cell(c) for c in row] for row in p.rows]))
        else:
            panels.append(TranscriptPanel(r(p.title), p.file))
    events = [
        Event(e.at, e.lane, _clip(r(e.text), MAX_EVENT_TEXT), e.end, e.component and r(e.component), e.file)
        for e in view.events
    ]
    summary = {k: r(v) if isinstance(v, str) else v for k, v in view.summary.items()}
    return View(summary, view.hide, events, panels)


def build(dest: Path) -> View:
    """The view of the collected operator directory ``dest`` (transcripts are written below ``dest/view``)."""
    incidents, inject = _select(dest, _incidents(dest))
    identities = _identities(dest, incidents)
    learned = learned_items(dest, incidents)
    events = sorted((e for i in incidents for e in incident_events(i)), key=lambda e: e.at)[:MAX_EVENTS]
    panels: list[Panel] = [incidents_table(incidents, inject), actions_table(incidents, inject)]
    panels += _write_transcripts(dest, incidents, identities)[: MAX_PANELS - 3]
    panels.append(learned_table(dest, incidents))
    return _scrub(View(summarize(incidents, learned), list(HIDE), events, panels), identities)


def write(dest: Path) -> dict[str, Any]:
    """Write ``dest/view.json`` and ``dest/view/transcripts/*.jsonl``; returns the view as JSON.

    Replaces an older ``dest/view`` directory; touches nothing else.
    """
    dest = Path(dest)
    shutil.rmtree(dest / VIEW_DIR, ignore_errors=True)
    view = build(dest)
    view.check_files(dest)
    value = view.to_json()
    tmp = dest / (VIEW_FILE + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n")
    tmp.replace(dest / VIEW_FILE)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate operator/view.json and operator/view/ of fuzzops-bench runs."
    )
    parser.add_argument("run_dirs", nargs="+", type=Path, metavar="RUN_DIR")
    args = parser.parse_args(argv)
    failed = 0
    for run_dir in args.run_dirs:
        operator = run_dir / "operator"
        if not operator.is_dir():
            print(f"{run_dir}: no operator directory")
            failed += 1
            continue
        try:
            value = write(operator)
        except Exception as exc:  # one run's failure never stops the others
            print(f"{run_dir}: error: {exc}")
            failed += 1
            continue
        transcripts = sum(1 for p in value["panels"] if p["type"] == "transcript")
        print(
            f"{run_dir}: {value['summary'].get('incidents', 0)} incidents, {len(value['events'])} events, "
            f"{transcripts} transcripts"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
