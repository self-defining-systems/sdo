"""Per-cell evidence manifest and validator for fuzzops-bench cells run with SDO.

One definition, used by the collect hook (writes the manifest), the validator (decides whether a cell's evidence is
complete) and the analysis loaders (read the manifest). A cell is one run directory ``<exp>/runs/<run>/`` of
fuzzops-bench; SDO's side of it is ``<run>/operator/`` (the collect hook's ``BENCH_OUT``). Evidence lives under the
run directory in sdo-exp, never in the repository.

``operator/evidence-manifest.json`` (schema ``sdo.cell-evidence/v1``) holds build identity, the grader's record, SDO's
incident timeline, every agent session with usage, and one item per evidence file with its status. Everything is
derived from files already collected; a value the sources lack is ``null`` and the validator names it.

The validator fails a cell as ``evidence incomplete: <problem>; ...`` for a missing or truncated required item. A
problem is waived only by ``operator/evidence-waiver.json`` (``{"waive": ["<item>", ...], "reason": "..."}``), which
the validator reports but does not count.

``python -m benchmarks.fuzzops.cellevidence validate|manifest|backfill|trial|finish RUN_DIR_OR_EXPERIMENT_DIR ...``
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import tempfile
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from benchmarks.fuzzops import sink

SCHEMA = "sdo.cell-evidence/v1"
MANIFEST = "evidence-manifest.json"
WAIVER = "evidence-waiver.json"
OBSERVABILITY = "observability.json"
BUILD = "build.json"
HOST_LOAD = "host-load.json"
INCOMPLETE = "evidence incomplete"
#: The captures that may be ``empty`` (the store ran through the window and holds nothing for it, see ``capture``): an
#: application that sends no traces has none. An empty Prometheus or an empty log capture is missing evidence.
MAY_BE_EMPTY = ("jaeger",)
#: Observability captures that must be present once the receipt says the stack answered.
CAPTURES = ("prometheus", "jaeger", "applogs")
#: The model name an operator is configured with when its agents are scripted stand-ins (no model, no cost, no
#: transcript). The controller records the model it ran in ``agent-resolved.jsonl`` and in every usage record; an
#: incident whose declared models are all this one is a stub incident (see :func:`declared_stub`).
STUB_MODEL = "stub"
#: Per incident files whose records name the model an agent ran.
_MODEL_RECORDS = ("agent-resolved.jsonl", "claude-turns.jsonl", "reflection-usage.jsonl")
_AGENT_ID = re.compile(r'agentId["\\: ]+(a[0-9a-f]{15,16})')


# ---- files ---------------------------------------------------------------------------------------------------------


def atomic_write(path: Path, data: bytes | str) -> None:
    """Write ``path`` through a temp file in the same directory and rename it; never leaves a half file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = data.encode() if isinstance(data, str) else data
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def atomic_write_json(path: Path, value: Any) -> None:
    atomic_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def atomic_write_gzip(path: Path, data: bytes | str) -> None:
    raw = data.encode() if isinstance(data, str) else data
    atomic_write(path, gzip.compress(raw, 6, mtime=0))


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _json(path: Path) -> Any:
    """The parsed JSON file, or None when absent or not whole JSON."""
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def read_json_object(path: Path) -> dict[str, Any]:
    value = _json(path)
    return value if isinstance(value, dict) else {}


def jsonl_problem(path: Path) -> str:
    """Why a JSONL file is not whole ("" when every line parses): a killed writer leaves a partial last line."""
    bad, line = "", b"\n"
    try:
        with gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb") as f:
            for n, line in enumerate(f, 1):
                if bad or not line.strip():
                    continue
                try:
                    json.loads(line)
                except ValueError:
                    bad = f"line {n} is not JSON"
    except (OSError, EOFError, gzip.BadGzipFile, zlib.error) as exc:
        return f"unreadable: {exc}"
    if not line.endswith(b"\n"):
        return "last line has no newline (partial write)"
    return bad


# ---- the manifest ----------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Problem:
    item: str
    kind: str  # missing | truncated | invalid | incomplete
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.item} {self.kind}" + (f" ({self.detail})" if self.detail else "")


@dataclass
class Item:
    name: str
    path: str  # relative to the run directory
    required: bool = True
    status: str = "present"  # present | missing | truncated | invalid | empty | waived
    bytes: int = 0
    sha256: str = ""
    detail: str = ""


@dataclass
class Build:
    sdo_revision: str | None = None
    fuzzops_revision: str | None = None
    operator_image_tag: str | None = None
    baseline_commit: str | None = None
    memory_seed_commit: str | None = None
    spec_sha256: str | None = None
    experiment: str | None = None
    lane: str | None = None
    host: str | None = None


@dataclass
class HostLoad:
    """Load at collect time plus the grader's own interference readings (sampled every 5 s through the run)."""

    collect_sample: dict[str, float] | None = None
    grader_interference: dict[str, Any] | None = None


@dataclass
class Grader:
    window_requests: int | None = None
    window_seconds: float | None = None
    journeys_per_second: float | None = None
    window_bad_seconds: int | None = None
    tail_bad_seconds: int | None = None
    recovered: bool | None = None
    last_bad_second: int | None = None
    per_phase_seconds: dict[str, float] | None = None


@dataclass
class Timeline:
    """One incident of the cell; every time is RFC 3339 or None."""

    incident: str
    outcome: str | None = None
    detected_at: str | None = None
    opened_at: str | None = None
    joined_alerts: list[dict[str, str]] = field(default_factory=list)
    dispatched_at: str | None = None
    first_agent_change_at: str | None = None
    health_cleared_at: str | None = None
    verified_at: str | None = None
    playbook_evaluated_at: str | None = None
    playbook_applied_at: str | None = None
    playbook_apply_started_at: str | None = None
    recovered_at: str | None = None
    closure_start_at: str | None = None
    closed_at: str | None = None
    closing_turn_dispatch_at: str | None = None
    closing_turn_end_at: str | None = None
    closing_turn_duration_seconds: float | None = None
    closing_turn_cost_usd: float | None = None
    learning_decision: str | None = None
    learned_playbooks: list[str] = field(default_factory=list)
    responder_usage: dict[str, Any] | None = None
    responder_cost_usd: float | None = None
    resolution_path: str | None = None
    closure_phase_timestamps: dict[str, str] = field(default_factory=dict)
    #: The incident's agents declared themselves stand-ins (:func:`declared_stub`).
    stub: bool = False


@dataclass
class Session:
    """One agent session SDO started: its raw files and usage."""

    incident: str
    role: str
    session_id: str
    path: str | None  # main session JSONL, relative to the run directory
    subagents: list[str] = field(default_factory=list)  # sidechain JSONL paths found
    subagents_expected: int = 0
    usage: dict[str, Any] | None = None
    cost_usd: float | None = None
    started_at: str | None = None
    ended_at: str | None = None
    #: The agent declared itself a stand-in (model ``stub``): no cost and no session file are required of it.
    stub: bool = False


@dataclass
class CellEvidence:
    schema: str = SCHEMA
    run: str = ""
    fault: str = ""
    interval: list[str] = field(default_factory=list)
    written_at: str = ""
    build: Build = field(default_factory=Build)
    host: HostLoad = field(default_factory=HostLoad)
    grader: Grader = field(default_factory=Grader)
    timelines: list[Timeline] = field(default_factory=list)
    agents: list[Session] = field(default_factory=list)
    items: list[Item] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    waived: list[str] = field(default_factory=list)
    #: Incident directories left out of this cell's evidence: late writes of an earlier cell's incident.
    excluded: list[str] = field(default_factory=list)
    #: Incident directories with no ledger and no seal: incidents still open when collect ran (not the cell's own, which
    #: the settle waits for). Their session files are checked; their ledger does not exist yet.
    open_incidents: list[str] = field(default_factory=list)
    #: Worker evidence directory digest to investigator ID. The ID carries its parent incident.
    investigators: dict[str, str] = field(default_factory=dict)
    #: True when any incident of the cell ran declared stand-in agents: a stub row, valid but not a measurement of a
    #: model. Real-agent incidents of the same cell keep the strict checks.
    stub: bool = False

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> CellEvidence:
        if raw.get("schema") != SCHEMA:
            raise ValueError(f"evidence manifest schema {raw.get('schema')!r}, want {SCHEMA}")
        return cls(
            run=raw.get("run", ""),
            fault=raw.get("fault", ""),
            interval=list(raw.get("interval") or []),
            written_at=raw.get("written_at", ""),
            build=Build(**raw.get("build", {})),
            host=HostLoad(**raw.get("host", {})),
            grader=Grader(**raw.get("grader", {})),
            timelines=[Timeline(**t) for t in raw.get("timelines", [])],
            agents=[Session(**a) for a in raw.get("agents", [])],
            items=[Item(**i) for i in raw.get("items", [])],
            problems=list(raw.get("problems", [])),
            waived=list(raw.get("waived", [])),
            excluded=list(raw.get("excluded", [])),
            open_incidents=list(raw.get("open_incidents", [])),
            investigators=dict(raw.get("investigators", {})),
            stub=bool(raw.get("stub", False)),
        )

    @property
    def complete(self) -> bool:
        return not self.problems


def load(run_dir: Path) -> CellEvidence:
    """The cell's manifest; raises FileNotFoundError or ValueError when absent or malformed."""
    raw = json.loads((run_dir / "operator" / MANIFEST).read_text())
    return CellEvidence.from_json(raw)


# ---- derivation ------------------------------------------------------------------------------------------------------


def _get(value: Any, *keys: str) -> Any:
    for k in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(k)
    return value


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _grader(record: Any) -> Grader:
    g = Grader()
    if not isinstance(record, dict):
        return g
    win, tail = record.get("window") or {}, record.get("tail") or {}
    g.window_requests = win.get("Requests")
    g.window_seconds = _num(record.get("window_seconds"))
    if g.window_requests is not None and g.window_seconds:
        g.journeys_per_second = round(g.window_requests / g.window_seconds, 3)
    g.window_bad_seconds = win.get("BadSeconds")
    g.tail_bad_seconds = tail.get("BadSeconds")
    g.recovered = record.get("recovered_in_window")
    g.last_bad_second = record.get("last_bad_second")
    g.per_phase_seconds = record.get("timing") if isinstance(record.get("timing"), dict) else None
    return g


def _timeline(
    incident: str, ledger: dict[str, Any], outcome: dict[str, Any], reflection_usage: list[dict[str, Any]]
) -> Timeline:
    doc = ledger.get("outcome_document") or outcome or {}
    closure = ledger.get("closure") or {}
    phases = ledger.get("closure_phase_timestamps") or {}
    stamps = doc.get("timestamps") or {}
    t = Timeline(incident=incident, outcome=doc.get("classification"))
    t.detected_at = closure.get("detected_at") or stamps.get("detected_at")
    t.opened_at = t.detected_at
    findings = doc.get("findings") or []
    t.joined_alerts = [
        {"key": str(k), "at": str(_get(closure, "recovery_verification", "since") or "")}
        for k in _get(doc, "recovery_verification", "joined_keys") or []
    ]
    t.dispatched_at = closure.get("dispatched_at") or stamps.get("dispatched_at")
    changes = doc.get("agent_changes") or []
    t.first_agent_change_at = changes[0].get("at") if changes and isinstance(changes[0], dict) else None
    t.health_cleared_at = closure.get("health_cleared_at")
    t.verified_at = closure.get("verified_at") or stamps.get("verified_at")
    t.recovered_at = t.verified_at
    t.closure_start_at = phases.get("closure_start")
    t.closed_at = phases.get("closure_ack") or ledger.get("acknowledged_at")
    t.closure_phase_timestamps = {k: str(v) for k, v in phases.items()}
    t.resolution_path = doc.get("resolution_path")
    t.responder_usage = doc.get("usage") if isinstance(doc.get("usage"), dict) else None
    t.responder_cost_usd = _num(_get(doc, "usage", "total_cost_usd"))
    stages = doc.get("prior_stages") or []
    stamps_eval = [str(s["started_at"]) for s in stages if isinstance(s, dict) and s.get("started_at")]
    t.playbook_evaluated_at = min(stamps_eval) if stamps_eval else None
    applied, started = [], []
    for s in stages:
        for m in (s.get("mitigations") or []) if isinstance(s, dict) else []:
            r = (m or {}).get("apply_receipt") or {}
            if r.get("completed_at"):
                applied.append(r["completed_at"])
            if r.get("started_at"):
                started.append(r["started_at"])
    for pa in doc.get("plan_applications") or []:
        if isinstance(pa, dict):
            applied += [v for k, v in pa.items() if k in ("completed_at", "applied_at") and v]
            started += [v for k, v in pa.items() if k == "started_at" and v]
    t.playbook_applied_at = min(applied) if applied else None
    t.playbook_apply_started_at = min(started) if started else None
    t.closing_turn_dispatch_at = phases.get("reflection_dispatch")
    # every way a closing turn ends: committed, ended without a commit, or failed (its memory check rejected it, the
    # worker errored): a failed turn ran and ended too, its failure is the outcome's to report, not missing evidence
    t.closing_turn_end_at = (
        phases.get("reflection_committed") or phases.get("reflection_without_commit") or phases.get("reflection_failed")
    )
    done = [u for u in reflection_usage if u.get("event", "complete") == "complete" or "duration_seconds" in u]
    t.closing_turn_duration_seconds = sum(_num(u.get("duration_seconds")) or 0.0 for u in done) if done else None
    t.closing_turn_cost_usd = sum(_num(u.get("cost_usd")) or 0.0 for u in done) if done else None
    results = ledger.get("reflection_results") or []
    if t.closing_turn_cost_usd is None:  # the ledger's own copy of the same usage
        costs = [_num(_get(r, "usage", "total_cost_usd")) for r in results]
        t.closing_turn_cost_usd = sum(c for c in costs if c is not None) if any(c is not None for c in costs) else None
    dec = ledger.get("reflection_decision")
    dec = dec if isinstance(dec, dict) else (results[-1] if results else {})
    t.learning_decision = (dec or {}).get("learning_decision")
    t.learned_playbooks = list((dec or {}).get("playbook_ids") or [])
    del findings
    return t


def _jsonl(path: Path) -> list[dict[str, Any]]:
    out = []
    try:
        for line in path.read_text().splitlines():
            try:
                v = json.loads(line)
            except ValueError:
                continue
            if isinstance(v, dict):
                out.append(v)
    except OSError:
        pass
    return out


def declared_stub(incident_dir: Path) -> bool:
    """Whether every agent of this incident declared the stand-in model (:data:`STUB_MODEL`).

    The declaration is the model the controller recorded for the agents it started. No declaration, or any other
    model among them, is a real agent: it keeps every cost and session requirement.
    """
    models = {
        str(rec["model"])
        for name in _MODEL_RECORDS
        for rec in _jsonl(incident_dir / name)
        if isinstance(rec.get("model"), str) and rec["model"]
    }
    return models == {STUB_MODEL}


def _sessions(run: Path, incident_dir: Path) -> tuple[list[Session], list[Problem]]:
    """The agent sessions of one evidence directory and the files each must have.

    Seen sessions come from the usage records (``claude-turns.jsonl``, ``reflection-usage.jsonl``) and the session
    files themselves; every one needs its main JSONL, and every sub-agent the main file names needs its sidechain
    file. A ``start`` record with no ``complete`` (a killed session) is kept as a partial session.
    """
    sessions: dict[str, Session] = {}
    problems: list[Problem] = []
    label = incident_dir.name[:12]
    stub = declared_stub(incident_dir)
    files = {p.stem: p for p in incident_dir.glob("claude/projects/*/*.jsonl")}
    rollouts = sink.rollout_files(incident_dir)  # a Codex session's transcript: rollout-<timestamp>-<session id>.jsonl
    started_roles: list[str] = []
    for rec in _jsonl(incident_dir / "claude-turns.jsonl"):
        sid, role = str(rec.get("session_id") or ""), str(rec.get("role") or "agent")
        if rec.get("event") == "start":
            started_roles.append(role)
        if not sid:
            continue
        s = sessions.setdefault(sid, Session(incident=label, role=role, session_id=sid, path=None))
        if rec.get("event") == "complete":
            s.usage = rec.get("usage") or s.usage
            s.cost_usd = _num(_get(rec, "usage", "total_cost_usd"))
    for rec in _jsonl(incident_dir / "reflection-usage.jsonl"):
        sid = str(rec.get("session_id") or "")
        if sid:
            s = sessions.setdefault(sid, Session(incident=label, role="closing", session_id=sid, path=None))
            s.cost_usd = _num(rec.get("cost_usd")) if s.cost_usd is None else s.cost_usd
            s.role = "responder" if s.role == "responder" else "closing"
    for sid in files:
        sessions.setdefault(sid, Session(incident=label, role="agent", session_id=sid, path=None))
    for sid, s in sessions.items():
        s.stub = stub
        main = files.get(sid) or next((p for p in rollouts if p.stem.endswith(f"-{sid}")), None)
        if main is None:
            if not stub:  # a declared stand-in writes no session file
                problems.append(
                    Problem(f"session {sid[:8]}", "missing", f"{label}: {s.role} session file not collected")
                )
            continue
        s.path = str(main.relative_to(run))
        bad = jsonl_problem(main)
        if bad:
            problems.append(Problem(f"session {sid[:8]}", "truncated", f"{label}: {bad}"))
        wanted = sorted(set(_AGENT_ID.findall(main.read_text(errors="replace"))))
        s.subagents_expected = len(wanted)
        subdir = main.with_suffix("") / "subagents"
        for aid in wanted:
            sub = subdir / f"agent-{aid}.jsonl"
            if not sub.is_file():
                problems.append(Problem(f"subagent {aid[:8]}", "missing", f"{label}: sidechain of session {sid[:8]}"))
                continue
            s.subagents.append(str(sub.relative_to(run)))
            bad = jsonl_problem(sub)
            if bad:
                problems.append(Problem(f"subagent {aid[:8]}", "truncated", f"{label}: {bad}"))
        s.subagents.extend(
            str(p.relative_to(run))
            for p in sorted(subdir.glob("agent-*.jsonl"))
            if str(p.relative_to(run)) not in s.subagents
        )
    if started_roles and not sessions and not stub and not any(incident_dir.rglob("rollout-*.jsonl")):
        problems.append(
            Problem("session files", "missing", f"{label}: {len(started_roles)} session(s) started, none collected")
        )
    for rollout in incident_dir.rglob("rollout-*.jsonl"):
        bad = jsonl_problem(rollout)
        if bad:
            problems.append(Problem(f"rollout {rollout.name[:24]}", "truncated", f"{label}: {bad}"))
    return list(sessions.values()), problems


def copy_problems(evidence: Path) -> dict[str, list[str]]:
    """What is missing from a copied sink run directory, by directory (``incidents/<digest>``, ``lifecycle/<key>``).

    The same checks a fuzzops cell gets, for a harness with no cell layout: every agent session a directory's usage
    records name has its main JSONL, every sub-agent that file names has its sidechain, none is truncated, and an
    incident whose ledger says agents ran holds at least one transcript. An investigator or open incident has no
    ledger: only a session it started and left no file for is a problem. The key ``""`` is the copy itself (no
    ``sdo.evidence/v1`` layout). Empty: complete.
    """
    if why := sink.layout_problem(evidence):
        return {"": [f"no sink copy: {why}"]}
    out: dict[str, list[str]] = {}

    def check(key: str, directory: Path, *, ledgered: bool) -> None:
        _, found = _sessions(evidence, directory)
        texts = [str(p) for p in found]
        if ledgered and sink.incident_coverage(directory).needs_rollout:
            texts.append(f"{directory.name[:12]}: agents ran but no transcript was copied")
        if texts:
            out[key] = texts

    for directory in sink.incident_dirs(evidence):
        check(f"incidents/{directory.name}", directory, ledgered=(directory / "ledger.json").is_file())
    tasks = evidence / sink.TASKS
    for directory in sorted(p for p in tasks.iterdir() if p.is_dir()) if tasks.is_dir() else []:
        check(f"{sink.TASKS}/{directory.name}", directory, ledgered=False)
    return out


#: A gzip file smaller than this may hold nothing at all (the header and trailer alone are 20 bytes and some).
MIN_GZIP_BYTES = 64


def _gzip_is_empty(path: Path) -> bool:
    try:
        return not gzip.decompress(path.read_bytes()).strip()
    except (OSError, EOFError, zlib.error):
        return False  # unreadable, which is another problem


def _item(run: Path, name: str, rel: str, required: bool = True) -> Item:
    p = run / rel
    if not p.is_file():
        return Item(name, rel, required, "missing")
    return Item(name, rel, required, "present", p.stat().st_size, _sha256(p))


def _check_json(it: Item, run: Path, problems: list[Problem]) -> Any:
    if it.status == "missing":
        if it.required:
            problems.append(Problem(it.name, "missing", it.path))
        return None
    path = run / it.path
    value = _json(path)
    if value is None:
        it.status, it.detail = "truncated", "not whole JSON"
        problems.append(Problem(it.name, "truncated", it.path))
    return value


def build(run_dir: Path, run_pending: bool = False) -> CellEvidence:
    """Derive the manifest from the collected files (reads only; see ``write_manifest``). ``run.json`` is the bench
    manager's, written after the step that ran ``collect`` has ended and its directory has moved into place: while
    ``run_pending`` it is not yet expected (a later ``validate`` expects it)."""
    run = run_dir.resolve()
    op = run / "operator"
    ev = CellEvidence(run=run.name)
    problems: list[Problem] = []
    run_json = _json(run / "run.json") or {}
    record = _json(run / "record.json")
    collect = _json(op / "collect.json") or {}
    ev.fault = str(run_json.get("fault") or collect.get("fault") or "")
    ev.interval = [str(x) for x in collect.get("interval") or []]
    ev.grader = _grader(record)
    ev.host.grader_interference = record.get("interference") if isinstance(record, dict) else None
    bld = _json(op / BUILD) or {}
    ev.build = Build(
        **{k: bld.get(k) for k in Build.__dataclass_fields__ if k in bld},
    )
    ev.build.fuzzops_revision = ev.build.fuzzops_revision or _get(run_json, "fuzzops", "revision")
    ev.build.experiment = ev.build.experiment or run_json.get("experiment")
    ev.build.lane = ev.build.lane or run_json.get("lane")
    ev.build.host = ev.build.host or run_json.get("host")
    lock = run.parent.parent / "spec.lock.yaml"
    if ev.build.spec_sha256 is None and lock.is_file():
        ev.build.spec_sha256 = _sha256(lock)
    ev.host.collect_sample = _json(op / HOST_LOAD)

    def add(name: str, rel: str, required: bool = True) -> Item:
        it = _item(run, name, rel, required)
        ev.items.append(it)
        return it

    _check_json(add("run.json", "run.json", required=not run_pending), run, problems)
    rec_item = add("record.json", "record.json")
    _check_json(rec_item, run, problems)
    # a commit-series record is the adapter's, not a run's; one the grader declared invalid has no measurement to miss
    if isinstance(record, dict) and "series" not in record and record.get("valid") is not False:
        for k in ("window", "tail", "timing", "interference"):
            if not isinstance(record.get(k), dict):
                problems.append(Problem("record.json", "incomplete", f"no {k}"))  # noqa: PERF401
    col = add("collect.json", "operator/collect.json")
    _check_json(col, run, problems)
    for part, why in (collect.get("parts") or {}).items():
        # collect's own verdict on the manifest, and the reset it blocked because of it, are this build's result
        # (recomputed here) and the reset `validate` still owes: not evidence that is missing
        if part == "evidence-manifest" or (part == "reset" and str(why).startswith("error: blocked")):
            continue
        if str(why).startswith("error"):
            problems.append(Problem(f"collect part {part}", "incomplete", str(why)[:160]))
    _check_json(add("controller-state.json", "operator/controller-state.json"), run, problems)
    log = add("controller.log", "operator/controller.log")
    if log.status == "missing":
        problems.append(Problem("controller.log", "missing", log.path))
    elif log.bytes == 0:
        log.status = "truncated"
        problems.append(Problem("controller.log", "truncated", "empty"))
    b = add("build.json", f"operator/{BUILD}")
    _check_json(b, run, problems)
    for k in ("sdo_revision", "baseline_commit"):
        if b.status == "present" and not bld.get(k):
            problems.append(Problem("build.json", "incomplete", f"no {k}"))  # noqa: PERF401
    _check_json(add("host-load.json", f"operator/{HOST_LOAD}"), run, problems)
    layout = add("evidence layout", "operator/evidence/layout.json")
    value = _check_json(layout, run, problems)
    if isinstance(value, dict) and value.get("schema") != sink.LAYOUT_SCHEMA:
        problems.append(Problem("evidence layout", "invalid", f"schema {value.get('schema')!r}"))
    index = add("evidence index", "operator/evidence/index.jsonl")
    if index.status == "missing":
        problems.append(Problem("evidence index", "missing", index.path))
    elif bad := jsonl_problem(run / index.path):
        index.status = "truncated"
        problems.append(Problem("evidence index", "truncated", bad))
    trans = _json(op / "transcripts.json")
    add("transcripts.json", "operator/transcripts.json")
    if isinstance(trans, dict) and trans.get("missing"):
        problems.append(Problem("transcripts", "missing", f"{len(trans['missing'])} incident(s) without a rollout"))
    elif trans is None:
        problems.append(Problem("transcripts.json", "missing", "operator/transcripts.json"))

    # observability: the capture receipt says what the stack answered; absent is recorded, an error is a failure
    obs_item = add("observability receipt", f"operator/{OBSERVABILITY}")
    obs = _check_json(obs_item, run, problems) or {}
    for name in CAPTURES:
        state = _get(obs, name, "status")
        if obs_item.status != "present":
            break
        if state == "ok":
            files = _get(obs, name, "files") or []
            if not files:
                problems.append(Problem(f"observability {name}", "missing", "receipt lists no files"))
            for rel in files:
                it = add(f"observability {name}", f"operator/{rel}")
                if it.status == "missing":
                    problems.append(Problem(it.name, "missing", it.path))
                elif rel.endswith(".jsonl.gz") and it.bytes < MIN_GZIP_BYTES and _gzip_is_empty(run / it.path):
                    it.status = "empty"  # an older capture recorded a store that held nothing as ok
                    if name not in MAY_BE_EMPTY:
                        problems.append(Problem(it.name, "missing", f"{it.path} holds nothing"))
                elif (
                    it.path.endswith((".gz", ".jsonl"))
                    and (bad := jsonl_problem(run / it.path))
                    and it.path.endswith(".jsonl.gz")
                ):
                    it.status = "truncated"
                    problems.append(Problem(it.name, "truncated", f"{it.path}: {bad}"))
        elif state == "empty" and name in MAY_BE_EMPTY:
            pass
        elif state not in ("absent", "deferred"):  # "deferred": cells of series from before every position captured
            problems.append(
                Problem(f"observability {name}", "incomplete", str(_get(obs, name, "detail") or state)[:160])
            )

    # incidents
    outcomes_dir = op / "memory" / ".sdo" / "outcomes"
    inc_root = op / "evidence" / "incidents"
    ledger_by_dir: dict[str, dict[str, Any]] = {}
    start = sink.parse_time(ev.interval[0]) if ev.interval else None
    for d in sorted(inc_root.glob("*/")) if inc_root.is_dir() else []:
        label = d.name[:12]
        opened = sink.incident_detected_at(d)
        if sink.leftover(d, [], start) or (start is not None and opened is not None and opened < start):
            # an earlier run's incident (late writes, or never closed): not this run's evidence
            ev.excluded.append(d.name)
            continue
        if not (d / "ledger.json").exists() and not (d / "seal.json").exists():
            sessions, session_problems = _sessions(run, d)
            if sink.is_investigator_dir(d):
                worker = sink.incident_identity(d)
                ev.investigators[d.name] = worker
                for session in sessions:
                    session.incident = sink.parent_incident_id(worker)
                    session.role = "investigator"
            else:
                ev.open_incidents.append(d.name)
            ev.agents.extend(sessions)
            problems += session_problems
            continue
        ident = _check_json(
            add(f"incident.json {label}", str(Path("operator/evidence/incidents") / d.name / "incident.json")),
            run,
            problems,
        )
        ledger_item = add(f"ledger.json {label}", str(Path("operator/evidence/incidents") / d.name / "ledger.json"))
        ledger = _check_json(ledger_item, run, problems) or {}
        add(f"seal.json {label}", str(Path("operator/evidence/incidents") / d.name / "seal.json"), required=False)
        incident_id = str(ledger.get("incident_id") or (ident or {}).get("incident_id") or d.name)
        outcome_path = next(iter(sorted(outcomes_dir.glob(f"{d.name}*.json"))), None) if outcomes_dir.is_dir() else None
        outcome: dict[str, Any] = {}
        if outcome_path is None:
            if not sink.frozen_memory(ledger):
                problems.append(Problem(f"outcome {label}", "missing", "no outcome document in memory/.sdo/outcomes"))
            elif not sink.has_outcome(ledger):  # frozen memory commits nothing: the ledger holds the only copy
                problems.append(Problem(f"outcome {label}", "missing", "no outcome document in the frozen ledger"))
        else:
            it = add(f"outcome {label}", str(outcome_path.relative_to(run)))
            outcome = _check_json(it, run, problems) or {}
        ledger_by_dir[d.name] = ledger
        reflection_usage = _jsonl(d / "reflection-usage.jsonl")
        tl = _timeline(incident_id, ledger, outcome, reflection_usage)
        tl.stub = declared_stub(d)
        ev.stub = ev.stub or tl.stub
        ev.timelines.append(tl)
        ran = sink.agents_ran(ledger, d)
        for field_name in ("detected_at", "closure_start_at", "closed_at"):
            if getattr(tl, field_name) is None:
                problems.append(Problem(f"timeline {label}", "incomplete", f"no {field_name}"))
        if ran:
            for field_name in ("dispatched_at", "verified_at", "health_cleared_at"):
                if getattr(tl, field_name) is None and tl.outcome in (None, "success"):
                    problems.append(Problem(f"timeline {label}", "incomplete", f"no {field_name}"))
            if tl.resolution_path == "responder" and tl.responder_cost_usd is None and not tl.stub:
                problems.append(Problem(f"responder usage {label}", "incomplete", "no total_cost_usd"))
        if ledger and sink.closing_turn_ran(ledger):
            if tl.closing_turn_end_at is None:
                problems.append(Problem(f"closing turn {label}", "incomplete", "no end time"))
            if tl.closing_turn_cost_usd is None and not tl.stub:
                problems.append(Problem(f"closing turn {label}", "incomplete", "no cost"))
        if outcome.get("applied_playbooks") and tl.playbook_applied_at is None:
            problems.append(Problem(f"playbook {label}", "incomplete", "applied but no apply timestamp"))
        if outcome and "agent_changes" not in outcome:
            problems.append(Problem(f"outcome {label}", "incomplete", "no agent_changes"))
        if outcome and "findings" not in outcome:
            problems.append(Problem(f"outcome {label}", "incomplete", "no findings"))
        sessions, session_problems = _sessions(run, d)
        ev.agents.extend(sessions)
        problems += session_problems
        if ran and not sessions and not tl.stub and not any(d.rglob("rollout-*.jsonl")):
            problems.append(Problem(f"sessions {label}", "missing", "an agent ran; no session files"))
    # lifecycle tasks (onboarding, designer, deployer): same session rules
    life_root = op / "evidence" / "lifecycle"
    for d in sorted(life_root.glob("*/")) if life_root.is_dir() else []:
        sessions, session_problems = _sessions(run, d)
        ev.agents.extend(sessions)
        problems += session_problems
    return _finish(ev, op, problems)


def _finish(ev: CellEvidence, op: Path, problems: list[Problem]) -> CellEvidence:
    waiver = _json(op / WAIVER)
    waived = set(waiver.get("waive") or []) if isinstance(waiver, dict) and waiver.get("reason") else set()
    keep, dropped = [], []
    for p in problems:
        (dropped if any(p.item == w or p.item.startswith(w) for w in waived) else keep).append(p)
    ev.problems = [str(p) for p in keep]
    ev.waived = [f"{p} [waived: {waiver.get('reason')}]" for p in dropped]  # type: ignore[union-attr]
    for it in ev.items:
        if any(it.name == w for w in waived) and it.status != "present":
            it.status = "waived"
    ev.written_at = _now()
    return ev


def _now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_manifest(run_dir: Path, run_pending: bool = False) -> CellEvidence:
    """Build and atomically write ``operator/evidence-manifest.json``; returns it."""
    ev = build(run_dir, run_pending)
    atomic_write_json(run_dir / "operator" / MANIFEST, ev.to_json())
    return ev


UNFINISHED = "collect did not finish"


def write_unfinished(run_dir: Path) -> None:
    """The manifest a collect that has begun leaves until it ends: whatever kills it, no file says the evidence is
    complete, and the one problem says why."""
    ev = CellEvidence(run=run_dir.name, written_at=_now(), problems=[UNFINISHED])
    atomic_write_json(run_dir / "operator" / MANIFEST, ev.to_json())


def verdict(ev: CellEvidence) -> str:
    """ "" when complete, else the loud one-line failure (``evidence incomplete: ...``)."""
    if ev.complete:
        return ""
    head = "; ".join(ev.problems[:8]) + (f"; +{len(ev.problems) - 8} more" if len(ev.problems) > 8 else "")
    return f"{INCOMPLETE}: {head}"


def validate(run_dir: Path) -> list[str]:
    """The problems of a cell directory (empty: complete). Does not write."""
    return build(run_dir).problems


def trial_problems(trial: Path) -> list[str]:
    """Trajectory problems of an onboarding trial directory (a driver run outside fuzzops-bench).

    A trial keeps the stream-json it read (``transcript*.jsonl``) and the CLI's own session directory
    (``session-dir/``, the driver's ``CLAUDE_CONFIG_DIR``). Every session id in the stream needs its session JSONL
    under ``session-dir`` and every sub-agent the stream names needs its ``agent-<id>.jsonl`` sidechain file.
    """
    problems: list[str] = []
    streams = sorted(trial.glob("transcript*.jsonl"))
    if not streams:
        return [f"{trial.name}: no stream-json transcript"]
    sids: set[str] = set()
    agents: set[str] = set()
    for stream in streams:
        if bad := jsonl_problem(stream):
            problems.append(f"{trial.name}/{stream.name} truncated ({bad})")
        text = stream.read_text(errors="replace")
        agents |= set(_AGENT_ID.findall(text))
        for rec in _jsonl(stream):
            if isinstance(rec.get("session_id"), str):
                sids.add(rec["session_id"])
    have = {p.name for p in (trial / "session-dir").rglob("*.jsonl")}
    problems += [
        f"{trial.name}: session {sid[:8]} missing (no {sid}.jsonl in session-dir)"
        for sid in sorted(sids)
        if f"{sid}.jsonl" not in have
    ]
    problems += [
        f"{trial.name}: subagent {aid[:8]} missing (no agent-{aid}.jsonl in session-dir)"
        for aid in sorted(agents)
        if f"agent-{aid}.jsonl" not in have
    ]
    return problems


def cell_problems(directory: Path, arm: str) -> list[str]:
    """The transcript problems of one eval cell directory, for either arm (empty: gradable).

    The SDO arm keeps its copy of the evidence sink in ``<cell>/evidence/`` (:func:`copy_problems`); the vanilla arm
    keeps the stream-json it read and the CLI's own session directory in ``<cell>/`` (:func:`trial_problems`). A cell
    that holds neither is incomplete, never complete by default.
    """
    if arm == "sdo":
        return [
            f"{key or 'sink copy'}: {text}"
            for key, texts in copy_problems(directory / "evidence").items()
            for text in texts
        ]
    if arm == "vanilla":
        return trial_problems(directory)
    raise ValueError(f"unknown arm {arm!r}: want 'sdo' or 'vanilla'")


def cells(root: Path) -> list[Path]:
    """Run directories under ``root``: itself, an experiment (``runs/*``) or a bench root (``*/runs/*``)."""
    if (root / "run.json").is_file():
        return [root]
    found = sorted(p.parent for p in root.glob("runs/*/run.json"))
    return found or sorted(p.parent for p in root.glob("*/runs/*/run.json"))


def finish(roots: list[Path]) -> int:
    """Batch end: write every cell's manifest; only when all are complete (or waived) run the analysis hook
    (``python -m benchmarks.sregym.analysis all <dir>``) on each root. Exit 1 lists the incomplete cells."""
    import subprocess

    incomplete = []
    for root in roots:
        for cell in cells(root):
            if why := verdict(write_manifest(cell)):
                incomplete.append(f"{cell.name}: {why}")  # noqa: PERF401
    if incomplete:
        print("\n".join(incomplete))
        print(f"{INCOMPLETE}: {len(incomplete)} cell(s); analysis not run")
        return 1
    for root in roots:
        proc = subprocess.run([sys.executable, "-m", "benchmarks.sregym.analysis", "all", str(root)], check=False)
        if proc.returncode != 0:
            print(f"analysis hook failed for {root} ({proc.returncode})")
            return proc.returncode
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=("validate", "manifest", "backfill", "trial", "finish"))
    p.add_argument("paths", nargs="+", type=Path)
    p.add_argument("--json", action="store_true", help="backfill: print one JSON object per cell")
    a = p.parse_args(argv)
    bad = 0
    if a.mode == "trial":
        for trial in a.paths:
            found = trial_problems(trial.resolve())
            bad += bool(found)
            print(
                f"{'FAIL' if found else 'ok  '} {trial.name}" + (f"  {INCOMPLETE}: {'; '.join(found)}" if found else "")
            )
        return 1 if bad else 0
    if a.mode == "finish":
        return finish([r.resolve() for r in a.paths])
    for root in a.paths:
        for cell in cells(root.resolve()):
            ev = write_manifest(cell) if a.mode == "manifest" else build(cell)
            why = verdict(ev)
            bad += bool(why)
            if a.json:
                print(json.dumps({"cell": str(cell), "problems": ev.problems, "waived": ev.waived}))
            else:
                print(
                    f"{'FAIL' if why else 'ok  '} {cell.parent.parent.name}/{cell.name}" + (f"  {why}" if why else "")
                )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
