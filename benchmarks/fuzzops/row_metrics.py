"""Measure FuzzOps lanes rows from collected evidence, without accessing a cluster.

The CLI uses only the standard library so it can also be piped to Python on
the evidence host. It prints one row per directory, including invalid runs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shlex
import sys
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

Document = dict[str, Any]


def _object(value: object) -> Document:
    return value if isinstance(value, dict) else {}


def _objects(value: object) -> Iterator[Document]:
    if isinstance(value, list):
        yield from (item for item in value if isinstance(item, dict))


def _read(path: Path) -> Document:
    try:
        return _object(json.loads(path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return {}
    except ValueError:
        print(f"{path}: invalid JSON", file=sys.stderr)
        return {}


def _events(path: Path) -> Iterator[Document]:
    try:
        with path.open(encoding="utf-8") as fp:
            for number, line in enumerate(fp, 1):
                if not line.strip():
                    continue
                try:
                    yield _object(json.loads(line))
                except ValueError:
                    print(f"{path}:{number}: invalid JSONL", file=sys.stderr)
    except FileNotFoundError:
        return


def _time(value: object) -> datetime | None:
    if isinstance(value, str):
        try:
            stamp = datetime.fromisoformat(value)
            if stamp.tzinfo is not None:
                return stamp
        except ValueError:
            pass
    return None


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    return round((end - start).total_seconds(), 3) if start is not None and end is not None and end >= start else None


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _flag(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


@dataclass(frozen=True)
class Usage:
    usd: float | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None

    @classmethod
    def read(cls, usage: Document, cost: Any = None, *, raw_claude: bool = False) -> Usage:
        tokens = _number(usage.get("input_tokens"))
        if raw_claude and tokens is not None:
            tokens += (_number(usage.get("cache_creation_input_tokens")) or 0) + (
                _number(usage.get("cache_read_input_tokens")) or 0
            )
        output = _number(usage.get("output_tokens"))
        return cls(
            _number(cost if cost is not None else usage.get("total_cost_usd")),
            int(tokens) if tokens is not None else None,
            int(output) if output is not None else None,
        )


def _total(usages: list[Usage]) -> Usage:
    def add(name: str) -> Any:
        values = [getattr(usage, name) for usage in usages]
        return None if None in values else sum(values)

    return Usage(add("usd"), add("input_tokens"), add("output_tokens"))


@dataclass
class Row:
    run: str
    arm: str
    fault: str | None
    valid: bool | None
    invalid_reason: str | None
    fixed_in_window: bool | None
    window_bad_user_s: float | None
    inject_to_last_bad_s: float | None
    inject_to_page_s: float | None = None
    page_source: str = "n.a."
    ttf_s: float | None = None
    ttf_source: str = "unavailable"
    anchor_to_last_write_s: float | None = None
    last_write_source: str | None = None
    last_write_at: str | None = None
    last_write_tool_end_at: str | None = None
    mutation_crosscheck: str | None = None
    fixed_by_playbook_alone: bool | None = None
    responder_cost_usd: float | None = None
    responder_input_tokens: int | None = None
    responder_output_tokens: int | None = None
    closing_turn_cost_usd: float | None = None
    closing_turn_input_tokens: int | None = None
    closing_turn_output_tokens: int | None = None
    incident_count: int | None = None


@dataclass(frozen=True)
class Write:
    at: datetime
    source: str
    targets: tuple[str, ...] = ()
    data: bool = False
    tool_end: datetime | None = None
    tool_start: datetime | None = None


_KIND = {
    "deploy": "deployment",
    "sts": "statefulset",
    "po": "pod",
    "svc": "service",
    "cm": "configmap",
    "ds": "daemonset",
    "pvc": "persistentvolumeclaim",
}
_KUBE_FLAGS_WITH_VALUE = {
    "-n",
    "-c",
    "--namespace",
    "--context",
    "--kubeconfig",
    "--cluster",
    "--user",
    "--server",
    "--request-timeout",
    "--type",
    "-p",
    "--patch",
    "--patch-file",
    "-f",
    "--filename",
    "--replicas",
    "--selector",
    "-l",
    "-o",
    "--output",
    "--timeout",
    "--field-manager",
}
_SQL_WRITE = re.compile(
    r"(?i)\b(?:update\s+[\w.\"]+\s+set|insert\s+into|delete\s+from|truncate\s+(?:table\s+)?\w"
    r"|alter\s+table|create\s+table|drop\s+table)\b"
)
_RUBY_WRITE = re.compile(
    r"\.(?:update(?:_all|_columns|_column)?|save|create|destroy(?:_all)?|delete(?:_all)?|touch)"
    r"(?:!\b|!?(?=\W|$))|\.execute\([^)]*\b\w+:"
)
_EXEC_WRITE = re.compile(
    r"(?:^|[;&|]\s*)(?:rm|mv|cp|mkdir|chmod|chown|install|truncate|tee|touch)\s"
    r"|\bsed\s+-i\b|(?<![\d<>=])>{1,2}\s*/(?!dev/null\b)"
    r"|\bcurl\b[^;&|]*\s(?:-X\s*|--request[= ])(?:POST|PUT|PATCH|DELETE)\b"
)
_NO_EFFECT = re.compile(
    r"(?im)^\s*(?:UPDATE|DELETE|INSERT\s+0)\s+0\s*$|"
    r"^\s*error:|^\s*ERROR\s*:|^.*\bError from server\b"
)


def _target(kind: str, name: str) -> str:
    kind = kind.lower().split(".")[0]
    return f"{_KIND.get(kind, kind)}/{name}"


def _segments(command: str) -> Iterator[list[str]]:
    lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|\n")
    lexer.whitespace = " \r\t"
    lexer.whitespace_split = True
    segment: list[str] = []
    try:
        for token in lexer:
            if token and not token.strip(";&|\n"):
                if segment:
                    yield segment
                segment = []
            else:
                segment.append(token)
    except ValueError:
        return
    if segment:
        yield segment


def _positionals(args: list[str]) -> list[str]:
    out = []
    skip = False
    for arg in args:
        if skip:
            skip = False
        elif arg in _KUBE_FLAGS_WITH_VALUE:
            skip = True
        elif not arg.startswith("-"):
            out.append(arg)
    return out


def _command_writes(command: str, output: str) -> tuple[bool, tuple[str, ...]]:
    """Recognize supported shell mutations and their comparable Kubernetes targets."""
    data = False
    targets = []
    for segment in _segments(command):
        while segment and (re.match(r"^\w+=", segment[0]) or segment[0] in ("command", "env", "sudo")):
            segment = segment[1:]
        if not segment:
            continue
        exe, *args = segment
        exe = Path(exe).name
        if exe in {"sdo", "sdo-operator"} and _positionals(args)[:1] in (["mitigate"], ["apply"]):
            targets.append("")
        if exe != "kubectl" or any(arg.startswith("--dry-run") and arg != "--dry-run=none" for arg in args):
            continue
        cut = args.index("--") if "--" in args else len(args)
        verb, *objects = _positionals(args[:cut]) or [""]
        if verb == "exec":
            payload = " ".join(args[cut + 1 :])
            data |= bool(
                (re.search(r"\bpsql\b", payload) and _SQL_WRITE.search(payload))
                or (re.search(r"\brails\s+runner\b", payload) and _RUBY_WRITE.search(payload))
                or _EXEC_WRITE.search(payload)
            )
            continue
        if verb == "rollout":
            action, *objects = objects or [""]
            if action not in {"restart", "undo", "pause", "resume"}:
                continue
        elif verb not in {"apply", "patch", "edit", "delete", "scale", "replace", "create", "set", "label", "annotate"}:
            continue
        if verb == "set":
            objects = objects[1:]
        if objects and all("/" in obj for obj in objects):
            targets.extend(_target(*obj.split("/", 1)) for obj in objects)
        elif len(objects) >= 2 and "/" not in objects[0]:
            targets.extend(_target(objects[0], name) for name in objects[1:] if "=" not in name)
        else:
            targets.append("")
    # Filename-based commands still gain targets from the CLI's object-level receipts.
    for kind, name in re.findall(
        r"(?m)^([\w.-]+)/([\w.-]+) (?:configured|created|patched|scaled|restarted|replaced)", output
    ):
        if targets:
            targets.append(_target(kind, name))
    return data, tuple(dict.fromkeys(targets))


@dataclass(frozen=True)
class ToolCall:
    at: datetime
    command: str


def _tool_results(path: Path) -> Iterator[tuple[ToolCall, datetime, str, str, bool]]:
    calls: dict[str, ToolCall] = {}
    for event in _events(path):
        at = _time(event.get("timestamp"))
        if at is None:
            continue
        payload = _object(event.get("payload"))
        kind = event.get("type")
        if kind == "response_item" and payload.get("type") == "function_call":
            if payload.get("name") != "exec_command":
                continue
            try:
                command = _object(json.loads(payload.get("arguments") or "{}")).get("cmd")
            except (ValueError, TypeError):
                continue
            if isinstance(command, str) and payload.get("call_id"):
                calls[payload["call_id"]] = ToolCall(at, command)
        if kind == "response_item" and payload.get("type") == "function_call_output":
            output = str(payload.get("output", ""))
            error = bool(re.search(r"(?i)(?:process exited with code|exit code:)\s*[1-9]", output))
            if call := calls.pop(str(payload.get("call_id", "")), None):
                yield call, at, "codex", output, error
        for block in _objects(_object(event.get("message")).get("content")):
            if kind == "assistant" and block.get("type") == "tool_use" and block.get("name") == "Bash":
                command = _object(block.get("input")).get("command")
                if isinstance(command, str) and block.get("id"):
                    calls[block["id"]] = ToolCall(at, command)
            if (
                kind == "user"
                and block.get("type") == "tool_result"
                and (call := calls.pop(str(block.get("tool_use_id", "")), None))
            ):
                content = block.get("content", "")
                output = (
                    content
                    if isinstance(content, str)
                    else "\n".join(str(item.get("text", "")) for item in _objects(content))
                )
                yield call, at, "claude", output, bool(block.get("is_error"))


_TIMESTAMP = r"\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:\.\d+)?"
_UPDATED_AT = re.compile(rf"\bupdated_at:\s*[\x22']({_TIMESTAMP}\s*(?:Z|[+-]\d{{2}}:?\d{{2}}))")
_SQL_VERIFY = re.compile(r"(?is)\bselect\s+[^;]*\bupdated_at\s+from\s+\w+")
_PSQL_TIMESTAMP = re.compile(rf"\|\s*({_TIMESTAMP})\s*$")
_PSQL_UTC = re.compile(rf"{_TIMESTAMP}(?:\+00(?=\s|\||$)|\+00:?00\b)")


def _db_stamps(command: str, output: str) -> Iterator[datetime]:
    for value in _UPDATED_AT.findall(output):
        if at := _time(value.replace(" +", "+").replace(" -", "-")):
            yield at
    # The verification query selects updated_at as the final column and also
    # prints the database's current time with its UTC offset.
    if "psql" in command and _SQL_VERIFY.search(command) and _PSQL_UTC.search(output):
        for line in output.splitlines():
            if match := _PSQL_TIMESTAMP.search(line):
                yield datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc)


def _transcript(path: Path) -> Iterator[Write]:
    writes: list[Write] = []
    stamps: set[datetime] = set()
    for call, end, provider, output, error in _tool_results(path):
        if error or _NO_EFFECT.search(output):
            continue
        if output.strip() and all(
            line.endswith((" unchanged", " not patched")) for line in output.strip().splitlines()
        ):
            continue
        data, targets = _command_writes(call.command, output)
        if data or targets:
            writes.append(Write(call.at, f"{provider}_tool_start", targets, data, end))
        stamps.update(_db_stamps(call.command, output))
    by_write: dict[int, datetime] = {}
    for stamp in sorted(stamps):
        matching = [
            index
            for index, write in enumerate(writes)
            if write.data and write.tool_end is not None and write.at <= stamp <= write.tool_end
        ]
        if len(matching) == 1:
            by_write[matching[0]] = stamp
    for index, write in enumerate(writes):
        if index in by_write:
            yield replace(write, at=by_write[index], source="transcript_db_updated_at", tool_start=write.at)
        else:
            yield write


def _canonical_target(value: Any) -> str | None:
    if isinstance(value, str) and "/" in value:
        return _target(*value.split("/", 1))
    return None


def _record_write(row: Row, start: datetime, write: Write, observed: list[Document]) -> None:
    row.anchor_to_last_write_s = _seconds(start, write.at)
    row.last_write_source = write.source
    row.last_write_at = write.at.isoformat()
    row.last_write_tool_end_at = write.tool_end.isoformat() if write.tool_end else None
    # Polls sample after the change. Unrelated pod churn cannot timestamp an agent write.
    matched = any(
        (at := _time(mutation.get("at"))) is not None
        and write.at <= at <= (write.tool_end or write.at) + timedelta(seconds=10)
        and any(_canonical_target(mutation.get(key)) in write.targets for key in ("target", "object"))
        for mutation in observed
    )
    if matched:
        row.mutation_crosscheck = "target_observed"
    elif write.data:
        row.mutation_crosscheck = "data_write_not_observed"
    else:
        row.mutation_crosscheck = "no_matching_mutation"


def _set_usage(row: Row, responder: Usage, closing: Usage) -> None:
    row.responder_cost_usd = responder.usd
    row.responder_input_tokens = responder.input_tokens
    row.responder_output_tokens = responder.output_tokens
    row.closing_turn_cost_usd = closing.usd
    row.closing_turn_input_tokens = closing.input_tokens
    row.closing_turn_output_tokens = closing.output_tokens


def _vanilla(row: Row, op: Path, inject: datetime | None, observed: list[Document]) -> None:
    done = _read(op / "done.json")
    start, end = _time(done.get("started_at")), _time(done.get("ended_at"))
    row.inject_to_page_s = _seconds(inject, start)
    row.ttf_s = _seconds(start, end)
    if row.ttf_s is not None:
        row.ttf_source = "agent_start_to_end"
    transcript = op / "transcript.jsonl"
    usage = _read(op / "usage.json")
    if not usage:
        result = {}
        for event in _events(transcript):
            if event.get("type") == "result":
                result = event
        usage = _object(result.get("usage")) | {"total_cost_usd": result.get("total_cost_usd")}
    _set_usage(row, Usage.read(usage, raw_claude=True), Usage())
    if start is None:
        return
    # LastBadSecond denotes the final bad bucket. Writes starting after that bucket
    # cannot explain graded recovery, even if the agent kept working.
    bound = end
    if row.fixed_in_window and inject is not None and row.inject_to_last_bad_s is not None:
        bad_end = inject + timedelta(seconds=row.inject_to_last_bad_s + 1)
        bound = min(end, bad_end) if end else bad_end
    paths = sorted((op / "session-dir").rglob("*.jsonl"))
    writes = (write for path in [*paths, transcript] for write in _transcript(path))
    last = max(
        (w for w in writes if start <= w.at and (bound is None or (w.tool_start or w.at) <= bound)),
        key=lambda w: w.at,
        default=None,
    )
    if last:
        _record_write(row, start, last, observed)


def _plan_writes(doc: Document) -> Iterator[Write]:
    plans = list(_objects(doc.get("plan_applications")))
    plans.extend(
        _object(m.get("apply_receipt"))
        for stage in _objects(doc.get("prior_stages"))
        for m in _objects(stage.get("mitigations"))
    )
    for plan in plans:
        if plan.get("applied") is not True or plan.get("dry_run") or plan.get("rolled_back"):
            continue
        targets = []
        for effect in _objects(plan.get("object_effects")):
            ref = _object(effect.get("ref"))
            if effect.get("confirmed") and ref.get("kind") and ref.get("name"):
                targets.append(_target(ref["kind"], ref["name"]))
        if at := _time(plan.get("applied_at")) or _time(plan.get("completed_at")):
            source = "plan_application_applied_at" if _time(plan.get("applied_at")) else "plan_application_completed_at"
            yield Write(at, source, tuple(targets))


def _incident_writes(doc: Document, paths: list[Path]) -> Iterator[Write]:
    yield from _plan_writes(doc)
    changes = [at for change in _objects(doc.get("agent_changes")) if (at := _time(change.get("at")))]
    for path in paths:
        for write in _transcript(path):
            # An outcome report may carry the write time to whole seconds.
            # Correlate it only within this successful tool invocation.
            matched = [
                at
                for at in changes
                if write.tool_end is not None
                and (write.tool_start or write.at).replace(microsecond=0) <= at <= write.tool_end
            ]
            if matched:
                yield replace(write, at=max(matched), source="transcript_and_agent_change_at")
            else:
                yield write


def _phase_usage(timeline: Document, ledger: Document, folder: Path, phase: str) -> Usage:
    usage = _object(timeline.get(f"{phase}_usage"))
    cost = timeline.get(f"{phase}_cost_usd")
    if usage:
        return Usage.read(usage, cost)
    if phase == "closing_turn":
        logged = [
            Usage.read(_object(event.get("usage")), event.get("cost_usd"))
            for event in _events(folder / "reflection-usage.jsonl")
            if event.get("event", "complete") == "complete" and event.get("usage")
        ]
        if logged:
            total = _total(logged)
            return Usage(total.usd if total.usd is not None else _number(cost), total.input_tokens, total.output_tokens)
        results = [
            Usage.read(_object(r.get("usage"))) for r in _objects(ledger.get("reflection_results")) if r.get("usage")
        ]
        if results:
            total = _total(results)
            return Usage(_number(cost) if cost is not None else total.usd, total.input_tokens, total.output_tokens)
    role = "responder" if phase == "responder" else "reflection"
    turns = [
        Usage.read(_object(event.get("usage")))
        for event in _events(folder / "claude-turns.jsonl")
        if event.get("role") == role and event.get("event") == "complete"
    ]
    if turns:
        total = _total(turns)
        return Usage(_number(cost) if cost is not None else total.usd, total.input_tokens, total.output_tokens)
    if cost is not None or timeline.get("dispatched_at" if phase == "responder" else "closing_turn_dispatch_at"):
        return Usage.read({}, cost)
    return Usage(0.0, 0, 0)


def _sdo(
    row: Row, run: Path, manifest: Document, collect: Document, inject: datetime | None, observed: list[Document]
) -> None:
    timelines = list(_objects(manifest.get("timelines")))
    opened = min((at for t in timelines if (at := _time(t.get("opened_at")))), default=None)
    page_source = collect.get("page_source")
    page = _time(collect.get("sdo_page_at"))
    if page_source == "fallback" or (page_source is None and collect.get("fallback_page_at")):
        page = _time(collect.get("fallback_page_at")) or page
        page_source = "fallback"
    page = page or opened
    row.inject_to_page_s = _seconds(inject, page)
    if page is not None:
        row.page_source = page_source if page_source in {"own", "fallback"} else "own"
    if "timelines" not in manifest:
        return
    root = run / "operator/evidence/incidents"
    ledgers = {}
    for path in sorted(root.glob("*/ledger.json")):
        ledger = _read(path)
        if ledger.get("incident_id"):
            ledgers[ledger["incident_id"]] = (path.parent, ledger)
    open_ids = manifest.get("open_incidents") or []
    row.incident_count = len(timelines) + len(open_ids)
    responders, closers = [], []
    writes: list[tuple[datetime, Write]] = []
    fallbacks: list[tuple[int, datetime, datetime, str]] = []
    applied = deterministic = False
    responder_dispatched = False
    for timeline in timelines:
        incident = str(timeline.get("incident", ""))
        digest = hashlib.sha256(incident.encode()).hexdigest()[:32]
        folder, ledger = ledgers.get(incident, (root / digest, {}))
        responders.append(_phase_usage(timeline, ledger, folder, "responder"))
        closers.append(_phase_usage(timeline, ledger, folder, "closing_turn"))
        start = _time(timeline.get("opened_at"))
        apply = _time(timeline.get("playbook_applied_at"))
        dispatched = _time(timeline.get("dispatched_at"))
        resolution = timeline.get("resolution_path")
        deterministic |= resolution == "deterministic"
        applied |= apply is not None
        responder_dispatched |= dispatched is not None or resolution == "responder"
        if start is None:
            continue
        clear = _time(timeline.get("health_cleared_at"))
        bound = clear or _time(timeline.get("recovered_at")) or _time(timeline.get("closing_turn_dispatch_at"))
        paths = []
        for agent in _objects(manifest.get("agents")):
            if agent.get("role") == "responder" and agent.get("incident") in {incident, folder.name[:12], folder.name}:
                paths.extend(
                    run / p
                    for p in [agent.get("path"), *(agent.get("subagents") or [])]
                    if isinstance(p, str) and not Path(p).is_absolute() and ".." not in Path(p).parts
                )
        if not paths:
            paths = [*folder.glob("claude/projects/**/*.jsonl"), *folder.rglob("rollout-*.jsonl")]
        doc = _object(ledger.get("outcome_document"))
        if not doc:
            doc = next(
                (_read(p) for p in sorted((run / "operator/memory/.sdo/outcomes").glob(f"{folder.name}*.json"))), {}
            )
        writes.extend(
            (start, w) for w in _incident_writes(doc, paths) if start <= w.at and (bound is None or w.at <= bound)
        )
        for priority, (field, at) in enumerate(
            (
                ("health_cleared_at", clear),
                ("playbook_applied_at", apply),
                ("first_agent_change_at", _time(timeline.get("first_agent_change_at"))),
            )
        ):
            if at is not None and start <= at and (bound is None or at <= bound):
                fallbacks.append((priority, at, start, field))
    for digest in open_ids:
        folder = root / Path(digest).name
        usage = _phase_usage({"dispatched_at": True}, {}, folder, "responder")
        responders.append(usage)
        responder_dispatched |= usage.input_tokens is not None
    row.fixed_by_playbook_alone = row.fixed_in_window and (deterministic or (applied and not responder_dispatched))
    _set_usage(row, _total(responders), _total(closers))
    if writes:
        start, last = max(writes, key=lambda pair: pair[1].at)
        _record_write(row, start, last, observed)
        row.ttf_s, row.ttf_source = row.anchor_to_last_write_s, last.source
    elif fallbacks:
        _, at, start, field = max(fallbacks)
        row.ttf_s, row.ttf_source = _seconds(start, at), f"fallback_{field}"


def measure(run: Path) -> Row:
    op = run / "operator"
    record, result, metadata = (_read(run / name) for name in ("record.json", "result.json", "run.json"))
    manifest, collect = (_read(op / name) for name in ("evidence-manifest.json", "collect.json"))
    hooks = _object(result.get("prepared")).get("hooks")
    label = str(record.get("operator") or metadata.get("operator_label") or "")
    if manifest.get("arm") == "vanilla" or hooks == "vanilla" or "vanilla" in label or (op / "done.json").is_file():
        arm = "vanilla"
    elif manifest or collect or hooks == "sdo" or label.startswith("exec:sdo"):
        arm = "sdo"
    else:
        arm = "unknown"
    lifecycle = _object(record.get("lifecycle"))
    inject = next(
        (
            at
            for value in (
                lifecycle.get("inject_at"),
                collect.get("inject_at"),
                result.get("inject_at"),
                metadata.get("inject_at"),
                _read(op / "onset.json").get("inject_at"),
            )
            if (at := _time(value))
        ),
        None,
    )
    status = result.get("status") or metadata.get("status")
    valid = False if status in {"invalid", "dropped", "failed"} else _flag(record.get("valid"))
    reason = result.get("reason") or record.get("invalid_reason") or metadata.get("reason") or status or "unspecified"
    experiment = metadata.get("experiment") or (run.parent.parent.name if run.parent.name == "runs" else "")
    row = Row(
        f"{experiment}/{run.name}" if experiment else run.name,
        arm,
        record.get("fault") or metadata.get("fault") or collect.get("fault"),
        valid,
        str(reason) if valid is False else None,
        _flag(record.get("recovered_in_window")),
        _number(_object(record.get("window")).get("BadSeconds")),
        _number(record.get("last_bad_second")),
    )
    observed = list(_objects(_object(lifecycle.get("observed")).get("mutations") or record.get("observed_mutations")))
    if arm == "sdo":
        _sdo(row, run, manifest, collect, inject, observed)
    elif arm == "vanilla":
        _vanilla(row, op, inject, observed)
    return row


def _cell(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.8g}"
    return str(value).replace("|", "&#124;").replace("\n", " ")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", type=Path, nargs="+", help="lanes run directories")
    parser.add_argument("--format", choices=("jsonl", "markdown"), default="jsonl")
    args = parser.parse_args(argv)
    for run in args.runs:
        if not run.is_dir():
            parser.error(f"run directory does not exist: {run}")
    if args.format == "markdown":
        print("| " + " | ".join(field.name for field in fields(Row)) + " |")
        print("| " + " | ".join("---" for _ in fields(Row)) + " |")
    for run in args.runs:
        row = asdict(measure(run))
        print(
            json.dumps(row, allow_nan=False)
            if args.format == "jsonl"
            else "| " + " | ".join(_cell(value) for value in row.values()) + " |"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
