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
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

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


def _total(usages: list[Usage], *, keep_partial: bool = False) -> Usage:
    def add(name: str) -> Any:
        values = [getattr(usage, name) for usage in usages]
        known = [value for value in values if value is not None]
        return sum(known) if len(known) == len(values) or (keep_partial and known) else None

    return Usage(add("usd"), add("input_tokens"), add("output_tokens"))


@dataclass(frozen=True)
class ResponseSessionUsage:
    incident: str
    worker: str
    role: str
    usage: Usage
    complete: bool


def _investigator_parent(worker: object) -> str | None:
    # Match the evidence identity convention in sink.py. This script also runs standalone on evidence hosts.
    match = re.fullmatch(r"(.+)-inv-\d+", worker) if isinstance(worker, str) else None
    return match[1] if match else None


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
    first_episode_end_s: float | None = None
    inject_to_page_s: float | None = None
    page_source: str = "n.a."
    ttf_s: float | None = None
    ttf_source: str = "unavailable"
    agent_wall_s: float | None = None
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
    incident_tokens: int | None = None
    reflection_tokens: int | None = None
    incident_count: int | None = None
    open_incident_count: int | None = None
    responder_usage_complete: bool | None = None
    response_sessions: list[ResponseSessionUsage] = field(default_factory=list)


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
    # Schema changes are app state too: any DDL on a table, trigger, function, index, view, rule, policy or role.
    r"|(?:alter|create(?:\s+or\s+replace)?|drop)\s+(?:table|trigger|function|procedure|index|view|rule|policy"
    r"|sequence|type|role|user|schema|extension)"
    r"|grant\s|revoke\s|comment\s+on|vacuum\s+full|reindex)\b"
    # Ending or cancelling another session changes the state of the store (it releases locks).
    r"|\bpg_(?:terminate|cancel)_backend\s*\("
)
_RUBY_WRITE = re.compile(
    r"\.(?:update(?:_all|_columns|_column)?|save|create|destroy(?:_all)?|delete(?:_all)?|touch)"
    r"(?:!\b|!?(?=\W|$))|\.execute\([^)]*\b\w+:"
    # Cache state is app state: expiring or deleting a cached value changes what users get.
    r"|\.expire_\w+|\bRails\.cache\.(?:delete|delete_matched|clear|write)\b"
)
_EXEC_WRITE = re.compile(
    # A command word: at the start, after a separator, a shell keyword, `sh -c`, `find -exec` or `xargs`.
    r"(?:^|[;&|({]\s*|(?:^|\s)(?:do|then|else|-c|-exec|xargs(?:\s+-\S+)*)\s+)"
    r"(?:rm|mv|cp|mkdir|chmod|chown|install|truncate|tee|touch)\s"
    r"|\bfind\b[^;&|]*\s-delete\b"
    r"|\bsed\s+-i\b|(?<![\d<>=])>{1,2}\s*/(?!dev/null\b)"
    r"|\bcurl\b[^;&|]*\s(?:-X\s*|--request[= ])(?:POST|PUT|PATCH|DELETE)\b"
    r"|\bredis-cli\b[^;&|]*?\s(?i:del|unlink|set|setex|mset|expire|persist|flushdb|flushall|sadd|srem|hset|hdel"
    r"|lpush|rpush|lrem|zadd|zrem|rename)\s"
)
_NO_EFFECT = re.compile(
    r"(?im)^\s*(?:UPDATE|DELETE|INSERT\s+0)\s+0\s*$|"
    r"^\s*error:|^\s*ERROR\s*:|^.*\bError from server\b"
)


def _target(kind: str, name: str) -> str:
    kind = kind.lower().split(".")[0]
    return f"{_KIND.get(kind, kind)}/{name}"


#: Words that come before the command itself: shell keywords and wrappers (`timeout` takes a duration too).
_COMMAND_PREFIX = {
    "command",
    "env",
    "sudo",
    "time",
    "timeout",
    "do",
    "then",
    "else",
    "if",
    "while",
    "until",
    "!",
    "{",
    "(",
}

#: A command the server or the database acknowledged: it took effect even when another command of the call failed.
_RECEIPT = re.compile(
    r"(?m)^(?:[\w.-]+/[\w.-]+ (?:configured|created|patched|scaled|restarted|replaced|labeled|annotated)\b"
    r"|[\w.-]+ \"[\w.-]+\" (?:force )?deleted\b|(?:UPDATE|DELETE|INSERT 0) [1-9])"
)


_REDIS_WRITE = re.compile(r"(?i)\bredis-cli\b[^;&|]*?\s(?:del|unlink|hdel|srem|zrem|lrem|expire|persist|set|flushdb)\b")
#: Redis answers a changing command with a count or OK, on a line of its own.
_REDIS_REPLY = re.compile(r"(?m)^(?:\(integer\) )?[1-9]\d*$|^OK$")


def _without_comments(command: str) -> str:
    """The command without shell comments. A `#` inside a word or a quote (`${#x}`, "a # b") is no comment."""
    out, quote, skipping, i = [], "", False, 0
    while i < len(command):
        char = command[i]
        if skipping:
            skipping = char != "\n"
        elif not quote and char == "#" and (i == 0 or command[i - 1] in " \t\n;&|("):
            skipping = True
        elif char == "\\" and quote != "'":
            out.append(command[i : i + 2])
            i += 2
            continue
        elif quote:
            quote = "" if char == quote else quote
        elif char in "'\"":
            quote = char
        if not skipping:
            out.append(char)
        i += 1
    return "".join(out)


def _segments(command: str) -> Iterator[list[str]]:
    lexer = shlex.shlex(_without_comments(command), posix=True, punctuation_chars=";&|\n")
    lexer.commenters = ""
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
        while segment and (re.match(r"^\w+=", segment[0]) or segment[0] in _COMMAND_PREFIX):
            segment = segment[2:] if segment[0] == "timeout" else segment[1:]
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
            if {"-i", "-it", "-ti", "--stdin"} & set(args[:cut]):
                payload += " " + command  # the statement comes on stdin, from a heredoc or a pipe
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
        acknowledged = _RECEIPT.search(output) or (_REDIS_WRITE.search(call.command) and _REDIS_REPLY.search(output))
        if (error or _NO_EFFECT.search(output)) and not acknowledged:
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
    if responder.input_tokens is not None and responder.output_tokens is not None:
        row.incident_tokens = responder.input_tokens + responder.output_tokens
    if closing.input_tokens is not None and closing.output_tokens is not None:
        row.reflection_tokens = closing.input_tokens + closing.output_tokens


def _vanilla(row: Row, op: Path, inject: datetime | None, observed: list[Document]) -> None:
    done = _read(op / "done.json")
    start, end = _time(done.get("started_at")), _time(done.get("ended_at"))
    row.inject_to_page_s = _seconds(inject, start)
    # The headline time to fix ends at the last fixing write for both arms (set by _record_write); the agent's
    # session end includes its own verification after the fix, so it is kept apart.
    row.agent_wall_s = _seconds(start, end)
    transcript = op / "transcript.jsonl"
    usage = _read(op / "usage.json")
    if not usage:
        result = {}
        for event in _events(transcript):
            if event.get("type") == "result":
                result = event
        usage = _object(result.get("usage")) | {"total_cost_usd": result.get("total_cost_usd")}
    _set_usage(row, Usage.read(usage, raw_claude=True), Usage(0.0, 0, 0))
    if start is None:
        return
    # LastBadSecond denotes the final bad bucket. Writes starting after that bucket
    # cannot explain graded recovery, even if the agent kept working.
    writes = [
        w
        for path in [*sorted((op / "session-dir").rglob("*.jsonl")), transcript]
        for w in _transcript(path)
        if start <= w.at
    ]
    last = _last_write(writes, *_capped(_recovery_bounds(row, inject), end), _last_bad(row, inject))
    if last:
        _record_write(row, start, last, observed)
        row.ttf_s, row.ttf_source = row.anchor_to_last_write_s, last.source


EPISODE_GAP_S = 30

#: A failing request is charged to the second it started and may hang for the client timeout (3 s) before it fails, so
#: the fix that ended it can start a few seconds after the last bad second.
RECOVERY_SLACK_S = 4


def _recovery_bounds(row: Row, inject: datetime | None) -> tuple[datetime | None, datetime | None]:
    """The latest start of a write that can explain the graded recovery (the last bad second's bucket), and that bound
    plus the slack a hanging request needs."""
    if not row.fixed_in_window or inject is None or row.inject_to_last_bad_s is None:
        return None, None
    bound = inject + timedelta(seconds=row.inject_to_last_bad_s + 1)
    return bound, bound + timedelta(seconds=RECOVERY_SLACK_S)


def _last_bad(row: Row, inject: datetime | None) -> datetime | None:
    if not row.fixed_in_window or inject is None or row.inject_to_last_bad_s is None:
        return None
    return inject + timedelta(seconds=row.inject_to_last_bad_s)


def _capped(
    bounds: tuple[datetime | None, datetime | None], end: datetime | None
) -> tuple[datetime | None, datetime | None]:
    """The recovery bounds, no later than the end of the agent session or incident that wrote."""
    if end is None:
        return bounds
    bound, slack = bounds
    return min(bound, end) if bound else end, min(slack, end) if slack else end


def _last_write(
    writes: Iterable[Write], bound: datetime | None, slack: datetime | None, last_bad: datetime | None
) -> Write | None:
    """The last write within the bound. A write in the slack is preferred when none precedes it, or when the one that
    does ended more than a clean gap before the last bad second: users still failed then, so it was not the fix (a
    second fault, or harm that shows only now and then)."""
    started = [w for w in writes if bound is None or (w.tool_start or w.at) <= (slack or bound)]
    within = [w for w in started if bound is None or (w.tool_start or w.at) <= bound]
    last = max(within, key=lambda w: w.at, default=None)
    if last is None or bound is None:
        return last or max(started, key=lambda w: w.at, default=None)
    stale = last_bad is not None and (last_bad - (last.tool_end or last.at)).total_seconds() > EPISODE_GAP_S
    later = [w for w in started if w not in within]
    return max(later, key=lambda w: w.at) if stale and later else last


def first_episode_end(record: Document, inject: datetime | None) -> float | None:
    """Seconds from the inject to the last bad second of the first failure episode: an episode ends at the first
    clean gap of EPISODE_GAP_S seconds, so isolated later blips (churn, a single failed request) do not extend it."""
    users = _object(record.get("users"))
    start, bad = _time(users.get("start")), users.get("bad")
    if inject is None or start is None or not isinstance(bad, list):
        return None
    offset = (inject - start).total_seconds()
    seconds = [i - offset for i, value in enumerate(bad) if value and i >= offset]
    if not seconds:
        return None
    end = seconds[0]
    for second in seconds[1:]:
        if second - end > EPISODE_GAP_S:
            break
        end = second
    return end


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


def _closing_usage(timeline: Document, ledger: Document, folder: Path) -> Usage:
    checkpoints = [
        _read(path) for phase in ("reflect", "source_repair") for path in (folder / "usage" / phase).glob("*.json")
    ]
    if checkpoints:
        return _total(
            [
                Usage.read(_object(checkpoint.get("usage"))) if checkpoint.get("complete") is True else Usage()
                for checkpoint in checkpoints
            ]
        )
    usage = _object(timeline.get("closing_turn_usage"))
    cost = timeline.get("closing_turn_cost_usd")
    if usage:
        return Usage.read(usage, cost)
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
    turns = [
        Usage.read(_object(event.get("usage")))
        for event in _events(folder / "claude-turns.jsonl")
        if event.get("role") == "reflection" and event.get("event") == "complete"
    ]
    if turns:
        total = _total(turns)
        return Usage(_number(cost) if cost is not None else total.usd, total.input_tokens, total.output_tokens)
    attempts = list(_objects(ledger.get("reflection_results")))
    if (
        attempts
        and all(
            attempt.get("no_change_reason") in {"cleared_without_action_no_learning", "native_memory_check_no_change"}
            for attempt in attempts
        )
        and not any(event.get("role") == "reflection" for event in _events(folder / "claude-turns.jsonl"))
    ):
        return Usage(0.0, 0, 0)
    if cost is not None or timeline.get("closing_turn_dispatch_at"):
        return Usage.read({}, cost)
    if timeline.get("resolution_path") == "deterministic" or ledger.get("reflection_skipped_reason"):
        return Usage(0.0, 0, 0)
    return Usage()


def _usage_complete(usage: Usage) -> bool:
    return usage.usd is not None and usage.input_tokens is not None and usage.output_tokens is not None


def _worker_usage(folder: Path, incident: str, worker: str, role: str) -> ResponseSessionUsage | None:
    phase = "investigate" if role == "investigator" else "respond"
    checkpoints = []
    for path in (folder / "usage").glob("*.json"):
        checkpoint = _read(path)
        if (
            checkpoint.get("incident_id") == worker
            and checkpoint.get("turn_id") == path.stem
            and checkpoint.get("phase") in {phase, "responder"}
        ):
            usage = Usage.read(_object(checkpoint.get("usage")))
            checkpoints.append((usage, checkpoint.get("complete") is True and _usage_complete(usage)))
    if checkpoints:
        return ResponseSessionUsage(
            incident, worker, role, _total([usage for usage, _ in checkpoints]), all(done for _, done in checkpoints)
        )
    turns = []
    pending = 0
    complete = True
    for event in _events(folder / "claude-turns.jsonl"):
        # Older workers used the responder journal role for investigations.
        if event.get("role") not in {"responder", role}:
            continue
        if event.get("event") == "start":
            pending += 1
        elif event.get("event") == "complete":
            pending = max(0, pending - 1)
            usage = Usage.read(_object(event.get("usage")))
            turns.append(usage)
            complete &= _usage_complete(usage) and event.get("usd_complete") is not False
    if not (turns or pending):
        return None
    usage = _total(turns, keep_partial=True) if turns else Usage()
    return ResponseSessionUsage(incident, worker, role, usage, not pending and complete)


def _responder_usage(
    timeline: Document, folder: Path, incident: str, investigators: list[ResponseSessionUsage]
) -> tuple[Usage, bool, list[ResponseSessionUsage]]:
    native = _object(timeline.get("responder_usage"))
    cost = timeline.get("responder_cost_usd")
    responder = _worker_usage(folder, incident, incident, "responder")
    aggregate = Usage.read(native, cost)
    if (
        responder is None
        and not investigators
        and not native.get("partial")
        and not any((aggregate.usd, aggregate.input_tokens, aggregate.output_tokens))
        and not timeline.get("dispatched_at")
        and timeline.get("resolution_path") == "deterministic"
    ):
        return Usage(0.0, 0, 0), True, []
    sessions = list(investigators)
    if responder is None and (
        native or cost is not None or timeline.get("dispatched_at") or timeline.get("resolution_path") == "responder"
    ):
        # A native total is attributable to one worker only when it had no investigators.
        usage = Usage() if investigators else aggregate
        responder = ResponseSessionUsage(
            incident, incident, "responder", usage, _usage_complete(usage) and not native.get("partial")
        )
    if responder is not None:
        sessions.insert(0, responder)
    journal = _total([session.usage for session in sessions], keep_partial=True)
    # The native outcome already includes investigators. Fill gaps from the
    # journals, but never add journal charges to the native total.
    usage = Usage(
        aggregate.usd if aggregate.usd is not None else journal.usd,
        aggregate.input_tokens if aggregate.input_tokens is not None else journal.input_tokens,
        aggregate.output_tokens if aggregate.output_tokens is not None else journal.output_tokens,
    )
    complete = _usage_complete(usage) and not native.get("partial") and all(session.complete for session in sessions)
    return usage, complete, sessions


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
    investigators: dict[str, list[ResponseSessionUsage]] = {}
    workers = dict(_object(manifest.get("investigators")))
    open_ids = []
    for digest in manifest.get("open_incidents") or []:
        # Backward compatibility for manifests written before worker attribution.
        worker = _read(root / Path(digest).name / "incident.json").get("incident_id")
        if _investigator_parent(worker):
            workers[digest] = worker
        else:
            open_ids.append(digest)
    for digest, worker in sorted(workers.items()):
        parent = _investigator_parent(worker)
        folder = root / Path(digest).name
        if parent is None:
            continue
        session = _worker_usage(folder, parent, worker, "investigator")
        if session is None and (any(folder.glob("claude/projects/*/*.jsonl")) or any(folder.rglob("rollout-*.jsonl"))):
            session = ResponseSessionUsage(parent, worker, "investigator", Usage(), False)
        if session is not None:
            investigators.setdefault(parent, []).append(session)
    row.open_incident_count = len(open_ids)
    row.incident_count = len(timelines) + len(open_ids)
    row.responder_usage_complete = True
    responders, closers = [], []
    writes: list[tuple[datetime, Write]] = []
    fallbacks: list[tuple[int, datetime, datetime, str]] = []
    applied = deterministic = False
    responder_dispatched = False
    for timeline in timelines:
        incident = str(timeline.get("incident", ""))
        digest = hashlib.sha256(incident.encode()).hexdigest()[:32]
        folder, ledger = ledgers.get(incident, (root / digest, {}))
        usage, complete, sessions = _responder_usage(timeline, folder, incident, investigators.pop(incident, []))
        responders.append(usage)
        row.responder_usage_complete &= complete
        row.response_sessions.extend(sessions)
        closers.append(_closing_usage(timeline, ledger, folder))
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
        ended = clear or _time(timeline.get("recovered_at")) or _time(timeline.get("closing_turn_dispatch_at"))
        bound, slack = _capped(_recovery_bounds(row, inject), ended)
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
        # One rule for both arms: the write that explains the graded recovery, never a later one.
        if last := _last_write(
            [w for w in _incident_writes(doc, paths) if start <= w.at], bound, slack, _last_bad(row, inject)
        ):
            writes.append((start, last))
        for priority, (marker, at) in enumerate(
            (
                ("playbook_applied_at", apply),
                ("first_agent_change_at", _time(timeline.get("first_agent_change_at"))),
            )
        ):
            if at is not None and start <= at and (slack is None or at <= slack):
                fallbacks.append((priority, at, start, marker))
    for digest in open_ids:
        folder = root / Path(digest).name
        incident = str(_read(folder / "incident.json").get("incident_id") or digest)
        usage, complete, sessions = _responder_usage(
            {"dispatched_at": True}, folder, incident, investigators.pop(incident, [])
        )
        responders.append(usage)
        row.responder_usage_complete &= complete
        row.response_sessions.extend(sessions)
        responder_dispatched |= bool(sessions) and sessions[0].usage.input_tokens is not None
        # An incident with no sealed timeline (still open at collect, or reset after the window) still holds the
        # responder's transcript: its writes count from the page, bounded by the graded recovery.
        if page is not None:
            bound, slack = _recovery_bounds(row, inject)
            paths = [*folder.glob("claude/projects/**/*.jsonl"), *folder.rglob("rollout-*.jsonl")]
            found = [w for w in _incident_writes({}, paths) if page <= w.at]
            if last := _last_write(found, bound, slack, _last_bad(row, inject)):
                writes.append((page, last))
    for sessions in investigators.values():
        # An investigator can be collected even when the parent evidence was lost.
        responders.extend(session.usage for session in sessions)
        row.responder_usage_complete = False
        row.response_sessions.extend(sessions)
    row.fixed_by_playbook_alone = row.fixed_in_window and (deterministic or (applied and not responder_dispatched))
    _set_usage(row, _total(responders, keep_partial=True), _total(closers))
    if writes:
        start, last = max(writes, key=lambda pair: pair[1].at)
        _record_write(row, start, last, observed)
        row.ttf_s, row.ttf_source = row.anchor_to_last_write_s, last.source
    elif fallbacks:
        _, at, start, marker = max(fallbacks)
        row.ttf_s, row.ttf_source = _seconds(start, at), f"fallback_{marker}"


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
    row.first_episode_end_s = first_episode_end(record, inject)
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
