"""Measure one reflection run: cost, validity, programs per hand repair and replay verdicts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

from agentshim import TokenUsage, cost_usd, default_pricing, price_for

# Paths a responder may change in the reflection phase (Go ownership grants), minus judge-owned files.
REFLECTION_PREFIXES = (
    ".sdo/playbooks/",
    ".sdo/lib/",
    ".sdo/corpus/",
    ".sdo/diagnostics/detectors/incidents/",
)
JUDGE_OWNED_PREFIXES = (".sdo/playbooks/health-objective/",)
PROGRAM_REPAIR_KIND = "program-plan"


@dataclass(frozen=True)
class Usage:
    model_calls: int = 0
    unique_requests: int | None = None
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    cache_write_input_tokens: int = 0
    cache_write_1h_input_tokens: int = 0
    weighted_tokens: float | None = None
    usd: float | None = None
    pricing_version: str = ""
    accounting_source: str = "logged reflection turns"
    seconds: float = 0.0
    shell_commands: int = 0
    shell_command_lines: tuple[str, ...] = ()

    @property
    def raw_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @classmethod
    def from_lines(cls, records: list[dict[str, Any]]) -> Usage:
        """Sum every reflection turn the worker logged (first turn plus in-session corrections)."""

        total: dict[str, Any] = {"shell_command_lines": []}
        for record in records:
            usage = record.get("usage") or {}
            total["model_calls"] = total.get("model_calls", 0) + int(record.get("model_requests") or 0)
            total["input_tokens"] = total.get("input_tokens", 0) + int(usage.get("input_tokens") or 0)
            total["cached_input_tokens"] = total.get("cached_input_tokens", 0) + int(
                usage.get("cached_input_tokens") or 0
            )
            total["output_tokens"] = total.get("output_tokens", 0) + int(usage.get("output_tokens") or 0)
            total["seconds"] = total.get("seconds", 0.0) + float(record.get("duration_seconds") or 0.0)
            total["shell_commands"] = total.get("shell_commands", 0) + int(record.get("shell_commands") or 0)
            total["shell_command_lines"] += list(record.get("shell_command_lines") or [])
        total["shell_command_lines"] = tuple(total["shell_command_lines"])
        return cls(**total)


def _claude_requests(directory: Path) -> dict[str, dict[str, Any]]:
    requests: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.rglob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            message = row.get("message") or {}
            usage = message.get("usage") or {}
            if row.get("type") != "assistant" or not usage:
                continue
            key = message.get("id") or row.get("uuid")
            if not key:
                raise ValueError(f"Claude usage has no request identity in {path}")
            previous = requests.get(key, {})
            if int(usage.get("output_tokens") or 0) >= int(previous.get("output_tokens") or 0):
                requests[key] = usage
    return requests


def claude_usage(before: Path, after: Path, *, model: str, logged: Usage | None = None) -> Usage:
    """Price only unique API messages appended after the frozen responder prefix.

    Resumed CLI total_cost_usd is session-cumulative. It is deliberately never
    summed as phase cost. Streaming revisions and copied project exports dedupe
    by provider message ID, keeping the final output-token counter.
    """
    baseline = _claude_requests(before)
    requests = {key: row for key, row in _claude_requests(after).items() if key not in baseline}
    plain = reads = writes = one_hour = output = 0
    for row in requests.values():
        plain += int(row.get("input_tokens") or 0)
        reads += int(row.get("cache_read_input_tokens") or 0)
        writes += int(row.get("cache_creation_input_tokens") or 0)
        one_hour += int((row.get("cache_creation") or {}).get("ephemeral_1h_input_tokens") or 0)
        output += int(row.get("output_tokens") or 0)
    tokens = TokenUsage(
        input_tokens=plain + reads + writes,
        output_tokens=output,
        cache_read_input_tokens=reads,
        cache_write_input_tokens=writes,
        cache_write_1h_input_tokens=one_hour,
    )
    pricing = price_for("claude", model)
    if pricing is None:
        raise ValueError(f"no frozen pricing for Claude model {model!r}")
    return Usage(
        model_calls=logged.model_calls if logged is not None else len(requests),
        unique_requests=len(requests),
        seconds=logged.seconds if logged is not None else 0.0,
        shell_commands=logged.shell_commands if logged is not None else 0,
        shell_command_lines=logged.shell_command_lines if logged is not None else (),
        input_tokens=tokens.input_tokens,
        cached_input_tokens=reads,
        output_tokens=output,
        cache_write_input_tokens=writes,
        cache_write_1h_input_tokens=one_hour,
        weighted_tokens=tokens.weighted_total(pricing.relative_weights()),
        usd=cost_usd(tokens, pricing),
        pricing_version=default_pricing().version,
        accounting_source="new unique Claude assistant message IDs",
    )


def disallowed_paths(changed: list[str]) -> list[str]:
    """Changed paths outside the responder's reflection-phase artifact classes."""

    return sorted(
        path for path in changed if not path.startswith(REFLECTION_PREFIXES) or path.startswith(JUDGE_OWNED_PREFIXES)
    )


def programs_written(changed: list[str]) -> list[str]:
    """Playbook program IDs whose Go sources the run added or changed."""

    programs = set()
    for path in changed:
        parts = path.split("/")
        if len(parts) >= 4 and parts[:2] == [".sdo", "playbooks"] and path.endswith(".go"):
            programs.add(parts[2])
    return sorted(programs)


def hand_repairs(outcome: dict[str, Any]) -> list[str]:
    """Distinct targets the responder repaired directly rather than through a program plan."""

    return sorted(
        {
            str(action.get("target") or action.get("summary") or action.get("action_id"))
            for action in outcome.get("repair_actions") or []
            if action.get("success") and action.get("kind") != PROGRAM_REPAIR_KIND
        }
    )


def replay_verdicts(stdout: str) -> list[dict[str, Any]]:
    rows = []
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
    return rows


def replay_failures(rows: list[dict[str, Any]]) -> tuple[int, list[str]]:
    """Cases replayed, and each case where an expected program missed the fault or matched healthy.

    Only rows for the case's own root-cause programs count; the replay matrix also
    runs every other program and health detector against every case.
    """

    cases: dict[str, list[str]] = {}
    for row in rows:
        if str(row.get("program_id", "")).startswith("detector:") or not row.get("expected_match"):
            cases.setdefault(str(row.get("case_id")), [])
            continue
        case = str(row.get("case_id"))
        faulted = (row.get("faulted") or {}).get("result")
        healthy = (row.get("healthy") or {}).get("result")
        problems = cases.setdefault(case, [])
        if faulted != "Match":
            problems.append(f"{case}: {row['program_id']} faulted={faulted or 'none'}")
        if healthy != "NoMatch":
            problems.append(f"{case}: {row['program_id']} healthy={healthy or 'none'}")
    return len(cases), [problem for problems in cases.values() for problem in problems]


def check_replay_notes(check: dict[str, Any] | None, cases: list[str]) -> list[dict[str, str]]:
    """The production check's own replay of this incident's cases: status, open and post diagnosis."""

    try:
        document = json.loads(str((check or {}).get("stdout") or ""))
    except ValueError:
        return []
    notes = []
    for note in document.get("mitigation_replay_notes") or []:
        if note.get("case_id") not in cases:
            continue
        effects = {
            key.strip(): value.strip()
            for key, _, value in (effect.partition(":") for effect in note.get("effects") or [])
            if key.strip() in ("open diagnosis", "post diagnosis")
        }
        notes.append(
            {
                "program_id": str(note.get("program_id")),
                "case_id": str(note.get("case_id")),
                "status": str(note.get("status")),
                "open": effects.get("open diagnosis", ""),
                "post": effects.get("post diagnosis", ""),
            }
        )
    return notes


def memory_check_passed(check: dict[str, Any] | None) -> bool | None:
    if not check:
        return None
    return check.get("exit_code") == 0 and not check.get("timed_out")


@dataclass(frozen=True)
class RunMetrics:
    scenario: str
    label: str
    status: str
    failure_reason: str
    wall_seconds: float
    usage: Usage
    memory_check_pass: bool | None
    changed_paths: list[str]
    disallowed_paths: list[str]
    programs_written: list[str]
    hand_repairs: list[str]
    decision: str
    run_id: str = ""
    reflection_seconds: float | None = None
    replay_cases: list[str] = field(default_factory=list)
    replay_exit_code: int | None = None
    replay_rows: list[dict[str, Any]] = field(default_factory=list)
    check_replay: list[dict[str, str]] = field(default_factory=list)

    @property
    def replay(self) -> dict[str, Any]:
        cases, failures = replay_failures(self.replay_rows)
        failed = {failure.split(":", 1)[0] for failure in failures}
        return {"cases": cases, "passed": cases - len(failed), "failures": failures}

    @property
    def check_open_match(self) -> dict[str, int]:
        """The production check's replay of this incident's cases: how many diagnose the fault capture."""

        return {
            "cases": len(self.check_replay),
            "match": sum(1 for note in self.check_replay if note["open"] == "Match"),
        }

    @property
    def programs_per_hand_repair(self) -> float | None:
        if not self.hand_repairs:
            return None
        return len(self.programs_written) / len(self.hand_repairs)

    def to_json(self) -> dict[str, Any]:
        data = asdict(self)
        data["usage"]["raw_tokens"] = self.usage.raw_tokens
        data["programs_per_hand_repair"] = self.programs_per_hand_repair
        data["replay"] = self.replay
        data["check_open_match"] = self.check_open_match
        return data


def decision_kind(result: dict[str, Any]) -> str:
    response = result.get("response") or {}
    for key in ("decision", "kind", "outcome"):
        if isinstance(response.get(key), str):
            return response[key]
    return "none" if not response else "edited"
