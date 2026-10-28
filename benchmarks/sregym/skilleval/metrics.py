"""Measure one reflection run: cost, validity, programs per hand repair and replay verdicts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

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
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    seconds: float = 0.0
    shell_commands: int = 0
    shell_command_lines: tuple[str, ...] = ()

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
    replay_cases: list[str] = field(default_factory=list)
    replay_exit_code: int | None = None
    replay_rows: list[dict[str, Any]] = field(default_factory=list)

    @property
    def programs_per_hand_repair(self) -> float | None:
        if not self.hand_repairs:
            return None
        return len(self.programs_written) / len(self.hand_repairs)

    def to_json(self) -> dict[str, Any]:
        data = asdict(self)
        data["programs_per_hand_repair"] = self.programs_per_hand_repair
        return data


def decision_kind(result: dict[str, Any]) -> str:
    response = result.get("response") or {}
    for key in ("decision", "kind", "outcome"):
        if isinstance(response.get(key), str):
            return response[key]
    return "none" if not response else "edited"
