"""Withhold uncheckable detector proposals without granting publication authority."""

from __future__ import annotations

import os
import subprocess
import time
from typing import TYPE_CHECKING

from google.protobuf import json_format

from sdo.contracts import MemoryCheckReport, MemoryOutcomes, ReflectionResult, SdkGap

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from sdo.agents.responder.memory_check import ValidationResult

_REASON = "detector withheld: no healthy baseline"


def missing_healthy_capture(check: ValidationResult, root: Path) -> bool:
    """Only the shared validator's missing-input diagnostic permits withholding."""
    if check.exit_code != 1 or check.timed_out:
        return False
    report = MemoryCheckReport()
    try:
        json_format.Parse(check.stdout, report)
    except json_format.ParseError:
        return False
    return (
        report.root == str(root.resolve())
        and report.base == os.environ.get("SDO_BASE_COMMIT")
        and report.actor == "responder"
        and report.phase == "reflection"
        and bool(report.diagnostics)
        and all(
            item.rule == "detector.healthy-baseline"
            and item.message == "controller pre-incident healthy capture is unavailable"
            for item in report.diagnostics
        )
    )


def withhold_unverifiable_detectors(
    root: Path,
    decision: ReflectionResult,
    check: ValidationResult,
    timeout_seconds: float,
    checker: Callable[[Path, ReflectionResult, float], ValidationResult],
) -> tuple[ReflectionResult, ValidationResult]:
    """Submit a checked, typed gap when detector publication lacks its capture.

    Executable playbooks require a validated detector-to-program replay chain,
    and accepted detectors may consume changed programs or shared libraries.
    Withhold the dependent proposal as a whole rather than publishing a broken
    chain. Restore accepted bytes exactly and recheck the resulting decision.
    """
    if not missing_healthy_capture(check, root) or timeout_seconds <= 0:
        return decision, check
    deadline = time.monotonic() + timeout_seconds

    def run(*args: str) -> str:
        return subprocess.check_output(args, cwd=root, text=True, timeout=max(0.001, deadline - time.monotonic()))

    base = os.environ["SDO_BASE_COMMIT"]
    # Use the storage edge's logical outcome API rather than deriving filenames.
    outcomes = json_format.Parse(run("sdo-operator", "memory", "outcomes", "--json"), MemoryOutcomes())
    outcome = next(item for item in outcomes.outcomes if item.incident_id == os.environ["SDO_INCIDENT_ID"])
    gaps = list(decision.sdk_gaps)
    resources = [list(item.resources) for item in outcome.confirmed_root_causes]
    resources.extend([item.resource] for item in outcome.undetected_causes)
    gaps.extend(
        SdkGap(
            fault_class="detector",
            primitive="controller.healthy_baseline",
            reason=_REASON,
            scratch_script="withheld detector proposal",
            resources=group,
        )
        for group in resources
        if group
    )
    if not gaps:
        # No verified cause needs executable learning; the complete check still
        # decides whether the resulting no-change decision is legitimate.
        fallback = ReflectionResult(summary=_REASON, learning_decision="no_change", no_change_reason=_REASON)
    else:
        fallback = ReflectionResult(
            summary=f"{decision.summary}; {_REASON}",
            learning_decision="sdk_gap",
            no_change_reason=_REASON,
            sdk_gaps=gaps,
        )
    accepted = set(run("git", "ls-tree", "-r", "--name-only", "-z", base).split("\0"))
    changed = set(
        run("git", "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--name-only", "-z", base, "--").split(
            "\0"
        )
    )
    changed.update(run("git", "ls-files", "--others", "-z", "--", ".sdo").split("\0"))
    changed.discard("")

    def restore(paths: set[str]) -> None:
        for relative in sorted(paths):
            path = root / relative
            if not path.parent.resolve().is_relative_to(root.resolve()):
                raise ValueError("withheld artifact escapes proposal checkout")
            if relative in accepted:
                run("git", "restore", "--source", base, "--staged", "--worktree", "--", relative)
            else:
                # Remove staged additions too, without changing accepted files.
                run("git", "rm", "--cached", "--ignore-unmatch", "--", relative)
                path.unlink(missing_ok=True)

    restore(changed)
    return fallback, checker(root, fallback, deadline - time.monotonic())
