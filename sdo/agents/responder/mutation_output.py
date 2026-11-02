"""Compact action output and complete, typed per-invocation report files."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

from sdo.agents.responder import tool_output

MAX_MUTATION_OUTPUT_BYTES = 2048


def command_report_schema() -> dict[str, str]:
    """Generated from runtime.CommandReport and its Go/proto field types."""
    return json.loads(Path(__file__).with_name("command_report_schema.json").read_text())


def finding_progress(
    before: Sequence[Mapping[str, Any]],
    after: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Compare controller snapshots; changed evidence is visible as a possible regression."""
    prior = {str(item.get("fingerprint", "")): item for item in before}
    current = {str(item.get("fingerprint", "")): item for item in after}
    return {
        "fixed": [dict(item) for key, item in prior.items() if key not in current],
        "new": [
            dict(item)
            for key, item in current.items()
            if key not in prior or item.get("evidence") != prior[key].get("evidence")
        ],
        "unchanged": [
            dict(item)
            for key, item in current.items()
            if key in prior and item.get("evidence") == prior[key].get("evidence")
        ],
    }


def _line(finding: Mapping[str, Any]) -> str:
    ref = finding.get("primary_resource") or {}
    resource = "/".join(str(ref.get(key, "")) for key in ("kind", "namespace", "name"))
    detector = tool_output._brief(finding.get("detector_id", "unknown"), 32)
    resource = tool_output._brief(resource, 40)
    reason = tool_output._brief(finding.get("summary", "finding remains active"), 48)
    return f"- {detector} {resource}: {reason}"


def _bounded_text(value: Any, limit: int) -> str:
    """Bound UTF-8 JSON string content, including escaping, without splitting code points."""
    text = " ".join(str(value).split())

    def size(candidate: str) -> int:
        return len(json.dumps(candidate, ensure_ascii=False).encode("utf-8")) - 2

    if size(text) <= limit:
        return text
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if size(text[:middle] + "…") <= limit:
            low = middle
        else:
            high = middle - 1
    return text[:low] + "…"


def _failure_line(action: Mapping[str, Any]) -> str:
    """Say why an action failed: reason, exit code and the first line of its error or output."""
    reason = _bounded_text(action.get("failure_reason") or "unspecified", 40)
    code = action.get("exit_code")
    detail = action.get("error") or next(
        (line.strip() for line in str(action.get("output") or "").splitlines() if line.strip()),
        action.get("summary", "see receipt"),
    )
    status = f"{reason}, exit {code}" if code not in (None, "") else reason
    return f"failed: {_bounded_text(detail, 120)} [{status}]"


def _warning(action: Mapping[str, Any]) -> dict[str, str] | None:
    warning = action.get("incident_verified")
    if not warning:
        return None
    return {key: _bounded_text(warning.get(key, ""), 120) for key in ("verified_at", "message")}


def compact_mutation_envelope(text: str, path: Path, action: Mapping[str, Any]) -> dict[str, Any]:
    """The bounded JSON view carries typed errors plus the full-report pointer."""
    return {
        "summary": text,
        "details_file": str(path),
        "command": action.get("command", "mitigate"),
        "incident_verified": _warning(action),
        **{key: _bounded_text(action[key], 80) for key in ("error", "failure_reason") if action.get(key)},
        **({"hint": _bounded_text(action["hint"], 200)} if action.get("hint") else {}),
    }


def clamp_mutation_summary(text: str, path: Path, action: Mapping[str, Any], *, json_output: bool = False) -> str:
    """Enforce the emitted budget after callers add option or pending summaries."""

    def size(lines: list[str]) -> int:
        rendered = "\n".join(lines)
        if json_output:
            rendered = json.dumps(
                compact_mutation_envelope(rendered, path, action), ensure_ascii=False, separators=(",", ":")
            )
        return len(rendered.encode("utf-8")) + 1

    lines = text.splitlines()
    removed_findings = False
    while size(lines) > MAX_MUTATION_OUTPUT_BYTES:
        candidates = [i for i, line in enumerate(lines) if line.startswith(("new: -", "unchanged: -", "option: "))]
        if not candidates:
            break
        removed = lines.pop(candidates[-1])
        if removed.startswith("option: "):
            shown = sum(line.startswith("option: ") for line in lines)
            lines = [line for line in lines if not line.startswith("Options omitted:")]
            lines.insert(
                1,
                f"Options omitted: {len(action.get('option_reports', [])) - shown}; see option_reports[] in Full JSON.",
            )
        elif not removed_findings:
            lines.insert(1, "Finding details omitted: see Full JSON findings/new/unchanged[].")
            removed_findings = True
    if size(lines) > MAX_MUTATION_OUTPUT_BYTES:
        lines = [line.split(": ", 2)[0] + ": " + line.split(": ", 2)[1] if "[]: " in line else line for line in lines]
    if size(lines) > MAX_MUTATION_OUTPUT_BYTES:
        raise ValueError(f"mutation response exceeds 2048-byte hard limit; full report: {path}")
    return "\n".join(lines)


def format_mutation_response(
    before: Sequence[Mapping[str, Any]] | None,
    after: Sequence[Mapping[str, Any]] | None,
    action_results: Sequence[dict[str, Any]],
    *,
    json_output: bool = False,
    controller_progress: Mapping[str, Any] | None = None,
) -> tuple[str, Path]:
    """Return bounded summary text and its complete JSON file; supports multiple actions."""
    actions = list(action_results)
    progress = (
        {key: list(controller_progress.get(key) or []) for key in ("fixed", "new", "unchanged")}
        if controller_progress is not None
        else finding_progress(before, after)
        if before is not None and after is not None
        else {key: [] for key in ("fixed", "new", "unchanged")}
    )
    snapshots_known = controller_progress is not None or (before is not None and after is not None)
    if after is None and controller_progress is not None:
        after = [*progress["new"], *progress["unchanged"]]
    option_reports = [item for action in actions for item in action.get("option_reports", [])]
    if not option_reports:
        option_reports = [action["report"] for action in actions if action.get("report")]
    report = {"actions": actions, "option_reports": option_reports, "findings": list(after or []), **progress}
    # Standalone invocations retain the existing private tool-output directory.
    command = str(actions[-1].get("command", "mitigate")) if actions else "mitigate"
    # Rollback already has a stricter shipped stdout policy than other mutations.
    output_budget = 1500 if command == "rollback" else MAX_MUTATION_OUTPUT_BYTES
    path = tool_output.write_json(report, command=command, root=tool_output.session_output_root())
    warning = next(
        (action.get("incident_verified") for action in reversed(actions) if action.get("incident_verified")), None
    )
    if len(actions) == 1:
        action = actions[0]
        if action.get("error") or action.get("success") is False or action.get("exit_code"):
            result = _failure_line(action)
        elif action.get("targeted_healthy"):
            result = "target finding cleared"
        elif action.get("success"):
            result = "mutation completed; verification follows the controller"
        else:
            result = _bounded_text(action.get("summary", "completed; see report"), 80)
        lines = [f"This action: {result}."]
        if action.get("hint"):
            lines.append(f"Fix: {_bounded_text(action['hint'], 200)}")
    else:
        failed = sum(bool(action.get("error") or action.get("success") is False) for action in actions)
        lines = [f"Actions: {len(actions) - failed} completed; {failed} failed (see individual receipts)."]
    remaining = len(after) if after is not None else "unknown"
    lines += [
        f"Incident: {'verified' if warning else 'NOT verified'}, still broken: {remaining}",
        f"fixed by/since this action: {len(progress['fixed'])}",
        f"new since this action: {len(progress['new'])} (possible regression)",
        f"still broken, unchanged: {len(progress['unchanged'])}",
    ]
    if not snapshots_known:
        lines[2:] = ["Controller snapshots unavailable; progress unknown (see full report)."]
    schema = command_report_schema()
    tail = [
        f"incident_verified: {_bounded_text(warning['message'], 120) if warning else 'false'}",
        f"Full JSON: {path}",
        "Schema (top-level keys):",
    ]
    tail.extend(f"{key}[]: {description.removeprefix('array of ')}" for key, description in schema.items())
    rows = []
    for key in ("new", "unchanged"):
        rows.extend(f"{key}: {_line(finding)}" for finding in progress[key])

    # The verdict, counts, warning, file and schema are never removed. When the
    # finding list itself exceeds the display budget, point explicitly to its
    # complete typed arrays instead of silently losing findings in truncation.
    def emitted_bytes(candidate_lines: list[str]) -> int:
        text = "\n".join(candidate_lines)
        if json_output:
            text = json.dumps(
                compact_mutation_envelope(text, path, actions[-1] if actions else {}),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        return len(text.encode("utf-8")) + 1

    # Preserve the exported original. A short duplicate keeps the pointer usable
    # even when scratch contains a very long path or line/control characters.
    if not str(path).isprintable() or emitted_bytes([*lines, *tail]) > output_budget:
        identity = hashlib.sha256(str(path).encode("utf-8")).hexdigest()
        path = tool_output.write_json(report, command=command, incident_id=identity, root=Path("/tmp/sdo"))
        tail[1] = f"Full JSON: {path}"
    if emitted_bytes([*lines, *tail]) > output_budget:
        tail[2:] = [
            "Schema (top-level keys):",
            *(
                f"{key}[]: {description.removeprefix('array of ').split(': ', 1)[0]}"
                for key, description in schema.items()
            ),
        ]

    omitted = "Additional finding lines are in the full report's new[] and unchanged[] arrays."
    for index, row in enumerate(rows):
        remainder = [omitted] if index + 1 < len(rows) else []
        if emitted_bytes([*lines, row, *remainder, *tail]) > output_budget:
            if emitted_bytes([*lines, omitted, *tail]) <= output_budget:
                lines.append(omitted)
            break
        lines.append(row)
    final_lines = [*lines, *tail]
    if emitted_bytes(final_lines) > MAX_MUTATION_OUTPUT_BYTES:
        raise ValueError(f"mutation response exceeds 2048-byte hard limit; full report: {path}")
    text = "\n".join(final_lines)
    return text, path
