"""Bound model-visible tool output while retaining complete, locally readable JSON."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

MAX_OUTPUT_CHARS = 3000
# Shared byte budget for every agent-facing MCP tool reply.
MAX_TOOL_REPLY_BYTES = 8192
OUTPUT_ROOT = Path("/tmp/sdo")


def _brief(value: Any, limit: int) -> str:
    text = " ".join(str(value).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def write_json(
    document: dict[str, Any], *, command: str, incident_id: str | None = None, root: Path | None = None
) -> Path:
    """Allocate a process-safe sequence in the incident's private output directory."""
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
    return write_text(encoded, command=command, suffix="json", incident_id=incident_id, root=root)


def write_text(
    text: str, *, command: str, suffix: str = "txt", incident_id: str | None = None, root: Path | None = None
) -> Path:
    """Write one complete tool reply under the next private sequence number."""
    if not re.fullmatch(r"[a-z][a-z-]*", command) or not re.fullmatch(r"[a-z]+", suffix):
        raise ValueError("invalid output command")
    directory = output_directory(incident_id=incident_id, root=root)
    with (directory / ".sequence").open("a+", encoding="utf-8") as sequence:
        fcntl.flock(sequence, fcntl.LOCK_EX)
        sequence.seek(0)
        number = int(sequence.read() or "0") + 1
        sequence.seek(0)
        sequence.truncate()
        sequence.write(str(number))
        sequence.flush()
        path = directory / f"{number:04d}-{command}.{suffix}"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(text)
    return path


def output_directory(*, incident_id: str | None = None, root: Path | None = None) -> Path:
    """Return the incident's private output directory, creating it when absent."""
    identity = incident_id or os.environ.get("SDO_INCIDENT_ID") or "standalone"
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", identity):
        identity = hashlib.sha256(identity.encode()).hexdigest()
    directory = (root or OUTPUT_ROOT) / identity
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    return directory


def session_output_root() -> Path | None:
    """Workers set SDO_SESSION_SCRATCH to their private session work directory."""
    directory = os.environ.get("SDO_SESSION_SCRATCH")
    return Path(directory) / "sdo-reports" if directory else None


def bound_reply(text: str, *, tool: str, budget: int = MAX_TOOL_REPLY_BYTES) -> str:
    """Keep an agent-facing reply within budget; a larger reply is written to a file the agent can search.

    The session resends every reply on each later model request, so the
    printed part names the size and the file instead of a truncated copy.
    """
    if len(text.encode("utf-8")) <= budget:
        return text
    name = re.sub(r"[^a-z]+", "-", tool.lower()).strip("-") or "tool"
    path = write_text(text, command=name, root=session_output_root())
    lines = text.splitlines()
    notice = (
        f"{tool} reply: {len(text.encode('utf-8'))} bytes, {len(lines)} lines; over the {budget}-byte "
        "reply budget, so it is saved whole to a file. Search or extract what you need from that file; "
        "the head below is partial.\n"
        f"Full reply: {path}\nHead:\n"
    )
    head = text.encode("utf-8")[: min(1024, budget // 2)].decode("utf-8", errors="ignore")
    return notice + head + "\n…"


def summarize_response(response: dict[str, Any], details_file: str) -> dict[str, Any]:
    """Preserve status and selected identifiers; never imply omitted evidence is complete."""
    summary: dict[str, Any] = {"details_file": details_file, "summarized": True}
    if "incident_verified" in response:
        summary["incident_verified"] = response["incident_verified"]
    for name in (
        "command",
        "summary",
        "error",
        "healthy",
        "targeted_healthy",
        "unresolved",
        "loops_used",
        "loops_allowed",
        "mitigation_rounds_used",
        "mitigation_rounds_allowed",
        "response_loops_used",
        "response_loops_allowed",
        "failure_reason",
        "plan_id",
        "application_id",
        "success",
        "exit_code",
    ):
        if name in response:
            value = response[name]
            summary[name] = _brief(value, 240) if isinstance(value, str) else value
    for name in ("findings", "drift_events", "program_runs", "detector_states", "patches"):
        if isinstance(response.get(name), list):
            summary[name.removesuffix("s") + "_count"] = len(response[name])
    findings = response.get("findings", response.get("mitigation_health", {}).get("findings"))
    if isinstance(findings, list):
        summary["finding_count"] = len(findings)
        summary["still_broken"] = [
            {name: _brief(finding[name], 160) for name in ("fingerprint", "summary") if name in finding}
            for finding in findings[:4]
            if isinstance(finding, dict)
        ]
        if len(findings) > 4:
            summary["still_broken_omitted"] = len(findings) - 4
    runs = response.get("program_runs", [])
    if isinstance(runs, list) and runs:
        summary["diagnoses"] = [
            {
                "program_id": _brief(run.get("program_id", ""), 100),
                "result": _brief(run.get("verdict", {}).get("result", ""), 40),
                "can_mitigate": run.get("can_mitigate"),
            }
            for run in runs[:4]
            if isinstance(run, dict)
        ]
        summary["diagnoses_omitted"] = max(0, len(runs) - 4)
    # A short report can explain a partial repair without reprinting its full
    # immutable receipts, diagnosis evidence, menu or execution history.
    report = response.get("report")
    if isinstance(report, dict):
        summary["report"] = {
            name: _brief(report[name], 128) for name in ("status", "attempt_id", "session_id") if name in report
        }
    menu = response.get("menu")
    if isinstance(menu, dict):
        summary["option_count"] = len(menu.get("options", []))
        summary["options"] = [
            {
                name: _brief(option[name], 100)
                for name in ("option_id", "program_id", "disabled_reason")
                if name in option
            }
            for option in menu.get("options", [])[:4]
            if isinstance(option, dict)
        ]
        summary["options_omitted"] = max(0, len(menu.get("options", [])) - 4)
    receipt = response.get("receipt")
    if isinstance(receipt, dict):
        summary["changed"] = {
            name: _brief(receipt[name], 160) if isinstance(receipt[name], str) else receipt[name]
            for name in ("action_id", "target", "success")
            if name in receipt
        }
    if response.get("error") or response.get("exit_code"):
        summary["next_commands"] = ["status", "source", "diagnose"]
    elif response.get("healthy"):
        summary["next_commands"] = []
    elif response.get("command") in ("apply", "mitigate", "act"):
        summary["next_commands"] = ["check", "report"]
    else:
        summary["next_commands"] = ["mitigate", "act", "check"]
    summary["details"] = "Full findings, options, source and receipts are in details_file; extract only what you need."
    # If several large sections coexist, retain the repair verdict and counts;
    # all per-item evidence is still available in the complete JSON.
    for name in ("diagnoses", "options", "still_broken"):
        if len(json.dumps(summary, ensure_ascii=False)) <= MAX_OUTPUT_CHARS:
            break
        if name in summary:
            count = {"diagnoses": "program_run_count", "options": "option_count", "still_broken": "finding_count"}[name]
            summary[name + "_omitted"] = summary.get(count, len(summary[name]))
            summary.pop(name)
    return summary


def write_response(response: dict[str, Any]) -> dict[str, Any]:
    """Persist before printing; command subprocesses share the worker's private temp storage."""
    path = write_json(response, command=str(response.get("command", "response")))
    return summarize_response(response, str(path))


def render_summary(summary: dict[str, Any]) -> str:
    """Render the human default; scripts explicitly choose the complete --json view."""
    lines = [f"sdo {summary.get('command', 'response')}: {summary.get('summary', '')}"]
    if summary.get("incident_verified"):
        lines.append(str(summary["incident_verified"]["message"]))
    if summary.get("error") and summary["error"] != summary.get("summary"):
        lines.append("error: " + str(summary["error"]))
    status = [
        f"{name}={str(summary[name]).lower() if isinstance(summary[name], bool) else summary[name]}"
        for name in (
            "healthy",
            "targeted_healthy",
            "success",
            "exit_code",
            "loops_used",
            "loops_allowed",
            "plan_id",
            "application_id",
        )
        if name in summary
    ]
    if status:
        lines.append(" ".join(status))
    counts = [f"{name.removesuffix('_count')}s={value}" for name, value in summary.items() if name.endswith("_count")]
    if counts:
        lines.append(" ".join(counts))
    for name in ("still_broken", "diagnoses", "options"):
        lines.extend(
            name + ": " + " ".join(f"{key}={value}" for key, value in item.items()) for item in summary.get(name, [])
        )
        if summary.get(name + "_omitted"):
            lines.append(f"{name}_omitted={summary[name + '_omitted']}")
    lines.extend(
        name + ": " + " ".join(f"{key}={value}" for key, value in summary[name].items())
        for name in ("report", "changed")
        if summary.get(name)
    )
    lines.append("Next commands: " + ", ".join(summary.get("next_commands", [])))
    lines.append("Full JSON: " + str(summary["details_file"]))
    return "\n".join(lines)
