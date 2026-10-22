"""One blocking, bounded memory-validation tool for reflection sessions."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import signal
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mcp.server.fastmcp import FastMCP

from sdo.agents.responder.reflection_authoring import SdkGap, install_authoring_tools
from sdo.agents.responder.reflection_evidence import install_evidence_tools

# Codex forwards only a small default environment to MCP servers. Carry the
# worker-pinned ownership and offline toolchain settings explicitly.
VALIDATION_ENVIRONMENT = (
    "SDO_ACTOR",
    "SDO_PHASE",
    "SDO_BASE_COMMIT",
    "SDO_INCIDENT_ID",
    "SDO_CAPTURED_CORPUS_DIR",
    "SDO_OPERATOR_ROOT",
    "SDO_PLAYBOOK_PROGRAM_EXAMPLE_DIR",
    "SDO_HEALTHY_BASELINE",
    "SDO_GO_CACHE_SEED",
    "PATH",
    "PYTHONPATH",
    "GOCACHE",
    "GOMODCACHE",
    "GOPROXY",
    "GOSUMDB",
    "GOMAXPROCS",
    "GOFLAGS",
    "CGO_ENABLED",
)
VALIDATION_TIMEOUT_SECONDS = 300
VALIDATION_TOOL_TIMEOUT_SECONDS = VALIDATION_TIMEOUT_SECONDS + 30
_MAX_OUTPUT_CHARS = 24_000


@dataclass(frozen=True)
class ValidationResult:
    exit_code: int
    timed_out: bool
    stdout: str
    stderr: str


def _output(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    if len(text) <= _MAX_OUTPUT_CHARS:
        return text
    half = _MAX_OUTPUT_CHARS // 2
    return f"{text[:half]}\n[... {len(text) - _MAX_OUTPUT_CHARS} characters omitted ...]\n{text[-half:]}"


def validation_server(root: Path, *, timeout_seconds: float = VALIDATION_TIMEOUT_SECONDS) -> FastMCP:
    if (
        isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= VALIDATION_TIMEOUT_SECONDS
    ):
        raise ValueError(f"validation timeout must be positive and at most {VALIDATION_TIMEOUT_SECONDS}s")
    checkout = root.resolve(strict=True)
    if not checkout.is_dir():
        raise ValueError("validation root must be a checkout directory")
    server = FastMCP("sdo_reflection")
    install_authoring_tools(server, checkout)
    install_evidence_tools(server)

    @server.tool()
    async def check_memory(
        decision_path: str | None = None,
        learning_decision: Literal["updated", "no_change", "sdk_gap"] | None = None,
        summary: str = "",
        no_change_reason: str = "",
        decline_reason: Literal["", "evidence_gap"] = "",
        proposed_artifact_ids: list[str] | None = None,
        sdk_gaps: list[SdkGap] | None = None,
    ) -> ValidationResult:
        """Run the complete memory check and wait for final diagnostics (at most 300s).

        Supply the typed decision fields directly; this tool writes the result
        JSON outside the authored tree. Return the same decision and logical IDs.
        Legacy decision_path files are also supported. One call waits for final
        diagnostics. The broker independently repeats the validation.
        """
        if learning_decision is not None:
            if decision_path is not None:
                raise ValueError("supply decision fields or decision_path, not both")
            # Proposal checkouts may be Git worktrees, where .git is a file.
            with tempfile.TemporaryDirectory(prefix="sdo-decision-") as temporary:
                decision = Path(temporary) / "decision.json"
                decision.write_text(
                    json.dumps(
                        {
                            "summary": summary,
                            "learning_decision": learning_decision,
                            "no_change_reason": no_change_reason,
                            "decline_reason": decline_reason,
                            "proposed_artifact_ids": proposed_artifact_ids or [],
                            "sdk_gaps": [gap.model_dump() for gap in sdk_gaps or []],
                        }
                    )
                )
                return await run_check(str(decision))
        return await run_check(decision_path)

    async def run_check(decision_path: str | None) -> ValidationResult:
        argv = ["sdo-operator", "memory", "check", "--json"]
        if decision_path is not None:
            argv.extend(["--decision", decision_path])
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=checkout,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            return ValidationResult(2, False, "", f"cannot start memory check: {exc}")
        communication = asyncio.create_task(process.communicate())
        try:
            stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), timeout_seconds)
        except (TimeoutError, asyncio.CancelledError) as exc:
            # Go builds/tests may have children holding the pipes. Terminate the
            # whole validation process group and reap it, including on tool cancellation.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = await communication
            if isinstance(exc, asyncio.CancelledError):
                raise
            return ValidationResult(
                2, True, _output(stdout), f"memory check timed out after {timeout_seconds:g}s\n{_output(stderr)}"
            )
        return ValidationResult(process.returncode or 0, False, _output(stdout), _output(stderr))

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=VALIDATION_TIMEOUT_SECONDS)
    args = parser.parse_args()
    validation_server(args.root, timeout_seconds=args.timeout_seconds).run(transport="stdio")


if __name__ == "__main__":
    main()
