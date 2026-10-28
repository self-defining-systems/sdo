"""One blocking, bounded memory-validation tool for reflection sessions."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import json
import math
import os
import signal
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import TextContent

from sdo.agents.context_output import budget_for, save_context
from sdo.agents.responder.memory_check import VALIDATION_TIMEOUT_SECONDS, ValidationResult, validation_output
from sdo.agents.responder.reflection_authoring import SdkGap, install_authoring_tools
from sdo.agents.responder.reflection_evidence import install_evidence_tools
from sdo.agents.responder.tool_output import bound_reply

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from mcp.types import ContentBlock

# Codex forwards only a small default environment to MCP servers. Carry the
# worker-pinned ownership and offline toolchain settings explicitly.
VALIDATION_ENVIRONMENT = (
    "SDO_ACTOR",
    "SDO_SESSION_SCRATCH",
    "SDO_SESSION_DIR",
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
VALIDATION_TOOL_TIMEOUT_SECONDS = VALIDATION_TIMEOUT_SECONDS + 30


class _TextToolServer(FastMCP):
    """Return each tool result once, as text within the shared reply budget.

    A resumed reflection carries every result for the rest of the session;
    FastMCP's structured copy would repeat each one, and an oversized reply
    is saved to a file the agent can search instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.help_texts: dict[str, str] = {}

    def add_tool(self, fn: Any, *args: Any, structured_output: bool | None = None, **kwargs: Any) -> None:
        name = kwargs.get("name") or fn.__name__
        budget_for("tool_reply." + name)
        limit = budget_for("tool_description." + name)
        help_text = kwargs.pop("description", None) or inspect.getdoc(fn) or ""
        self.help_texts[name] = help_text
        if len(help_text) > limit:
            _, reference = save_context(help_text)
            description = help_text.split(". ", 1)[0].split("\n", 1)[0].rstrip(".") + "."
            suffix = f" Full help: {reference}"
            if len(description + suffix) > limit:
                raise ValueError(f"{name}: first help sentence exceeds listing budget {limit}")
            help_text = description + suffix
        super().add_tool(fn, *args, description=help_text, structured_output=False, **kwargs)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Sequence[ContentBlock] | dict[str, Any]:
        try:
            result = await super().call_tool(name, arguments)
        except ToolError as exc:
            result = [TextContent(type="text", text="Tool error: " + str(exc))]
        if isinstance(result, dict) or not all(isinstance(block, TextContent) for block in result):
            raise TypeError(f"{name}: SDO tools must return text through the capped boundary")
        text = "\n".join(block.text for block in result if isinstance(block, TextContent))
        bounded = bound_reply(text, tool=name, budget=budget_for("tool_reply." + name))
        return result if bounded == text else [TextContent(type="text", text=bounded)]


def validation_server(
    root: Path,
    *,
    timeout_seconds: float = VALIDATION_TIMEOUT_SECONDS,
    expire: Callable[[float], Awaitable[object]] = asyncio.sleep,
) -> _TextToolServer:
    """Serve the reflection tools; ``expire(timeout_seconds)`` returns when a check times out."""
    if (
        isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= VALIDATION_TIMEOUT_SECONDS
    ):
        raise ValueError(f"validation timeout must be positive and at most {VALIDATION_TIMEOUT_SECONDS}s")
    checkout = root.resolve(strict=True)
    if not checkout.is_dir():
        raise ValueError("validation root must be a checkout directory")
    server = _TextToolServer("sdo_reflection")
    install_authoring_tools(server, checkout)
    install_evidence_tools(server)

    @server.tool()
    async def check_memory(
        decision_path: str | None = None,
        learning_decision: Literal["updated", "no_change", "sdk_gap"] | None = None,
        summary: str = "",
        no_change_reason: str = "",
        decline_reason: Literal["", "no_recorded_repair"] = "",
        proposed_artifact_ids: list[str] | None = None,
        sdk_gaps: list[SdkGap] | None = None,
    ) -> ValidationResult:
        """Run the complete memory check and wait for final diagnostics (at most 300s).

        Supply the typed decision fields directly; this tool writes the result
        JSON outside the authored tree. Return the same decision and logical IDs.
        Legacy decision_path files are also supported. One call waits for final
        diagnostics. Compile/import/format/provenance checks remain required;
        replay verdicts are information and never block saving. The broker repeats the check.
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

        async def stop() -> tuple[bytes, bytes]:
            # Go builds/tests may have children holding the pipes. Terminate the
            # whole validation process group and reap it, including on tool cancellation.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            return await communication

        deadline = asyncio.ensure_future(expire(timeout_seconds))
        try:
            await asyncio.wait((communication, deadline), return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            await stop()
            raise
        finally:
            deadline.cancel()
        if communication.done():
            stdout, stderr = communication.result()
            return ValidationResult(
                process.returncode or 0, False, validation_output(stdout), validation_output(stderr)
            )
        stdout, stderr = await stop()
        return ValidationResult(
            2,
            True,
            validation_output(stdout),
            f"memory check timed out after {timeout_seconds:g}s\n{validation_output(stderr)}",
        )

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=VALIDATION_TIMEOUT_SECONDS)
    args = parser.parse_args()
    validation_server(args.root, timeout_seconds=args.timeout_seconds).run(transport="stdio")


if __name__ == "__main__":
    main()
