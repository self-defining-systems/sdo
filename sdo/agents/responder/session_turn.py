"""The turn configuration shared by the responder and its reflection turn.

Reflection resumes the responder's session. Codex keys its prompt cache on a
request prefix that leads with the output schema and the tool list, so the two
turns send one wrapper schema and one MCP server set; otherwise the resumed
turn's first request misses the responder's cached history. Each caller reads
only its own branch of the wrapper.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from agentshim import StdioMcpServer

from sdo.agents.prompt_templates import render_prompt
from sdo.agents.responder.reflection_validation import VALIDATION_ENVIRONMENT, VALIDATION_TOOL_TIMEOUT_SECONDS
from sdo.agents.responder.writer_termination import KubernetesWriterObserver, worker_identity

if TYPE_CHECKING:
    from typing import Any

    from agentshim import McpServer

TurnBranch = Literal["incident_result", "reflection"]

_BRANCH_DESCRIPTIONS: dict[TurnBranch, str] = {
    "incident_result": render_prompt("responder", "branch_description_01", part="session.instructions"),
    "reflection": render_prompt("responder", "branch_description_02", part="session.instructions"),
}


class WrongBranchError(ValueError):
    """The turn answered in the other branch of the wrapper, or left its own branch null."""


def other_branch(branch: TurnBranch) -> TurnBranch:
    return "reflection" if branch == "incident_result" else "incident_result"


def branch_instruction(branch: TurnBranch) -> str:
    """One line for a prompt's output instructions."""

    return render_prompt(
        "responder",
        "branch_instruction_01",
        part="session.instructions",
        branch=branch,
        other_branch=other_branch(branch),
    )


def branch_correction(branch: TurnBranch) -> str:
    """The resumed turn that asks for the same answer in the right branch."""

    return render_prompt(
        "responder",
        "branch_correction_01",
        part="session.instructions",
        branch=branch,
        branch_2=branch,
        other_branch=other_branch(branch),
    )


def session_output_schema() -> dict[str, object]:
    """One strict root object whose nullable branches carry each turn's own result schema."""

    # Imported here: both modules import this one for their turn options.
    from sdo.agents.responder.codex import _incident_result_schema
    from sdo.agents.responder.reflection import reflection_output_schema

    branches: dict[TurnBranch, dict[str, object]] = {
        "incident_result": _incident_result_schema(),
        "reflection": reflection_output_schema(),
    }
    properties: dict[str, object] = {
        name: {"description": _BRANCH_DESCRIPTIONS[name], "anyOf": [schema, {"type": "null"}]}
        for name, schema in branches.items()
    }
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def session_mcp_servers(worktree: Path, provider: str) -> list[McpServer]:
    """The read-only memory-validation server; reflection calls it, the responder may ignore it."""

    return [
        StdioMcpServer(
            name="sdo_reflection",
            command=sys.executable,
            args=["-m", "sdo.agents.responder.reflection_validation", "--root", str(worktree.resolve())],
            env={key: os.environ[key] for key in VALIDATION_ENVIRONMENT if key in os.environ},
            tool_timeout_s=VALIDATION_TOOL_TIMEOUT_SECONDS if provider == "codex" else None,
        )
    ]


def session_turn_options(
    provider: str, worktree: Path, *, phase: Literal["responder", "reflection"] = "responder"
) -> dict[str, Any]:
    """Shared model-facing configuration and phase-bound writer ownership."""

    return {
        "session_ownership_role": phase,
        "session_ownership_worker": worker_identity(),
        "session_termination_observer": KubernetesWriterObserver(),
        "session_ownership_directory": Path(os.environ["SDO_SESSION_DIR"]) / "thread-ownership"
        if os.environ.get("SDO_SESSION_DIR")
        else None,
        "output_schema": session_output_schema(),
        "mcp_servers": session_mcp_servers(worktree, provider),
        "environment": {"MCP_TOOL_TIMEOUT": str(VALIDATION_TOOL_TIMEOUT_SECONDS * 1000)}
        if provider == "claude"
        else None,
    }


def unwrap_branch(output_json: str, branch: TurnBranch) -> dict[str, Any]:
    """The caller's own branch of a turn's output.

    Raises:
        WrongBranchError: The caller's branch is null or missing.
        ValueError: The output is not the wrapper object.
    """

    document = json.loads(output_json)
    if not isinstance(document, dict):
        raise ValueError("turn output is not an object")
    payload = document.get(branch)
    if not isinstance(payload, dict):
        other = [name for name, value in document.items() if name != branch and value is not None]
        detail = f"; it filled {', '.join(other)} instead" if other else ""
        raise WrongBranchError(f"turn output has no {branch} object{detail}")
    return payload
