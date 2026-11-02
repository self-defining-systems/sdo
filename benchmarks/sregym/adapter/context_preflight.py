"""Offline live-shape gate through prompt/CLI producers and the scripted provider's rollout."""

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

from sdo.agents.responder import (
    command_surface,
    prepared_incident_data,
    print_tool_response,
    render_incident_prompt,
    render_reflection_prompt,
)
from sdo.agents.session_context import measure_session
from sdo.contracts import IncidentClosure, IncidentRequest, OperationalMemorySnapshot, OutcomeRecord


class ContextRollout(Protocol):
    path: Path

    def user_message(self, text: str) -> None: ...
    def function_call_output(self, call_id: str, output: str, exit_code: int) -> None: ...


class _OfflineRollout:
    """Canonical provider response items for the host preflight, without a provider process."""

    def __init__(self, home: Path) -> None:
        home.mkdir(parents=True, exist_ok=True)
        self.path = home / "rollout.jsonl"
        self.path.write_text("")

    def _write(self, payload: dict[str, object]) -> None:
        with self.path.open("a") as stream:
            stream.write(json.dumps({"type": "response_item", "payload": payload}) + "\n")

    def user_message(self, text: str) -> None:
        self._write({"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]})

    def function_call_output(self, call_id: str, output: str, exit_code: int) -> None:
        self._write({"type": "function_call_output", "call_id": call_id, "output": f"Exit code: {exit_code}\n{output}"})


def check_context_shapes(
    directory: Path,
    *,
    rollout_factory: Callable[[Path], ContextRollout] = _OfflineRollout,
) -> dict[str, dict[str, int]]:
    """No cluster, provider process or model; emit/read the real scripted session format."""
    directory.mkdir(parents=True, exist_ok=True)
    request = IncidentRequest(
        incident_id="synthetic-context-gate",
        namespace="synthetic",
        application="synthetic",
        repository_worktree=str(directory),
        memory_snapshot=OperationalMemorySnapshot(accepted_commit="a" * 40),
    )
    for index in range(100):
        request.findings.add(
            detector_id=f"detector-{index}",
            fingerprint=f"finding-{index}",
            summary=f"Synthetic unavailable resource {index}",
            evidence="observed evidence " * 1000,
            playbook_ids=[f"repair-{index}"],
        )
        request.memory_snapshot.detectors.add(id=f"detector-{index}", description="Synthetic detector")
        request.memory_snapshot.playbooks.add(
            id=f"repair-{index}",
            markdown="---\ndescription: Synthetic repair\n---\n" + "body " * 1000,
        )
    outcome = OutcomeRecord(
        schema_version=1, incident_id=request.incident_id, classification="success", findings=request.findings
    )
    before = os.environ.get("SDO_SESSION_SCRATCH")
    os.environ["SDO_SESSION_SCRATCH"] = str(directory)
    try:
        with prepared_incident_data(request) as data:
            responder = render_incident_prompt(request, incident_data=data)
        reflection = render_reflection_prompt(
            outcome=outcome,
            history=[],
            outcome_commit="b" * 40,
            closure=IncidentClosure(request=request),
            guidance="baseline",
        )
        rollouts = {}
        for role, prompt in (("responder", responder), ("reflection", reflection)):
            rollout = rollout_factory(directory / role)
            rollout.user_message(prompt)
            rollouts[role] = rollout
        findings = [
            {"fingerprint": f"finding-{index}", "summary": "unavailable", "evidence": "observed " * 2000}
            for index in range(100)
        ]
        # Enumerate the parser's registration surface. Future commands are
        # exercised automatically here and require their own contract fixture in tests.
        for index, command in enumerate(command_surface()["commands"]):
            response = {
                "command": command,
                "healthy": False,
                "summary": "Synthetic live-shape response",
                "findings": findings,
                "before_findings": [],
                "controller_findings": findings,
                "progress": {"fixed": [], "new": findings, "unchanged": []},
                "source": {"data": "manifest " * 20000},
                "menu": {"options": [{"option_id": "synthetic", "description": "schema " * 7000}]},
                "report": {"status": "STATUS_UNRESOLVED", "rounds": findings},
            }
            for json_output in (False, True):
                output = io.StringIO()
                with redirect_stdout(output):
                    print_tool_response(response, full_json=json_output)
                rollouts["responder"].function_call_output(f"{index}-{json_output}", output.getvalue(), 0)
        metrics = {role: measure_session(rollout.path) for role, rollout in rollouts.items()}
        for role, measured in metrics.items():
            measured.check(role)
        return {role: measured.as_dict() for role, measured in metrics.items()}
    finally:
        if before is None:
            os.environ.pop("SDO_SESSION_SCRATCH", None)
        else:
            os.environ["SDO_SESSION_SCRATCH"] = before


def context_preflight() -> dict[str, dict[str, int]]:
    with TemporaryDirectory(prefix="sdo-context-preflight-") as directory:
        return check_context_shapes(Path(directory))


run_context_preflight = context_preflight
