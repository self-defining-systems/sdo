"""Offline live-shape gate through prompt/CLI producers and the scripted provider's rollout."""

from __future__ import annotations

import io
import os
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.incident_context import prepared_incident_data
from sdo.agents.responder.reflection import _logical_learning_request
from sdo.agents.responder.tools_cli import _print_response, command_surface
from sdo.agents.session_context import measure_session
from sdo.contracts import IncidentClosure, IncidentRequest, OperationalMemorySnapshot, OutcomeRecord

from .scripted_codex.wire import Rollout


def check_context_shapes(directory: Path) -> dict[str, dict[str, int]]:
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
            responder = _responder_prompt(request, incident_data=data)
        reflection = _logical_learning_request(
            outcome=outcome,
            history=[],
            outcome_commit="b" * 40,
            closure=IncidentClosure(request=request),
            guidance="baseline",
        )
        rollouts = {}
        for role, prompt in (("responder", responder), ("reflection", reflection)):
            rollout = Rollout.create(directory / role, cwd=str(directory))
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
                    _print_response(response, full_json=json_output)
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
