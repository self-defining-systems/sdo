"""Synthetic three-fault and large incident responses through the production CLI."""

from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest

from sdo.agents.responder import tool_output, tools_cli
from sdo.contracts import Action

ACTION_OPTIONS = tuple(sorted(field.message_type.name for field in Action.DESCRIPTOR.oneofs[0].fields))


def large_response(command: str, count: int) -> dict[str, Any]:
    findings = [
        {
            "fingerprint": f"health/route-{i}",
            "summary": f"route-{i} unavailable " + "detail " * 1000,
            "evidence": "complete fresh observation " * 1000,
            "primary_resource": {"kind": "Service", "namespace": "toy", "name": f"route-{i}"},
        }
        for i in range(count)
    ]
    response: dict[str, Any] = {
        "command": command,
        "summary": f"{count} findings; two covered, remaining unexplained",
        "healthy": False,
        "loops_used": 2,
        "loops_allowed": 5,
    }
    if command in ("status", "detect", "verify"):
        response.update(findings=findings, drift_events=[{"summary": "drift " * 1000}] * count)
    if command in ("status", "diagnose", "apply"):
        response["program_runs"] = [
            {"program_id": f"repair-{i}", "can_mitigate": True, "verdict": {"result": "Match", "evidence": findings[i]}}
            for i in range(min(count, 2))
        ]
    if command in ("menu", "mitigate"):
        response["menu"] = {
            "round_limit": 5,
            "remaining_batches": 3,
            "options": [
                {"option_id": name, "description": "Full schema description " * 1000}
                for name in ("repair-0", "repair-1", *ACTION_OPTIONS)
            ],
        }
    if command in ("mitigate", "apply", "rollback"):
        response.update(application_id="application", targeted_healthy=True)
        response["report"] = {"status": "STATUS_UNRESOLVED", "attempt_id": "attempt", "rounds": findings}
        response["mitigation_health"] = {"findings": findings, "healthy": False, "targeted_healthy": True}
    if command == "report":
        response["report"] = {"status": "STATUS_UNRESOLVED", "attempt_id": "attempt", "rounds": findings}
    if command == "plan":
        response.update(
            plan_id="plan",
            patches=[{"ref": finding["primary_resource"], "changes": "patch " * 1000} for finding in findings],
        )
    if command == "source":
        response["source"] = {"spec": {"data": "complete accepted manifest " * 1000}}
    return response


def rendered_stdout_cases() -> dict[str, str]:
    """Invoke every CLI subcommand; only the controller I/O edge is replaced."""
    arguments = {
        "status": [],
        "detect": [],
        "check": [],
        "menu": [],
        "diagnose": ["repair-0"],
        "plan": ["repair-0"],
        "mitigate": ["repair-0"],
        "apply": ["plan"],
        "source": ["Service/toy/api"],
        "verify": [],
        "rollback": ["application"],
        "report": ["attempt", "--incident-id", "toy", "--session-id", "session", "--epoch", "1"],
        "act": ["--resource", "Service/toy/api", "--", sys.executable, "-c", "print('change ' * 10000)"],
    }
    assert set(arguments) == set(tools_cli.command_surface()["commands"])
    samples = {}
    with TemporaryDirectory() as scratch, pytest.MonkeyPatch.context() as patch:
        patch.setattr(tool_output, "OUTPUT_ROOT", Path(scratch) / "sdo")
        patch.setenv("SDO_INCIDENT_ID", "synthetic-three-fault")
        patch.setenv("SDO_SESSION_SCRATCH", scratch)
        for count in (3, 100):
            patch.setattr(
                tools_cli, "_exchange", lambda request, _, n=count: large_response(str(request["command"]), n)
            )
            for command, args in arguments.items():
                output = io.StringIO()
                with redirect_stdout(output):
                    code = tools_cli.main([command, *args])
                assert code == 0, command
                text = output.getvalue()
                details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
                full = json.loads(details.read_text())
                assert full["command"] == command
                assert "healthy=true" not in text.split()
                if command in ("detect", "status", "verify"):
                    assert len(full["findings"]) == count
                samples[f"{command}.{count}"] = output.getvalue()
        for command, args in arguments.items():
            if command == "act":
                continue

            def failed(request: dict[str, Any], _directory: Path) -> dict[str, Any]:
                return {
                    "command": request["command"],
                    "error": "bounded observation failure " * 30,
                    "summary": "bounded observation failure " * 30,
                }

            patch.setattr(tools_cli, "_exchange", failed)
            output = io.StringIO()
            with redirect_stdout(output):
                assert tools_cli.main([command, *args]) == 1
            text = output.getvalue()
            details = Path(text.split("Full JSON: ", 1)[1].splitlines()[0])
            assert json.loads(details.read_text())["error"]
            samples[f"{command}.error"] = text
    return samples
