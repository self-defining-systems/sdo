from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import scripted_worker
from sdo.agents.views import IncidentRequest, StateChanges

if TYPE_CHECKING:
    import pytest

FIXTURES = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"


def test_matched_program_runs_use_the_real_tool_binding_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    def tool(*args: str) -> dict[str, str]:
        calls.append(args)
        return {"plan_id": "plan-1"}

    monkeypatch.setattr(scripted_worker, "_tool", tool)
    plans, programs = scripted_worker._matched_plans(
        {
            "program_runs": [
                {
                    "program_id": "service-targetport-mismatch",
                    "bindings": {
                        "service": {
                            "api_version": "v1",
                            "kind": "Service",
                            "namespace": "hotel",
                            "name": "front",
                        }
                    },
                    "verdict": {"result": "Match"},
                },
                {"program_id": "other", "bindings": {}, "verdict": {"result": "NoMatch"}},
            ]
        }
    )

    assert (plans, programs) == (["plan-1"], ["service-targetport-mismatch"])
    assert calls == [("plan", "service-targetport-mismatch", "--bind", "service=Service/hotel/front")]


def test_captured_pair_selects_changed_service_and_healthy_peer(tmp_path: Path) -> None:
    open_snapshot = {"services": [{"metadata": {"name": "front"}, "spec": {"ports": [{"targetPort": 9999}]}}]}
    fixed_snapshot = {
        "services": [
            {"metadata": {"name": "front"}, "spec": {"ports": [{"targetPort": 8080}]}},
            {"metadata": {"name": "empty"}, "spec": {"selector": {"app": "empty"}, "ports": [{"targetPort": 8080}]}},
            {"metadata": {"name": "peer"}, "spec": {"selector": {"app": "peer"}, "ports": [{"targetPort": 8080}]}},
        ],
        "pods": [
            {
                "metadata": {"name": "peer-1", "labels": {"app": "peer"}},
                "spec": {"containers": [{"ports": [{"containerPort": 8080}]}]},
            }
        ],
    }
    (tmp_path / "open@pre-apply.json").write_text(json.dumps(open_snapshot), encoding="utf-8")
    (tmp_path / "fixed.json").write_text(json.dumps(fixed_snapshot), encoding="utf-8")

    assert scripted_worker._captured_pair(tmp_path) == ("front", "peer")


def test_scripted_source_repair_preserves_deployment_container_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = {
        "kind": "Deployment",
        "metadata": {"name": "front"},
        "spec": {
            "template": {
                "spec": {"containers": [{"name": "front", "image": "good:v1", "env": [{"name": "A", "value": "B"}]}]}
            }
        },
    }
    (tmp_path / "front.yaml").write_text(json.dumps(manifest), encoding="utf-8")
    request = IncidentRequest.model_validate_json((FIXTURES / "incident_request.json").read_text()).model_copy(
        update={"repository_worktree": str(tmp_path), "namespace": "hotel"}
    )
    calls: list[list[str]] = []

    def command(argv: list[str], **_kwargs: Any) -> str:
        calls.append(argv)
        return ""

    monkeypatch.setattr(scripted_worker, "_command", command)
    scripted_worker._patch_from_source(request, {"kind": "Deployment", "name": "front"})

    assert calls[0][:7] == ["kubectl", "-n", "hotel", "patch", "deployment", "front", "--type=strategic"]
    assert calls[1][:6] == ["kubectl", "-n", "hotel", "rollout", "status", "deployment/front"]


def test_scripted_responder_returns_typed_result_after_source_repair(monkeypatch: pytest.MonkeyPatch) -> None:
    request = IncidentRequest.model_validate_json((FIXTURES / "incident_request.json").read_text())
    request = request.model_copy(
        update={
            "findings": [
                request.findings[0].model_copy(
                    update={
                        "primary_resource": request.findings[0].primary_resource.model_copy(
                            update={"kind": "Service", "name": "front"}
                        )
                    }
                )
            ]
        }
    )
    calls: list[str] = []
    repairs: list[dict[str, str]] = []

    def tool(*args: str) -> dict[str, Any]:
        calls.append(args[0])
        if args[0] == "diagnose":
            return {"program_runs": []}
        return {"healthy": len(calls) >= 3, "summary": "clear"}

    monkeypatch.setattr(scripted_worker, "_tool", tool)
    monkeypatch.setattr(scripted_worker, "_patch_from_source", lambda _request, ref: repairs.append(ref))
    result = scripted_worker.scripted_responder(request)

    assert result.status.value == "completed"
    assert result.incident_id == request.incident_id
    assert result.confirmed_root_causes[0].explained_detectors == [request.findings[0].detector_id]
    assert result.repair_actions[0].resources[0].name == "front"
    assert result.verification_evidence[0].passed
    assert result.usage.llm_calls == 0
    assert calls == ["diagnose", "verify", "verify"]
    assert repairs[0]["name"] == "front"


def test_scripted_responder_repairs_changed_service_behind_deployment_finding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = IncidentRequest.model_validate_json((FIXTURES / "incident_request.json").read_text())
    request = request.model_copy(
        update={
            "state_changes": StateChanges.model_validate(
                {
                    "baseline_at": request.response_deadline,
                    "observed_at": request.response_deadline,
                    "changes": [
                        {"kind": "Service", "name": "front", "change": "modified"},
                        {"kind": "Deployment", "name": "back", "change": "modified"},
                    ],
                }
            )
        }
    )
    repairs: list[str] = []
    calls = 0

    def tool(*args: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if args[0] == "diagnose":
            return {"program_runs": []}
        return {"healthy": calls >= 3, "summary": "clear"}

    monkeypatch.setattr(scripted_worker, "_tool", tool)
    monkeypatch.setattr(scripted_worker, "_patch_from_source", lambda _request, ref: repairs.append(ref["name"]))
    result = scripted_worker.scripted_responder(request)

    assert {"front", "back"} <= set(repairs)
    assert {"front", "back"} <= {resource.name for action in result.repair_actions for resource in action.resources}
