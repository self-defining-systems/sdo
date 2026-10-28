from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as ProtoAny

from benchmarks.sregym.adapter import scripted_worker
from sdo.contracts import (
    AgentResult,
    AgentTask,
    IncidentRequest,
    IncidentResult,
    ReflectionRequest,
    ReflectionResult,
    StateChanges,
)
from sdo.contracts.proto import parse_json, to_canonical_json
from tests.proto_messages import evolve, from_dict
from tests.unit.sdo.agents.responder.test_reflection_outcomes import _outcome
from tests.unit.sdo.agents.responder.test_reflection_worker import _closure

FIXTURES = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts"
ROOT = Path(__file__).resolve().parents[5]
SEED_SOURCES = [FIXTURES.parent / "scripted_seed"]


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
                        "service": {"api_version": "v1", "kind": "Service", "namespace": "hotel", "name": "front"}
                    },
                    "verdict": {"result": "Match"},
                },
                {"program_id": "other", "bindings": {}, "verdict": {"result": "NoMatch"}},
            ]
        }
    )
    assert (plans, programs) == (["plan-1"], ["service-targetport-mismatch"])
    assert calls == [("plan", "service-targetport-mismatch", "--bind", "service=Service/hotel/front")]


def test_scripted_gate_publishes_source_without_changing_topology(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = from_dict(IncidentRequest, json.loads((FIXTURES / "incident_request.json").read_text()))
    request.repository_worktree = str(tmp_path)
    (tmp_path / "kubernetes").mkdir()
    path = tmp_path / "kubernetes/frontend.yaml"
    original = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "frontend", "namespace": request.namespace},
        "spec": {"ports": [{"port": 80}]},
    }
    path.write_text(json.dumps(original))
    monkeypatch.setenv("SDO_SCRIPTED_SOURCE_REPAIR", "1")
    monkeypatch.setattr(
        scripted_worker, "_tool", lambda *args: {"program_runs": []} if args[0] == "diagnose" else {"healthy": True}
    )
    commands: list[list[str]] = []
    monkeypatch.setattr(scripted_worker, "_command", lambda args, **kwargs: commands.append(args) or "")
    result = scripted_worker.scripted_responder(request)
    import yaml

    documents = [
        obj for file in (tmp_path / "kubernetes").glob("*.yaml") for obj in yaml.safe_load_all(file.read_text())
    ]
    assert [(obj["kind"], obj["metadata"]["name"]) for obj in documents] == [("Service", "frontend")]
    assert (
        yaml.safe_load(path.read_text())["metadata"]["annotations"]["sdo.dev/scripted-verification"]
        == "independent-health"
    )
    assert commands == [["kubectl", "-n", request.namespace, "apply", "-f", str(path)]]
    assert any(action.resources[0].name == "frontend" for action in result.repair_actions)
    assert result.usage.llm_calls == 0
    repeated = scripted_worker.scripted_responder(request)
    assert len(commands) == 1
    assert not repeated.repair_actions


@pytest.mark.parametrize("seed_source", SEED_SOURCES)
@pytest.mark.parametrize("proposal_encoding", ["snake_case", "camel_case"])
@pytest.mark.parametrize(
    ("opened_policies", "fixed_policies"),
    [("absent", "absent"), ("null", "absent"), ("absent", "null"), ("null", "null")],
)
def test_scripted_entry_points_complete_on_seed_copy_with_fake_kubernetes_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    seed_source: Path,
    proposal_encoding: str,
    opened_policies: str,
    fixed_policies: str,
) -> None:
    seed = tmp_path / "seed"
    shutil.copytree(seed_source, seed, ignore=shutil.ignore_patterns(".git"))
    diagnostics = seed / ".sdo/diagnostics"
    diagnostics.mkdir(parents=True, exist_ok=True)
    (diagnostics / "go.mod").write_text(
        "module fixture-diagnostics\n\ngo 1.24\n\nrequire sdo.dev/operator/sdk v0.0.0\n"
    )
    raw = json.loads((FIXTURES / "incident_request.json").read_text(encoding="utf-8"))
    raw["repository_worktree"] = str(seed)
    raw["namespace"] = "hotel"
    raw["findings"][0]["primary_resource"].update(
        {"kind": "Service", "api_version": "v1", "name": "reservation", "namespace": "hotel"}
    )
    raw["findings"][0].setdefault("playbook_ids", raw["findings"][0].pop("playbooks", []))
    raw["surfaced_playbooks"] = []
    raw["memory_snapshot"] = {"accepted_commit": raw["repository_base_commit"]}
    raw["state_changes"] = {
        "baseline_at": raw["response_deadline"],
        "observed_at": raw["response_deadline"],
        "changes": [
            {"kind": "Service", "name": "reservation", "change": "modified"},
            {"kind": "Deployment", "name": "rate", "change": "modified"},
        ],
    }
    request = json_format.ParseDict(raw, IncidentRequest())
    packed = ProtoAny()
    packed.Pack(request)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="response-1",
        agent_id="sdo-responder",
        task_type="incident-response",
        request=packed,
        workspace={"proposal_id": "proposal-1", "checkout_path": str(seed)},
    )
    task_path = tmp_path / "agent-task.json"
    task_path.write_text(to_canonical_json(task), encoding="utf-8")

    class FakeKubernetesAPI:
        def __init__(self, *, namespace: str) -> None:
            assert namespace == "hotel"
            self.objects: dict[tuple[str, str], dict[str, Any]] = {
                ("Service", "reservation"): {"spec": {"ports": [{"port": 8084, "targetPort": 9999}]}},
                ("Deployment", "rate"): {
                    "spec": {"template": {"spec": {"containers": [{"name": "rate", "image": "missing:tag"}]}}}
                },
            }
            self.results: dict[str, str] = {}

        def publish_payload(self, _name: str, key: str, payload: str) -> None:
            self.results[key] = payload

    api = FakeKubernetesAPI(namespace="hotel")
    monkeypatch.setattr(scripted_worker, "KubernetesResultPublisher", lambda **kwargs: api)

    def command(argv: list[str], **_kwargs: Any) -> str:
        if argv[:4] == ["kubectl", "-n", "hotel", "patch"]:
            kind = "Service" if argv[4] == "service" else "Deployment"
            api.objects[kind, argv[5]] = json.loads(argv[-1])
            return ""
        if argv[:4] == ["kubectl", "-n", "hotel", "rollout"]:
            return ""
        raise AssertionError(argv)

    def tool(*args: str) -> dict[str, Any]:
        if args[0] == "diagnose":
            return {"program_runs": []}
        if args[0] == "source":
            kind, _, name = args[1].split("/")
            if kind == "Service":
                return {"source": {"spec": {"ports": [{"port": 8084, "targetPort": 8087}]}}}
            return {
                "source": {"spec": {"template": {"spec": {"containers": [{"name": name, "image": "built:source"}]}}}}
            }
        if args[0] == "verify":
            service = api.objects["Service", "reservation"]["spec"]["ports"][0]
            image = api.objects["Deployment", "rate"]["spec"]["template"]["spec"]["containers"][0]["image"]
            return {"healthy": service["targetPort"] != 9999 and image != "missing:tag", "summary": "checked fake API"}
        raise AssertionError(args)

    monkeypatch.setattr(scripted_worker, "_command", command)
    monkeypatch.setattr(scripted_worker, "_tool", tool)
    monkeypatch.setenv("SDO_NAMESPACE", "hotel")
    monkeypatch.setenv("SDO_RESULT_CONFIGMAP", "result-map")
    monkeypatch.setenv("SDO_TASK_PATH", str(task_path))
    assert scripted_worker.main(["responder"]) == 0
    result = parse_json(api.results["agent-result.json"], AgentResult())
    response = IncidentResult()
    assert result.response.Unpack(response)
    assert result.status == "completed"
    assert {action.target for action in response.repair_actions} == {"Service/reservation", "Deployment/rate"}
    assert api.objects["Deployment", "rate"]["spec"]["template"]["spec"]["containers"][0]["image"] == "built:source"
    capture = tmp_path / "capture"
    capture.mkdir()
    (capture / "open@detect.json").write_text(
        json.dumps({"services": [{"metadata": {"name": "reservation"}, "spec": {"ports": [{"targetPort": 9999}]}}]}),
        encoding="utf-8",
    )
    (capture / "open@detect-recordings.json").write_text("[]")
    (capture / "fixed.json").write_text(
        json.dumps(
            {
                "services": [
                    {"metadata": {"name": "reservation"}, "spec": {"ports": [{"targetPort": 8087}]}},
                    {
                        "metadata": {"name": "rate"},
                        "spec": {"selector": {"app": "rate"}, "ports": [{"targetPort": 8084}]},
                    },
                ],
                "pods": [
                    {
                        "metadata": {"labels": {"app": "rate"}},
                        "spec": {"containers": [{"ports": [{"containerPort": 8084}]}]},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    # Native captures can omit empty collections or retain legacy JSON null.
    for filename, policies in (("open@detect.json", opened_policies), ("fixed.json", fixed_policies)):
        if policies == "null":
            snapshot_path = capture / filename
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            snapshot["networkPolicies"] = None
            snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(capture))
    program_source = ROOT / "benchmarks/sregym/fastloop/scripted_program"
    monkeypatch.setattr(scripted_worker, "PROGRAM_SOURCE", program_source)
    closure = _closure()
    closure.request.repository_worktree = str(seed)
    closure.request.namespace = "hotel"
    outcome = _outcome(closure.request.incident_id)

    def operator_command(argv: list[str], **_kwargs: Any) -> str:
        if argv[1:3] == ["program", "scaffold"]:
            target = Path(argv[argv.index("--out") + 1])
            target.mkdir()
            (target / "diagnose_test.go").write_text("package targetport\n", encoding="utf-8")
            return ""
        assert argv[1:3] == ["memory", "check"]
        target = seed / ".sdo/playbooks" / scripted_worker.PROGRAM_ID
        assert (target / "diagnose.go").is_file()
        labels = json.loads(next((seed / ".sdo/corpus").glob("*/labels.json")).read_text())
        assert labels["bindings"]["service"]["name"] == "reservation"
        assert labels["peer_bindings"]["service"]["name"] == "rate"
        return ""

    monkeypatch.setattr(scripted_worker, "_command", operator_command)
    reflection = ReflectionRequest(closure=closure, outcome=outcome, outcome_commit="outcome-commit")
    reflection.memory_snapshot.accepted_commit = "outcome-commit"
    packed.Pack(reflection)
    task = AgentTask(
        schema_version="sdo.dev/v1alpha1",
        task_id="reflection-1",
        agent_id="sdo-reflector",
        task_type="outcome-reflection-direct-v1",
        request=packed,
        workspace={"proposal_id": "proposal-2", "checkout_path": str(seed), "base_commit": "outcome-commit"},
    )
    task_path.write_text(to_canonical_json(task), encoding="utf-8")
    assert scripted_worker.main(["reflection"]) == 0
    reflected = parse_json(api.results["agent-result.json"], AgentResult())
    decision = ReflectionResult()
    assert reflected.response.Unpack(decision)
    assert reflected.status == "completed"
    assert decision.learning_decision == "updated"
    assert list(decision.proposed_artifact_ids) == [scripted_worker.PROGRAM_ID]
    assert not decision.HasField("artifact_proposal")


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
    (tmp_path / "open@detect.json").write_text(json.dumps(open_snapshot), encoding="utf-8")
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
    (tmp_path / "kubernetes").mkdir()
    (tmp_path / "kubernetes" / "front.yaml").write_text(json.dumps(manifest), encoding="utf-8")
    request = evolve(
        from_dict(IncidentRequest, json.loads((FIXTURES / "incident_request.json").read_text())),
        repository_worktree=str(tmp_path),
        namespace="hotel",
    )
    calls: list[list[str]] = []

    def command(argv: list[str], **_kwargs: Any) -> str:
        calls.append(argv)
        return ""

    monkeypatch.setattr(scripted_worker, "_command", command)
    monkeypatch.setattr(scripted_worker, "_tool", lambda *_args: {"source": manifest})
    scripted_worker._patch_from_source(request, {"kind": "Deployment", "name": "front"})
    assert calls[0][:7] == ["kubectl", "-n", "hotel", "patch", "deployment", "front", "--type=strategic"]
    assert calls[1][:6] == ["kubectl", "-n", "hotel", "rollout", "status", "deployment/front"]


def test_scripted_responder_returns_typed_result_after_source_repair(monkeypatch: pytest.MonkeyPatch) -> None:
    request = from_dict(IncidentRequest, json.loads((FIXTURES / "incident_request.json").read_text()))
    request = evolve(
        request,
        findings=[
            evolve(
                request.findings[0],
                primary_resource=evolve(request.findings[0].primary_resource, kind="Service", name="front"),
            )
        ],
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
    assert result.status == "completed"
    assert result.incident_id == request.incident_id
    assert result.confirmed_root_causes[0].explained_detectors == [request.findings[0].detector_id]
    assert result.repair_actions[0].resources[0].name == "front"
    assert result.verification_evidence[0].passed
    assert result.usage.llm_calls == 0
    assert calls == ["diagnose", "verify", "verify"]
    assert repairs[0]["name"] == "front"


def test_scripted_responder_repairs_changed_service_behind_deployment_finding(monkeypatch: pytest.MonkeyPatch) -> None:
    request = from_dict(IncidentRequest, json.loads((FIXTURES / "incident_request.json").read_text()))
    request = evolve(
        request,
        state_changes=from_dict(
            StateChanges,
            {
                "baseline_at": request.response_deadline,
                "observed_at": request.response_deadline,
                "changes": [
                    {"kind": "Service", "name": "front", "change": "modified"},
                    {"kind": "Deployment", "name": "back", "change": "modified"},
                ],
            },
        ),
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


def test_scripted_responder_removes_added_objects_and_restores_config_from_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = from_dict(IncidentRequest, json.loads((FIXTURES / "incident_request.json").read_text()))
    request = evolve(
        request,
        namespace="hotel",
        findings=[],
        drift_events=[],
        state_changes=from_dict(
            StateChanges,
            {
                "baseline_at": request.response_deadline,
                "observed_at": request.response_deadline,
                "changes": [
                    {"kind": "ResourceQuota", "name": "tight", "change": "added"},
                    {"kind": "ValidatingWebhookConfiguration", "name": "deny", "change": "added"},
                    {"kind": "ConfigMap", "name": "cfg", "change": "modified"},
                ],
            },
        ),
    )
    calls: list[list[str]] = []
    verifies = 0

    def tool(*args: str) -> dict[str, Any]:
        nonlocal verifies
        if args[0] == "diagnose":
            return {"program_runs": []}
        if args[0] == "source":
            return {"source": {"data": {"k": "v"}}}
        verifies += 1
        return {"healthy": verifies >= 2, "summary": "clear"}

    monkeypatch.setattr(scripted_worker, "_tool", tool)
    monkeypatch.setattr(scripted_worker, "_command", lambda argv, **_kwargs: calls.append(argv) or "")
    result = scripted_worker.scripted_responder(request)
    assert ["kubectl", "-n", "hotel", "delete", "resourcequota", "tight", "--ignore-not-found"] in calls
    assert ["kubectl", "delete", "validatingwebhookconfiguration", "deny", "--ignore-not-found"] in calls
    patch = next(argv for argv in calls if "patch" in argv)
    assert patch[:6] == ["kubectl", "-n", "hotel", "patch", "configmap", "cfg"]
    assert "--type=merge" in patch
    assert {action.kind for action in result.repair_actions} == {"kubectl-delete", "kubectl-patch"}


@pytest.mark.parametrize("change", ["empty", "corpus", "playbook", "committed", "staged"])
def test_existing_scripted_program_reports_actual_checkout_diff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    import subprocess

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(tmp_path), *args], text=True)

    git("init", "-q")
    git("config", "user.email", "fixture@example.invalid")
    git("config", "user.name", "Synthetic fixture")
    program = tmp_path / ".sdo/playbooks" / scripted_worker.PROGRAM_ID / "diagnose.go"
    program.parent.mkdir(parents=True)
    program.write_text("package fixture\n")
    git("add", ".")
    git("commit", "-qm", "synthetic accepted program")
    base = git("rev-parse", "HEAD").strip()
    monkeypatch.setenv("SDO_BASE_COMMIT", base)
    monkeypatch.delenv("SDO_SCRIPTED_REPLAY_SLOT", raising=False)
    if change != "empty":
        path = program if change == "playbook" else tmp_path / ".sdo/corpus/repeat/labels.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n")
        if change in {"committed", "staged"}:
            git("add", ".")
        if change == "committed":
            git("commit", "-qm", "synthetic prepared corpus")
    checked: list[list[str]] = []
    monkeypatch.setattr(scripted_worker, "_check_reflection", lambda _root, ids: checked.append(ids))
    turn = scripted_worker.ScriptedReflector().resume(worktree=tmp_path, incident_id="synthetic-repeat")
    assert turn.learning_decision == ("no_change" if change == "empty" else "updated")
    expected_ids = [scripted_worker.PROGRAM_ID] if change == "playbook" else []
    assert turn.playbook_ids == expected_ids
    assert checked == ([] if change == "empty" else [expected_ids])


@pytest.mark.parametrize("health", ["drift-only", "same-object", "selecting-service", "other-service"])
def test_scripted_health_repair_is_not_suppressed_by_unsure_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, health: str
) -> None:
    ref = {"api_version": "apps/v1", "kind": "Deployment", "namespace": "demo", "name": "catalog"}
    service = {"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "catalog"}
    findings = [{"detector_id": "drift.unexplained", "primary_resource": ref}]
    if health == "same-object":
        findings.append({"detector_id": "workload-health", "primary_resource": ref})
    elif health != "drift-only":
        # The usual first signal: the Service has no ready endpoints before the Pod reports its image error.
        findings.append({"detector_id": "health-objective", "primary_resource": service})
    selector = {"app": "other" if health == "other-service" else "catalog"}
    accepted = {
        "Service/demo/catalog": {"kind": "Service", "spec": {"selector": selector}},
        "Deployment/demo/catalog": {
            "kind": "Deployment",
            "spec": {"template": {"metadata": {"labels": {"app": "catalog"}}}},
        },
    }
    request = from_dict(
        IncidentRequest,
        {
            "incident_id": "synthetic-image-fault",
            "namespace": "demo",
            "repository_worktree": str(tmp_path),
            "findings": findings,
            "drift_events": [
                {
                    "ref": ref,
                    "fields": [
                        {"path": "spec.template.spec.containers[0].image", "old": "accepted:v1", "new": "missing:v1"}
                    ],
                }
            ],
        },
    )
    patches: list[dict[str, str]] = []
    monkeypatch.delenv("SDO_SCRIPTED_SOURCE_REPAIR", raising=False)
    monkeypatch.setattr(scripted_worker, "_patch_from_source", lambda _request, resource: patches.append(resource))

    def tool(op: str, *args: str) -> dict[str, object]:
        if op == "diagnose":
            return {"program_runs": []}
        if op == "source":
            return {"source": accepted[args[0]]}
        # Only restoring the faulted Deployment returns health; patching the Service alone does not.
        return {"healthy": ref in patches}

    monkeypatch.setattr(scripted_worker, "_tool", tool)
    response = scripted_worker.scripted_responder(request)
    authorized = health in {"same-object", "selecting-service"}
    assert (ref in patches) == authorized
    assert response.status == ("completed" if authorized else "failed")
    assert response.drift_events[0].investigation_verdict == "unsure"


@pytest.mark.parametrize("classification", ["failed", "success"])
def test_scripted_notes_require_eligible_learning_on_failed_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, classification: str
) -> None:
    import subprocess

    from sdo.contracts import OutcomeRecord

    for args in (
        ["init", "-q"],
        [
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "synthetic outcome",
        ],
    ):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True)
    monkeypatch.setenv("SDO_BASE_COMMIT", "HEAD")
    monkeypatch.setenv("SDO_SCRIPTED_MEMORY_CHANGE", "1")
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(tmp_path / "no-capture"))
    monkeypatch.setattr(scripted_worker, "_check_reflection", lambda *_args: None)
    turn = scripted_worker.ScriptedReflector().resume(
        worktree=tmp_path,
        incident_id="synthetic-outcome",
        outcome=OutcomeRecord(classification=classification),
        cause_work=[],
    )
    assert turn.learning_decision == ("no_change" if classification == "failed" else "updated")
    assert (tmp_path / ".sdo/playbooks/verified-repair-notes/README.md").exists() == (classification == "success")
