from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter import scripted_worker
from sdo.contracts import DriftEvent, IncidentRequest, ObjectRef


def test_scripted_source_repair_removes_injected_probe_when_source_has_none(monkeypatch) -> None:
    import json

    request = IncidentRequest(incident_id="repair", namespace="app")
    commands = []
    monkeypatch.setattr(
        scripted_worker,
        "_tool",
        lambda *_args: {
            "source": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "frontend",
                                    "image": "app:accepted",
                                    "livenessProbe": {"tcpSocket": {"port": 80}},
                                }
                            ]
                        }
                    }
                }
            }
        },
    )
    monkeypatch.setattr(scripted_worker, "_command", lambda argv, **_kwargs: commands.append(argv))

    scripted_worker._patch_from_source(request, {"kind": "Deployment", "name": "frontend"})

    patch = json.loads(commands[0][-1])["spec"]["template"]["spec"]["containers"][0]
    # A strategic merge needs explicit null to delete an injected optional field.
    assert "readinessProbe" in patch
    assert patch["readinessProbe"] is None
    assert patch["startupProbe"] is None
    assert patch["livenessProbe"] == {"tcpSocket": {"port": 80}}
    assert patch["image"] == "app:accepted"


def test_scripted_drift_requires_source_mismatch_and_observed_listening_ports(monkeypatch, tmp_path: Path) -> None:
    from contextlib import nullcontext

    manifest = tmp_path / "service.yaml"
    manifest.write_text(
        "kind: Service\nmetadata:\n  name: api\nspec:\n  ports:\n    - port: 80\n      targetPort: 8080\n"
    )
    request = IncidentRequest(
        incident_id="batch",
        namespace="app",
        repository_worktree=str(tmp_path),
        drift_events=[
            {
                "ref": {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"},
                "fields": [{"path": "spec.ports[0].targetPort", "old": "8080", "source": "8080", "new": "9999"}],
                "classification": "unexplained",
            }
        ],
    )
    monkeypatch.setattr(
        scripted_worker,
        "_command",
        lambda argv, **_: (
            (
                '{"items": [{"metadata": {"name": "api-pod", "labels": {"app": "api"}}, '
                '"status": {"podIP": "10.0.0.2"}, '
                '"spec": {"containers": [{"ports": [{"containerPort": 8080}]}]}}]}'
            )
            if "pods" in argv
            else '{"spec": {"selector": {"app": "api"}, "ports": [{"port": 80, "targetPort": 9999}]}}'
        ),
    )

    def connect(address, **_kwargs):
        if address[1] == 9999:
            raise ConnectionRefusedError("no listener")
        return nullcontext()

    monkeypatch.setattr("socket.create_connection", connect)

    result = scripted_worker._assess_drift(request, request.drift_events[0])

    assert result.investigation_verdict == "bug"
    assert {item.kind for item in result.investigation_evidence} == {"source_contradiction", "violated_relation"}


def test_scripted_drift_is_unsure_without_selected_pods(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "service.yaml").write_text(
        "kind: Service\nmetadata:\n  name: api\nspec:\n  ports:\n    - port: 80\n      targetPort: 8080\n"
    )
    request = IncidentRequest(
        incident_id="batch",
        namespace="app",
        repository_worktree=str(tmp_path),
        drift_events=[
            {
                "ref": {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"},
                "fields": [{"path": "spec.ports[0].targetPort", "old": "8080", "source": "8080", "new": "9999"}],
                "classification": "unexplained",
            }
        ],
    )
    monkeypatch.setattr(
        scripted_worker,
        "_command",
        lambda argv, **_: (
            '{"items": []}'
            if "pods" in argv
            else '{"spec": {"selector": {"app": "api"}, "ports": [{"port": 80, "targetPort": 9999}]}}'
        ),
    )

    assert scripted_worker._assess_drift(request, request.drift_events[0]).investigation_verdict == "unsure"


def test_scripted_responder_does_not_repair_prior_incident_drift(monkeypatch) -> None:
    old = ObjectRef(api_version="v1", kind="Service", namespace="app", name="reservation")
    current = ObjectRef(api_version="v1", kind="Service", namespace="app", name="peer")
    stale = DriftEvent(
        ref=old, observed_at=datetime.now(timezone.utc), classification="fault", investigation_verdict="bug"
    )
    request = SimpleNamespace(
        incident_id="second",
        namespace="app",
        drift_events=[stale],
        state_changes=None,
        findings=[
            SimpleNamespace(
                detector_id="drift.service-targetport-mismatch",
                primary_resource=current,
                summary="peer targetPort drift",
                evidence="peer is broken",
            )
        ],
    )
    patches = []
    monkeypatch.setattr(scripted_worker, "_patch_from_source", lambda _request, ref: patches.append(ref))
    monkeypatch.setattr(scripted_worker, "_tool", lambda *_args: {"healthy": True})

    result = scripted_worker.scripted_responder(request)

    assert patches == []
    assert result.repair_actions == []


@pytest.mark.parametrize("probe_result", ["refused", "timeout", "accepts", "source_refused"])
def test_scripted_drift_assesses_scratch_pods_without_exec_utilities(
    monkeypatch, tmp_path: Path, probe_result: str
) -> None:
    import json
    from contextlib import nullcontext

    (tmp_path / "service.yaml").write_text(
        "kind: Service\nmetadata:\n  name: api\nspec:\n  ports:\n    - port: 80\n      targetPort: 8080\n"
    )
    request = IncidentRequest(
        incident_id="scratch-pod",
        namespace="app",
        repository_worktree=str(tmp_path),
        drift_events=[
            {
                "ref": {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"},
                "fields": [{"path": "spec.ports[0].targetPort", "old": "8080", "source": "8080", "new": "9999"}],
                "classification": "unexplained",
            }
        ],
    )
    connections = []

    def command(argv, **_kwargs):
        if "exec" in argv:
            raise RuntimeError('exec: "cat": executable file not found')
        if "pods" in argv:
            return json.dumps(
                {"items": [{"metadata": {"name": "api", "labels": {"app": "api"}}, "status": {"podIP": "10.0.0.2"}}]}
            )
        return json.dumps({"spec": {"selector": {"app": "api"}, "ports": [{"targetPort": 9999}]}})

    def connect(address, **_kwargs):
        connections.append(address)
        if address[1] == 8080 and probe_result == "source_refused":
            raise ConnectionRefusedError("source port unavailable")
        if address[1] == 9999:
            if probe_result == "refused":
                raise ConnectionRefusedError("no listener")
            if probe_result == "timeout":
                raise TimeoutError("network unavailable")
        return nullcontext()

    monkeypatch.setattr(scripted_worker, "_command", command)
    monkeypatch.setattr("socket.create_connection", connect)

    label = scripted_worker._assess_drift(request, request.drift_events[0])
    assert label.investigation_verdict == ("bug" if probe_result == "refused" else "unsure")
    assert connections == [("10.0.0.2", 8080)] + ([] if probe_result == "source_refused" else [("10.0.0.2", 9999)])
    if probe_result == "refused":
        assert {item.kind for item in label.investigation_evidence} == {"source_contradiction", "violated_relation"}


@pytest.mark.parametrize(
    "near_miss", ["", "allow_rule", "accepted_policy", "no_pod", "changed_service", "unknown_selector"]
)
def test_scripted_policy_drift_requires_source_absence_and_total_ingress_isolation(monkeypatch, near_miss: str) -> None:
    import json
    from copy import deepcopy

    spec = {"podSelector": {"matchLabels": {"app": "backend"}}, "policyTypes": ["Ingress"]}
    if near_miss == "unknown_selector":
        spec["podSelector"] = {"matchExpressions": [{"key": "app", "operator": "Exists"}]}
    policy = {"metadata": {"name": "deny"}, "spec": spec}
    policies = [policy]
    if near_miss == "allow_rule":
        policies.append({"metadata": {"name": "allow"}, "spec": {**spec, "ingress": [{}]}})
    service = {
        "metadata": {"name": "backend"},
        "spec": {"selector": {"app": "backend"}, "ports": [{"port": 8080, "targetPort": 8080}]},
    }
    accepted = deepcopy(service)
    if near_miss == "changed_service":
        accepted["spec"]["ports"][0]["targetPort"] = 9000
    pods = [] if near_miss == "no_pod" else [{"metadata": {"name": "backend", "labels": {"app": "backend"}}}]
    request = IncidentRequest(
        incident_id="policy",
        namespace="app",
        drift_events=[
            {
                "ref": {
                    "api_version": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "namespace": "app",
                    "name": "deny",
                },
                "source": "differs from accepted manifest"
                if near_miss == "accepted_policy"
                else "absent from accepted source",
                "fields": [{"path": "spec", "new": json.dumps(spec)}],
            }
        ],
        findings=[
            {
                "detector_id": "drift.unexplained",
                "summary": "unexplained policy",
                "evidence": "policy isolation",
                "primary_resource": {
                    "api_version": "networking.k8s.io/v1",
                    "kind": "NetworkPolicy",
                    "namespace": "app",
                    "name": "deny",
                },
            }
        ],
    )
    deleted = []

    def command(argv, **_kwargs):
        if "delete" in argv:
            deleted.append(argv[-1])
            return ""
        kind = argv[argv.index("get") + 1]
        return json.dumps({"items": {"networkpolicies": policies, "services": [service], "pods": pods}[kind]})

    def tool(*args):
        return {"source": accepted} if args[0] == "source" else {"healthy": bool(deleted)}

    monkeypatch.setattr(scripted_worker, "_command", command)
    monkeypatch.setattr(scripted_worker, "_tool", tool)

    result = scripted_worker._assess_drift(request, request.drift_events[0])
    assert result.investigation_verdict == ("unsure" if near_miss else "bug")
    if not near_miss:
        assert {item.kind for item in result.investigation_evidence} == {"source_contradiction", "violated_relation"}
    response = scripted_worker.scripted_responder(request)
    assert deleted == ([] if near_miss else ["deny"])
    assert response.status == ("failed" if near_miss else "completed")
    if not near_miss:
        assert response.repair_actions[0].kind == "kubectl-delete"
        assert response.repair_actions[0].target == "NetworkPolicy/deny"


def test_scripted_source_restore_allows_slow_init_clone(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 170s init clone can start 41s after the repair, within a 600s rollout."""
    request = IncidentRequest(namespace="social-network")
    monkeypatch.setattr(
        scripted_worker,
        "_tool",
        lambda *args: {
            "source": {"spec": {"template": {"spec": {"containers": [{"name": "api", "image": "api@sha256:abc"}]}}}}
        },
    )

    def command(argv: list[str], *, timeout: int = 180) -> str:
        if "status" in argv:
            bound = int(
                next(arg.removeprefix("--timeout=").removesuffix("s") for arg in argv if arg.startswith("--timeout="))
            )
            if bound < 211 or timeout <= bound:
                raise RuntimeError("rollout timed out before the recorded init clone finished")
        return ""

    monkeypatch.setattr(scripted_worker, "_command", command)
    scripted_worker._patch_from_source(request, {"kind": "Deployment", "name": "api"})
