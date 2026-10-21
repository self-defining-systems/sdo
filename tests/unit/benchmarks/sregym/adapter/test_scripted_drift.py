from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from benchmarks.sregym.adapter import scripted_worker
from sdo.agents.views.models import DriftEvent, ObjectRef
from sdo.contracts import DriftInvestigationTask


def test_scripted_drift_requires_source_mismatch_and_unserved_target_port(monkeypatch, tmp_path: Path) -> None:
    manifest = tmp_path / "service.yaml"
    manifest.write_text(
        "kind: Service\nmetadata:\n  name: api\nspec:\n  ports:\n    - port: 80\n      targetPort: 8080\n"
    )
    request = DriftInvestigationTask(
        batch_id="batch",
        namespace="app",
        repository_worktree=str(tmp_path),
        events=[
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
            '{"items": [{"metadata": {"labels": {"app": "api"}}, '
            '"spec": {"containers": [{"ports": [{"containerPort": 8080}]}]}}]}'
        )
        if "pods" in argv
        else '{"spec": {"selector": {"app": "api"}, "ports": [{"port": 80, "targetPort": 9999}]}}',
    )

    result = scripted_worker.scripted_drift_investigation(request)

    assert result.events[0].verdict == "bug"
    assert {item.kind for item in result.events[0].evidence} == {"source_contradiction", "violated_relation"}


def test_scripted_drift_is_unsure_without_selected_pods(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "service.yaml").write_text(
        "kind: Service\nmetadata:\n  name: api\nspec:\n  ports:\n    - port: 80\n      targetPort: 8080\n"
    )
    request = DriftInvestigationTask(
        batch_id="batch",
        namespace="app",
        repository_worktree=str(tmp_path),
        events=[
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
        lambda argv, **_: '{"items": []}'
        if "pods" in argv
        else '{"spec": {"selector": {"app": "api"}, "ports": [{"port": 80, "targetPort": 9999}]}}',
    )

    assert scripted_worker.scripted_drift_investigation(request).events[0].verdict == "unsure"


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
                detector_id="drift/service-targetport-mismatch",
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
