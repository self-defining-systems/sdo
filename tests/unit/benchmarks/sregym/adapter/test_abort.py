from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from benchmarks.sregym.adapter import persistent


class AbortOps:
    def __init__(self, leftovers=()):
        self.remaining = list(leftovers)
        self.stopped = []

    def stop_runtime(self, target):
        self.stopped.append(target.control_namespace)

    def abort_workload(self, target):
        pass

    def abort_leftovers(self, target):
        return self.remaining

    def runtime_state(self, *_):
        raise AssertionError("abort must never drain or inspect controller state")


def seed_state(path: Path):
    record = persistent.ControllerRecord(
        application="demo",
        namespace="demo",
        control_namespace="demo-sdo",
        lifecycle_fingerprint="f",
        controller_pod_uid="gone",
        controller_pod_name="gone",
        installed_by_stage="test",
        installed_at=datetime.now(timezone.utc),
        runtime_config={},
        pending=persistent.PendingIncident(
            incident_id="i1",
            stage_label="test",
            receipt_dir=path.parent,
            repository=path.parent,
            verified_at=None,
            stage_receipt={},
        ),
    )
    persistent.PersistentState(controllers={"demo": record}).save(path)


def test_abort_never_drains_and_records_aborted_incident(tmp_path):
    path = tmp_path / "state.json"
    seed_state(path)
    ops = AbortOps()
    assert persistent.abort(path, ops=ops) == []
    archived = persistent.PersistentState.load(path.with_name(path.name + ".abort-evidence.json"))
    assert archived.controllers["demo"].pending.incident_id == "i1"
    assert ops.stopped == ["demo-sdo"]
    state = persistent.PersistentState.load(path)
    assert state.controllers == {}
    assert state.aborted_incidents[0].incident_id == "i1"
    assert persistent.abort(path, ops=ops) == []


def test_abort_stops_transport_and_preserves_app_namespace(monkeypatch: Any) -> None:
    calls: list[list[str]] = []

    def invoke(argv: list[str], **_kwargs: Any) -> SimpleNamespace:
        calls.append(argv)
        return SimpleNamespace(stdout="", returncode=0)

    monkeypatch.setattr(persistent, "kubectl", invoke)
    target = persistent.CleanupTarget(namespace="hotel-reservation", control_namespace="hotel-reservation-sdo")
    persistent.KubectlClusterOps().abort_workload(target)
    assert calls
    assert not any(argv[:2] == ["delete", "namespace"] for argv in calls)
    persistent.KubectlClusterOps(preserve_applications=False).abort_workload(target)
    assert any(argv[:2] == ["delete", "namespace"] for argv in calls)


def test_preserved_namespace_is_not_abort_leftover(monkeypatch: Any) -> None:
    from benchmarks.sregym.adapter import native_operator

    monkeypatch.setattr(native_operator, "stop_runtime", lambda *_args, **_kwargs: {"leftovers": ["pod/stuck"]})

    def invoke(argv: list[str], **_kwargs: Any) -> SimpleNamespace:
        if argv[:2] == ["get", "namespace/hotel-reservation"]:
            return SimpleNamespace(stdout="namespace/hotel-reservation\n", returncode=0)
        return SimpleNamespace(stdout="{}" if "json" in argv else "", returncode=0)

    monkeypatch.setattr(persistent, "kubectl", invoke)
    target = persistent.CleanupTarget(namespace="hotel-reservation", control_namespace="hotel-reservation-sdo")
    assert persistent.KubectlClusterOps().abort_leftovers(target) == ["pod/stuck"]


def test_abort_lists_leftovers_and_keeps_retry_targets(tmp_path):
    path = tmp_path / "state.json"
    seed_state(path)
    errors = persistent.abort(path, ops=AbortOps(["pod/stuck"]))
    assert any("pod/stuck" in error for error in errors)
    assert persistent.PersistentState.load(path).cleanup_targets
    assert persistent.abort(path, ops=AbortOps()) == []


def test_abort_cli_nonzero_for_leftovers(tmp_path, monkeypatch, capsys):
    path = tmp_path / "state.json"
    seed_state(path)
    monkeypatch.setattr(persistent, "KubectlClusterOps", lambda: AbortOps(["pod/stuck"]))
    assert persistent._main(["abort", "--state", str(path)]) == 1
    assert "pod/stuck" in capsys.readouterr().out


def test_abort_attempts_every_target_after_one_stop_fails(tmp_path):
    path = tmp_path / "state.json"
    state = persistent.PersistentState(
        cleanup_targets={
            name: persistent.CleanupTarget(namespace=name, control_namespace=f"{name}-sdo", kubeconfig=f"/{name}")
            for name in ("first", "second")
        }
    )
    state.save(path)
    ops = AbortOps()
    seen = []

    def stop(target):
        seen.append(target.kubeconfig)
        if target.namespace == "first":
            raise RuntimeError("forbidden")

    ops.stop_runtime = stop
    assert any("forbidden" in error for error in persistent.abort(path, ops=ops))
    assert seen == ["/first", "/second"]
