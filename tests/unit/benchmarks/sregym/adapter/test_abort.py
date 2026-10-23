from datetime import datetime, timezone
from pathlib import Path

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
    assert ops.stopped == ["demo-sdo"]
    state = persistent.PersistentState.load(path)
    assert state.controllers == {}
    assert state.aborted_incidents[0].incident_id == "i1"
    assert persistent.abort(path, ops=ops) == []


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
