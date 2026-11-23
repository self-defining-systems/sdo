from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import controller_proof
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, reconcile_application
from benchmarks.sregym.fastloop.warm_lane import LaneIdentity, WarmLaneError, prepare_lane

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
REVISION = "a" * 40


class Lane:
    """Resource store consumed by the real reconciler and real controller proof."""

    def __init__(self) -> None:
        self.identity = LaneIdentity("controller-1", "standby-1", REVISION)
        self.objects = [self.service(99), self.service(80, "peer")]
        self.baseline = ApplicationBaseline("toy", [self.service(80), self.service(80, "peer")])
        self.writes: list[str] = []
        self.events: list[str] = []
        self.evaluated_at = NOW + timedelta(seconds=1)
        self.changed_at: str | None = None
        self.failed_at: str | None = None
        self.fallbacks = 0
        self.replaced_by_reseed = False

    @staticmethod
    def service(port: int, name: str = "app") -> dict[str, Any]:
        return {"kind": "Service", "metadata": {"name": name, "namespace": "toy"}, "spec": {"ports": [{"port": port}]}}

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        return self.objects

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        self.writes.append(value["metadata"]["name"])
        self.objects = [obj for obj in self.objects if obj["metadata"]["name"] != value["metadata"]["name"]] + [value]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.objects = [obj for obj in self.objects if obj["metadata"]["name"] != name]

    def create_namespace(self, namespace: str) -> None:
        raise AssertionError("toy namespace exists")

    def step(self, name: str) -> None:
        self.events.append(name)
        if self.failed_at == name:
            raise RuntimeError("synthetic failure")
        if self.changed_at == name:
            self.identity = LaneIdentity("replacement", "standby-1", REVISION)

    def inspect(self) -> LaneIdentity:
        self.step("inspect")
        return self.identity

    def pause(self) -> None:
        self.step("pause")

    def reset_changed(self, restored: Callable[[], None] | None = None) -> None:
        self.step("reset")
        reconcile_application(self, self.baseline, "toy")
        if restored is not None:
            restored()
        self.events.append("restored")
        self.step("verify")

    def reidentify(self) -> LaneIdentity:
        return self.identity

    def reseed(self, application_restored: Callable[[], None] | None = None) -> None:
        self.step("reseed")
        if application_restored is not None:
            application_restored()
        self.step("install")
        if self.replaced_by_reseed:
            # The reseed replaces the controller process by design: the new identity is the one that must hold.
            self.identity = LaneIdentity("controller-2", "standby-2", REVISION)

    def adopt(self) -> str:
        self.step("adopt")
        return self.identity.accepted_revision

    def prove(self, not_before: datetime, revision: str) -> dict[str, Any]:
        self.step("prove")
        return controller_proof(
            {"history": [{"detector_id": "health", "status": "clear", "evaluated_at": self.evaluated_at.isoformat()}]},
            {"health"},
            not_before.isoformat(),
        )

    def full_reset(self) -> None:
        self.fallbacks += 1
        self.step("full_reset")


def test_changed_only_reset_then_reseed_then_adopt_then_fresh_proof(tmp_path: Path) -> None:
    lane = Lane()
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert receipt.reused
    assert lane.writes == ["app"]
    # The new controller is installed on the restored application; both branches end before adoption and the proof.
    assert lane.events.index("restored") < lane.events.index("install") < lane.events.index("adopt")
    assert lane.events.index("verify") < lane.events.index("adopt") < lane.events.index("prove")
    assert not lane.fallbacks
    # Real restoration producer -> real resource consumer, rather than a supplied clean flag.
    assert {obj["metadata"]["name"]: obj["spec"]["ports"][0]["port"] for obj in lane.objects} == {"app": 80, "peer": 80}


@pytest.mark.parametrize("step", ["inspect", "pause", "reset", "verify", "reseed", "install", "adopt", "prove"])
def test_each_uncertain_boundary_falls_back(tmp_path: Path, step: str) -> None:
    lane = Lane()
    lane.failed_at = step
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert not receipt.reused
    assert receipt.fallback_reason
    assert lane.fallbacks == 1


# A replacement during the restore is not listed: the reseed stops the controller concurrently with it, so the identity
# is bound from the inspection to that stop (``pause``) and from the reseed on (``adopt``, ``prove``).
@pytest.mark.parametrize("step", ["pause", "adopt", "prove"])
def test_process_replacement_other_than_the_reseed_invalidates_transaction(tmp_path: Path, step: str) -> None:
    lane = Lane()
    lane.changed_at = step
    assert not prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW).reused
    assert lane.fallbacks == 1


def test_old_clear_sample_cannot_admit_warm_lane(tmp_path: Path) -> None:
    lane = Lane()
    lane.evaluated_at = NOW - timedelta(seconds=1)
    assert not prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW).reused
    assert lane.fallbacks == 1


def test_fallback_failure_propagates_and_never_publishes_ready(tmp_path: Path) -> None:
    lane = Lane()
    lane.evaluated_at = NOW - timedelta(seconds=1)
    lane.failed_at = "full_reset"
    with pytest.raises(WarmLaneError, match="full reset"):
        prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)


def test_busy_lane_is_never_wiped(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy

    class BusyLane(Lane):
        def inspect(self) -> LaneIdentity:
            raise WarmLaneBusy("prior incident still owns the lane")

    lane = BusyLane()
    with pytest.raises(WarmLaneBusy):
        prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert lane.fallbacks == 0


def test_warm_option_is_explicit(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
    assert not args.warm_lane
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--warm-lane"])
    assert args.warm_lane


@pytest.mark.parametrize("field", ["accepted_revision", "standby_uid"])
@pytest.mark.parametrize("step", ["pause", "adopt", "prove"])
def test_every_identity_component_is_bound_throughout_transaction(tmp_path: Path, field: str, step: str) -> None:
    from dataclasses import replace

    class ChangingLane(Lane):
        def step(self, name: str) -> None:
            super().step(name)
            if name == step:
                replacement = "b" * 40 if field == "accepted_revision" else "standby-2"
                self.identity = replace(self.identity, **{field: replacement})

    lane = ChangingLane()
    assert not prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW).reused
    assert lane.fallbacks == 1


def test_same_lane_cannot_have_two_warm_owners(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy, own_lane

    with own_lane(tmp_path, "toy-lane"):
        with pytest.raises(WarmLaneBusy):
            with own_lane(tmp_path, "toy-lane"):
                pytest.fail("second owner entered the lane")
        # A distinct lane never shares a lock.
        with own_lane(tmp_path, "peer-lane"):
            pass
    # Kernel ownership is released on exit; the lock inode must remain.
    with own_lane(tmp_path, "toy-lane"):
        pass


def test_changed_runtime_settings_cannot_claim_controller_reuse(tmp_path: Path, monkeypatch) -> None:
    from dataclasses import replace
    from types import SimpleNamespace

    from benchmarks.sregym.adapter import (
        ControllerRecord,
        PersistentState,
        RuntimeConfig,
        runtime_config_payload,
    )
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.warm_environment import WarmEnvironment
    from benchmarks.sregym.fastloop.wipe import LaneInjectionState

    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="app",
        application="synthetic",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    monkeypatch.setenv("KUBECONFIG", str(env.kubeconfig))
    monkeypatch.delenv("SREGYM_BASE_KUBECONFIG", raising=False)
    config = RuntimeConfig(
        repository=env.workspace,
        namespace=env.namespace,
        application=env.application,
        controller_image="controller:synthetic",
        responder_image="responder:synthetic",
        repository_pvc="synthetic",
        credentials_secret="reference-only",
        model="scripted",
        timeout_seconds=300,
        controller_namespace="app-sdo",
    )
    state = PersistentState()
    state.controllers[env.namespace] = ControllerRecord(
        application=env.application,
        namespace=env.namespace,
        control_namespace="app-sdo",
        lifecycle_fingerprint="same",
        controller_pod_uid="controller-1",
        controller_pod_name="controller",
        installed_by_stage="fixture",
        installed_at=NOW,
        runtime_config=runtime_config_payload(replace(config, timeout_seconds=600)),
    )
    state.save(env.state_path)
    LaneInjectionState(env.kubeconfig, env.namespace).recreated()
    from benchmarks.sregym.fastloop import warm_environment

    monkeypatch.setattr(warm_environment, "planned_application_unchanged", lambda *_args: None)
    ops = SimpleNamespace(
        controller_pod=lambda _: SimpleNamespace(name="controller", uid="controller-1"),
        runtime_state=lambda _: {"version": "sdo.controller/v1"},
        install=lambda _: pytest.fail("settings mismatch reached native reuse inspection"),
    )
    warm = WarmEnvironment(
        env, ops, SimpleNamespace(), config, tmp_path / "proof", incoming_data_faults=(), full_reset=lambda: None
    )
    with pytest.raises(ValueError, match="controller settings"):
        warm.inspect()


def test_warm_fallback_keeps_current_accepted_memory_unless_revision_is_explicit(tmp_path: Path) -> None:
    from argparse import Namespace

    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.wipe import reseed_workspace
    from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source

    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="app",
        application="synthetic",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    seed = commit_accepted_source(env.workspace).revision
    memory = env.workspace / ".sdo/playbooks/synthetic/README.md"
    memory.parent.mkdir(parents=True)
    memory.write_text("# Synthetic accepted memory\n")
    accepted = commit_accepted_source(env.workspace).revision
    env.seed_commit = seed
    options = cli._warm_reset_args(Namespace(planned_commit=None), env)
    reseed_workspace(env.workspace, options.planned_commit)
    assert options.planned_commit == accepted
    assert memory.is_file()
    options = cli._warm_reset_args(Namespace(planned_commit=seed), env)
    reseed_workspace(env.workspace, options.planned_commit)
    assert not memory.exists()


@pytest.mark.parametrize("warm", [False, True])
def test_every_run_obeys_the_same_lane_owner(tmp_path: Path, monkeypatch, warm: bool) -> None:
    from argparse import Namespace

    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.warm_lane import WarmLaneBusy, own_lane

    locks = tmp_path / "locks"
    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic-lane",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="app",
        application="synthetic",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        worker_env={"FASTLOOP_HOST_LOCK_DIR": str(locks)},
    )
    env.save()
    monkeypatch.setattr(cli, "_run_owned", lambda _args: pytest.fail("competing caller mutated an owned lane"))
    with own_lane(locks, env.cluster), pytest.raises(WarmLaneBusy):
        cli._run(Namespace(run_dir=tmp_path, warm_lane=warm, agent="sdo"))


@pytest.mark.parametrize("deliver", [False, True])
def test_a_warm_run_delivers_its_sealed_evidence_before_returning_only_when_asked(
    tmp_path: Path, monkeypatch, deliver: bool
) -> None:
    """A caller that copies the row's results out the moment ``run`` returns (the lanes workload) asks for delivery to
    finish first; the default leaves delivery queued for the next row, as a long-lived warm lane does."""
    from argparse import Namespace

    from benchmarks.sregym.fastloop import cli, evidence_queue
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic-lane",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="app",
        application="synthetic",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        worker_env={"FASTLOOP_HOST_LOCK_DIR": str(tmp_path / "locks")},
    )
    env.save()
    closed: list[bool] = []

    class Queue:
        def close(self, *, drain: bool, timeout: float | None = None) -> None:
            closed.append(drain)

    monkeypatch.setattr(evidence_queue, "lane_queue", lambda _environment: Queue())
    monkeypatch.setattr(cli, "_run_owned", lambda _args, **_kw: 0)
    args = Namespace(run_dir=tmp_path, warm_lane=True, agent="sdo", deliver_evidence_before_exit=deliver)
    assert cli._run(args) == 0
    assert closed == [deliver]
    parsed = cli.build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--warm-lane"])
    assert parsed.deliver_evidence_before_exit is False
    parsed = cli.build_parser().parse_args(
        ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--warm-lane", "--deliver-evidence-before-exit"]
    )
    assert parsed.deliver_evidence_before_exit is True


def test_a_fallback_keeps_the_violations_a_fresh_install_comparison_named(tmp_path: Path) -> None:
    """The receipt keeps what differed (our own strings, never an external error's text): reports count the cause."""
    import json

    from benchmarks.sregym.fastloop.resident_start import ResidentStartMismatch

    class Mismatch(Lane):
        def prove(self, not_before: datetime, revision: str) -> dict[str, Any]:
            raise ResidentStartMismatch(
                ["startup period was not begun and established in this row", "dispatch state x"]
            )

    lane = Mismatch()
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert not receipt.reused
    assert receipt.fallback_reason == "prove:ResidentStartMismatch"
    assert receipt.violations == (
        "startup period was not begun and established in this row",
        "dispatch state x",
    )
    assert json.loads((tmp_path / "warm.json").read_text())["violations"] == list(receipt.violations)


def test_any_errors_text_is_kept_on_one_line_truncated_so_a_fallback_is_diagnosable(tmp_path: Path) -> None:
    import json

    lane = Lane()
    lane.failed_at = "prove"
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert receipt.violations == ()
    assert receipt.detail == "synthetic failure"
    assert json.loads((tmp_path / "warm.json").read_text())["detail"] == "synthetic failure"

    class Noisy(Lane):
        def pause(self) -> None:
            raise RuntimeError("kubectl failed:\n" + "stderr line " * 100)

    detail = prepare_lane(Noisy(), tmp_path / "noisy.json", now=lambda: NOW).detail
    assert "\n" not in detail
    assert detail.startswith("kubectl failed: stderr line stderr line")
    assert len(detail) == 300


def test_the_reseed_replaces_the_controller_by_design_and_the_new_process_must_hold(tmp_path: Path) -> None:
    lane = Lane()
    lane.replaced_by_reseed = True
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert receipt.reused
    assert receipt.identity == LaneIdentity("controller-2", "standby-2", REVISION)
    assert not lane.fallbacks


def test_the_boundary_follows_the_restore_and_precedes_the_install_so_the_new_controllers_evidence_postdates_it(
    tmp_path: Path,
) -> None:
    lane = Lane()

    def clock() -> datetime:
        lane.events.append("boundary")
        return NOW

    assert prepare_lane(lane, tmp_path / "warm.json", now=clock).reused
    assert lane.events.index("restored") < lane.events.index("boundary") < lane.events.index("install")
    assert lane.events.index("install") < lane.events.index("prove")


class OverlapLane(Lane):
    """The restore cannot finish its writes until the old controller is stopped: only an overlapped transaction
    completes (a serial one, restore then reseed, waits out ``WAIT`` and fails)."""

    WAIT = 3.0

    def __init__(self) -> None:
        super().__init__()
        self.stopped = threading.Event()
        self.reseed_failure = False

    def reset_changed(self, restored: Callable[[], None] | None = None) -> None:
        self.step("reset")
        if not self.stopped.wait(self.WAIT):
            raise RuntimeError("the restore ran alone: the reseed did not run beside it")
        if self.reseed_failure:
            time.sleep(0.2)  # the reseed's failure is recorded before the restore reaches its restored signal
        super().reset_changed(restored)

    def reseed(self, application_restored: Callable[[], None] | None = None) -> None:
        try:
            self.step("stop")
        finally:
            self.stopped.set()
        super().reseed(application_restored)


def test_the_restore_and_the_reseed_overlap_and_the_controller_installs_only_on_the_restored_application(
    tmp_path: Path,
) -> None:
    lane = OverlapLane()
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert receipt.reused, (receipt.fallback_reason, receipt.detail)
    events = lane.events
    assert events.index("stop") < events.index("restored") < events.index("install")
    assert events.index("verify") < events.index("adopt")
    # Each branch and the wall of both are timed in the receipt (warm-transaction.json).
    assert {
        "reset",
        "reset.restore",
        "reset.verify",
        "pre_reseed_identity",
        "reseed",
        "reseed.await_application",
        "reset_and_reseed",
    } <= set(receipt.phase_seconds)


def test_a_failed_restore_never_installs_the_controller_and_falls_back_after_both_branches_stop(tmp_path: Path) -> None:
    lane = OverlapLane()
    lane.failed_at = "reset"
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert not receipt.reused
    assert receipt.fallback_reason == "reset:RuntimeError"
    assert "install" not in lane.events
    assert lane.events[-1] == "full_reset"
    assert lane.fallbacks == 1


def test_a_failed_reseed_stops_the_restore_before_its_verification_and_names_the_reseed(tmp_path: Path) -> None:
    lane = OverlapLane()
    lane.reseed_failure = True
    lane.failed_at = "stop"
    receipt = prepare_lane(lane, tmp_path / "warm.json", now=lambda: NOW)
    assert not receipt.reused
    assert receipt.fallback_reason == "reseed:RuntimeError"
    assert "verify" not in lane.events
    assert "install" not in lane.events
    assert lane.events[-1] == "full_reset"


def test_a_reseed_that_does_not_wait_for_the_restored_application_is_refused(tmp_path: Path) -> None:
    class Impatient(Lane):
        def reseed(self, application_restored: Callable[[], None] | None = None) -> None:
            super().reseed(None)

    receipt = prepare_lane(Impatient(), tmp_path / "warm.json", now=lambda: NOW)
    assert not receipt.reused
    assert receipt.fallback_reason == "reseed:ValueError"


def test_a_refusal_the_warm_lane_made_keeps_its_own_text_truncated(tmp_path: Path) -> None:
    """``inspect:ValueError`` told a report nothing; the lane's own refusals carry their reason."""
    import json

    from benchmarks.sregym.fastloop.warm_lane import WarmLaneRefusal

    class Refusing(Lane):
        def inspect(self) -> LaneIdentity:
            raise WarmLaneRefusal("previous attempt did not finish cleanly: " + "x" * 400)

    receipt = prepare_lane(Refusing(), tmp_path / "warm.json", now=lambda: NOW)
    assert receipt.fallback_reason == "inspect:WarmLaneRefusal"
    assert receipt.detail.startswith("previous attempt did not finish cleanly: xxx")
    assert len(receipt.detail) <= 300
    assert json.loads((tmp_path / "warm.json").read_text())["detail"] == receipt.detail


def test_the_row_runs_the_lanes_operator_before_any_row_start_work(tmp_path: Path, monkeypatch) -> None:
    """The warm transaction and the row-start wipe call the Go operator (inspect, sync, install-existing) and install
    the controller: they run the lane's pinned host operator with the lane's environment, as the fresh path's install
    does, not a source build of whatever checkout runs the harness."""
    import os
    from argparse import Namespace

    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    operator = tmp_path / "bin" / "sdo-operator"
    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="synthetic-lane",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="app",
        application="synthetic",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        host_operator=operator,
    )
    env.save()
    monkeypatch.setenv("SDO_OPERATOR_BIN", "")
    seen: list[str] = []

    def execute(_args: Namespace, **_kwargs: Any) -> int:
        seen.append(os.environ.get("SDO_OPERATOR_BIN", ""))
        return 0

    monkeypatch.setattr(cli, "_execute_owned", execute)
    args = Namespace(run_dir=tmp_path, run_id="synthetic-row", agent="sdo", model="claude-sonnet-5-5", warm_lane=True)
    assert cli._run_owned(args) == 0
    assert seen == [str(operator)]
