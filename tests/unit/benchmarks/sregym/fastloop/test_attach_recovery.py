"""Attachment recovers its own real journal after interruption at every durable boundary."""

import json
import os
import subprocess
import threading
from pathlib import Path

import pytest

from benchmarks.sregym import adapter
from benchmarks.sregym.fastloop import shared_memory
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from tests.unit.benchmarks.sregym.fastloop.test_parallel_prepare import (
    _commit,
    _environments,
    _git,
    _lane_repository,
    _plan,
)


class Interrupted(BaseException):
    """A process death, not an ordinary step error."""


class AttachmentCluster:
    """Only cluster I/O is fake; Git, journal and environment persistence are real."""

    def __init__(self, monkeypatch, tmp_path, *, late=False, cut=""):
        self.plan = _plan(tmp_path, ["a", "b"])
        _environments(tmp_path, self.plan)
        self.accepted = self.plan.accepted_repository
        self.accepted.mkdir(parents=True)
        _git(self.accepted, "init", "-q")
        self.head = _commit(self.accepted, {"README.md": "synthetic\n"}, "seed")
        if late:
            (self.accepted / ".git/sdo-shared-id").write_text("existing-memory\n")
        self.lanes = {
            f"kind-{name}": _lane_repository(tmp_path, name, self.accepted, self.head, {}) for name in self.plan.lanes
        }
        self.jobs = {
            f"app-{name}-sdo": {
                "metadata": {"name": shared_memory.JOB_NAME},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "controller", "args": ["operate", "--application", f"app-{name}"]}]
                        }
                    }
                },
            }
            for name in self.plan.lanes
        }
        self.mounts = set()
        self.creates = []
        self.cut = cut
        self.interrupted = False
        self.lock = threading.Lock()
        self.report = tmp_path / "attachment.json"
        real_command = shared_memory._command

        def command(*args):
            if args[:2] == ("stat", "-f"):
                return "ext2/ext3"
            if args[0] != "docker":
                return real_command(*args)
            if "mount" in args:
                node = args[args.index("--privileged") + 1]
                self.mounts.add(node)
                self.stop(f"mount:{node}")
            elif "chown" in args:
                self.stop("ownership")
            elif "stat" in args:
                node = args[2]
                return "1:2" if args[-1].endswith("/accepted") or node in self.mounts else "3:4"
            return ""

        def json_(environment, *args, **kwargs):
            control = environment.namespace + "-sdo"
            if args[:2] == ("get", "pods"):
                return {
                    "items": (
                        [{"spec": {"nodeName": environment.cluster}, "status": {"phase": "Running"}}]
                        if control in self.jobs
                        else []
                    )
                }
            return self.jobs[control]

        def kubectl(args, *, namespace, input_text=None, **kwargs):
            output = ""
            if args[:2] == ["get", "job"]:
                output = json.dumps(self.jobs[namespace]) if namespace in self.jobs else ""
            elif args[:2] == ["delete", "job"]:
                if namespace not in self.jobs and "--ignore-not-found=true" not in args:
                    raise RuntimeError("Job already deleted")
                self.jobs.pop(namespace, None)
                self.stop(f"delete:{namespace}")
            elif args[0] == "create":
                if namespace in self.jobs:
                    raise RuntimeError("Job already exists")
                self.jobs[namespace] = json.loads(input_text)
                self.creates.append(namespace)
                self.stop(f"create:{namespace}")
            elif args[0] == "wait":
                assert namespace in self.jobs
            else:
                pytest.fail(f"unexpected cluster command: {args}")
            return subprocess.CompletedProcess(args, 0, output, "")

        def pause(_ops, control):
            assert control in self.jobs, "cannot pause a missing controller"
            self.stop(f"pause:{control}")

        def verify(environment):
            assert environment.namespace + "-sdo" in self.jobs, "live invariant requires a controller"
            assert environment.cluster in self.mounts
            assert environment.shared_accepted_id == (self.accepted / ".git/sdo-shared-id").read_text().strip()

        real_save = FastloopEnvironment.save

        def save(environment):
            real_save(environment)
            self.stop(f"environment:{environment.cluster}")

        real_replace = os.replace

        def replace(source, target, *args, **kwargs):
            real_replace(source, target, *args, **kwargs)
            if Path(target) == self.report:
                self.stop("phase:" + json.loads(self.report.read_text())["phase"])

        monkeypatch.setattr(shared_memory, "_command", command)
        monkeypatch.setattr(shared_memory, "_json", json_)
        monkeypatch.setattr(shared_memory, "kubectl", kubectl)
        monkeypatch.setattr(shared_memory, "lane_nodes", lambda e: [e.cluster])
        monkeypatch.setattr(shared_memory, "repository_volume_path", lambda e: f"/opt/sdo-parallel/lanes/{e.cluster}")
        monkeypatch.setattr(
            shared_memory, "lane_git_command", lambda e, _v, _a, *args: ("git", "-C", str(self.lanes[e.cluster]), *args)
        )
        monkeypatch.setattr(shared_memory, "assert_shared_lane", verify)
        monkeypatch.setattr(adapter, "pause_for_recovery", pause)
        monkeypatch.setattr(FastloopEnvironment, "save", save)
        monkeypatch.setattr(os, "replace", replace)

    def stop(self, point):
        with self.lock:
            if point == self.cut and not self.interrupted:
                self.interrupted = True
                raise Interrupted(point)


@pytest.mark.parametrize("late", [False, True])
@pytest.mark.parametrize(
    "cut",
    [
        "phase:preflight",
        "pause:app-a-sdo",
        "pause:app-b-sdo",
        "phase:all-paused",
        "delete:app-a-sdo",
        "delete:app-b-sdo",
        "phase:all-stopped",
        "mount:kind-a",
        "mount:kind-b",
        "phase:all-bound",
        "create:app-a-sdo",
        "create:app-b-sdo",
        "environment:kind-a",
        "environment:kind-b",
        "phase:attached",
    ],
)
def test_attach_resumes_after_each_durable_boundary(monkeypatch, tmp_path, late, cut):
    cluster = AttachmentCluster(monkeypatch, tmp_path, late=late, cut=cut)
    with pytest.raises(Interrupted):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    saved = json.loads(cluster.report.read_text())
    applications = {row["cluster"]: row["application"] for row in saved["lanes"]}
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    assert json.loads(cluster.report.read_text())["phase"] == "attached"
    assert sorted(cluster.creates) == ["app-a-sdo", "app-b-sdo"]
    assert _git(cluster.accepted, "rev-parse", "HEAD") == cluster.head
    for name, run_dir in cluster.plan.lanes.items():
        environment = FastloopEnvironment.load(run_dir)
        assert environment.application == applications[environment.cluster]
        assert environment.parallel_lane == name
        assert environment.workspace == cluster.accepted


def test_attach_recovers_missing_jobs_before_live_identity_check(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path, cut="create:app-a-sdo")
    with pytest.raises(Interrupted):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    assert json.loads(cluster.report.read_text())["phase"] == "all-bound"
    assert "app-b-sdo" not in cluster.jobs
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    assert sorted(cluster.jobs) == ["app-a-sdo", "app-b-sdo"]


def test_prepare_resumes_journal_before_repreparing_a_stopped_lane(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path, cut="create:app-a-sdo")
    prepared = []
    with pytest.raises(Interrupted):
        shared_memory.prepare_shared_lanes(
            cluster.plan, cluster.report, prepare=lambda _plan, name: prepared.append(name)
        )
    assert sorted(prepared) == ["a", "b"]
    shared_memory.prepare_shared_lanes(
        cluster.plan, cluster.report, prepare=lambda *_: pytest.fail("reprepared a stopped attachment")
    )
    assert json.loads(cluster.report.read_text())["phase"] == "attached"


def test_completed_attach_preserves_rebuilt_host_registry(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path)
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    for path in cluster.plan.lanes.values():
        FastloopEnvironment.load(path).state_path.write_text("synthetic host registry\n")
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    for path in cluster.plan.lanes.values():
        assert FastloopEnvironment.load(path).state_path.read_text() == "synthetic host registry\n"


def test_legacy_all_bound_report_recovers_saved_jobs(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path, cut="create:app-a-sdo")
    with pytest.raises(Interrupted):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    saved = json.loads(cluster.report.read_text())
    del saved["shared_id"], saved["accepted_repository"]
    cluster.report.write_text(json.dumps(saved))
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    assert json.loads(cluster.report.read_text())["phase"] == "attached"


def test_completed_attach_still_checks_all_selected_live_lanes(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path)
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    cluster.report.unlink()
    shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    cluster.jobs.clear()
    with pytest.raises(AssertionError, match="live invariant"):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)


def test_recovery_refuses_an_unrelated_existing_job(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path, cut="create:app-a-sdo")
    with pytest.raises(Interrupted):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    cluster.jobs["app-a-sdo"]["spec"]["template"]["spec"]["containers"][0]["args"] = ["unrelated"]
    with pytest.raises(ValueError, match="differs from the saved attachment"):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    assert cluster.creates == ["app-a-sdo"]


def test_recovery_requires_the_journal_shared_identity(monkeypatch, tmp_path):
    cluster = AttachmentCluster(monkeypatch, tmp_path, cut="phase:all-bound")
    with pytest.raises(Interrupted):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    (cluster.accepted / ".git/sdo-shared-id").write_text("unrelated-memory\n")
    with pytest.raises(ValueError, match="another shared identity"):
        shared_memory.attach_shared_memory(cluster.plan, cluster.report)
    assert not cluster.creates
