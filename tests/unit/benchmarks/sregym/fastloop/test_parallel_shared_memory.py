"""Shared mode never seeds, resets or overlays an accepted checkout."""

from subprocess import CompletedProcess

import pytest

from benchmarks.sregym.adapter import native_operator
from benchmarks.sregym.adapter.native_operator import NativeInstallConfig, NativeOperatorError
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.shared_memory import shared_controller_job

TOKEN = "a" * 32


def config(tmp_path):
    return NativeInstallConfig(
        repository=tmp_path / "accepted",
        namespace="app",
        control_namespace="app-sdo",
        application="app-left",
        controller_image="controller:synthetic",
        responder_image="responder:synthetic",
        validator_image="validator:synthetic",
        repository_pvc="repository",
        credentials_secret="credentials",
        model="scripted",
        kubeconfig=tmp_path / "kubeconfig",
        shared_accepted_id=TOKEN,
    )


def test_shared_sync_checks_identity_without_archive_copy(monkeypatch, tmp_path):
    selected = config(tmp_path)
    identity = selected.repository / ".git" / "sdo-shared-id"
    identity.parent.mkdir(parents=True)
    identity.write_text(TOKEN)
    monkeypatch.setattr(native_operator, "execute_operator", lambda *a, **k: pytest.fail("archive copy in shared mode"))
    monkeypatch.setattr(
        native_operator,
        "_kubectl_json",
        lambda *_: {"items": [{"metadata": {"name": "controller"}, "status": {"phase": "Running"}}]},
    )
    calls = []

    def command(argv, **kwargs):
        calls.append(argv)
        return CompletedProcess(argv, 0, TOKEN + "\n", "")

    monkeypatch.setattr(native_operator.subprocess, "run", command)
    native_operator.sync_existing(selected)
    assert len(calls) == 1
    assert calls[0][-2:] == ["cat", "/workspace/application/.git/sdo-shared-id"]
    monkeypatch.setattr(native_operator.subprocess, "run", lambda argv, **kw: CompletedProcess(argv, 0, "wrong", ""))
    with pytest.raises(NativeOperatorError, match="identity"):
        native_operator.sync_existing(selected)


def test_shared_install_is_rejected_before_any_launch(monkeypatch, tmp_path):
    monkeypatch.setattr(native_operator, "_record_launch", lambda *_: pytest.fail("unsafe shared launch"))
    with pytest.raises(NativeOperatorError, match="shared"):
        native_operator.install_existing(config(tmp_path))


def test_shared_receipt_collection_does_not_copy_repository(monkeypatch, tmp_path):
    monkeypatch.setattr(native_operator, "assert_shared_repository", lambda *_: None)

    def invoke(command, args, **kwargs):
        assert command == "incident-evidence"
        assert "--repository" not in args
        return {"incident_id": "app-left-1", "responder_job_name": "worker", "ledger": {"incident_id": "app-left-1"}}

    monkeypatch.setattr(native_operator, "_run", invoke)
    native_operator.read_incident_evidence(config(tmp_path), "app-left-1", sync_repository=True)


def test_shared_reset_refuses_before_any_cluster_write(tmp_path):
    from benchmarks.sregym.fastloop.reset import reset_environment

    env = FastloopEnvironment(
        run_dir=tmp_path / "lane",
        cluster="lane",
        kubeconfig=tmp_path / "config",
        namespace="app",
        application="app-left",
        workspace=tmp_path / "accepted",
        sregym_dir=tmp_path / "sregym",
        private_tmp=tmp_path / "tmp",
        seed_commit="b" * 40,
        parallel_root=tmp_path,
        parallel_lane="left",
        shared_accepted_id=TOKEN,
    )
    with pytest.raises(ValueError, match="shared"):
        reset_environment(env, None, None)


def test_shared_job_changes_only_incident_namespace_and_server_owned_fields():
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "sdo-controller-run",
            "namespace": "app-sdo",
            "uid": "old",
            "resourceVersion": "4",
            "annotations": {"fingerprint": "keep"},
        },
        "status": {"active": 1},
        "spec": {
            "selector": {"matchLabels": {"controller-uid": "old"}},
            "template": {
                "metadata": {"labels": {"controller-uid": "old", "app": "sdo"}},
                "spec": {
                    "volumes": [{"name": "repository", "persistentVolumeClaim": {"claimName": "private"}}],
                    "containers": [
                        {
                            "name": "controller",
                            "args": ["operate", "--application", "app", "--worktree-root", "/workspace/worktrees"],
                        }
                    ],
                },
            },
        },
    }
    shared = shared_controller_job(job, "app-left")
    assert "status" not in shared
    assert "uid" not in shared["metadata"]
    assert "selector" not in shared["spec"]
    assert shared["metadata"]["annotations"] == {"fingerprint": "keep"}
    assert shared["spec"]["template"]["metadata"]["labels"] == {"app": "sdo"}
    container = shared["spec"]["template"]["spec"]["containers"][0]
    assert container["args"] == ["operate", "--application", "app-left", "--worktree-root", "/workspace/worktrees"]
    assert shared["spec"]["template"]["spec"]["volumes"] == job["spec"]["template"]["spec"]["volumes"]
    assert job["status"] == {"active": 1}, "input must remain intact for recovery evidence"


def test_shared_volume_is_hostpath_so_nested_accepted_mount_survives_into_pods(tmp_path):
    from benchmarks.sregym.fastloop.shared_memory import parallel_volume_objects

    env = FastloopEnvironment(
        run_dir=tmp_path / "lane",
        cluster="parallel-left",
        kubeconfig=tmp_path / "config",
        namespace="app",
        application="app",
        workspace=tmp_path / "lane-app",
        sregym_dir=tmp_path / "sregym",
        private_tmp=tmp_path / "tmp",
        parallel_root=tmp_path / "shared",
    )
    objects = parallel_volume_objects(env, "parallel-left-control-plane")
    pv = next(row for row in objects if row["kind"] == "PersistentVolume")
    pvc = next(row for row in objects if row["kind"] == "PersistentVolumeClaim")
    assert pv["spec"]["hostPath"] == {"path": "/opt/sdo-parallel/lanes/parallel-left", "type": "Directory"}
    assert "local" not in pv["spec"], "local-volume intermediary bind strips nested accepted mounts"
    assert pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"] == [
        "parallel-left-control-plane"
    ]
    assert pvc["spec"]["volumeName"] == pv["metadata"]["name"]
    assert pvc["spec"]["storageClassName"] == ""


def test_shared_stage_rebuilds_host_registry_by_inspecting_without_deleting_controller(tmp_path):
    from dataclasses import replace

    from benchmarks.sregym.adapter import run_persistent_stage
    from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps, _clock, _inputs

    inputs = _inputs(tmp_path, "shared")
    inputs = replace(
        inputs,
        runtime_config=replace(inputs.runtime_config, shared_accepted_id=TOKEN),
        lifecycle_installs_controller=True,
    )
    ops = FakeOps()
    ops.install(replace(inputs.runtime_config, persistent=True))
    original = ops.controller_pod("hotel-sdo")
    ops.events.clear()
    result = run_persistent_stage(
        inputs, ops=ops, run_lifecycle=lambda: pytest.fail("shared reinstall"), inject=lambda: None, clock=_clock(ops)
    )
    assert result["incident_id"]
    assert result["persistent_controller"]["installed_this_stage"] is False
    assert ops.controller_pod("hotel-sdo") == original
    assert ("reuse", "hotel-sdo") in ops.events
    assert not any(event[0] in ("delete", "install") for event in ops.events)


def test_parallel_volume_selects_schedulable_node_before_any_controller_install(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import shared_memory

    env = FastloopEnvironment(
        run_dir=tmp_path / "lane",
        cluster="lane",
        kubeconfig=tmp_path / "config",
        namespace="app",
        application="app",
        workspace=tmp_path / "app",
        sregym_dir=tmp_path / "sregym",
        private_tmp=tmp_path / "tmp",
        parallel_root=tmp_path / "shared",
    )
    monkeypatch.setattr(shared_memory, "lane_nodes", lambda *_: ["lane-control-plane", "lane-worker"])
    monkeypatch.setattr(
        shared_memory,
        "_json",
        lambda *_args, **_kw: {
            "items": [
                {"metadata": {"name": "lane-control-plane"}, "spec": {"taints": [{"effect": "NoSchedule"}]}},
                {"metadata": {"name": "lane-worker"}, "spec": {}},
            ]
        },
    )
    monkeypatch.setattr(shared_memory, "_command", lambda *_: "")
    calls = []

    def kubectl(argv, **kwargs):
        if argv[0] == "apply":
            import json

            calls.append(json.loads(kwargs["input_text"]))
        return CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(shared_memory, "kubectl", kubectl)
    shared_memory.provision_parallel_volume(env)
    pv = next(row for row in calls if row["kind"] == "PersistentVolume")
    assert pv["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"] == [
        "lane-worker"
    ]
