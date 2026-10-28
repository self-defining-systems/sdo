"""Host caps apply to every node and concurrency is shared across lane processes."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import cli


def test_up_limits_reach_worker_environment(tmp_path: Path) -> None:
    args = cli.build_parser().parse_args(["up", "--run-dir", str(tmp_path)])
    env = cli.up_worker_environment(args, workspace=tmp_path)
    assert env["SREGYM_WORKER_MEMORY_LIMIT"] == "16g"
    assert env["SREGYM_WORKER_PIDS_LIMIT"] == "8192"
    args = cli.build_parser().parse_args(
        [
            "up",
            "--run-dir",
            str(tmp_path),
            "--memory-limit",
            "24g",
            "--pids-limit",
            "4096",
        ]
    )
    env = cli.up_worker_environment(args, workspace=tmp_path)
    assert env["SREGYM_WORKER_MEMORY_LIMIT"] == "24g"
    assert env["SREGYM_WORKER_PIDS_LIMIT"] == "4096"


def test_every_kind_node_receives_all_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop.host_limits import NodeLimits, apply_node_limits

    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, "lane-control-plane\nlane-worker\n", "")

    monkeypatch.setattr(subprocess, "run", run)
    apply_node_limits("lane", NodeLimits(cpu="2.5", memory="24g", pids=4096))
    assert [argv for argv, _ in calls[1:]] == [
        ["docker", "update", "--cpus", "2.5", "--memory", "24g", "--memory-swap", "24g", "--pids-limit", "4096", node]
        for node in ("lane-control-plane", "lane-worker")
    ]
    assert all(kwargs["check"] for _, kwargs in calls)


@pytest.mark.parametrize("kwargs", [{"cpu": "0"}, {"memory": "0"}, {"memory": "unlimited"}, {"pids": -1}])
def test_unbounded_node_limits_rejected(kwargs: dict) -> None:
    from benchmarks.sregym.fastloop.host_limits import NodeLimits

    with pytest.raises(ValueError, match="node .* limit"):
        NodeLimits(**kwargs)


def _fake_delivery(directory: str, cluster: str, entered, release, attempted, fallback: bool) -> None:
    from benchmarks.sregym.fastloop import toy_images
    from benchmarks.sregym.fastloop.host_limits import HostLimits

    def deliver(*_args, **_kwargs):
        entered.put(cluster)
        if not release.wait(10):
            raise RuntimeError("test delivery was not released")
        return {}

    toy_images.assert_host_disk_headroom = lambda **_kwargs: {"passed": True}
    toy_images.retain_lane_images = lambda *_args: {}
    if fallback:

        def registry(*_args, **_kwargs):
            raise RuntimeError("fake registry offline")

        toy_images.registry_delivery = registry
        toy_images._load_archive_images = deliver
    else:
        toy_images.registry_delivery = deliver
    attempted.set()
    toy_images.load_images(cluster, ["synthetic:v1"], host_limits=HostLimits(Path(directory)))


@pytest.mark.parametrize("fallback", [False, True])
def test_two_lane_processes_serialize_real_delivery_entrypoint(tmp_path: Path, fallback: bool) -> None:
    import multiprocessing
    import queue

    ctx = multiprocessing.get_context("fork")
    entered, release = ctx.Queue(), ctx.Event()
    first, second = ctx.Event(), ctx.Event()
    processes = [
        ctx.Process(target=_fake_delivery, args=(str(tmp_path), name, entered, release, event, fallback))
        for name, event in (("lane-a", first), ("lane-b", second))
    ]
    try:
        processes[0].start()
        assert entered.get(timeout=10) == "lane-a"
        processes[1].start()
        assert second.wait(10)
        with pytest.raises(queue.Empty):
            entered.get(timeout=0.3)
        release.set()
        assert entered.get(timeout=10) == "lane-b"
    finally:
        release.set()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()
                    process.join()
        entered.close()
    assert [process.exitcode for process in processes] == [0, 0]


def _fake_tier2(directory: str, lane: str, entered, release, attempted, slots: int) -> None:
    from benchmarks.sregym.fastloop.host_limits import HostLimits
    from benchmarks.sregym.fastloop.wipe import Tier1Receipt, Tier2Reason, Tier2Trigger, run_tier2

    class Cluster:
        def list_objects(self):
            return frozenset()

        def delete(self, identity):
            raise AssertionError(f"unexpected object {identity}")

    def deploy():
        # The worker client can enter the same admission domain without deadlocking.
        with HostLimits(Path(directory), slots).admit("redeploy"):
            entered.put(lane)
            if not release.wait(10):
                raise RuntimeError("test redeploy was not released")

    attempted.set()
    run_tier2(
        Tier2Reason(Tier2Trigger.PODS_UNHEALTHY, "synthetic"),
        Tier1Receipt(0, (), False, ()),
        cluster=Cluster(),
        namespaces=(),
        deploy=deploy,
        timeout_seconds=5,
        host_limits=HostLimits(Path(directory), slots),
    )


@pytest.mark.parametrize("slots", [1, 2, 3])
def test_tier2_capacity_is_host_wide(tmp_path: Path, slots: int) -> None:
    import multiprocessing
    import queue

    ctx = multiprocessing.get_context("fork")
    entered, release = ctx.Queue(), ctx.Event()
    attempts = [ctx.Event() for _ in range(slots + 1)]
    processes = [
        ctx.Process(target=_fake_tier2, args=(str(tmp_path), str(i), entered, release, event, slots))
        for i, event in enumerate(attempts)
    ]
    try:
        for process in processes[:slots]:
            process.start()
        assert {entered.get(timeout=10) for _ in range(slots)} == {str(i) for i in range(slots)}
        processes[-1].start()
        assert attempts[-1].wait(10)
        with pytest.raises(queue.Empty):
            entered.get(timeout=0.3)
        release.set()
        assert entered.get(timeout=10) == str(slots)
    finally:
        release.set()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=10)
                if process.is_alive():
                    process.kill()
                    process.join()
        entered.close()
    assert [process.exitcode for process in processes] == [0] * len(processes)


def test_shared_capacity_mismatch_fails_closed_and_exception_releases_slot(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.host_limits import HostLimits

    with pytest.raises(RuntimeError, match="synthetic"):
        with HostLimits(tmp_path).admit("redeploy"):
            raise RuntimeError("synthetic")
    with HostLimits(tmp_path).admit("redeploy"):
        pass
    with pytest.raises(ValueError, match="capacity is 2"):
        with HostLimits(tmp_path, 3).admit("redeploy"):
            pytest.fail("capacity mismatch admitted")


def test_external_factory_hook_caps_before_image_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.host_limits import install_node_limits

    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "lane-control-plane\nlane-worker\n", "")

    monkeypatch.setattr(subprocess, "run", run)
    infra = SimpleNamespace(preflight_cluster_requirements=lambda *_args: calls.append(["images"]))
    install_node_limits(infra)
    infra.apply_worker_cpu_limit("lane")
    infra.preflight_cluster_requirements("lane", "/synthetic/config")
    assert calls[-1] == ["images"]
    assert sum(argv[:2] == ["docker", "update"] for argv in calls) == 4


@pytest.mark.requires_sregym
@pytest.mark.parametrize("reuse", [False, True])
def test_real_external_factory_consumes_up_limits_before_images(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reuse: bool
) -> None:
    import importlib.util

    from benchmarks.sregym.fastloop.host_limits import install_node_limits

    root = Path(__file__).resolve().parents[5]
    spec = importlib.util.spec_from_file_location(
        "limits_test_infra", root / "third_party/sregym/sregym/worker_infra.py"
    )
    assert spec is not None
    assert spec.loader is not None
    infra = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(infra)
    args = cli.build_parser().parse_args(
        ["up", "--run-dir", str(tmp_path), "--memory-limit", "24g", "--pids-limit", "4096"]
    )
    for key, value in cli.up_worker_environment(args, workspace=tmp_path).items():
        monkeypatch.setenv(key, value)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "lane-control-plane\nlane-worker\n", "")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(infra, "preflight_cluster_requirements", lambda *_args: calls.append(["images"]))
    install_node_limits(infra)
    if reuse:
        monkeypatch.setattr(infra, "existing_cluster_is_reusable", lambda *_args: (True, ""))
        monkeypatch.setattr(infra, "_attach_existing_cluster", lambda *_args: ("lane", str(tmp_path / "config")))
        infra.create_worker_cluster(0, str(tmp_path))
    else:
        config = tmp_path / "kind.yaml"
        config.touch()
        monkeypatch.setattr(infra, "prepare_kind_config", lambda *_args: str(config))
        monkeypatch.setattr(infra, "attach_registry_caches", lambda *_args: None)
        monkeypatch.setattr(infra, "install_calico", lambda *_args: calls.append(["calico"]))
        infra.create_kind_cluster("lane", str(tmp_path / "config"))
        assert calls.index(["calico"]) > next(i for i, argv in enumerate(calls) if argv[:2] == ["docker", "update"])
    assert calls[-1] == ["images"]
    updates = [argv for argv in calls if argv[:2] == ["docker", "update"]]
    assert {argv[-1] for argv in updates} == {"lane-control-plane", "lane-worker"}
    assert all(
        argv[argv.index("--memory") + 1] == "24g" and argv[argv.index("--pids-limit") + 1] == "4096" for argv in updates
    )


def test_worker_bridge_installs_node_caps_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import host_limits, worker_bridge

    calls = []
    infra = SimpleNamespace(preflight_cluster_requirements=lambda *_args: calls.append(["images"]))
    source = SimpleNamespace()

    class Worker:
        def handle(self, request):
            return {}

    def worker_main():
        infra.apply_worker_cpu_limit("lane")
        infra.preflight_cluster_requirements("lane", "/synthetic/config")
        return 0

    worker = SimpleNamespace(FastloopWorker=Worker, main=worker_main, probe_pod_manifest=lambda *args: {})
    monkeypatch.setattr(worker_bridge, "install_node_limits", host_limits.install_node_limits)
    monkeypatch.setattr(
        worker_bridge,
        "importlib",
        SimpleNamespace(
            import_module=lambda name: {
                "sregym.worker_infra": infra,
                "sregym.service.source_deploy": source,
            }.get(name, worker)
        ),
    )

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "lane-control-plane\nlane-worker\n", "")

    monkeypatch.setattr(subprocess, "run", run)
    assert worker_bridge.main() == 0
    assert calls[-1] == ["images"]
    updates = [argv for argv in calls if argv[:2] == ["docker", "update"]]
    assert {argv[-1] for argv in updates} == {"lane-control-plane", "lane-worker"}
    assert all("--memory" in argv and "--pids-limit" in argv for argv in updates)
