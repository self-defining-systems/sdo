"""Fast lane defaults fail before slow or unsafe setup work starts."""

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import cli, dev


def test_mount_defaults_to_current_worktree_and_accepts_bare_flag():
    for flags in ([], ["--dev-mount"]):
        args = cli.build_parser().parse_args(["up", "--run-dir", "unused", *flags])
        assert args.dev_mount.resolve() == Path.cwd()
    args = cli.build_parser().parse_args(["up", "--run-dir", "unused", "--no-dev-mount"])
    assert args.dev_mount is None


def test_up_preflight_precedes_every_build(monkeypatch, tmp_path):
    calls = []

    def reject(_args):
        calls.append("preflight")
        raise ValueError("preflight rejected fixture")

    monkeypatch.setattr(cli, "up_preflight", reject)
    monkeypatch.setattr(dev, "prepare_dev_mount", lambda *a: pytest.fail("build before preflight"))
    with pytest.raises(ValueError, match="preflight rejected"):
        cli.main(["up", "--app", "toy", "--run-dir", str(tmp_path)])
    assert calls == ["preflight"]


def test_toy_cluster_name_obeys_lane_prefix():
    from benchmarks.sregym.fastloop.toy import cluster_name

    args = cli.build_parser().parse_args(
        ["up", "--app", "toy", "--run-dir", "unused", "--cluster-prefix", "img1-test-", "--worker-id", "7"]
    )
    assert cluster_name(args) == "img1-test-7"


def test_registry_failure_is_automatic_and_recorded(monkeypatch, tmp_path, capsys):
    import json

    from benchmarks.sregym.fastloop import toy_images

    monkeypatch.setattr(
        toy_images, "registry_delivery", lambda *a: (_ for _ in ()).throw(RuntimeError("registry offline"))
    )
    (tmp_path / "fastloop.json").write_text("{}")
    loaded = []
    monkeypatch.setattr(toy_images, "_load_archive_images", lambda *a: loaded.append(a))
    toy_images.load_images("img1-test", ["image:v1"], record_path=tmp_path / "delivery.jsonl")
    assert loaded == [("img1-test", ["image:v1"])]
    row = json.loads((tmp_path / "delivery.jsonl").read_text())
    assert row["path"] == "kind-load-fallback"
    assert "registry offline" in row["reason"]
    warning = capsys.readouterr().err
    assert all(text in warning for text in ("WARNING", "registry offline", "image:v1", "fallback took"))
    lane = json.loads((tmp_path / "fastloop.json").read_text())
    assert lane["image_delivery"] == "kind-load-fallback"
    assert lane["image_delivery_reason"] == "registry offline"


def test_successful_registry_never_exports_an_archive(monkeypatch, tmp_path):
    import json

    from benchmarks.sregym.fastloop import toy_images

    monkeypatch.setattr(toy_images, "registry_delivery", lambda *a: {"node": {"image:v1": "sha256:one"}})
    monkeypatch.setattr(toy_images, "_load_archive_images", lambda *a: pytest.fail("archive on success"))
    toy_images.load_images("img1-test", ["image:v1"], record_path=tmp_path / "delivery.jsonl")
    assert json.loads((tmp_path / "delivery.jsonl").read_text())["path"] == "registry"


def test_legacy_live_launcher_requires_explicit_opt_out(monkeypatch):
    import sys

    from benchmarks.sregym import run

    monkeypatch.setattr(sys, "argv", ["sregym", "absent.toml"])
    with pytest.raises(SystemExit, match="--allow-non-fastloop"):
        run.main()


def test_run_repeats_fallback_warning_before_starting_workers(monkeypatch, tmp_path, capsys):
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    lane = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="img1-test",
        kubeconfig=tmp_path / "config",
        namespace="toy",
        application="toy",
        workspace=tmp_path / "app",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        image_delivery="kind-load-fallback",
        image_delivery_reason="registry offline",
        image_delivery_images=["image:v1"],
        image_delivery_seconds=12,
    )
    lane.save()

    def stop(*a, **kw):
        raise RuntimeError("stop before worker")

    monkeypatch.setattr(cli, "kubectl", stop)
    with pytest.raises(RuntimeError, match="stop before worker"):
        cli.main(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
    assert "WARNING: image_delivery: kind-load-fallback" in capsys.readouterr().err


def test_explicit_mount_opt_out_warns(monkeypatch):
    from subprocess import CompletedProcess

    monkeypatch.setattr(cli.subprocess, "run", lambda *a, **kw: CompletedProcess(a, 0, "", ""))
    monkeypatch.setattr(cli, "check_host_capacity", lambda _: None)
    args = cli.build_parser().parse_args(["up", "--run-dir", "unused", "--no-dev-mount"])
    with pytest.warns(UserWarning, match="--no-dev-mount"):
        cli.up_preflight(args)


def test_registry_only_uses_owned_lane_and_configures_every_node(monkeypatch):
    import json
    from subprocess import CompletedProcess

    from benchmarks.sregym.fastloop import image_delivery

    digest = "sha256:" + "a" * 64
    local = ""
    calls = []
    connected = False
    ready = []
    monkeypatch.setattr(image_delivery, "_wait_registry", ready.append)

    def run(argv, **kwargs):
        nonlocal local, connected
        calls.append((argv, kwargs))
        output = ""
        if argv[:2] == ["docker", "inspect"]:
            assert argv[-1] == "img1-contract-registry"
            output = json.dumps(
                [
                    {
                        "Config": {"Labels": {"sdo.fastloop.lane": "img1-contract"}},
                        "State": {"Running": True},
                        "NetworkSettings": {
                            "Ports": {"5000/tcp": [{"HostPort": "32124" if connected else "32123"}]},
                            "Networks": {},
                        },
                    }
                ]
            )
        elif argv[:3] == ["docker", "network", "connect"]:
            connected = True
        elif argv[:3] == ["kind", "get", "nodes"]:
            output = "img1-contract-control-plane\nimg1-contract-worker\n"
        elif argv[:2] == ["docker", "tag"]:
            local = argv[-1]
        elif argv[:2] == ["docker", "push"]:
            output = f"current: digest: {digest} size: 100"
        elif "target.digest==" + digest in argv:
            output = local + "\n"
        return CompletedProcess(argv, 0, output, "")

    monkeypatch.setattr(image_delivery.subprocess, "run", run)
    evidence = image_delivery.registry_delivery("img1-contract", ["sdo-controller:contract"])
    assert len(evidence) == 2
    assert ready == ["127.0.0.1:32124"]
    assert local.startswith("127.0.0.1:32124/")
    configs = [kwargs["input"] for argv, kwargs in calls if "/dev/stdin" in argv]
    assert len(configs) == 2
    assert all("http://img1-contract-registry:5000" in config for config in configs)
    assert not any(argv[:2] == ["kind", "load"] for argv, _ in calls)
    assert all("rs-regcache" not in " ".join(argv) for argv, _ in calls)


def test_registry_rejects_unowned_container_before_mutation(monkeypatch):
    from subprocess import CompletedProcess

    from benchmarks.sregym.fastloop import image_delivery

    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        return CompletedProcess(argv, 0, '[{"Config":{"Labels":{}}}]', "")

    monkeypatch.setattr(image_delivery.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="unowned registry"):
        image_delivery.registry_delivery("img1-contract", ["image:v1"])
    assert calls == [["docker", "inspect", "img1-contract-registry"]]


def test_legacy_opt_out_reaches_existing_launcher(monkeypatch, tmp_path):
    import sys

    from benchmarks.sregym import run

    config = tmp_path / "config.toml"
    config.touch()
    monkeypatch.setattr(sys, "argv", ["sregym", str(config), "--allow-non-fastloop"])
    monkeypatch.setattr(run, "is_pipeline_config", lambda *_: False)
    launched = []
    monkeypatch.setattr(run, "run_single_experiment", lambda *a, **kw: launched.append(a))
    run.main()
    assert launched == [(config, [])]


def test_no_script_prefers_kind_load():
    import re

    root = Path(__file__).resolve().parents[5]
    for script in (root / "scripts").glob("*.sh"):
        assert not re.search(r"kind\s+load\s", script.read_text()), script


@pytest.mark.parametrize("problem", ["disk", "neighbors"])
def test_host_capacity_fails_before_builds(monkeypatch, problem):
    from subprocess import CompletedProcess

    monkeypatch.setattr(cli, "_neighbor_capacity", lambda: 1 if problem == "neighbors" else 65536)
    monkeypatch.setattr(cli.subprocess, "run", lambda *a, **kw: CompletedProcess(a, 0, "", ""))
    monkeypatch.setattr(cli, "_free_bytes", lambda *_: 1 if problem == "disk" else 200_000_000_000)
    args = cli.build_parser().parse_args(["preflight"])
    with pytest.raises(ValueError, match="fastloop preflight"):
        cli.check_host_capacity(args)


def test_external_worker_uses_registry_for_sdo_and_platform_images(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import toy_images, worker_bridge

    infra = SimpleNamespace()
    source = SimpleNamespace()

    class Worker:
        def handle(self, request):
            return {}

    calls = []
    monkeypatch.setattr(
        toy_images, "load_images", lambda cluster, images, **kw: calls.append((cluster, images, kw)) or {}
    )
    monkeypatch.setenv("FASTLOOP_DELIVERY_RECORD", str(tmp_path / "delivery.jsonl"))

    def run():
        infra.ensure_kind_images("img1-contract", ["sdo:v1"])
        infra.ensure_kind_platform_images("img1-contract", ["calico:v1"], "linux/amd64")
        source._kind_load_image(cluster_name="img1-contract", image_ref="app:v1")
        return 0

    worker = SimpleNamespace(FastloopWorker=Worker, main=run)
    monkeypatch.setattr(
        worker_bridge.importlib,
        "import_module",
        lambda name: {"sregym.worker_infra": infra, "sregym.service.source_deploy": source}.get(name, worker),
    )
    assert worker_bridge.main() == 0
    assert [call[1] for call in calls] == [["sdo:v1"], ["calico:v1"], ["app:v1"]]
    assert all(call[2]["record_path"] == tmp_path / "delivery.jsonl" for call in calls)


def test_partial_registry_success_cannot_hide_another_image_fallback(tmp_path):
    import json

    from benchmarks.sregym.fastloop.image_delivery import delivery_state, save_delivery

    (tmp_path / "fastloop.json").write_text("{}")
    path = tmp_path / "image_delivery.jsonl"
    save_delivery(
        path,
        {
            "path": "kind-load-fallback",
            "reason": "registry offline",
            "seconds": 12,
            "images": ["a:v1", "b:v1"],
            "cluster": "img1-test",
        },
    )
    save_delivery(path, {"path": "registry", "reason": "", "seconds": 1, "images": ["a:v1"], "cluster": "img1-test"})
    lane = json.loads((tmp_path / "fastloop.json").read_text())
    assert lane["image_delivery"] == "kind-load-fallback"
    assert lane["image_delivery_images"] == ["b:v1"]
    save_delivery(path, {"path": "registry", "reason": "", "seconds": 1, "images": ["b:v1"], "cluster": "img1-test"})
    assert delivery_state(path)["path"] == "registry"
    assert json.loads((tmp_path / "fastloop.json").read_text())["image_delivery_reason"] == ""
