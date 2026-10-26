"""Fail before worker discovery when a nested source checkout is missing."""

import json
import subprocess

import pytest


def test_initialized_harness_with_missing_nested_chart_is_rejected(tmp_path):
    from benchmarks.sregym.fastloop.source_preflight import assert_harness_sources

    def git(directory, *args):
        return subprocess.run(
            ["git", "-C", str(directory), *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    git(tmp_path, "init", "-q")
    metadata = tmp_path / "sregym/service/metadata"
    metadata.mkdir(parents=True)
    for app in ("astronomy-shop", "social-network", "hotel-reservation"):
        (metadata / f"{app}.json").write_text(json.dumps({"Namespace": app, "Helm Config": {"chart_path": app}}))
    apps = tmp_path / "SREGym-applications"
    apps.mkdir()
    git(apps, "init", "-q")
    for app in ("astronomy-shop", "social-network", "hotel-reservation"):
        (apps / app).mkdir()
        (apps / app / "Chart.yaml").write_text("name: fixture\n")
    git(apps, "add", ".")
    git(apps, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qm", "sources")
    git(tmp_path, "add", ".")
    git(tmp_path, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qm", "harness")
    assert_harness_sources(tmp_path)
    (apps / "astronomy-shop/Chart.yaml").unlink()
    with pytest.raises(ValueError, match="application source prerequisite.*astronomy-shop"):
        assert_harness_sources(tmp_path)
    git(apps, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qam", "wrong revision")
    with pytest.raises(ValueError, match="pinned applications revision"):
        assert_harness_sources(tmp_path)


@pytest.mark.parametrize("failure", ["controller", "validator", "missing-label", "toolchain", "dirty", None])
def test_runtime_images_require_current_source_and_working_validator(tmp_path, monkeypatch, failure):
    from benchmarks.sregym.fastloop import source_preflight
    from benchmarks.sregym.fastloop.environment import Images

    calls = []

    def run(args, **kwargs):
        calls.append(args)
        if args[0] == "git":
            output = "?? sdo/operator/new.go" if failure == "dirty" else ""
            if "rev-parse" in args:
                output = "current\n"
            return subprocess.CompletedProcess(args, 0, stdout=output)
        if args[:3] == ["docker", "image", "inspect"]:
            role = "controller" if "controller" in args[3] else "validator"
            revision = "old" if failure == role else "current"
            labels = {} if failure == "missing-label" else {"org.opencontainers.image.revision": revision}
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps([{"Config": {"Labels": labels}}]))
        assert args[:2] == ["docker", "run"]
        if failure == "toolchain":
            raise subprocess.CalledProcessError(127, args, stderr="go: not found")
        return subprocess.CompletedProcess(args, 0, stdout="go version go1.26.8 linux/amd64\n")

    monkeypatch.setattr(source_preflight.subprocess, "run", run)
    if failure:
        with pytest.raises(ValueError, match="eval:.*(source|toolchain)"):
            source_preflight.assert_runtime_images(Images(), tmp_path)
    else:
        source_preflight.assert_runtime_images(Images(), tmp_path)
        probes = [args for args in calls if args[:2] == ["docker", "run"]]
        assert len(probes) == 1
        assert "--network" in probes[0]
        assert "none" in probes[0]
        assert "--read-only" in probes[0]
        assert "65532:65532" in probes[0]
        assert "go" in probes[0]
        assert "version" in probes[0]


def test_worker_rejects_image_preflight_before_process_creation(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import cli, source_preflight
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="lane",
        kubeconfig=tmp_path / "config",
        namespace="app",
        application="app",
        workspace=tmp_path,
        sregym_dir=tmp_path,
        private_tmp=None,
    )
    monkeypatch.setattr(source_preflight, "assert_harness_sources", lambda _: None)

    def reject(*_):
        raise ValueError("eval: stale image source")

    monkeypatch.setattr(source_preflight, "assert_runtime_images", reject)
    monkeypatch.setattr(cli, "SregymWorker", lambda *a, **kw: pytest.fail("worker created before image preflight"))
    with pytest.raises(ValueError, match="stale image source"):
        cli._worker(environment)
