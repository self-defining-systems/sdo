"""Real lanes refuse accidental builds and preserve their run settings."""

import asyncio
import json
import socket
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import app_images
from benchmarks.sregym.pool.real_apps import RealAppSetup


def test_pool_setup_requires_fresh_build_seed_and_pull_only(tmp_path: Path):
    setup = RealAppSetup(
        code_root=tmp_path,
        seed=tmp_path / "seed",
        seed_revision="a" * 40,
        registry="127.0.0.1:55302",
        credentials_hook=tmp_path / "credentials.sh",
        images={"controller": "c:1", "responder": "r:1", "validator": "v:1"},
    )
    assert setup.process_environment()["FASTLOOP_APP_IMAGE_PULL_ONLY"] == "1"
    assert setup.process_environment()["FASTLOOP_APP_IMAGE_REGISTRY"] == setup.registry
    with pytest.raises(ValueError, match="source revision"):
        setup.verify_seed()


@pytest.mark.parametrize("local", [None, "1" * 64])
def test_pull_only_requires_registry_manifest_even_with_local_image(monkeypatch, tmp_path, local):
    monkeypatch.setenv("FASTLOOP_APP_IMAGE_PULL_ONLY", "1")
    monkeypatch.setattr(app_images, "_manifest", lambda *_: None)
    builds = []
    with pytest.raises(ValueError, match="pull-only"):
        app_images.ensure_app_image(
            "org/app:sregym-src-" + "1" * 64,
            "1" * 64,
            lambda: builds.append(True),
            local_identity=lambda _: local,
            registry="127.0.0.1:1",
            lock_dir=tmp_path,
        )
    assert not builds


def test_relay_carries_bidirectional_streams_and_survives_failed_clients():
    from benchmarks.sregym.pool.relay import RelayConfig, serve

    async def exercise():
        async def echo(reader, writer):
            writer.write(await reader.read())
            await writer.drain()
            writer.close()

        upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
        upstream_port = upstream.sockets[0].getsockname()[1]
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        relay = asyncio.create_task(
            serve(
                RelayConfig(
                    name="test",
                    listen_port=port,
                    upstream_host="127.0.0.1",
                    upstream_port=upstream_port,
                )
            )
        )
        try:
            deadline = asyncio.get_running_loop().time() + 10
            while asyncio.get_running_loop().time() < deadline:
                try:
                    reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    break
                except OSError:
                    await asyncio.sleep(0.01)
            else:
                pytest.fail("relay never listened")
            writer.close()
            for payload in (b"registry-manifest", b"blob" * 100_000):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(payload)
                await writer.drain()
                writer.write_eof()
                assert await asyncio.wait_for(reader.read(), timeout=2) == payload
                writer.close()
        finally:
            relay.cancel()
            with pytest.raises(asyncio.CancelledError):
                await relay
            upstream.close()
            await upstream.wait_closed()

    asyncio.run(exercise())


def test_registration_settings_reach_all_roles(tmp_path):
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.pool.scheduler import PairSpec
    from benchmarks.sregym.pool.worker import worker_registrations

    spec = PairSpec(
        run_id="proof",
        lane="c5-l0",
        problem="p",
        app="hotel_reservation",
        encounters=["first", "repeat"],
        run_dir=tmp_path,
        output=tmp_path / "out",
        provider="claude",
        model="claude-opus-5-5",
        reasoning_effort="high",
        timeout_seconds=600,
        diagnosis_grading="off",
        capability_mode="permissive",
        lane_memory="shared",
        memory={"repo": "o/hotel-sdo", "branch": "live/test"},
    )
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="pool2-c5-l0-0",
        kubeconfig=tmp_path / "kc",
        namespace="hotel",
        application="hotel-reservation",
        workspace=tmp_path / "accepted",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    directory = worker_registrations(spec, environment)
    assert {p.stem for p in directory.glob("*.json")} == {"deployer", "health-judge", "responder", "reflector"}
    for path in directory.glob("*.json"):
        job = json.loads(path.read_text())["kubernetes_job"]
        assert job["command"][:2] == ["python3", "-m"]
        assert job["image"] == environment.images.responder


@pytest.mark.parametrize("application", ["hotel-reservation", "social-network", "opentelemetry-demo-astronomy-shop"])
def test_current_seed_renderer_feeds_scripted_judge_without_source_run_memory(tmp_path, application):
    from benchmarks.sregym.adapter import scripted_judge_seed
    from benchmarks.sregym.pool.seed import render_seed

    root = Path(__file__).resolve().parents[5]
    source = tmp_path / "source"
    source.mkdir()
    (source / "kubernetes").mkdir()
    name = "astronomy-shop.yaml" if application.startswith("opentelemetry") else "app.yaml"
    (source / "kubernetes" / name).write_text(
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: synthetic\nspec:\n  replicas: 1\n"
    )
    (source / ".sdo").mkdir()
    (source / ".sdo/outcomes.jsonl").write_text("SOURCE_RUN_MEMORY_CANARY")
    (source / ".sdo-runtime").mkdir()
    (source / ".sdo-runtime/credential").write_text("SOURCE_RUNTIME_CANARY")
    subprocess.run(["git", "init", "-q", str(source)], check=True)
    subprocess.run(["git", "-C", str(source), "add", "."], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(source),
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@invalid",
            "commit",
            "-qm",
            "synthetic inputs",
        ],
        check=True,
    )
    target = tmp_path / "seed"
    revision = render_seed(root, source, target, application, application)
    setup = RealAppSetup(
        code_root=root,
        seed=target,
        seed_revision=revision,
        registry="127.0.0.1:1",
        credentials_hook=tmp_path / "hook",
        images={"controller": "c", "responder": "r", "validator": "v"},
    )
    setup.verify_seed()
    health = scripted_judge_seed(target)
    assert "health-objective" in health.detector_ids
    assert "service-endpoints" in health.detector_ids
    assert "health-objective/README.md" in (target / ".sdo/playbooks/README.md").read_text()
    assert not (target / ".sdo-runtime").exists()
    assert not (target / ".sdo/outcomes.jsonl").exists()
    assert "Deployment synthetic" in (target / ".sdo/arch.md").read_text()
    import yaml

    header = yaml.safe_load((target / ".sdo/arch.md").read_text().split("---\n", 2)[1])
    assert (
        subprocess.run(
            ["git", "-C", str(target), "merge-base", "--is-ancestor", header["generated_at_commit"], revision],
            capture_output=True,
        ).returncode
        == 0
    )


def test_remote_endpoint_mismatch_fails_before_any_api_call(tmp_path, monkeypatch):
    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.pool.real_apps import preflight_memory

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="pool2-c5-l0-0",
        kubeconfig=tmp_path / "kc",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "memory",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    calls = []
    monkeypatch.setattr(adapter, "kubectl", lambda *_args, **_kwargs: calls.append(True))
    with pytest.raises(ValueError, match="not bound"):
        preflight_memory(environment, "o/app-sdo", "live/test")
    assert not calls


def test_keeper_fence_stops_secret_payload_reads_and_all_calls_after_lease_loss(tmp_path, monkeypatch):
    import os

    from benchmarks.sregym.pool.guard import keeper_environment

    binaries = tmp_path / "real-bin"
    binaries.mkdir()
    real = binaries / "kubectl"
    real.write_text('#!/bin/sh\necho "secret/name"\necho called >> "$KEEPER_CALLS"\n')
    real.chmod(0o755)
    coord = tmp_path / "coord"
    lost = tmp_path / "lost"
    coord.write_text(f'#!/bin/sh\ntest ! -f "{lost}"\n')
    coord.chmod(0o755)
    monkeypatch.setenv("PATH", str(binaries) + os.pathsep + os.environ["PATH"])
    calls = tmp_path / "calls"
    environment = {
        **os.environ,
        **keeper_environment(
            tmp_path / "shim", cluster="pool2-test-0", kubeconfig=tmp_path / "kc", lease="test", coord=coord
        ),
        "KEEPER_CALLS": str(calls),
    }
    shim = str(tmp_path / "shim/kubectl")
    allowed = subprocess.run(
        [shim, "get", "secret", "sdo-agent-credentials", "-o", "name"], env=environment, capture_output=True, text=True
    )
    assert allowed.returncode == 0
    assert allowed.stdout.strip() == "secret/name"
    rejected = subprocess.run(
        [shim, "get", "secret", "sdo-agent-credentials", "-o", "json"], env=environment, capture_output=True, text=True
    )
    assert rejected.returncode != 0
    lost.touch()
    rejected = subprocess.run(
        [shim, "apply", "-f", "-"], env=environment, input="CREDENTIAL_CANARY", capture_output=True, text=True
    )
    assert rejected.returncode != 0
    assert "CREDENTIAL_CANARY" not in rejected.stdout + rejected.stderr
    assert calls.read_text().splitlines() == ["called"]


def test_relay_cannot_certify_an_unowned_listener(tmp_path, monkeypatch):
    from subprocess import CompletedProcess

    from benchmarks.sregym.pool import relay

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("time.sleep", lambda _: None)
    monkeypatch.setattr(
        relay.subprocess, "run", lambda argv, **_: CompletedProcess(argv, 0, "ActiveState=active\nMainPID=123\n")
    )
    http = []
    monkeypatch.setattr(relay.urllib.request, "urlopen", lambda *_args, **_kwargs: http.append(True))
    with pytest.raises(RuntimeError, match="no healthy upstream"):
        relay.ensure_relay(
            relay.RelayConfig(name="owned", listen_port=55302, upstream_host="127.0.0.1", upstream_port=5000),
            tmp_path,
            tmp_path / "state",
        )
    assert not http


def test_prepared_lane_persists_pull_only_for_restarted_workers(tmp_path):
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.pool.real_apps import persist_image_settings

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="pool2-c5-l0-0",
        kubeconfig=tmp_path / "kc",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "source",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    setup = RealAppSetup(
        code_root=tmp_path,
        seed=tmp_path / "seed",
        seed_revision="a" * 40,
        registry="127.0.0.1:55302",
        credentials_hook=tmp_path / "hook",
        images={"controller": "c", "responder": "r", "validator": "v"},
    )
    persist_image_settings(setup, environment)
    restarted = FastloopEnvironment.load(tmp_path)
    assert restarted.worker_process_env()["FASTLOOP_APP_IMAGE_PULL_ONLY"] == "1"
    assert restarted.worker_process_env()["FASTLOOP_APP_IMAGE_REGISTRY"] == setup.registry
    restarted.workspace = tmp_path / "branch-mirror"
    assert restarted.worker_process_env()["SREGYM_APP_SOURCE_DIR"] == str(restarted.workspace)
