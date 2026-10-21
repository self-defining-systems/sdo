from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path
from typing import Any, cast

import pytest
import yaml


def _worker_infra() -> Any:
    path = Path(__file__).resolve().parents[5] / "third_party" / "sregym" / "sregym" / "worker_infra.py"
    spec = importlib.util.spec_from_file_location("test_worker_infra_module", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast("Any", module)


def test_kind_image_preflight_loads_and_verifies_every_node(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _worker_infra()
    calls: list[list[str]] = []
    digests = {
        "controller:run": "sha256:" + "1" * 64,
        "responder:run": "sha256:" + "2" * 64,
    }

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0, digests[command[3]] + "\n", "")
        if command[:3] == ["kind", "get", "nodes"]:
            return subprocess.CompletedProcess(command, 0, "node-a\nnode-b\n", "")
        if command[:3] == ["docker", "exec", "node-a"] or command[:3] == ["docker", "exec", "node-b"]:
            image = command[-1].removeprefix("name==").removeprefix("docker.io/library/")
            return subprocess.CompletedProcess(command, 0, f"image @{'@' if False else ''}{digests[image]}\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    evidence = module.ensure_kind_images("sregym-w0", list(digests))

    assert evidence == {
        "node-a": digests,
        "node-b": digests,
    }
    assert ["kind", "load", "docker-image", "--name", "sregym-w0", *digests] in calls
    assert sum(command[:2] == ["docker", "exec"] for command in calls) == 8  # crictl and ctr per node and image


def test_kind_image_preflight_rejects_stale_node_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _worker_infra()
    expected = "sha256:" + "1" * 64
    stale = "sha256:" + "0" * 64

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0, expected + "\n", "")
        if command[:3] == ["kind", "get", "nodes"]:
            return subprocess.CompletedProcess(command, 0, "node-a\n", "")
        if command[:2] == ["docker", "exec"]:
            return subprocess.CompletedProcess(command, 0, f"image @{stale}\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="node-a.*controller:run.*digest mismatch"):
        module.ensure_kind_images("sregym-w0", ["controller:run"])


def test_reused_cluster_runs_required_image_preflight_before_return(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _worker_infra()
    monkeypatch.setenv("SREGYM_REUSE_CLUSTER", "1")
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_PREFIX", raising=False)
    monkeypatch.setenv("SREGYM_KIND_REQUIRED_IMAGES", json.dumps(["controller:run"]))
    monkeypatch.setattr(module, "stable_kubeconfig_path", lambda _worker: str(tmp_path / "stable"))
    monkeypatch.setattr(module, "existing_cluster_is_reusable", lambda *_args: (True, ""))
    monkeypatch.setattr(
        module,
        "_attach_existing_cluster",
        lambda *_args: ("sregym-w0", str(tmp_path / "worker.kubeconfig")),
    )
    observed: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        module,
        "ensure_kind_images",
        lambda cluster, images: observed.append((cluster, images)),
    )

    result = module.create_worker_cluster(0, str(tmp_path))

    assert result == ("sregym-w0", str(tmp_path / "worker.kubeconfig"))
    assert observed == [("sregym-w0", ["controller:run"])]


def test_network_policy_kind_config_disables_default_cni(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _worker_infra()
    source = tmp_path / "kind.yaml"
    source.write_text(
        "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n  - role: control-plane\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("SREGYM_KIND_REQUIRE_NETWORK_POLICY", "1")

    patched = Path(module.prepare_kind_config(str(source), None, None))
    try:
        config = yaml.safe_load(patched.read_text(encoding="utf-8"))
    finally:
        patched.unlink()

    assert config["networking"] == {
        "disableDefaultCNI": True,
        "podSubnet": "192.168.0.0/16",
    }


def test_cluster_preflight_loads_images_before_network_policy_canary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _worker_infra()
    kubeconfig = tmp_path / "worker.kubeconfig"
    monkeypatch.setenv("SREGYM_KIND_REQUIRED_IMAGES", json.dumps(["controller:run", "validator:run"]))
    monkeypatch.setenv("SREGYM_KIND_REQUIRE_NETWORK_POLICY", "1")
    monkeypatch.setenv("SREGYM_KIND_NETWORK_POLICY_CANARY_IMAGE", "validator:run")
    events: list[object] = []
    monkeypatch.setattr(
        module,
        "ensure_kind_images",
        lambda cluster, images: events.append(("images", cluster, images)),
    )
    monkeypatch.setattr(
        module,
        "verify_network_policy_enforcement",
        lambda cluster, image, path: events.append(("network-policy", cluster, image, path)),
    )

    module.preflight_cluster_requirements("sregym-w0", str(kubeconfig))

    assert events == [
        ("images", "sregym-w0", ["controller:run", "validator:run"]),
        ("network-policy", "sregym-w0", "validator:run", str(kubeconfig)),
    ]


def test_fresh_network_policy_cluster_installs_calico_before_ready_wait(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _worker_infra()
    source = tmp_path / "kind.yaml"
    source.write_text(
        "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n  - role: control-plane\n",
        encoding="utf-8",
    )
    kubeconfig = tmp_path / "worker.kubeconfig"
    monkeypatch.setenv("SREGYM_KIND_REQUIRE_NETWORK_POLICY", "1")
    monkeypatch.setattr(module, "worker_kind_config_path", lambda: str(source))
    monkeypatch.setattr(module, "apply_worker_cpu_limit", lambda _cluster: None)
    events: list[object] = []

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        events.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        module,
        "install_calico",
        lambda cluster, path: events.append(("install-calico", cluster, path)),
    )

    module.create_kind_cluster("sregym-w0", str(kubeconfig))

    create = next(
        command for command in events if isinstance(command, list) and command[:3] == ["kind", "create", "cluster"]
    )
    assert "--wait" not in create
    assert ("install-calico", "sregym-w0", str(kubeconfig)) in events
    assert [
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "wait",
        "--for=condition=Ready",
        "nodes",
        "--all",
        "--timeout=300s",
    ] in events


def test_calico_images_are_pulled_and_digest_verified_before_manifest_apply(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _worker_infra()
    kubeconfig = tmp_path / "worker.kubeconfig"
    events: list[object] = []
    manifest = b"apiVersion: v1\nkind: List\nitems: []\n"
    monkeypatch.setattr(module, "_CALICO_SHA256", hashlib.sha256(manifest).hexdigest())

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        events.append(command)
        if command and command[0] == "curl":
            Path(command[-1]).write_bytes(manifest)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        module,
        "ensure_kind_platform_images",
        lambda cluster, images, platform: events.append(("verify-platform-images", cluster, images, platform)),
    )
    monkeypatch.setattr(module, "container_platform", lambda: "linux/arm64")

    module.install_calico("sregym-w0", str(kubeconfig))

    for mirror, target in module._CALICO_IMAGE_MIRRORS:
        assert ["docker", "pull", "--platform", "linux/arm64", mirror] in events
        assert ["docker", "tag", mirror, target] in events
    verified = (
        "verify-platform-images",
        "sregym-w0",
        [target for _, target in module._CALICO_IMAGE_MIRRORS],
        "linux/arm64",
    )
    assert verified in events
    apply = next(
        command
        for command in events
        if isinstance(command, list) and command[:4] == ["kubectl", "--kubeconfig", str(kubeconfig), "create"]
    )
    assert events.index(verified) < events.index(apply)


def test_platform_archive_saves_pulled_image_and_records_its_registry_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _worker_infra()
    digest = "sha256:" + "a" * 64
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["docker", "save"]:
            Path(command[command.index("--output") + 1]).write_bytes(b"archive")
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(command, 0, f"calico/cni@{digest}\n", "")
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    archive, observed = module.platform_image_archive("calico/cni:test", "linux/arm64")
    try:
        assert observed == digest
        assert Path(archive).read_bytes() == b"archive"
        assert ["docker", "save", "--output", archive, "calico/cni:test"] in calls
    finally:
        Path(archive).unlink()


def test_platform_image_preflight_imports_selected_archive_and_verifies_every_node(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _worker_infra()
    digest = "sha256:" + "b" * 64
    archive = tmp_path / "image.tar"
    archive.write_bytes(b"archive")
    calls: list[list[str]] = []
    imported_archives: dict[str, str] = {}
    monkeypatch.setattr(module, "platform_image_archive", lambda _image, _platform: (str(archive), digest))

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:3] == ["kind", "get", "nodes"]:
            return subprocess.CompletedProcess(command, 0, "node-a\nnode-b\n", "")
        if command[:3] == ["docker", "exec", "-i"]:
            stdin = kwargs["stdin"]
            imported_archives[command[3]] = cast("Any", stdin).name
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ["docker", "exec"]:
            return subprocess.CompletedProcess(command, 0, f"manifest @{digest}\n", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    evidence = module.ensure_kind_platform_images("sregym-w0", ["calico/cni:test"], "linux/arm64")

    assert evidence == {
        "node-a": {"calico/cni:test": digest},
        "node-b": {"calico/cni:test": digest},
    }
    assert imported_archives == {"node-a": str(archive), "node-b": str(archive)}
    imports = [index for index, command in enumerate(calls) if command[:3] == ["docker", "exec", "-i"]]
    verifications = [index for index, command in enumerate(calls) if "inspecti" in command]
    assert len(verifications) == 2
    assert max(imports) < min(verifications)
    assert not archive.exists()
