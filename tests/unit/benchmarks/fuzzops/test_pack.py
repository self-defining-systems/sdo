"""The fuzzops pack conversion: SDO gets exactly the pack's manifests; fuzzops never redeploys SDO's app."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from benchmarks.fuzzops.install import InstallConfig, digest_tags, normalized
from benchmarks.fuzzops.pack import (
    PackError,
    SourcePack,
    deployment_plan,
    images,
    write_app_repository,
    write_attached_pack,
)

_MANIFEST = """apiVersion: v1
kind: Service
metadata: {name: web}
spec: {type: NodePort, ports: [{port: 80}]}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec: {}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: store}
spec: {}
"""


def _pack(root: Path, manifest: str = _MANIFEST) -> SourcePack:
    (root / "manifests").mkdir(parents=True)
    (root / "manifests" / "00-app.yaml").write_text(manifest)
    (root / "app.yaml").write_text("name: synth\nup: ./up.sh\ndown: ./down.sh\nentry:\n  service: web\n  port: 80\n")
    (root / "journeys.yaml").write_text("rate: 5\njourneys:\n  - name: get\n    request: {path: /}\n")
    (root / "faults").mkdir()
    (root / "faults" / "synthetic-fault.yaml").write_text("name: synthetic-fault\n")
    return SourcePack(root)


def test_plan_expects_every_workload_in_the_app_namespace(tmp_path: Path) -> None:
    pack = _pack(tmp_path / "pack")
    plan = deployment_plan(pack.manifests(), "app-ns")
    assert plan["source"]["renderer_id"] == "raw"
    assert plan["source"]["config"]["paths"] == ["k8s"]
    assert plan["expected_workload_ids"] == ["Deployment/app-ns/web", "StatefulSet/app-ns/store"]


def test_manifest_with_a_namespace_is_refused(tmp_path: Path) -> None:
    pack = _pack(
        tmp_path / "pack", _MANIFEST.replace("metadata: {name: store}", "metadata: {name: store, namespace: x}")
    )
    with pytest.raises(PackError, match="sets a namespace"):
        deployment_plan(pack.manifests(), "app-ns")


def test_pack_without_manifests_is_refused(tmp_path: Path) -> None:
    (tmp_path / "app.yaml").write_text("name: x\n")
    with pytest.raises(PackError, match="manifests"):
        SourcePack(tmp_path)


def test_app_repository_holds_the_manifests_in_one_commit(tmp_path: Path) -> None:
    pack = _pack(tmp_path / "pack")
    repo = write_app_repository(pack, tmp_path / "app", description="Synthetic app.")
    assert (repo / "k8s" / "00-app.yaml").read_text() == _MANIFEST
    log = subprocess.run(["git", "-C", str(repo), "log", "--oneline"], capture_output=True, text=True, check=True)
    assert len(log.stdout.splitlines()) == 1
    status = subprocess.run(
        ["git", "-C", str(repo), "status", "--porcelain"], capture_output=True, text=True, check=True
    )
    assert status.stdout == ""
    with pytest.raises(PackError, match="not empty"):
        write_app_repository(pack, repo, description="again")


def test_attached_pack_waits_and_never_deploys_or_removes(tmp_path: Path) -> None:
    pack = _pack(tmp_path / "pack")
    out = write_attached_pack(pack, tmp_path / "attached")
    app = json.loads((out / "app.yaml").read_text())
    assert app["entry"] == {"service": "web", "port": 80}
    assert (out / "journeys.yaml").read_text() == (pack.root / "journeys.yaml").read_text()
    assert (out / "faults" / "synthetic-fault.yaml").is_file()
    up = (out / "up.sh").read_text()
    kubectl = [line for line in up.splitlines() if line.startswith("kubectl")]
    assert kubectl
    assert all(" wait --for=condition=Available " in line for line in kubectl)
    down = subprocess.run(
        [str(out / "down.sh")],
        env={"NAMESPACE": "x", "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert down.returncode != 0
    assert "must not remove" in down.stderr


def _install_config(tmp_path: Path, **overrides: object) -> InstallConfig:
    values: dict[str, object] = {
        "pack": tmp_path,
        "work": tmp_path / "w",
        "namespace": "app",
        "control_namespace": "app-sdo",
        "objective": "users are served",
        "controller_image": "c:1",
        "responder_image": "r:1",
        "operator_binary": tmp_path / "sdo-operator",
        "kubeconfig": tmp_path / "kc",
        "auth_file": tmp_path / "auth.json",
    }
    values.update(overrides)
    return InstallConfig(**values)  # type: ignore[arg-type]


def test_host_launch_selects_agent_model_and_effort_through_the_environment(tmp_path: Path) -> None:
    cfg = _install_config(tmp_path, effort="xhigh")
    args = cfg.host_args(tmp_path / "w" / "app", {"source": {}})
    assert args[1] == "host"
    assert not {"--agent-provider", "--model", "--agent-effort"} & set(args)
    assert json.loads(args[args.index("--deployment-plan") + 1]) == {"source": {}}
    env = cfg.host_env()
    assert (env["SDO_AGENT_PROVIDER"], env["SDO_AGENT_MODEL"], env["SDO_AGENT_EFFORT"]) == (
        "codex",
        "gpt-6-luna",
        "xhigh",
    )
    assert "SDO_AGENT_EFFORT" not in _install_config(tmp_path).host_env()


def test_host_launch_names_the_evidence_sink_only_when_configured(tmp_path: Path) -> None:
    assert "SDO_EVIDENCE_HOST_PATH" not in _install_config(tmp_path).host_env()
    env = _install_config(tmp_path, evidence_sink_host_path="/opt/sdo-evidence", evidence_run_id="row-1").host_env()
    assert (env["SDO_EVIDENCE_HOST_PATH"], env["SDO_EVIDENCE_RUN_ID"]) == ("/opt/sdo-evidence", "row-1")
    with pytest.raises(ValueError, match="requires an evidence host path"):
        _install_config(tmp_path, evidence_run_id="row-1")


@pytest.mark.parametrize(
    ("overrides", "error"),
    [({"control_namespace": "app"}, ValueError), ({"objective": ""}, ValueError), ({"work": Path("rel")}, ValueError)],
)
def test_install_config_is_validated(tmp_path: Path, overrides: dict[str, object], error: type[Exception]) -> None:
    with pytest.raises(error):
        _install_config(tmp_path, **overrides)


def test_images_lists_every_container_image(tmp_path: Path) -> None:
    manifest = """apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  template:
    spec:
      initContainers: [{name: init, image: busybox:1}]
      containers: [{name: web, image: synth/web:dev}, {name: side, image: busybox:1}]
"""
    pack = _pack(tmp_path / "pack", manifest)
    assert images(pack.manifests()) == ["busybox:1", "synth/web:dev"]


def test_digest_tags_names_preloaded_images_by_their_node_digest() -> None:
    listing = (
        "REF TYPE DIGEST SIZE PLATFORMS LABELS\n"
        "docker.io/synth/web:dev application/x sha256:" + "a" * 64 + " 1MiB linux/amd64 -\n"
        "docker.io/library/busybox:1 application/x sha256:" + "b" * 64 + " 1MiB linux/amd64 -\n"
        "import-2026@sha256:" + "a" * 64 + " application/x sha256:" + "a" * 64 + " 1MiB linux/amd64 -\n"
    )
    pairs = digest_tags(["synth/web:dev", "busybox:1", "absent:2", "ghcr.io/x/y@sha256:" + "c" * 64], listing)
    assert pairs == [
        ("docker.io/synth/web:dev", "docker.io/synth/web@sha256:" + "a" * 64),
        ("docker.io/library/busybox:1", "docker.io/library/busybox@sha256:" + "b" * 64),
    ]
    assert normalized("registry.local:5000/a/b:1") == "registry.local:5000/a/b:1"


def test_app_repository_carries_source_without_vendor_or_upstream_deploy_files(tmp_path: Path) -> None:
    src = tmp_path / "upstream"
    for rel in ("cmd/web/main.go", "vendor/x/x.go", "kubernetes/web.yaml", "go.mod"):
        (src / rel).parent.mkdir(parents=True, exist_ok=True)
        (src / rel).write_text("synthetic\n")
    repo = write_app_repository(_pack(tmp_path / "pack"), tmp_path / "app", description="Synthetic.", source=src)
    assert (repo / "src" / "cmd" / "web" / "main.go").is_file()
    assert (repo / "src" / "go.mod").is_file()
    assert not (repo / "src" / "vendor").exists()
    assert not (repo / "src" / "kubernetes").exists()
