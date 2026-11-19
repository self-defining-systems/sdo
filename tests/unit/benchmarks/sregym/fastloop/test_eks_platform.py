"""An EKS lane in fastloop: attach-only, pinned images, node-mirror app delivery, evidence by claim, no kind calls."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.fuzzops.config import operator_env
from benchmarks.lanes.eks import EksLane
from benchmarks.sregym.adapter.cluster_resources import (
    BenchmarkClusterResourcesConfig,
    evidence_claim_volume,
    prepare_benchmark_resources,
)
from benchmarks.sregym.adapter.native_operator import validate_evidence_sink
from benchmarks.sregym.fastloop import cli, evidence_sink, worker_bridge
from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images

REGISTRY = "000000000000.dkr.ecr.us-west-2.amazonaws.com"
DIGEST = "sha256:" + "c" * 64
LANE = EksLane(registry=REGISTRY, server="https://api.example", cluster_uid="uid-1")


def eks_environment(tmp_path: Path, **overrides: Any) -> FastloopEnvironment:
    values: dict[str, Any] = {
        "platform": "eks",
        "eks": LANE,
        "pinned_tools": True,
        "run_dir": tmp_path,
        "cluster": "eks-lane-1",
        "kubeconfig": tmp_path / "kubeconfig",
        "namespace": "hotel-reservation",
        "application": "hotel-reservation",
        "workspace": tmp_path / "workspace",
        "sregym_dir": tmp_path / "sregym",
        "private_tmp": None,
        "images": Images(
            controller=f"{REGISTRY}/sdo/sdo-controller@{DIGEST}",
            responder=f"{REGISTRY}/sdo/sdo-sregym-responder@{DIGEST}",
        ),
        "evidence_root": tmp_path / "evidence",
        "host_operator": tmp_path / "sdo-operator",
    }
    return FastloopEnvironment(**{**values, **overrides})


def test_an_eks_lane_is_attach_only_with_pinned_images(tmp_path: Path) -> None:
    environment = eks_environment(tmp_path)
    assert FastloopEnvironment.model_validate_json(environment.model_dump_json()) == environment
    with pytest.raises(ValueError, match="node mounts"):
        eks_environment(tmp_path, dev_mount=tmp_path)
    with pytest.raises(ValueError, match="pinned images"):
        eks_environment(tmp_path, pinned_tools=False)
    with pytest.raises(ValueError, match="only an EKS lane"):
        eks_environment(tmp_path, eks=None)
    with pytest.raises(ValueError, match="only an EKS lane"):
        eks_environment(tmp_path, platform="kind")


def test_an_eks_lane_runs_the_host_operator_it_built(tmp_path: Path) -> None:
    environ: dict[str, str] = {"SDO_DEV_MOUNT": "1"}
    eks_environment(tmp_path).apply_operator_env(environ)
    assert environ["SDO_OPERATOR_BIN"] == str(tmp_path / "sdo-operator")
    assert "SDO_DEV_MOUNT" not in environ


def test_an_eks_worker_builds_from_source_and_never_caps_kind_nodes(tmp_path: Path) -> None:
    args = argparse.Namespace(
        cluster_prefix="eks-", worker_id=1, run_dir=tmp_path, host_lock_dir=tmp_path / "locks", host_redeploys=2
    )
    env = cli.eks_worker_environment(args, workspace=tmp_path / "ws", lane=LANE)
    assert env["SREGYM_CLUSTER_PLATFORM"] == "eks"
    assert env["SREGYM_IMAGE_DELIVERY"] == "build"
    assert env["SREGYM_KIND_CLUSTER_NAME"] == "eks-1"
    assert EksLane.model_validate_json(env["FASTLOOP_EKS_LANE"]) == LANE
    assert not {key for key in env if key.startswith(("SREGYM_WORKER_", "SDO_LANE_", "SREGYM_KIND_REQUIRED"))}


def test_eks_images_are_current_while_ecr_holds_their_digests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    held: list[str] = []
    monkeypatch.setattr(
        "benchmarks.lanes.eks.EksProvider.holds", lambda self, reference: held.append(reference) or True
    )
    monkeypatch.setattr(cli, "node_images_match", lambda *a: pytest.fail("an EKS lane has no kind nodes"))
    environment = eks_environment(tmp_path)
    assert cli.lane_images_current(environment)
    assert held == [environment.images.controller, environment.images.responder]


def test_eks_delivery_publishes_built_images_under_their_declared_names(monkeypatch: pytest.MonkeyPatch) -> None:
    published: list[tuple[str, str]] = []
    monkeypatch.setattr(
        worker_bridge, "publish_app_image", lambda ecr, local, declared: published.append((local, declared)) or DIGEST
    )
    infra, source_deploy = SimpleNamespace(), SimpleNamespace()
    worker_bridge.install_eks_delivery(infra, source_deploy, ecr=object())  # type: ignore[arg-type]
    source_deploy._kind_load_image(cluster_name="eks-1", image_ref="hotel:sregym-src-abc")
    source_deploy._declare_image(
        cluster_name="eks-1", built_ref="yinfangchen/hotelreservation:sregym-src-abc", declared_tag="latest"
    )
    assert published == [("yinfangchen/hotelreservation:sregym-src-abc", "yinfangchen/hotelreservation:latest")]
    for loader in (infra.ensure_kind_images, infra.ensure_kind_platform_images, infra.tag_image_in_kind_nodes):
        with pytest.raises(RuntimeError, match="EKS lane"):
            loader("eks-1", [])


def test_the_eks_attach_fences_by_cluster_identity_without_kind(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fenced: list[str] = []
    monkeypatch.setenv("FASTLOOP_EKS_LANE", LANE.model_dump_json())
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "eks-1")
    monkeypatch.setenv("KUBECONFIG", str(tmp_path / "kc"))
    monkeypatch.setattr(worker_bridge, "assert_lane_cluster", lambda kubeconfig, lane: fenced.append(kubeconfig))
    infra = SimpleNamespace(existing_cluster_is_reusable=lambda *a: pytest.fail("kind inventory on an EKS lane"))
    assert worker_bridge.attach_existing_cluster(infra) == {"cluster": "eks-1", "kubeconfig": str(tmp_path / "kc")}
    assert fenced == [str(tmp_path / "kc")]


def test_eks_benchmark_resources_reach_nodes_through_the_agent_and_carry_the_evidence_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FASTLOOP_DOCKERHUB_AUTH_FILE", raising=False)
    applied: list[dict[str, Any]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if command[0] in {"kind", "docker"}:
            pytest.fail(f"kind/docker call on an EKS lane: {command}")
        if "apply" in command:
            applied.extend(json.loads(kwargs["input"])["items"])
        if "service/sdo-benchmark-registry" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"spec": {"clusterIP": "10.0.0.7"}}), "")
        if "deployment/sdo-benchmark-registry" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"status": {"readyReplicas": 1}}), "")
        return subprocess.CompletedProcess(command, 0, "", "")

    written: list[tuple[str, str]] = []
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(
        "benchmarks.lanes.eks.write_node_file", lambda kc, relative, content: written.append((relative, content)) or []
    )
    config = BenchmarkClusterResourcesConfig(
        namespace="hotel-reservation",
        control_namespace="hotel-reservation-sdo",
        responder_image="responder@" + DIGEST,
        credentials_secret="sdo-agent-credentials",
        registry_push_secret="sdo-registry-push",
        registry_insecure=True,
        kind_cluster_name="eks-1",
        lane_platform="eks",
        evidence_claim=True,
    )
    prepare_benchmark_resources(config, relay_target_base=None, kubeconfig=tmp_path / "kc")
    claims = {item["metadata"]["name"]: item for item in applied if item["kind"] == "PersistentVolumeClaim"}
    assert claims["sdo-evidence"]["metadata"]["namespace"] == "hotel-reservation-sdo"
    ((relative, content),) = written
    assert relative == f"{config.registry_host}/hosts.toml"
    assert 'host."http://10.0.0.7:5000"' in content


def test_an_evidence_volume_sink_is_its_own_sdo_variable() -> None:
    volume = evidence_claim_volume()
    assert json.loads(volume) == {"persistentVolumeClaim": {"claimName": "sdo-evidence"}}
    env = operator_env(evidence_volume=volume, evidence_run_id="run-1")
    assert env == {"SDO_EVIDENCE_VOLUME": volume, "SDO_EVIDENCE_RUN_ID": "run-1"}
    validate_evidence_sink("", "run-1", volume)
    with pytest.raises(ValueError, match="not both"):
        validate_evidence_sink("/opt/sdo-evidence", "run-1", volume)
    with pytest.raises(ValueError, match="requires an evidence sink"):
        validate_evidence_sink("", "run-1")


def test_only_an_eks_lane_pulls_its_sink_from_the_cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    kind = eks_environment(tmp_path).model_copy(update={"platform": "kind", "eks": None})
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("a kind lane's sink is the host directory"))
    assert evidence_sink.pull_cluster_sink(kind) is False

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, "", 'persistentvolumeclaims "sdo-evidence" NotFound')

    monkeypatch.setattr(subprocess, "run", run)
    # The control namespace (and its claim) is gone: nothing in the cluster to lose.
    assert evidence_sink.pull_cluster_sink(eks_environment(tmp_path)) is False


def test_an_eks_lane_runs_opus_through_a_per_node_pool_named_by_its_cluster(tmp_path: Path) -> None:
    environ = {"SDO_OPUS_POOL_HOST_PATH": "/opt/sdo-evidence/opus-pool", "SDO_OPUS_POOL_DIR": "/x"}
    eks_environment(tmp_path).apply_operator_env(environ)
    assert environ["SDO_OPUS_POOL_MODE"] == "per_host"
    assert environ["SDO_OPUS_POOL_HOST_ID"] == "eks-lane-1"
    assert environ["SDO_OPUS_POOL_HOST_PATH"] == "/var/lib/sdo-lane/opus-pool"
    assert environ["SDO_OPUS_MAX_SESSIONS"] == "4"
    assert "SDO_OPUS_POOL_DIR" not in environ


def test_the_node_pool_is_created_once_and_never_resized() -> None:
    from benchmarks.sregym.fastloop.eks_lane import opus_pool_script

    script = opus_pool_script(3)
    assert "touch /lane/opus-pool/slot-0.lock /lane/opus-pool/slot-1.lock /lane/opus-pool/slot-2.lock" in script
    assert "[ -s /lane/opus-pool/capacity ] || printf '3\\n'" in script
    assert 'not 3" >&2; exit 1' in script
    assert "chown -R 65532:65532 /lane/opus-pool" in script
    completed = subprocess.run(["sh", "-n", "-c", script], check=False)
    assert completed.returncode == 0
