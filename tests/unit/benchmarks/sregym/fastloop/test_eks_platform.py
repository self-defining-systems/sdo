"""An EKS lane in fastloop: attach-only, pinned images, node-mirror app delivery, evidence by claim, no kind calls."""

from __future__ import annotations

import argparse
import json
import subprocess
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import sink
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

if TYPE_CHECKING:
    from pathlib import Path

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
        if "nodes" in command:
            ready = [{"type": "Ready", "status": "True"}]
            nodes = {"items": [{"metadata": {"name": "node-a"}, "status": {"conditions": ready}}]}
            return subprocess.CompletedProcess(command, 0, json.dumps(nodes), "")
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
    assert evidence_sink.pull_cluster_sink(kind, ["r"]) is False

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, "", 'persistentvolumeclaims "sdo-evidence" NotFound')

    monkeypatch.setattr(subprocess, "run", run)
    # The control namespace (and its claim) is gone after the run was copied out: nothing in the cluster to lose.
    saved = tmp_path / "evidence" / "r"
    saved.mkdir(parents=True)
    (saved / sink.LAYOUT_FILE).write_text(json.dumps({"schema": sink.LAYOUT_SCHEMA}))
    assert evidence_sink.pull_cluster_sink(eks_environment(tmp_path), ["r"]) is False


def test_an_eks_lane_runs_opus_through_a_per_node_pool_named_by_its_cluster(tmp_path: Path) -> None:
    environ = {"SDO_OPUS_POOL_HOST_PATH": "/opt/sdo-evidence/opus-pool", "SDO_OPUS_POOL_DIR": "/x"}
    eks_environment(tmp_path).apply_operator_env(environ, model="claude-opus-5-5")
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


def test_an_opus_row_without_a_session_pool_fails_before_it_injects() -> None:
    assert cli.opus_pool_missing("claude-opus-5-5", {}) is not None
    assert cli.opus_pool_missing("claude-opus-5-5", {"SDO_OPUS_POOL_MODE": "per_host"}) is not None
    assert (
        cli.opus_pool_missing(
            "claude-opus-5-5",
            {"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_PATH": "/var/lib/sdo-lane/opus-pool"},
        )
        is None
    )
    assert cli.opus_pool_missing("claude-opus-5-5", {"SDO_OPUS_POOL_NFS_SERVER": "nfs.example"}) is None
    assert cli.opus_pool_missing("claude-sonnet-5-5", {}) is None


def _claim(node: str = "node-b") -> str:
    return json.dumps(
        {"metadata": {"name": "sdo-evidence", "annotations": {"volume.kubernetes.io/selected-node": node}}}
    )


def test_the_eks_evidence_claim_is_bound_on_one_node_chosen_by_the_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FASTLOOP_DOCKERHUB_AUTH_FILE", raising=False)
    applied: list[dict[str, Any]] = []
    nodes = {
        "items": [
            {"metadata": {"name": n}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
            for n in ("node-a", "node-b")
        ]
    }

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "apply" in command:
            applied.extend(json.loads(kwargs["input"])["items"])
        if "nodes" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps(nodes), "")
        if "service/sdo-benchmark-registry" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"spec": {"clusterIP": "10.0.0.7"}}), "")
        if "deployment/sdo-benchmark-registry" in command:
            return subprocess.CompletedProcess(command, 0, json.dumps({"status": {"readyReplicas": 1}}), "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr("benchmarks.lanes.eks.write_node_file", lambda *a: [])
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
    claim = next(i for i in applied if i["kind"] == "PersistentVolumeClaim" and i["metadata"]["name"] == "sdo-evidence")
    selected = claim["metadata"]["annotations"]["volume.kubernetes.io/selected-node"]
    assert selected == sink.volume_node(nodes, "eks-1")


def test_the_evidence_puller_runs_on_the_claims_node_and_pulls_only_the_run_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []
    pods: list[dict[str, Any]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[Any]:
        calls.append(command)
        if "apply" in command:
            pods.append(json.loads(kwargs["input"]))
        if "pvc" in command:
            return subprocess.CompletedProcess(command, 0, _claim(), "")
        if "pods" in command and "get" in command:
            holder = {
                "spec": {
                    "nodeName": "node-b",
                    "volumes": [{"persistentVolumeClaim": {"claimName": sink.EVIDENCE_CLAIM}}],
                },
                "status": {"phase": "Running"},
            }
            return subprocess.CompletedProcess(command, 0, json.dumps({"items": [holder]}), b"")
        return subprocess.CompletedProcess(command, 0, "", b"" if command[0] == "tar" else "")

    class Tar:
        stdout = SimpleNamespace(close=lambda: None)
        stderr = None

        def __init__(self, command: list[str], **_: Any) -> None:
            calls.append(command)

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(subprocess, "Popen", Tar)
    assert evidence_sink.pull_cluster_sink(eks_environment(tmp_path), ["lane-r1"])
    assert pods[0]["spec"]["nodeName"] == "node-b"
    pulled = next(c for c in calls if "exec" in c)
    assert pulled[-1] == "lane-r1"
    assert "." not in pulled[pulled.index("--") + 1 :]


def test_a_vanished_evidence_claim_with_unsaved_evidence_is_a_named_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "namespace" in command:
            return subprocess.CompletedProcess(command, 0, "name", "")
        return subprocess.CompletedProcess(command, 1, "", 'persistentvolumeclaims "sdo-evidence" NotFound')

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(evidence_sink.EvidenceClaimMissing, match="lane-r1"):
        evidence_sink.pull_cluster_sink(eks_environment(tmp_path), ["lane-r1"])


def test_node_agents_are_waited_for_before_the_pool_is_provisioned() -> None:
    from benchmarks.lanes import eks

    ready_condition = [{"type": "Ready", "status": "True"}]
    nodes = json.dumps({"items": [{"metadata": {"name": "n1"}, "status": {"conditions": ready_condition}}]})
    agent = {
        "items": [
            {"metadata": {"name": "agent-1"}, "spec": {"nodeName": "n1"}, "status": {"conditions": ready_condition}}
        ]
    }
    answers = iter([{"items": []}, {"items": []}, agent])
    now = [0.0]

    def run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "nodes" in argv:
            return subprocess.CompletedProcess(argv, 0, nodes, "")
        if "exec" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 0, json.dumps(next(answers)), "")

    def sleep(seconds: float) -> None:
        now[0] += seconds

    assert eks.run_on_nodes("/k", "true", run=run, wait=60, sleep=sleep, clock=lambda: now[0]) == ["n1"]
    assert now[0] == 10
    answers = iter([{"items": []}] * 100)
    with pytest.raises(eks.EksLaneError, match="not ready"):
        eks.run_on_nodes("/k", "true", run=run, wait=0, sleep=sleep, clock=lambda: now[0])


def test_only_an_opus_model_provisions_the_node_pool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import eks_lane

    provisioned: list[str] = []
    monkeypatch.setattr(eks_lane, "provision_opus_pool", lambda env, environ=None: provisioned.append("x") or ["n1"])
    environment = eks_environment(tmp_path)
    assert eks_lane.ensure_opus_pool(environment, "gpt-6-luna") == []
    assert eks_lane.ensure_opus_pool(environment, "claude-sonnet-5-5") == []
    assert provisioned == []
    assert eks_lane.ensure_opus_pool(environment, "claude-opus-5-5") == ["n1"]


def test_an_ecr_error_is_not_an_answer_about_the_images() -> None:
    from benchmarks.lanes import eks

    digest = "sha256:" + "b" * 64
    denied = eks.EksProvider(
        LANE,
        "/k",
        run=lambda argv, **k: subprocess.CompletedProcess(argv, 255, "", "AccessDeniedException: token expired"),
    )
    with pytest.raises(eks.EksLaneError, match="token expired"):
        denied.holds(f"{REGISTRY}/sdo/sdo-controller@{digest}")


def test_kind_only_operations_are_refused_on_an_eks_lane(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import host_limits
    from benchmarks.sregym.fastloop.environment import KindOnlyError

    environment = eks_environment(tmp_path)
    environment.save()
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("kind call on an EKS lane"))
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: pytest.fail("kind call on an EKS lane"))
    retire = argparse.Namespace(run_dir=tmp_path)
    with pytest.raises(KindOnlyError, match="app retirement"):
        cli._retire_app(retire)
    reset = argparse.Namespace(run_dir=tmp_path, retarget_dev_mount=True, dev_mount=tmp_path)
    with pytest.raises(KindOnlyError, match="dev mount"):
        cli._reset(reset)
    monkeypatch.setenv("SREGYM_CLUSTER_PLATFORM", "eks")
    with pytest.raises(KindOnlyError, match="node caps"):
        host_limits.apply_node_limits("eks-1")


def test_up_proves_the_mirror_serves_with_one_bounded_pull(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop import eks_lane

    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    assert eks_lane.verify_mirror_serves(eks_environment(tmp_path), run=run) == "busybox:1.36"
    assert [c[3] for c in calls] == ["apply", "wait", "delete"]
    assert any(a.startswith("--timeout=") for a in calls[1])

    def failing(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "wait" in command:
            return subprocess.CompletedProcess(command, 1, "", "timed out")
        if "get" in command:
            return subprocess.CompletedProcess(command, 0, "back-off pulling image", "")
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(eks_lane.MirrorCheckError, match="back-off pulling image"):
        eks_lane.verify_mirror_serves(eks_environment(tmp_path), run=failing)


def test_kind_rows_keep_opus_pool_semantics() -> None:
    """Kind parity (a deliberate change on main: an Opus row without a pool now stops at row start, before the fault
    is injected, instead of failing every dispatch after it). Non-Opus models and kind pool settings are unaffected."""
    assert cli.opus_pool_missing("claude-opus-5-5", {}) is not None
    assert cli.opus_pool_missing("claude-opus-5-5", {"SDO_OPUS_POOL_DIR": "/shared/pool"}) is None
    assert cli.opus_pool_missing("claude-opus-5-5", {"SDO_OPUS_POOL_NFS_SERVER": "nfs"}) is None
    assert cli.opus_pool_missing("gpt-6-luna", {}) is None
    assert cli.opus_pool_missing("claude-sonnet-5-5", {}) is None


def test_a_virtual_lane_uses_the_platforms_storage_and_a_cluster_lane_keeps_openebs(tmp_path: Path) -> None:
    args = argparse.Namespace(
        cluster_prefix="vc", worker_id=1, run_dir=tmp_path, host_lock_dir=tmp_path / "locks", host_redeploys=2
    )
    virtual = cli.eks_worker_environment(
        args, workspace=tmp_path / "ws", lane=LANE.model_copy(update={"virtual": True})
    )
    assert virtual["SREGYM_MANAGED_STORAGE"] == "1"
    assert "SREGYM_MANAGED_STORAGE" not in cli.eks_worker_environment(args, workspace=tmp_path / "ws", lane=LANE)


def test_a_lane_whose_sdo_was_never_installed_has_no_claim_to_lose(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first row of a fresh lane wipes before it installs SDO: the control namespace and its claim do not exist yet,
    and the (not yet started) run directories have no evidence to save."""

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command, 1, "", 'namespaces "sdo-ns" NotFound, persistentvolumeclaims NotFound'
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert not evidence_sink.pull_cluster_sink(eks_environment(tmp_path), ["lane-r1"])


def test_settling_asks_the_claim_only_for_rows_that_injected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A row that never injected has no incident evidence in the sink: after the wipe recreated the control namespace
    (its claim comes with the next SDO install), the missing claim is not lost evidence."""
    from benchmarks.sregym.fastloop.injection_marker import write_injection_marker

    environment = eks_environment(tmp_path)
    (tmp_path / "results" / "never-injected").mkdir(parents=True)
    calls: list[list[str]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if "namespace" in command:
            return subprocess.CompletedProcess(command, 0, "namespace/x", "")
        return subprocess.CompletedProcess(command, 1, "", 'persistentvolumeclaims "sdo-evidence" NotFound')

    monkeypatch.setattr(subprocess, "run", run)
    assert evidence_sink.settle_lane_evidence(environment) == []
    assert not calls  # nothing to pull
    injected = tmp_path / "results" / "injected"
    injected.mkdir()
    from datetime import UTC, datetime

    write_injection_marker(injected, datetime.now(UTC), frozenset())
    assert "evidence claim" in evidence_sink.settle_lane_evidence(environment)[0]


def test_a_virtual_lane_writes_no_node_files_through_a_node_agent_it_cannot_reach(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        out = ""
        if "nodes" in command:
            out = json.dumps(
                {
                    "items": [
                        {"metadata": {"name": "n"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
                    ]
                }
            )
        if "service/sdo-benchmark-registry" in command:
            out = json.dumps({"spec": {"clusterIP": "10.0.0.7"}})
        if "deployment/sdo-benchmark-registry" in command:
            out = json.dumps({"status": {"readyReplicas": 1}})
        return subprocess.CompletedProcess(command, 0, out, "")

    written: list[str] = []
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr("benchmarks.lanes.eks.write_node_file", lambda *a: written.append(a[1]) or [])
    monkeypatch.setenv("FASTLOOP_EKS_LANE", LANE.model_copy(update={"virtual": True}).model_dump_json())
    config = BenchmarkClusterResourcesConfig(
        namespace="hotel-reservation",
        control_namespace="hotel-reservation-sdo",
        responder_image="responder@" + DIGEST,
        credentials_secret="sdo-agent-credentials",
        registry_push_secret="sdo-registry-push",
        registry_insecure=True,
        kind_cluster_name="vc-1",
        lane_platform="eks",
        evidence_claim=True,
    )
    prepare_benchmark_resources(config, relay_target_base=None, kubeconfig=tmp_path / "kc")
    assert written == []


def test_the_run_process_knows_its_lane_is_virtual_like_the_worker_does(tmp_path: Path) -> None:
    """Native resources and the oracle observability helper run in the row process, not the worker: they read the lane
    record and the storage mode from the same environment the worker gets."""
    virtual = eks_environment(tmp_path, eks=LANE.model_copy(update={"virtual": True}))
    environ: dict[str, str] = {}
    virtual.apply_operator_env(environ)
    assert EksLane.model_validate_json(environ["FASTLOOP_EKS_LANE"]).virtual
    assert environ["SREGYM_CLUSTER_PLATFORM"] == "eks"
    assert environ["SREGYM_MANAGED_STORAGE"] == "1"
    cluster_lane: dict[str, str] = {"SREGYM_MANAGED_STORAGE": "1"}
    eks_environment(tmp_path).apply_operator_env(cluster_lane)
    assert "SREGYM_MANAGED_STORAGE" not in cluster_lane


def test_a_row_of_another_model_gets_no_opus_pool_mount(tmp_path: Path) -> None:
    """The controller mounts the pool path from the node when it is configured; only an Opus row provisions it."""
    environ = {"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_PATH": "/stale"}
    eks_environment(tmp_path).apply_operator_env(environ, model="claude-sonnet-5-5")
    assert not [key for key in environ if key.startswith("SDO_OPUS_POOL_") or key == "SDO_OPUS_MAX_SESSIONS"]
    unmodelled: dict[str, str] = {}
    eks_environment(tmp_path).apply_operator_env(unmodelled)
    assert "SDO_OPUS_POOL_MODE" not in unmodelled
