"""The fuzzops hooks on an EKS lane: the provider switch, ECR pins for the operator images, the storage check."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from benchmarks.fuzzops import eks, hooks, sink
from benchmarks.fuzzops.hooks import HookError

REGISTRY = "000000000000.dkr.ecr.us-west-2.amazonaws.com"
DIGEST = "sha256:" + "ab" * 32


def _kubectl(*args: str) -> list[str]:
    return ["kubectl", "--kubeconfig", "/k", *args]


class Run:
    """Answers kubectl/docker/aws by their words and keeps the calls."""

    def __init__(self, missing_images: tuple[str, ...] = ()) -> None:
        self.calls: list[list[str]] = []
        self.missing = missing_images

    def __call__(self, argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        out, code = "", 0
        if argv[:3] == ["kubectl", "--kubeconfig", "/k"] and "view" in argv:
            out = "https://api.example"
        elif "namespace" in argv and "kube-system" in argv:
            out = "uid-1"
        elif argv[:3] == ["docker", "image", "inspect"] and argv[3] in self.missing:
            code = 1
        elif argv[:3] == ["aws", "ecr", "describe-repositories"]:
            out = "{}"
        elif argv[:3] == ["aws", "ecr", "describe-images"]:
            out = DIGEST
        elif argv[:3] == ["aws", "ecr", "get-login-password"]:
            out = "pw"
        return subprocess.CompletedProcess(argv, code, out, "")


# ---- the platform switch -------------------------------------------------------------------------------------


def test_the_provider_defaults_to_kind_and_names_only_known_providers() -> None:
    assert eks.lane_provider({}) == "kind"
    assert eks.lane_provider({"BENCH_LANE_PROVIDER": "eks"}) == "eks"
    with pytest.raises(eks.EksHookError, match="BENCH_LANE_PROVIDER"):
        eks.lane_provider({"BENCH_LANE_PROVIDER": "gke"})


@pytest.mark.parametrize(("provider", "pulled"), [("", False), ("kind", False), ("vcluster", True), ("eks", True)])
def test_evidence_is_pulled_from_the_claim_for_lanes_without_a_node_mount(provider: str, pulled: bool) -> None:
    assert sink.pull_mode({"BENCH_LANE_PROVIDER": provider}) is pulled


def test_an_eks_lane_installs_the_sink_as_a_claim_with_no_node_path() -> None:
    env = {"BENCH_LANE_PROVIDER": "eks", "BENCH_EVIDENCE_RUN": "e-l-1"}
    got = sink.install_env(env)
    assert got["SDO_EVIDENCE_RUN_ID"] == "e-l-1"
    assert "SDO_EVIDENCE_VOLUME" in got
    assert "SDO_EVIDENCE_HOST_PATH" not in got


def test_only_a_kind_lane_needs_a_kind_cluster() -> None:
    assert hooks.kind_cluster({"BENCH_LANE_PROVIDER": "eks"}) == ""
    assert hooks.kind_cluster({"BENCH_LANE_PROVIDER": "vcluster", "FUZZOPS_KIND_CLUSTER": "x"}) == "x"
    assert hooks.kind_cluster({"FUZZOPS_KIND_CLUSTER": "lane1"}) == "lane1"
    with pytest.raises(HookError, match="FUZZOPS_KIND_CLUSTER"):
        hooks.kind_cluster({})


# ---- the lane and its images ---------------------------------------------------------------------------------


def test_the_lane_comes_from_the_registry_and_the_kubeconfig() -> None:
    env = {"BENCH_IMAGE_REGISTRY": REGISTRY, "BENCH_IMAGE_TAGS": "Project=sdo,Owner=lanes", "BENCH_IMAGE_PREFIX": "p"}
    lane = eks.lane_from_env(env, _kubectl, Run())
    assert (lane.registry, lane.server, lane.cluster_uid, lane.image_prefix) == (
        REGISTRY,
        "https://api.example",
        "uid-1",
        "p",
    )
    assert lane.repository_tags == {"Project": "sdo", "Owner": "lanes"}


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({}, "BENCH_IMAGE_REGISTRY"),
        ({"BENCH_IMAGE_REGISTRY": "docker.io"}, "ECR"),
        ({"BENCH_IMAGE_REGISTRY": REGISTRY, "BENCH_IMAGE_TAGS": "oops"}, "key=value"),
    ],
)
def test_a_lane_without_a_usable_registry_is_refused(env: dict[str, str], message: str) -> None:
    with pytest.raises(eks.EksHookError, match=message):
        eks.lane_from_env(env, _kubectl, Run())


def test_operator_images_are_pushed_to_ecr_and_named_by_digest() -> None:
    run = Run()
    lane = eks.lane_from_env({"BENCH_IMAGE_REGISTRY": REGISTRY}, _kubectl, run)
    lines: list[str] = []
    images = {"controller": "sdo-controller:t", "responder": "sdo-responder:t", "script": "sdo-script-tools:t"}
    pinned = eks.pin_operator_images(lane, images, lines.append, run)
    assert pinned == {
        "sdo-controller:t": f"{REGISTRY}/sdo/sdo-controller@{DIGEST}",
        "sdo-responder:t": f"{REGISTRY}/sdo/sdo-responder@{DIGEST}",
        "sdo-script-tools:t": f"{REGISTRY}/sdo/sdo-script-tools@{DIGEST}",
    }
    pushes = [c for c in run.calls if c[:2] == ["docker", "push"]]
    assert len(pushes) == 3
    assert not any(c[0] == "kind" for c in run.calls)


def test_a_missing_local_image_fails_before_any_push() -> None:
    run = Run(missing_images=("sdo-responder:t",))
    lane = eks.lane_from_env({"BENCH_IMAGE_REGISTRY": REGISTRY}, _kubectl, run)
    with pytest.raises(eks.EksHookError, match="sdo-responder:t does not exist"):
        eks.pin_operator_images(lane, {"controller": "sdo-controller:t", "responder": "sdo-responder:t"}, print, run)


# ---- storage -------------------------------------------------------------------------------------------------


def _storage_class(name: str, *, default: bool, binding: str) -> dict[str, Any]:
    annotations = {"storageclass.kubernetes.io/is-default-class": "true"} if default else {}
    return {"metadata": {"name": name, "annotations": annotations}, "volumeBindingMode": binding}


def test_the_default_storage_class_must_wait_for_the_first_consumer() -> None:
    good = {"items": [_storage_class("gp3", default=True, binding="WaitForFirstConsumer")]}
    assert eks.storage_problem(good) == ""
    early = {"items": [_storage_class("gp2", default=True, binding="Immediate")]}
    assert "WaitForFirstConsumer" in eks.storage_problem(early)
    assert "no default StorageClass" in eks.storage_problem(
        {"items": [_storage_class("x", default=False, binding="Immediate")]}
    )
    assert "no default StorageClass" in eks.storage_problem({})
