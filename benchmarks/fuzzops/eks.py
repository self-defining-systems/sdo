"""The fuzzops hooks on an EKS lane (``BENCH_LANE_PROVIDER=eks``).

An EKS lane is a cluster the host reaches only through its API: no kind node to ``kind load`` into, no host mount for
the evidence sink, no ``docker exec`` into a node. The hooks already serve lanes like that for provider ``vcluster``
(pulled evidence, claim-based dev tree); what EKS adds is the image path and a storage check. The lane platform sets:

``BENCH_LANE_PROVIDER=eks``
    the platform switch (``kind`` when unset);
``BENCH_IMAGE_REGISTRY``
    the ECR registry host of the account (``<account>.dkr.ecr.<region>.amazonaws.com``); the hooks push the operator
    images there and install them by digest, through ``benchmarks.lanes.eks`` (the host needs ``aws`` and ``docker``);
``BENCH_IMAGE_PREFIX`` / ``BENCH_IMAGE_MIRROR_PREFIX`` / ``BENCH_IMAGE_TAGS``
    optional: the repository prefix of pinned images (``sdo``), of the app-image mirror (``sdo-mirror``) and
    ``key=value,...`` tags for repositories the hooks create;
``FUZZOPS_ENTRY_HOST``
    an address of a node that the host reaches (the apps' NodePorts), as for provider ``vcluster``.

Evidence is always pulled from the control namespace's claim (``sink.pull_mode`` is true for this provider).
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING, Any

from benchmarks.lanes.eks import (
    Ecr,
    EksLane,
    EksLaneError,
    mirror_repository,
    pin_image,
    publish_app_image,
    split_reference,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

PROVIDER_ENV = "BENCH_LANE_PROVIDER"
REGISTRY_ENV = "BENCH_IMAGE_REGISTRY"
PREFIX_ENV = "BENCH_IMAGE_PREFIX"
MIRROR_PREFIX_ENV = "BENCH_IMAGE_MIRROR_PREFIX"
TAGS_ENV = "BENCH_IMAGE_TAGS"
KIND = "kind"
EKS = "eks"
VCLUSTER = "vcluster"
#: Providers whose lanes have no node directory the host can mount: their evidence and dev tree live in a claim.
PULLED = frozenset({EKS, VCLUSTER})
DEFAULT_CLASS_ANNOTATIONS = (
    "storageclass.kubernetes.io/is-default-class",
    "storageclass.beta.kubernetes.io/is-default-class",
)


class EksHookError(RuntimeError):
    """The lane is not set up the way the EKS path needs; the message says what to change."""


def lane_provider(env: Mapping[str, str]) -> str:
    """``kind`` (the default), ``vcluster`` or ``eks``."""
    value = env.get(PROVIDER_ENV, "") or KIND
    if value not in (KIND, VCLUSTER, EKS):
        raise EksHookError(f"{PROVIDER_ENV}={value!r}: want {KIND}, {VCLUSTER} or {EKS}")
    return value


def is_eks(env: Mapping[str, str]) -> bool:
    return lane_provider(env) == EKS


def _tags(raw: str) -> dict[str, str]:
    tags: dict[str, str] = {}
    for item in filter(None, (part.strip() for part in raw.split(","))):
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise EksHookError(f"{TAGS_ENV}: {item!r} is not key=value")
        tags[key] = value
    return tags


def lane_from_env(
    env: Mapping[str, str], kubectl: Callable[..., list[str]], run: Callable[..., Any] = subprocess.run
) -> EksLane:
    """The lane the hooks push to: the registry from the environment, the cluster identity from the kubeconfig."""
    registry = env.get(REGISTRY_ENV, "")
    if not registry:
        raise EksHookError(f"{REGISTRY_ENV} is not set (the ECR registry host of the lane's account)")

    def read(*args: str) -> str:
        done = run(kubectl(*args), capture_output=True, text=True, timeout=120, check=False)
        if done.returncode != 0:
            raise EksHookError(f"kubectl {' '.join(args[:3])}: {done.stderr.strip()[-200:]}")
        return str(done.stdout).strip()

    extra: dict[str, Any] = {}
    if env.get(PREFIX_ENV):
        extra["image_prefix"] = env[PREFIX_ENV]
    if env.get(MIRROR_PREFIX_ENV):
        extra["mirror_prefix"] = env[MIRROR_PREFIX_ENV]
    try:
        return EksLane(
            registry=registry,
            server=read("config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"),
            cluster_uid=read("get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"),
            repository_tags=_tags(env.get(TAGS_ENV, "")),
            **extra,
        )
    except ValueError as exc:
        raise EksHookError(f"{REGISTRY_ENV}/{PREFIX_ENV}: {exc}") from exc


def pin_operator_images(
    lane: EksLane,
    images: Mapping[str, str],
    log: Callable[[str], None],
    run: Callable[..., Any] = subprocess.run,
) -> dict[str, str]:
    """Push each local operator image (tag -> ``images`` value is the local name) to ECR; returns local name -> the
    ``<registry>/<prefix>/<name>@sha256:...`` reference every node pulls. The images must exist on this host."""
    ecr = Ecr(lane, run=run)
    pinned: dict[str, str] = {}
    for local in dict.fromkeys(images.values()):
        found = run(["docker", "image", "inspect", local], capture_output=True, text=True, check=False)
        if found.returncode != 0:
            raise EksHookError(f"image {local} does not exist on this host (build it with scripts/build_sdo_images.sh)")
        try:
            pinned[local] = pin_image(ecr, local)
        except EksLaneError as exc:
            raise EksHookError(str(exc)) from exc
        log(f"pinned {local} as {pinned[local]}")
    return pinned


def storage_problem(classes: Mapping[str, Any]) -> str:
    """Why the cluster cannot hold the lane's claims ("" when it can): the evidence claim is bound to the node the
    lane's other volumes use, which an EBS volume allows only when it is created where its first pod runs, so the
    default StorageClass must wait for a consumer (``volumeBindingMode: WaitForFirstConsumer``)."""
    defaults = [
        item
        for item in classes.get("items", [])
        if any(item.get("metadata", {}).get("annotations", {}).get(a) == "true" for a in DEFAULT_CLASS_ANNOTATIONS)
    ]
    if not defaults:
        return "the cluster has no default StorageClass (an EKS lane needs gp3 as default)"
    first = defaults[0]
    if first.get("volumeBindingMode") != "WaitForFirstConsumer":
        name = first.get("metadata", {}).get("name", "?")
        return (
            f"the default StorageClass {name} binds volumes before a pod is scheduled "
            "(volumeBindingMode must be WaitForFirstConsumer, or a claim pinned to a node can land in another zone)"
        )
    return ""


#: Tags that name a moving image: published every time, whatever the mirror holds.
MOVING_TAGS = frozenset({"dev", "latest"})


def mirror_holds(ecr: Ecr, repository: str, tag: str, run: Callable[..., Any]) -> bool:
    found = run(
        [
            "aws",
            "ecr",
            "describe-images",
            "--region",
            ecr.lane.region,
            "--repository-name",
            repository,
            "--image-ids",
            f"imageTag={tag}",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    return found.returncode == 0


def publish_images(
    lane: EksLane,
    images: Sequence[str],
    log: Callable[[str], None],
    run: Callable[..., Any] = subprocess.run,
) -> list[str]:
    """Publish each image to the mirror under its declared name; returns the ones published.

    An image from a registry the lane does not mirror is left to the nodes to pull. A tag other than a moving one is
    published once: the mirror already holding it is enough."""
    ecr = Ecr(lane, run=run)
    published: list[str] = []
    for image in images:
        try:
            repository, tag = mirror_repository(lane, image)
        except EksLaneError as exc:
            log(f"not mirrored ({exc}); nodes pull {image} themselves")
            continue
        if (
            split_reference(image)[1] not in MOVING_TAGS
            and "@" not in image
            and mirror_holds(ecr, repository, tag, run)
        ):
            log(f"{image}: already in the mirror")
            continue
        if run(["docker", "image", "inspect", image], capture_output=True, text=True, check=False).returncode != 0:
            pulled = run(["docker", "pull", "-q", image], capture_output=True, text=True, check=False)
            if pulled.returncode != 0:
                raise EksHookError(f"image {image} is not on this host and cannot be pulled: {pulled.stderr[-200:]}")
        publish_app_image(ecr, image, image)
        log(f"{image}: published to {lane.registry}/{repository}:{tag}")
        published.append(image)
    return published
