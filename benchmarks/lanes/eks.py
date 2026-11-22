"""EKS lanes: a lane on an existing Amazon EKS cluster, reached only through its API server.

A kind lane is a set of Docker containers on this host; fastloop reaches its nodes with ``docker exec`` and loads
images into them. An EKS lane has no node access from the host, so every node-level step goes through the cluster:

* **Images.** Immutable images (for SDO: its controller and workers) are pushed to ECR
  (``<registry>/<image_prefix>/<name>``) and installed by digest.
  Application images keep the names the source declares: the node agent the lane provider installs (a
  ``kube-system`` DaemonSet, ``benchmarks/lanes/deploy/eks-lane.sh``; this module only verifies it) writes a
  containerd registry mirror for each upstream (``/etc/containerd/certs.d/<upstream>/hosts.toml``) that tries
  ``<registry>/v2/<mirror_prefix>/<upstream>`` first with an ECR token from the node's instance role, and falls back
  to the upstream. A source-built image is published under its declared name by pushing it to that path.
* **Node writes.** Whatever a kind lane writes into a node with ``docker exec`` (an extra registry host) an EKS lane
  writes through the node agent on that node with ``kubectl exec``.
* **Identity.** A lane is fenced by its API server URL and the UID of its ``kube-system`` namespace. Node names are
  not part of the fence: a node group scaled to zero and back has new nodes in the same cluster.

This module is the EKS lane *provider* (``benchmarks.lanes.provider.LaneProvider``): it knows Kubernetes, ECR and
containerd, and nothing about SREGym or SDO; workload adapters (fastloop) call it. Cluster creation, node groups,
the node agent and the mirror configuration belong to the lanes ``eks`` provider (``deploy/eks-lane.sh``); this
module attaches to a cluster that exists and verifies them.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

ECR_HOST = re.compile(r"^(?P<account>[0-9]{12})\.dkr\.ecr\.(?P<region>[a-z0-9-]+)\.amazonaws\.com$")
#: The registry of a local kind host cluster (lanes ``eks.local_kind``): a plain-HTTP registry on this machine that
#: every node resolves, in place of ECR. No AWS call is made for it.
LOCAL_REGISTRY = re.compile(r"^localhost:[0-9]{2,5}$")
NODE_AGENT = "sdo-lane-node-agent"
NODE_AGENT_NAMESPACE = "kube-system"
#: Where the agent mounts the node's ``/etc/containerd/certs.d``.
AGENT_CERTS = "/certs"
#: A node directory the lane's workloads own (pools, caches); the agent mounts it at ``AGENT_LANE``.
NODE_LANE_ROOT = "/var/lib/sdo-lane"
AGENT_LANE = "/lane"
#: The upstream registries containerd resolves for a bare name, and the servers it falls back to.
UPSTREAM_SERVERS = {
    "docker.io": "https://registry-1.docker.io",
    "ghcr.io": "https://ghcr.io",
    "quay.io": "https://quay.io",
    "registry.k8s.io": "https://registry.k8s.io",
}

Runner = Callable[..., subprocess.CompletedProcess[str]]


class EksLaneError(RuntimeError):
    """The EKS lane cannot be reached, is not the recorded cluster, or a node step failed."""


class EksLane(BaseModel):
    """What fastloop records about one EKS lane in ``fastloop.json``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    #: ECR registry host ``<account>.dkr.ecr.<region>.amazonaws.com``.
    registry: str
    #: The API server URL of the lane kubeconfig and the cluster's ``kube-system`` namespace UID.
    server: str
    cluster_uid: str
    mirror_prefix: str = "sdo-mirror"
    image_prefix: str = "sdo"
    upstreams: tuple[str, ...] = ("docker.io", "ghcr.io", "quay.io", "registry.k8s.io")
    #: Tags every ECR repository fastloop creates carries (account bookkeeping, for example ``Project``).
    repository_tags: dict[str, str] = Field(default_factory=dict)
    #: A vcluster lane: its ``kube-system`` and nodes are the host cluster's, so NetworkPolicy enforcement and the node
    #: agent are the lanes provider's to guarantee and cannot be read or run from inside the lane.
    virtual: bool = False

    @field_validator("registry")
    @classmethod
    def ecr_registry(cls, value: str) -> str:
        if not ECR_HOST.fullmatch(value) and not LOCAL_REGISTRY.fullmatch(value):
            raise ValueError(
                "registry must be an ECR host <account>.dkr.ecr.<region>.amazonaws.com "
                "or a local kind host's registry localhost:<port>"
            )
        return value

    @property
    def local_registry(self) -> bool:
        """Whether the registry is a local kind host's (no ECR, no AWS credentials)."""
        return LOCAL_REGISTRY.fullmatch(self.registry) is not None

    @field_validator("upstreams")
    @classmethod
    def known_upstreams(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        unknown = sorted(set(value) - set(UPSTREAM_SERVERS))
        if unknown or not value:
            raise ValueError(f"upstreams must be a non-empty subset of {sorted(UPSTREAM_SERVERS)}; unknown {unknown}")
        return value

    @field_validator("mirror_prefix", "image_prefix")
    @classmethod
    def repository_prefix(cls, value: str) -> str:
        if not re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*", value):
            raise ValueError(f"invalid ECR repository prefix {value!r}")
        return value

    @property
    def region(self) -> str:
        match = ECR_HOST.fullmatch(self.registry)
        if match is None:
            raise EksLaneError(f"registry {self.registry} is not an ECR host: it has no AWS region")
        return match["region"]


def image_ref(image: str) -> str:
    """``image`` with Docker's implicit registry and ``library/`` spelled out (containerd's view of the name)."""

    first = image.split("/", 1)[0]
    if "/" not in image:
        return "docker.io/library/" + image
    if "." not in first and ":" not in first and first != "localhost":
        return "docker.io/" + image
    return image


def split_reference(image: str) -> tuple[str, str]:
    """``(registry/path, tag)`` of a tagged reference, with Docker's implicit registry and ``library/`` spelled out."""

    full = image_ref(image)
    if "@" in full:
        raise EksLaneError(f"{image}: a digest reference cannot be published under a declared name")
    name, _, tag = full.rpartition(":")
    if not name or "/" in tag:
        return full, "latest"
    return name, tag


def mirror_repository(lane: EksLane, image: str) -> tuple[str, str]:
    """The ECR repository and tag a node resolves ``image`` from: ``<mirror_prefix>/<upstream>/<path>``."""

    name, tag = split_reference(image)
    upstream = name.split("/", 1)[0]
    if upstream not in lane.upstreams:
        raise EksLaneError(f"{image}: the lane mirrors {list(lane.upstreams)}, not {upstream}")
    return f"{lane.mirror_prefix}/{name}", tag


def image_repository(lane: EksLane, image: str) -> tuple[str, str]:
    """The ECR repository and tag of a pinned lane image (``sdo-controller:t`` -> ``<image_prefix>/sdo-controller``)."""

    name, tag = split_reference(image)
    return f"{lane.image_prefix}/{name.rsplit('/', 1)[-1]}", tag


class Ecr:
    """Pushes to the lane's ECR registry with the host's AWS credentials (``aws`` and ``docker`` on PATH)."""

    #: A login is reused for this long; ECR tokens last 12 h.
    LOGIN_SECONDS = 6 * 3600

    def __init__(self, lane: EksLane, *, run: Runner = subprocess.run, clock: Callable[[], float] = time.monotonic):
        self.lane = lane
        self._run = run
        self._clock = clock
        self._logged_in_at: float | None = None

    def _call(self, argv: Sequence[str], *, input_text: str | None = None, timeout: float = 900) -> str:
        completed = self._run(
            list(argv), input=input_text, capture_output=True, text=True, check=False, timeout=timeout
        )
        if completed.returncode != 0:
            # Never echo a password; stderr of docker login and aws carries none.
            raise EksLaneError(
                f"{argv[0]} {argv[1] if len(argv) > 1 else ''} failed: {completed.stderr.strip()[-600:]}"
            )
        return completed.stdout

    def login(self) -> None:
        if self.lane.local_registry:
            return
        now = self._clock()
        if self._logged_in_at is not None and now - self._logged_in_at < self.LOGIN_SECONDS:
            return
        password = self._call(["aws", "ecr", "get-login-password", "--region", self.lane.region]).strip()
        self._call(
            ["docker", "login", "--username", "AWS", "--password-stdin", self.lane.registry], input_text=password
        )
        self._logged_in_at = now

    def ensure_repository(self, repository: str) -> None:
        if self.lane.local_registry:
            return  # a plain registry creates a repository on first push
        described = self._run(
            ["aws", "ecr", "describe-repositories", "--region", self.lane.region, "--repository-names", repository],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        if described.returncode == 0:
            return
        if "RepositoryNotFoundException" not in described.stderr:
            raise EksLaneError(f"aws ecr describe-repositories {repository}: {described.stderr.strip()[-600:]}")
        tags = [f"Key={key},Value={value}" for key, value in sorted(self.lane.repository_tags.items())]
        self._call(
            [
                "aws",
                "ecr",
                "create-repository",
                "--region",
                self.lane.region,
                "--repository-name",
                repository,
                "--image-scanning-configuration",
                "scanOnPush=false",
                *(["--tags", *tags] if tags else []),
            ],
            timeout=120,
        )

    def push(self, local: str, repository: str, tag: str) -> str:
        """Push the local image to ``repository:tag``; returns the manifest digest ECR stores."""

        self.login()
        self.ensure_repository(repository)
        target = f"{self.lane.registry}/{repository}:{tag}"
        self._call(["docker", "tag", local, target], timeout=60)
        self._call(["docker", "push", "--quiet", target])
        if self.lane.local_registry:
            return self._local_digest(target)
        digest = self._call(
            [
                "aws",
                "ecr",
                "describe-images",
                "--region",
                self.lane.region,
                "--repository-name",
                repository,
                "--image-ids",
                f"imageTag={tag}",
                "--query",
                "imageDetails[0].imageDigest",
                "--output",
                "text",
            ],
            timeout=120,
        ).strip()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise EksLaneError(f"ECR returned no digest for {repository}:{tag}: {digest!r}")
        return digest

    def _local_digest(self, target: str) -> str:
        """The manifest digest the local registry stored for the pushed ``target``."""

        found = self._call(["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", target], timeout=60)
        repository = target.rsplit(":", 1)[0]
        for reference in json.loads(found):
            name, _, digest = reference.partition("@")
            if name == repository and re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                return digest
        raise EksLaneError(f"the local registry returned no digest for {target}: {found.strip()[-300:]}")


def _holds(ecr: Ecr, reference: str) -> bool:
    """Whether ECR still stores the image a pinned ``<registry>/<repository>@sha256:...`` reference names."""

    name, _, digest = reference.partition("@")
    registry, _, repository = name.partition("/")
    if registry != ecr.lane.registry or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        return False
    if ecr.lane.local_registry:
        return _local_holds(ecr, registry, repository, digest)
    found = ecr._run(
        [
            "aws",
            "ecr",
            "describe-images",
            "--region",
            ecr.lane.region,
            "--repository-name",
            repository,
            "--image-ids",
            f"imageDigest={digest}",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    if found.returncode == 0:
        return True
    if "ImageNotFoundException" in found.stderr or "RepositoryNotFoundException" in found.stderr:
        return False
    # Credentials, throttling, network: "could not ask" is not "the images differ".
    raise EksLaneError(f"cannot tell whether ECR holds {reference}: {found.stderr.strip()[-600:]}")


def _local_holds(ecr: Ecr, registry: str, repository: str, digest: str) -> bool:
    """Whether the local registry still serves the manifest ``digest`` of ``repository`` (HTTP 200; 404 is gone)."""

    found = ecr._run(
        [
            "curl",
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-I",
            "-H",
            "Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, "
            "application/vnd.docker.distribution.manifest.v2+json, "
            "application/vnd.docker.distribution.manifest.list.v2+json",
            f"http://{registry}/v2/{repository}/manifests/{digest}",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    status = found.stdout.strip()
    if status == "200":
        return True
    if status == "404":
        return False
    raise EksLaneError(
        f"cannot tell whether the local registry holds {registry}/{repository}@{digest}: HTTP {status!r}"
    )


def publish_app_image(ecr: Ecr, local: str, declared: str) -> str:
    """Make nodes resolve ``declared`` to the bytes of ``local`` through the mirror; returns the digest."""

    repository, tag = mirror_repository(ecr.lane, declared)
    return ecr.push(local, repository, tag)


def pin_image(ecr: Ecr, local: str) -> str:
    """Push a lane image and return its immutable ``<registry>/<image_prefix>/<name>@sha256:...`` reference."""

    repository, tag = image_repository(ecr.lane, local)
    digest = ecr.push(local, repository, tag)
    return f"{ecr.lane.registry}/{repository}@{digest}"


def _kubectl(kubeconfig: str, *args: str) -> list[str]:
    return ["kubectl", "--kubeconfig", kubeconfig, *args]


def _ready_nodes(nodes: Mapping[str, Any]) -> list[str]:
    return sorted(
        item["metadata"]["name"]
        for item in nodes.get("items", [])
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in item["status"].get("conditions", []))
    )


def agent_pods(pods: Mapping[str, Any], nodes: Sequence[str]) -> dict[str, str]:
    """``node -> ready agent pod`` for every node; raises when a node has none."""

    ready = {
        item["spec"].get("nodeName"): item["metadata"]["name"]
        for item in pods.get("items", [])
        if not item["metadata"].get("deletionTimestamp")
        and any(
            c.get("type") == "Ready" and c.get("status") == "True" for c in item.get("status", {}).get("conditions", [])
        )
    }
    missing = [node for node in nodes if node not in ready]
    if missing:
        raise EksLaneError(f"node agent {NODE_AGENT} is not ready on {missing}")
    return {node: ready[node] for node in nodes}


def _json(run: Runner, argv: list[str]) -> dict[str, Any]:
    completed = run(argv, capture_output=True, text=True, check=False, timeout=120)
    if completed.returncode != 0:
        raise EksLaneError(f"{' '.join(argv[3:6])}: {completed.stderr.strip()[-600:]}")
    return json.loads(completed.stdout)


def node_agents(kubeconfig: str, *, run: Runner = subprocess.run) -> dict[str, str]:
    nodes = _ready_nodes(_json(run, _kubectl(kubeconfig, "get", "nodes", "-o", "json")))
    if not nodes:
        raise EksLaneError("the lane has no Ready node")
    pods = _json(
        run,
        _kubectl(
            kubeconfig,
            "-n",
            NODE_AGENT_NAMESPACE,
            "get",
            "pods",
            "-l",
            f"app.kubernetes.io/name={NODE_AGENT}",
            "-o",
            "json",
        ),
    )
    return agent_pods(pods, nodes)


def wait_node_agents(
    kubeconfig: str,
    timeout: float,
    *,
    run: Runner = subprocess.run,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, str]:
    """``node -> ready agent pod`` once every Ready node has one, waiting up to ``timeout`` seconds.

    A node that just joined (scale-up, replacement) is Ready before its agent pod is; a caller that acts on every
    node must wait for the agents, never read them once."""

    deadline = clock() + timeout
    while True:
        try:
            return node_agents(kubeconfig, run=run)
        except EksLaneError:
            if clock() >= deadline:
                raise
            sleep(5)


def verify_node_agent(
    kubeconfig: str,
    lane: EksLane,
    *,
    run: Runner = subprocess.run,
    timeout: float = 300,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, str]:
    """Check the node agent the lane provider installed (``deploy/eks-lane.sh up``); never applies or changes it.

    The agent must mirror every upstream of ``lane`` to ``<registry>/<mirror_prefix>`` and be ready on every Ready
    node (waited for up to ``timeout``). A missing or different agent is a named error: fix the lane's cluster-up,
    not the adapter.
    """

    daemonset = _json(
        run, _kubectl(kubeconfig, "-n", NODE_AGENT_NAMESPACE, "get", "daemonset", NODE_AGENT, "-o", "json")
    )
    containers = daemonset["spec"]["template"]["spec"].get("containers") or [{}]
    script = " ".join(containers[0].get("command") or [])
    served = re.search(r"for up in ([^;]+); do", script)
    missing = sorted(set(lane.upstreams) - set(served.group(1).split() if served else ()))
    if missing:
        raise EksLaneError(
            f"node agent {NODE_AGENT} does not mirror {missing}: the lane provider installs the mirror "
            "(set the upstreams in the lane's cluster-up, not in the adapter)"
        )
    if f"https://{lane.registry}/v2/{lane.mirror_prefix}/$up" not in script:
        raise EksLaneError(
            f"node agent {NODE_AGENT} does not mirror to {lane.registry}/{lane.mirror_prefix}: "
            "the lane's cluster-up was made for another registry or prefix"
        )
    return wait_node_agents(kubeconfig, timeout, run=run, sleep=sleep, clock=clock)


def run_on_nodes(
    kubeconfig: str,
    script: str,
    *,
    input_text: str | None = None,
    run: Runner = subprocess.run,
    wait: float = 0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> list[str]:
    """Run ``sh -c script`` on every Ready node through its agent; returns the nodes.

    The script sees the node's containerd ``certs.d`` at ``/certs`` and the lane's node directory
    (``/var/lib/sdo-lane``) at ``/lane``, as root, and nothing else of the node. ``wait`` seconds are allowed for
    every Ready node's agent to be ready (:func:`wait_node_agents`).
    """

    agents = wait_node_agents(kubeconfig, wait, run=run, sleep=sleep, clock=clock)
    for node, pod in agents.items():
        completed = run(
            _kubectl(
                kubeconfig, "-n", NODE_AGENT_NAMESPACE, "exec", "-i", pod, "-c", "agent", "--", "sh", "-c", script
            ),
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        if completed.returncode != 0:
            raise EksLaneError(f"node script on {node}: {completed.stderr.strip()[-600:]}")
    return sorted(agents)


def write_node_file(kubeconfig: str, relative: str, content: str, *, run: Runner = subprocess.run) -> list[str]:
    """Write ``/etc/containerd/certs.d/<relative>`` on every node through its agent; returns the nodes."""

    if relative.startswith("/") or ".." in relative.split("/"):
        raise EksLaneError(f"node file path must stay inside certs.d: {relative!r}")
    target = shlex.quote(f"{AGENT_CERTS}/{relative}")
    directory = shlex.quote(f"{AGENT_CERTS}/{relative}".rsplit("/", 1)[0])
    script = f"mkdir -p {directory} && cat > {target}.tmp && mv {target}.tmp {target}"
    return run_on_nodes(kubeconfig, script, input_text=content, run=run)


def cluster_identity(kubeconfig: str, *, run: Runner = subprocess.run) -> tuple[str, str]:
    """``(API server URL, kube-system UID)`` the kubeconfig reaches."""

    server = run(
        _kubectl(kubeconfig, "config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    uid = run(
        _kubectl(kubeconfig, "get", "namespace", "kube-system", "-o", "jsonpath={.metadata.uid}"),
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if server.returncode != 0 or uid.returncode != 0 or not server.stdout.strip() or not uid.stdout.strip():
        raise EksLaneError(f"cannot read the lane cluster identity: {(server.stderr + uid.stderr).strip()[-600:]}")
    return server.stdout.strip(), uid.stdout.strip()


def assert_lane_cluster(kubeconfig: str, lane: EksLane, *, run: Runner = subprocess.run) -> None:
    """Refuse a kubeconfig that reaches any cluster but the lane's."""

    server, uid = cluster_identity(kubeconfig, run=run)
    if (server, uid) != (lane.server, lane.cluster_uid):
        raise EksLaneError(
            "the kubeconfig does not reach this lane's cluster "
            f"(server {'matches' if server == lane.server else 'differs'}, "
            f"kube-system UID {'matches' if uid == lane.cluster_uid else 'differs'})"
        )


def require_network_policy(kubeconfig: str, *, run: Runner = subprocess.run) -> None:
    """The lane enforces NetworkPolicy (VPC CNI's policy agent), as every kind lane does with Calico."""

    daemonset = _json(run, _kubectl(kubeconfig, "-n", "kube-system", "get", "daemonset", "aws-node", "-o", "json"))
    for container in daemonset["spec"]["template"]["spec"].get("containers", []):
        # The policy agent container is ``aws-eks-nodeagent`` (older add-ons: ``aws-network-policy-agent``).
        if "--enable-network-policy=true" in (container.get("args") or []):
            return
    raise EksLaneError(
        "the cluster does not enforce NetworkPolicy: enable it in the vpc-cni add-on "
        '(configuration {"enableNetworkPolicy": "true"})'
    )


class EksProvider:
    """The EKS implementation of :class:`benchmarks.lanes.provider.LaneProvider` for one attached cluster."""

    platform = "eks"

    def __init__(self, lane: EksLane, kubeconfig: str, *, run: Runner = subprocess.run):
        self.lane = lane
        self._kubeconfig = kubeconfig
        self._run = run
        self._ecr = Ecr(lane, run=run)

    @classmethod
    def attach(cls, kubeconfig: str, *, registry: str, run: Runner = subprocess.run, **settings: Any) -> EksProvider:
        """Fence the cluster ``kubeconfig`` reaches, require NetworkPolicy and verify the provider's node agent."""

        server, uid = cluster_identity(kubeconfig, run=run)
        lane = EksLane(registry=registry, server=server, cluster_uid=uid, **settings)
        if not lane.virtual:
            require_network_policy(kubeconfig, run=run)
            verify_node_agent(kubeconfig, lane, run=run)
        return cls(lane, kubeconfig, run=run)

    def kubeconfig(self) -> str:
        return self._kubeconfig

    def fence(self) -> None:
        assert_lane_cluster(self._kubeconfig, self.lane, run=self._run)

    def pin(self, local: str) -> str:
        return pin_image(self._ecr, local)

    def holds(self, reference: str) -> bool:
        """Whether a reference :meth:`pin` returned still names a stored image."""
        return _holds(self._ecr, reference)

    def publish(self, local: str, declared: str) -> str:
        return publish_app_image(self._ecr, local, declared)

    def write_node_file(self, relative: str, content: str) -> list[str]:
        return write_node_file(self._kubeconfig, relative, content, run=self._run)

    def run_on_nodes(self, script: str, *, wait: float = 0) -> list[str]:
        if self.lane.virtual:
            raise EksLaneError("a virtual (vcluster) lane has no node agent in its cluster: it cannot run node scripts")
        return run_on_nodes(self._kubeconfig, script, run=self._run, wait=wait)

    def release(self) -> None:
        """Attach-only: the cluster and its node group stay; scaling and deletion belong to the account's tooling."""
