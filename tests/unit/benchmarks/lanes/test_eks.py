"""EKS lane provider: image names, the node mirror, the cluster fence and node writes, with a fake kubectl/aws."""

from __future__ import annotations

import json
import subprocess
from typing import Any

import pytest

from benchmarks.lanes import eks

REGISTRY = "000000000000.dkr.ecr.us-west-2.amazonaws.com"


def lane(**overrides: Any) -> eks.EksLane:
    values: dict[str, Any] = {"registry": REGISTRY, "server": "https://api.example", "cluster_uid": "uid-1"}
    return eks.EksLane(**{**values, **overrides})


class FakeRun:
    """Answers ``kubectl``/``aws``/``docker`` argv by the first matching rule and records every call."""

    def __init__(self, rules: list[tuple[tuple[str, ...], int, str, str]]):
        self.rules = rules
        self.calls: list[list[str]] = []
        self.inputs: list[str | None] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        self.inputs.append(kwargs.get("input"))
        for needles, code, out, err in self.rules:
            if all(needle in argv for needle in needles):
                return subprocess.CompletedProcess(argv, code, out, err)
        raise AssertionError(f"unexpected command {argv}")


def test_lane_requires_an_ecr_registry_and_known_upstreams() -> None:
    with pytest.raises(ValueError, match="ECR host"):
        lane(registry="registry.example:5000")
    with pytest.raises(ValueError, match="upstreams"):
        lane(upstreams=("example.org",))
    assert lane().region == "us-west-2"


@pytest.mark.parametrize(
    ("image", "repository", "tag"),
    [
        ("mongo:4.4.6", "sdo-mirror/docker.io/library/mongo", "4.4.6"),
        ("yinfangchen/hotelreservation:latest", "sdo-mirror/docker.io/yinfangchen/hotelreservation", "latest"),
        ("memcached", "sdo-mirror/docker.io/library/memcached", "latest"),
        ("docker.io/library/busybox:1.36", "sdo-mirror/docker.io/library/busybox", "1.36"),
    ],
)
def test_declared_names_map_to_the_mirror_path_containerd_requests(image: str, repository: str, tag: str) -> None:
    assert eks.mirror_repository(lane(), image) == (repository, tag)


def test_mirror_refuses_digests_and_unmirrored_upstreams() -> None:
    with pytest.raises(eks.EksLaneError, match="digest"):
        eks.mirror_repository(lane(), "mongo@sha256:" + "0" * 64)
    with pytest.raises(eks.EksLaneError, match="mirrors"):
        eks.mirror_repository(lane(), "quay.io/prometheus/prometheus:v2.47.2")


def test_pinned_images_keep_only_their_name_under_the_image_prefix() -> None:
    assert eks.image_repository(lane(), "sdo-controller:eks-1") == ("sdo/sdo-controller", "eks-1")


def test_node_agent_writes_one_mirror_per_upstream_and_is_unready_when_stale() -> None:
    manifest = eks.node_agent_manifest(lane(upstreams=("docker.io", "quay.io")))
    pod = manifest["spec"]["template"]["spec"]
    script = pod["containers"][0]["command"][2]
    assert pod["hostNetwork"] is True
    assert pod["volumes"][0]["hostPath"]["path"] == "/etc/containerd/certs.d"
    for upstream, server in (("docker.io", "https://registry-1.docker.io"), ("quay.io", "https://quay.io")):
        assert f"https://{REGISTRY}/v2/sdo-mirror/{upstream}" in script
        assert f'server = "{server}"' in script
        assert f"mv /certs/{upstream}/hosts.toml.tmp /certs/{upstream}/hosts.toml" in script
    assert "override_path = true" in script
    assert "aws ecr get-login-password --region" in script
    probe = " ".join(pod["containers"][0]["readinessProbe"]["exec"]["command"])
    assert f"-mmin -{eks.STALE_MINUTES}" in probe


def _ready(name: str, node: str = "", deleting: bool = False) -> dict[str, Any]:
    metadata: dict[str, Any] = {"name": name}
    if deleting:
        metadata["deletionTimestamp"] = "now"
    return {
        "metadata": metadata,
        "spec": {"nodeName": node},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }


def test_every_ready_node_needs_a_ready_agent() -> None:
    pods = {"items": [_ready("a", "n1"), _ready("b", "n2", deleting=True)]}
    assert eks.agent_pods(pods, ["n1"]) == {"n1": "a"}
    with pytest.raises(eks.EksLaneError, match=r"not ready on \['n2'\]"):
        eks.agent_pods(pods, ["n1", "n2"])


def test_node_files_are_written_atomically_on_every_node_through_its_agent() -> None:
    nodes = {"items": [_ready("n1"), _ready("n2")]}
    pods = {"items": [_ready("a1", "n1"), _ready("a2", "n2")]}
    run = FakeRun(
        [
            (("get", "nodes"), 0, json.dumps(nodes), ""),
            (("get", "pods"), 0, json.dumps(pods), ""),
            (("exec",), 0, "", ""),
        ]
    )
    assert eks.write_node_file("/k", "reg.svc:5000/hosts.toml", "body", run=run) == ["n1", "n2"]
    execs = [(call, text) for call, text in zip(run.calls, run.inputs, strict=True) if "exec" in call]
    assert [call[call.index("exec") + 2] for call, _ in execs] == ["a1", "a2"]
    assert all(text == "body" for _, text in execs)
    assert "mv /certs/reg.svc:5000/hosts.toml.tmp /certs/reg.svc:5000/hosts.toml" in execs[0][0][-1]
    with pytest.raises(eks.EksLaneError, match="inside certs.d"):
        eks.write_node_file("/k", "../etc/passwd", "x", run=run)


def test_the_fence_is_the_server_and_the_kube_system_uid_not_node_names() -> None:
    def run_for(server: str, uid: str) -> FakeRun:
        return FakeRun([(("config", "view"), 0, server, ""), (("namespace", "kube-system"), 0, uid, "")])

    eks.assert_lane_cluster("/k", lane(), run=run_for("https://api.example", "uid-1"))
    with pytest.raises(eks.EksLaneError, match="kube-system UID differs"):
        eks.assert_lane_cluster("/k", lane(), run=run_for("https://api.example", "uid-2"))
    with pytest.raises(eks.EksLaneError, match="server differs"):
        eks.assert_lane_cluster("/k", lane(), run=run_for("https://other", "uid-1"))


def test_network_policy_must_be_enforced_by_the_vpc_cni() -> None:
    def daemonset(args: list[str]) -> str:
        return json.dumps(
            {"spec": {"template": {"spec": {"containers": [{"name": "aws-eks-nodeagent", "args": args}]}}}}
        )

    on = FakeRun([(("daemonset", "aws-node"), 0, daemonset(["--enable-network-policy=true"]), "")])
    eks.require_network_policy("/k", run=on)
    off = FakeRun([(("daemonset", "aws-node"), 0, daemonset(["--enable-network-policy=false"]), "")])
    with pytest.raises(eks.EksLaneError, match="enableNetworkPolicy"):
        eks.require_network_policy("/k", run=off)


def test_push_creates_the_tagged_repository_once_and_returns_the_stored_digest() -> None:
    digest = "sha256:" + "a" * 64
    run = FakeRun(
        [
            (("get-login-password",), 0, "secret\n", ""),
            (("docker", "login"), 0, "", ""),
            (("describe-repositories",), 254, "", "RepositoryNotFoundException"),
            (("create-repository",), 0, "{}", ""),
            (("docker", "tag"), 0, "", ""),
            (("docker", "push"), 0, "", ""),
            (("describe-images",), 0, digest + "\n", ""),
        ]
    )
    ecr = eks.Ecr(lane(repository_tags={"Project": "p"}), run=run, clock=lambda: 0.0)
    assert eks.pin_image(ecr, "sdo-controller:t") == f"{REGISTRY}/sdo/sdo-controller@{digest}"
    create = next(call for call in run.calls if "create-repository" in call)
    assert create[create.index("--tags") + 1] == "Key=Project,Value=p"
    assert (
        run.inputs[run.calls.index(["docker", "login", "--username", "AWS", "--password-stdin", REGISTRY])] == "secret"
    )
    assert eks.publish_app_image(ecr, "built:abc", "yinfangchen/hotelreservation:latest") == digest
    assert sum("get-login-password" in call for call in run.calls) == 1


def test_holds_checks_the_pinned_digest_in_the_lanes_registry_only() -> None:
    digest = "sha256:" + "b" * 64
    run = FakeRun([(("describe-images",), 0, "{}", "")])
    provider = eks.EksProvider(lane(), "/k", run=run)
    assert provider.holds(f"{REGISTRY}/sdo/sdo-controller@{digest}")
    assert not provider.holds(f"other.example/sdo/sdo-controller@{digest}")
    assert not provider.holds("sdo-controller:tag")
    missing = eks.EksProvider(lane(), "/k", run=FakeRun([(("describe-images",), 254, "", "ImageNotFoundException")]))
    assert not missing.holds(f"{REGISTRY}/sdo/sdo-controller@{digest}")
