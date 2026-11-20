"""The fuzzops workload adapter: the built-in workload everywhere, an image-publishing ``kind`` on EKS lanes."""

from __future__ import annotations

import json
import os
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.fuzzops import eks, workload
from benchmarks.lanes.eks import EksLane

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

REGISTRY = "000000000000.dkr.ecr.us-west-2.amazonaws.com"
BUILTIN = ["/fz/bin/fuzzops", "app", "images", "--pack", "/p"]
DIGEST = "sha256:" + "cd" * 32


class Calls:
    def __init__(self) -> None:
        self.argv: list[Sequence[str]] = []
        self.env: list[Mapping[str, str]] = []
        self.shim_seen = False

    def __call__(self, argv: Sequence[str], env: Mapping[str, str]) -> int:
        self.argv.append(argv)
        self.env.append(env)
        first = env["PATH"].split(os.pathsep)[0]
        self.shim_seen = os.access(os.path.join(first, "kind"), os.X_OK)
        return 0


def _env(provider: str = "", action: str = "images") -> dict[str, str]:
    env = {"WORKLOAD_BUILTIN": json.dumps(BUILTIN), "WORKLOAD_ACTION": action, "PATH": "/usr/bin"}
    if provider:
        env["BENCH_LANE_PROVIDER"] = provider
    return env


@pytest.mark.parametrize("provider", ["", "kind", "vcluster"])
def test_a_lane_that_is_not_eks_gets_the_built_in_command_unchanged(provider: str) -> None:
    calls = Calls()
    env = _env(provider)
    assert workload.handle("app-up", env, calls) == 0
    assert calls.argv == [BUILTIN]
    assert calls.env == [env]


def test_on_eks_only_images_and_run_get_a_kind_shim() -> None:
    calls = Calls()
    workload.handle("app-up", _env("eks"), calls)
    assert calls.argv == [BUILTIN]
    assert calls.shim_seen
    other = Calls()
    for verb, action in (("app-up", "up"), ("gate", "check")):
        env = _env("eks", action)
        workload.handle(verb, env, other)
    assert other.env == [_env("eks", "up"), _env("eks", "check")]


def test_a_verb_the_built_in_workload_lacks_is_unsupported(capsys: pytest.CaptureFixture[str]) -> None:
    env = {"PATH": "/usr/bin"}
    assert workload.handle("run", env, Calls()) == workload.UNSUPPORTED
    assert json.loads(capsys.readouterr().out.splitlines()[-1]) == {"unsupported": True}


def test_gitlab_asks_for_a_big_lane_and_other_apps_for_the_default(capsys: pytest.CaptureFixture[str]) -> None:
    assert workload.handle("footprint", {"BENCH_PACK": "/fz/examples/gitlab"}, Calls()) == 0
    assert json.loads(capsys.readouterr().out) == {"cpus": 8, "memory": "30Gi"}
    assert (
        workload.handle("footprint", {"BENCH_PACK": "/fz/examples/hotel-reservation"}, Calls()) == workload.UNSUPPORTED
    )


# ---- the kind shim -------------------------------------------------------------------------------------------


class Fake:
    """aws/docker answers: ``in_mirror`` tags are already in ECR; ``local`` images are on the host."""

    def __init__(self, in_mirror: tuple[str, ...] = (), local: tuple[str, ...] = ()) -> None:
        self.calls: list[list[str]] = []
        self.in_mirror = in_mirror
        self.local = local

    def __call__(self, argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        code, out = 0, ""
        if argv[:3] == ["docker", "image", "inspect"]:
            code = 0 if argv[3] in self.local else 1
        elif argv[:3] == ["aws", "ecr", "describe-images"] and "--image-ids" in argv:
            ids = argv[argv.index("--image-ids") + 1]
            repo = argv[argv.index("--repository-name") + 1]
            if "--query" in argv:
                out = DIGEST
            else:
                code = 0 if f"{repo}:{ids.removeprefix('imageTag=')}" in self.in_mirror else 1
        elif argv[:3] == ["aws", "ecr", "describe-repositories"]:
            out = "{}"
        return subprocess.CompletedProcess(argv, code, out, "")

    def pushed(self) -> list[str]:
        return [c[-1].split("/", 1)[1] for c in self.calls if c[:2] == ["docker", "push"]]


def _lane() -> EksLane:
    return EksLane(registry=REGISTRY, server="https://s", cluster_uid="u")


def test_a_local_build_is_published_under_its_declared_name_every_time() -> None:
    fake = Fake(in_mirror=("sdo-mirror/docker.io/fuzzops/hotel:dev",), local=("fuzzops/hotel:dev",))
    published = eks.publish_images(_lane(), ["fuzzops/hotel:dev"], lambda _: None, fake)
    assert published == ["fuzzops/hotel:dev"]
    assert fake.pushed() == ["sdo-mirror/docker.io/fuzzops/hotel:dev"]


def test_a_pinned_upstream_image_already_in_the_mirror_is_not_pushed_again() -> None:
    fake = Fake(in_mirror=("sdo-mirror/docker.io/library/mongo:4.4.6",), local=("mongo:4.4.6", "memcached:1.6.7"))
    published = eks.publish_images(_lane(), ["mongo:4.4.6", "memcached:1.6.7"], lambda _: None, fake)
    assert published == ["memcached:1.6.7"]


def test_an_image_from_a_registry_the_lane_does_not_mirror_is_left_to_the_nodes() -> None:
    fake = Fake()
    lines: list[str] = []
    assert eks.publish_images(_lane(), ["registry.gitlab.com/x/y:1"], lines.append, fake) == []
    assert fake.pushed() == []
    assert "nodes pull" in lines[0]


def test_an_image_not_on_the_host_is_pulled_before_it_is_published() -> None:
    fake = Fake()
    eks.publish_images(_lane(), ["jaegertracing/all-in-one:1.62.0"], lambda _: None, fake)
    assert ["docker", "pull", "-q", "jaegertracing/all-in-one:1.62.0"] in fake.calls
    assert fake.pushed() == ["sdo-mirror/docker.io/jaegertracing/all-in-one:1.62.0"]


def test_kind_get_nodes_lists_none_and_other_kind_commands_are_refused() -> None:
    assert workload.kind_shim(["get", "nodes", "--name", "c"], {}, lambda _: None) == 0
    lines: list[str] = []
    assert workload.kind_shim(["create", "cluster"], {}, lines.append) == 2
    assert "no kind node" in lines[0]


def test_kind_load_names_only_the_images() -> None:
    assert workload._load_images(["--name", "c1", "--nodes", "a,b", "img:1", "other/img:2"]) == ["img:1", "other/img:2"]
    assert workload._load_images(["--name=c1", "img:1"]) == ["img:1"]


def test_the_shim_runs_this_module_with_the_interpreter(tmp_path: Path) -> None:
    workload.write_shim(tmp_path)
    text = (tmp_path / "kind").read_text()
    assert "-m benchmarks.fuzzops.workload kind" in text
    assert os.access(tmp_path / "kind", os.X_OK)


def test_on_eks_the_run_verb_gets_an_image_publisher_next_to_the_shim() -> None:
    seen: list[Mapping[str, str]] = []

    def call(_argv: Sequence[str], env: Mapping[str, str]) -> int:
        seen.append(env)
        return 0

    env = {**_env("eks", "run"), "WORKLOAD_BUILTIN": json.dumps(["/fz/bin/fuzzops", "run"])}
    workload.handle("run", env, call)
    publish = seen[0]["FUZZOPS_IMAGE_PUBLISH"]
    assert publish.endswith("/kind load docker-image")
    assert seen[0]["PATH"].split(os.pathsep)[0] == publish.removesuffix("/kind load docker-image")
    kind = {**_env("kind", "run"), "WORKLOAD_BUILTIN": env["WORKLOAD_BUILTIN"]}
    workload.handle("run", kind, call)
    assert "FUZZOPS_IMAGE_PUBLISH" not in seen[1]


def test_the_builtin_workload_gets_the_entry_host_under_the_name_fuzzops_reads() -> None:
    seen: list[Mapping[str, str]] = []

    def call(_argv: Sequence[str], env: Mapping[str, str]) -> int:
        seen.append(env)
        return 0

    assert workload.main(["gate"], {**_env("eks", "check"), "BENCH_ENTRY_HOST": "203.0.113.7"}, call) == 0
    assert seen[0]["FUZZOPS_ENTRY_HOST"] == "203.0.113.7"
