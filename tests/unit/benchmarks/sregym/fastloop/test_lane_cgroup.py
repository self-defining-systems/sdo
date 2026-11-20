"""Lane cgroup isolation: parent-slice resolution, the docker shim's flags, per-node limits, read-back and re-apply."""

from __future__ import annotations

import argparse
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.host_limits import NodeLimits, apply_lane_cpu, apply_node_limits, preferred_cpus
from benchmarks.sregym.fastloop.lane_cgroup import (
    CgroupMismatch,
    CpusetExhausted,
    NodeCgroup,
    describe_mode,
    install_docker_shim,
    kind_run_flags,
    place_cpus,
    place_in_block,
    read_host_cores,
    read_node_cgroup,
    resolve_cgroup_parent,
    shim_environment,
    slice_memory_max,
    split_cpus,
)

if TYPE_CHECKING:
    from pathlib import Path

GIB = 1024**3


# --- configuration ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("setting", "exists", "expected"),
    [
        (None, True, "sdo-eval.slice"),
        ("auto", True, "sdo-eval.slice"),
        ("auto", False, None),
        ("none", True, None),
        ("off", True, None),
        ("my-lanes.slice", True, "my-lanes.slice"),
    ],
)
def test_cgroup_parent_resolution(setting: str | None, exists: bool, expected: str | None) -> None:
    assert resolve_cgroup_parent(setting, exists=lambda _name: exists) == expected


@pytest.mark.parametrize("setting", ["lanes", "../x.slice", "a/b.slice"])
def test_a_parent_that_is_not_a_slice_name_is_rejected(setting: str) -> None:
    with pytest.raises(ValueError, match="slice name"):
        resolve_cgroup_parent(setting, exists=lambda _name: True)


def test_an_explicit_parent_the_host_lacks_is_rejected_not_silently_dropped() -> None:
    with pytest.raises(ValueError, match="no systemd unit"):
        resolve_cgroup_parent("missing.slice", exists=lambda _name: False)


def test_each_mode_is_logged_in_one_line() -> None:
    assert "sdo-eval.slice" in describe_mode("sdo-eval.slice")
    assert "per-container limits only" in describe_mode(None)


def test_lane_cpus_split_across_nodes() -> None:
    assert split_cpus(6, 2) == "3"
    assert split_cpus(6, 3) == "2"
    assert split_cpus(5, 2) == "2.5"
    assert split_cpus(6, 4) == "1.5"
    assert NodeLimits().node_cpus(2) == "3"
    assert NodeLimits(lane_cpus="4").node_cpus(2) == "2"
    assert NodeLimits(cpu="5", lane_cpus="4").node_cpus(2) == "5"  # an explicit per-node value wins


def test_node_limits_read_the_lane_cap_from_the_environment() -> None:
    assert NodeLimits.from_env({"SDO_LANE_CPUS": "8"}).node_cpus(2) == "4"
    assert NodeLimits.from_env({"SREGYM_WORKER_CPU_LIMIT": "2"}).node_cpus(2) == "2"
    with pytest.raises(ValueError, match="lane CPU limit"):
        NodeLimits(lane_cpus="0")


def test_up_resolves_lane_cpus_and_parent_into_the_worker_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("SDO_LANE_CPUS", raising=False)
    monkeypatch.delenv("SDO_LANE_CGROUP_PARENT", raising=False)
    monkeypatch.setattr(
        cli, "resolve_cgroup_parent", lambda setting: setting if setting not in (None, "auto") else None
    )
    args = cli.build_parser().parse_args(["up", "--run-dir", str(tmp_path), "--lane-cgroup-parent", "x.slice"])
    env = cli.up_worker_environment(args, workspace=tmp_path)
    assert (args.cpu_limit, args.lane_cpus) == (None, "6")
    assert "SREGYM_WORKER_CPU_LIMIT" not in env
    assert env["SDO_LANE_CPUS"] == "6"
    assert env["SDO_LANE_CGROUP_PARENT"] == "x.slice"
    off = cli.build_parser().parse_args(["up", "--run-dir", str(tmp_path), "--lane-cpus", "4", "--cpu-limit", "2"])
    env = cli.up_worker_environment(off, workspace=tmp_path)
    assert (env["SDO_LANE_CPUS"], env["SREGYM_WORKER_CPU_LIMIT"], env["SDO_LANE_CGROUP_PARENT"]) == ("4", "2", "")


def test_the_slice_memory_max_is_the_lane_budget_when_none_is_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SDO_HOST_LANE_BUDGET_GIB", raising=False)
    monkeypatch.setattr(cli, "slice_memory_max", lambda parent: 128 * GIB)
    assert cli.up_host_memory_policy(argparse.Namespace(lane_cgroup="sdo-eval.slice")).lane_budget_bytes == 128 * GIB
    assert cli.up_host_memory_policy(argparse.Namespace(lane_cgroup=None)).lane_budget_bytes is None
    configured = argparse.Namespace(lane_cgroup="sdo-eval.slice", host_lane_budget_gib=64)
    assert cli.up_host_memory_policy(configured).lane_budget_bytes == 64 * GIB


def test_slice_memory_max_parses_systemctl_and_treats_infinity_as_unbounded() -> None:
    def run(out: str):
        return lambda argv, **kw: subprocess.CompletedProcess(argv, 0, out, "")

    assert slice_memory_max("sdo-eval.slice", run("137438953472\n")) == 128 * GIB
    assert slice_memory_max("sdo-eval.slice", run("infinity\n")) is None
    assert slice_memory_max("sdo-eval.slice", run("18446744073709551615\n")) is None

    def missing(argv, **kw):
        raise FileNotFoundError("systemctl")

    assert slice_memory_max("sdo-eval.slice", missing) is None


# --- the docker shim (how kind gets --cgroup-parent) ------------------------------------------------------------------


def _fake_docker(directory: Path) -> Path:
    directory.mkdir()
    fake = directory / "docker"
    fake.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
    fake.chmod(0o755)
    return directory


def _through_shim(
    tmp_path: Path, args: list[str], *, parent: str | None = "sdo-eval.slice", prefix: str = "lane-"
) -> list[str]:
    real = _fake_docker(tmp_path / "real")
    shim = install_docker_shim(tmp_path / "shim")
    env = {"PATH": f"{shim}:{real}:/usr/bin:/bin"}
    if parent is not None:
        env.update({"SDO_LANE_CGROUP_PARENT": parent, "SDO_LANE_CGROUP_PREFIX": prefix})
    return subprocess.run(
        [str(shim / "docker"), *args], env=env, capture_output=True, text=True, check=True
    ).stdout.splitlines()


KIND_RUN = [
    "run",
    "--detach",
    "--name",
    "lane-w0-control-plane",
    "--label",
    "io.x-k8s.kind.cluster=lane-w0",
    "kindest/node:v1",
]


def test_shim_adds_the_parent_to_our_kind_nodes(tmp_path: Path) -> None:
    out = _through_shim(tmp_path, KIND_RUN)
    assert out == ["run", *kind_run_flags("sdo-eval.slice"), *KIND_RUN[1:]]


def test_shim_handles_the_equals_label_form_and_create(tmp_path: Path) -> None:
    args = ["create", "--label=io.x-k8s.kind.cluster=lane-w1", "img"]
    assert _through_shim(tmp_path, args) == ["create", "--cgroup-parent=sdo-eval.slice", *args[1:]]


@pytest.mark.parametrize(
    "args",
    [
        ["run", "--label", "io.x-k8s.kind.cluster=someone-elses", "img"],  # another person's kind cluster
        ["run", "--name", "web", "img"],  # not a kind node at all
        ["ps", "--label", "io.x-k8s.kind.cluster=lane-w0"],  # not a create
        ["update", "--cpus", "2", "lane-w0-worker"],
    ],
)
def test_shim_leaves_everything_else_untouched(tmp_path: Path, args: list[str]) -> None:
    assert _through_shim(tmp_path, args) == args


def test_shim_is_inert_without_a_parent(tmp_path: Path) -> None:
    assert _through_shim(tmp_path, KIND_RUN, parent=None) == KIND_RUN


def test_shim_environment_is_empty_without_a_parent_and_puts_the_shim_first_with_one(tmp_path: Path) -> None:
    assert shim_environment(None, "lane-", tmp_path / "shim", {"PATH": "/usr/bin"}) == {}
    env = shim_environment("sdo-eval.slice", "lane-", tmp_path / "shim", {"PATH": "/usr/bin"})
    assert env["PATH"] == f"{tmp_path / 'shim'}:/usr/bin"
    assert (env["SDO_LANE_CGROUP_PARENT"], env["SDO_LANE_CGROUP_PREFIX"]) == ("sdo-eval.slice", "lane-")
    assert (tmp_path / "shim" / "docker").stat().st_mode & 0o111


# --- per-node limits: idempotent update, read back, re-apply ---------------------------------------------------------


class Docker:
    """Records docker/kind calls; ``cpusets`` is every running container's ``--cpuset-cpus`` (updated by calls)."""

    def __init__(self, nodes: str = "lane-control-plane\nlane-worker\n", cpusets: dict[str, str] | None = None) -> None:
        self.nodes = nodes
        self.calls: list[list[str]] = []
        self.cpusets = dict.fromkeys(nodes.split(), "") | (cpusets or {})

    def __call__(self, argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        self.calls.append(argv)
        out = ""
        if argv[0] == "kind":
            out = self.nodes
        elif argv[1:3] == ["ps", "-q"]:
            out = "".join(f"{name}\n" for name in self.cpusets)
        elif argv[1] == "inspect":
            out = "".join(f"/{name}|{self.cpusets[name]}\n" for name in argv[4:])
        elif argv[1] == "update" and "--cpuset-cpus" in argv:
            self.cpusets[argv[-1]] = argv[argv.index("--cpuset-cpus") + 1]
        return subprocess.CompletedProcess(argv, 0, out, "")

    def updates(self) -> list[list[str]]:
        return [c for c in self.calls if c[:2] == ["docker", "update"]]


def test_applying_node_limits_twice_issues_the_same_updates_and_reads_back(monkeypatch: pytest.MonkeyPatch) -> None:
    docker = Docker()
    monkeypatch.setattr(subprocess, "run", docker)
    held = NodeCgroup("/sdo-eval.slice/docker-a.scope", 3.0, 16 * GIB)
    reads: list[str] = []
    limits = NodeLimits(memory="16g", pids=4096)
    for _ in range(2):
        apply_node_limits("lane", limits, read=lambda node: reads.append(node) or held)
    first, second = docker.updates()[:2], docker.updates()[2:]
    assert first == second
    assert first[0] == [
        "docker",
        "update",
        "--cpus",
        "3",
        "--memory",
        "16g",
        "--memory-swap",
        "16g",
        "--pids-limit",
        "4096",
        "--cpuset-cpus",
        "0-1,8",
        "lane-control-plane",
    ]
    assert first[1][-3:] == ["--cpuset-cpus", "2-3,10", "lane-worker"]
    assert reads == ["lane-control-plane", "lane-worker"] * 2


def test_a_limit_the_cgroup_lost_is_reapplied_once(monkeypatch: pytest.MonkeyPatch) -> None:
    docker = Docker("lane-control-plane\n")
    monkeypatch.setattr(subprocess, "run", docker)
    reads = iter([NodeCgroup("/p/docker-a.scope", None, None), NodeCgroup("/p/docker-a.scope", 6.0, 16 * GIB)])
    apply_node_limits("lane", NodeLimits(), read=lambda _node: next(reads))
    assert len(docker.updates()) == 2  # the update, then the re-apply after the read-back disagreed


def test_a_limit_that_will_not_hold_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", Docker("lane-control-plane\n"))
    with pytest.raises(CgroupMismatch, match="expected cpus=6"):
        apply_node_limits("lane", NodeLimits(), read=lambda _node: NodeCgroup("/p/docker-a.scope", 1.0, 16 * GIB))


def test_an_unreadable_cgroup_is_logged_not_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    docker = Docker()
    monkeypatch.setattr(subprocess, "run", docker)
    apply_node_limits("lane", NodeLimits(), read=lambda _node: None)
    assert len(docker.updates()) == 2


def test_claim_cpu_reapply_touches_only_cpus_of_our_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    docker = Docker()
    monkeypatch.setattr(subprocess, "run", docker)
    held = NodeCgroup("/docker-a.scope", 3.0, None)
    apply_lane_cpu("lane", "6", read=lambda _node: held)
    apply_lane_cpu("lane", "6", read=lambda _node: held)
    assert docker.calls[0] == ["kind", "get", "nodes", "--name", "lane"]
    assert (
        docker.updates()
        == [
            ["docker", "update", "--cpus", "3", "--cpuset-cpus", cpuset, node]
            for node, cpuset in (("lane-control-plane", "0-1,8"), ("lane-worker", "2-3,10"))
        ]
        * 2
    )


# --- visible CPUs: runtimes size thread pools and direct buffers from the CPUs they can see ------------------------


CORES = tuple(frozenset({core, core + 8}) for core in range(8))  # SMT siblings i and i+8, as conftest pins


def test_the_cpuset_size_is_the_cap_taken_from_free_whole_cores_first() -> None:
    """Visibility: a node capped at 3 CPUs sees exactly 3, starting with cores nobody else holds."""
    assert place_cpus(3, CORES, []).cpus == frozenset({0, 8, 1})
    assert place_cpus(2, CORES, [frozenset({0, 1})]).cpus == frozenset({2, 10})


def test_a_core_whose_sibling_another_container_holds_is_not_preferred() -> None:
    placement = place_cpus(2, CORES, [frozenset({8})])
    assert (placement.cpus, placement.shared) == (frozenset({1, 9}), frozenset())


def test_without_enough_free_cores_the_least_loaded_cpus_fill_the_set_and_the_overlap_is_named() -> None:
    """Placement is best effort: share cores rather than refuse the lane."""
    placement = place_cpus(6, CORES, [frozenset(range(6))], node="lane-worker")
    assert placement.cpus == frozenset({6, 14, 7, 15, 8, 9})
    assert placement.shared == frozenset({8, 9})


def test_only_a_cap_larger_than_the_host_is_refused() -> None:
    with pytest.raises(CpusetExhausted, match="lane-worker is capped at 17 CPUs but this host has only 16"):
        place_cpus(17, CORES, [], node="lane-worker")


def test_a_capped_node_sees_only_its_cap_on_cores_no_other_container_holds(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 3-CPU node on a big host sees 3 CPUs (opensearch seeing 224 exhausted its direct memory)."""
    docker = Docker("lane-control-plane\nlane-worker\n", {"someone-else": "0-3", "unpinned": ""})
    monkeypatch.setattr(subprocess, "run", docker)
    placements = apply_node_limits("lane", NodeLimits(cpu="2.5", memory="16g"), read=lambda _node: None)
    assert docker.cpusets["lane-control-plane"] == "4-5,12"
    assert docker.cpusets["lane-worker"] == "6-7,14"
    assert docker.cpusets["someone-else"] == "0-3"
    assert all(not placement.shared for placement in placements)


def test_a_lane_that_does_not_fit_in_free_cores_still_starts_and_logs_its_overlap(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    docker = Docker("lane-control-plane\nlane-worker\n", {"other-experiment": "0-5"})
    monkeypatch.setattr(subprocess, "run", docker)
    placements = apply_node_limits("lane", NodeLimits(cpu="3", memory="16g"), read=lambda _node: None)
    assert docker.cpusets["lane-control-plane"] == "6-7,14"
    assert docker.cpusets["lane-worker"] == "8-10"
    assert [placement.record()["shared"] for placement in placements] == ["", "8-10"]
    assert "lane-worker pinned to 8-10 shares cores" in caplog.text


def test_a_node_already_on_cpus_no_one_else_holds_keeps_them(monkeypatch: pytest.MonkeyPatch) -> None:
    docker = Docker("lane-worker\n", {"lane-worker": "4,12", "other": "0-2"})
    monkeypatch.setattr(subprocess, "run", docker)
    apply_lane_cpu("lane", "2", read=lambda _node: None)
    assert docker.updates() == [["docker", "update", "--cpus", "2", "--cpuset-cpus", "4,12", "lane-worker"]]


def test_a_node_pinned_onto_another_containers_cpus_is_moved_to_free_cores(monkeypatch: pytest.MonkeyPatch) -> None:
    docker = Docker("lane-worker\n", {"lane-worker": "4,12", "other": "12"})
    monkeypatch.setattr(subprocess, "run", docker)
    apply_lane_cpu("lane", "2", read=lambda _node: None)
    assert docker.cpusets["lane-worker"] == "0,8"


def test_host_cores_group_smt_siblings_from_sysfs(tmp_path: Path) -> None:
    for cpu, siblings in ((0, "0,2"), (1, "1,3"), (2, "0,2"), (3, "1,3")):
        topology = tmp_path / f"cpu{cpu}" / "topology"
        topology.mkdir(parents=True)
        (topology / "thread_siblings_list").write_text(f"{siblings}\n")
    assert read_host_cores(tmp_path, frozenset(range(4))) == (frozenset({0, 2}), frozenset({1, 3}))
    assert read_host_cores(tmp_path, frozenset({0, 1})) == (frozenset({0}), frozenset({1}))  # affinity limits it


def test_a_node_that_still_shows_every_host_cpu_fails_closed_naming_the_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subprocess, "run", Docker("lane-control-plane\n"))
    with pytest.raises(CgroupMismatch, match="exposes 224 CPUs to its workloads but is capped at 6"):
        apply_node_limits(
            "lane",
            NodeLimits(),
            read=lambda _node: NodeCgroup("/p/docker-a.scope", 6.0, 16 * GIB, visible_cpus=224),
        )


def test_the_node_cgroup_is_read_from_the_containers_own_scope(tmp_path: Path) -> None:
    proc = tmp_path / "proc" / "42"
    proc.mkdir(parents=True)
    scope = "sdo-eval.slice/docker-abc123.scope"
    (proc / "cgroup").write_text(f"0::/{scope}/init.scope\n")
    directory = tmp_path / "cg" / scope
    directory.mkdir(parents=True)
    (directory / "cpu.max").write_text("300000 100000\n")
    (directory / "memory.max").write_text(f"{16 * GIB}\n")
    (directory / "cpuset.cpus.effective").write_text("4-6,10\n")

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 0, "42\n", "")

    state = read_node_cgroup("lane-worker", run=run, root=tmp_path / "cg", proc=tmp_path / "proc")
    assert state == NodeCgroup(f"/{scope}", 3.0, 16 * GIB, visible_cpus=4)
    assert state is not None
    assert state.under("sdo-eval.slice")
    assert not state.under("system.slice")
    nested = NodeCgroup("/sdo.slice/sdo-eval.slice/docker-a.scope", None, None)  # systemd nests a-b.slice under a.slice
    assert nested.under("sdo-eval.slice")
    (directory / "cpu.max").write_text("max 100000\n")
    (directory / "memory.max").write_text("max\n")
    unlimited = read_node_cgroup("lane-worker", run=run, root=tmp_path / "cg", proc=tmp_path / "proc")
    assert unlimited is not None
    assert (unlimited.cpus, unlimited.memory_bytes) == (None, None)


def test_a_container_that_cannot_be_inspected_is_unverifiable(tmp_path: Path) -> None:
    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(1, argv)

    assert read_node_cgroup("gone", run=run, root=tmp_path, proc=tmp_path) is None


# --- preferred CPU block: a host's reserved cores are used first ----------------------------------------------------

BLOCK = frozenset({4, 5, 6, 7, 12, 13, 14, 15})  # cores 4-7 and their SMT siblings


def test_a_preferred_block_takes_free_whole_cores_inside_it_before_any_outside_core() -> None:
    placement = place_cpus(3, CORES, [], preferred=BLOCK)
    assert placement.cpus == frozenset({4, 12, 5})


def test_a_preferred_block_fills_half_used_cores_before_spilling_outside() -> None:
    held = [frozenset({4, 12, 5}), frozenset({6, 14, 7})]  # both 3-CPU nodes leave a sibling free: 13 and 15
    assert place_in_block(2, CORES, held, BLOCK) == frozenset({13, 15})
    assert place_cpus(2, CORES, held, preferred=BLOCK).cpus == frozenset({13, 15})


def test_a_node_the_block_cannot_hold_spills_whole_never_split_across_the_edge() -> None:
    held = [frozenset({4, 12, 5, 13, 6, 14})]
    placement = place_cpus(3, CORES, held, preferred=BLOCK)
    assert placement.cpus == frozenset({0, 8, 1})
    assert place_in_block(3, CORES, held, BLOCK) is None


def test_a_node_outside_the_block_moves_in_when_it_has_room(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_LANE_PREFERRED_CPUS", "4-7,12-15")
    docker = Docker("lane-worker\n", {"lane-worker": "0,8,1", "other": "9"})
    monkeypatch.setattr(subprocess, "run", docker)
    apply_lane_cpu("lane", "3", read=lambda _node: None)
    assert docker.cpusets["lane-worker"] == "4-5,12"


def test_a_node_already_inside_the_block_keeps_its_cpus(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_LANE_PREFERRED_CPUS", "4-7,12-15")
    docker = Docker("lane-worker\n", {"lane-worker": "6-7,14", "other": "4"})
    monkeypatch.setattr(subprocess, "run", docker)
    apply_lane_cpu("lane", "3", read=lambda _node: None)
    assert docker.cpusets["lane-worker"] == "6-7,14"


def test_a_node_outside_a_full_block_stays_where_it_is(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_LANE_PREFERRED_CPUS", "4-7,12-15")
    docker = Docker("lane-worker\n", {"lane-worker": "0,8,1", "other": "4-7,12-15"})
    monkeypatch.setattr(subprocess, "run", docker)
    apply_lane_cpu("lane", "3", read=lambda _node: None)
    assert docker.cpusets["lane-worker"] == "0-1,8"


def test_a_malformed_preferred_block_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_LANE_PREFERRED_CPUS", "fast")
    with pytest.raises(ValueError, match="SDO_LANE_PREFERRED_CPUS"):
        preferred_cpus()
