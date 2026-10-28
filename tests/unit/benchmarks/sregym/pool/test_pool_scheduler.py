"""Scheduler contract against fake hosts that act out prepare and pair processes."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.records import IncidentRecord, Injected, NotInjected
from benchmarks.sregym.pool.registry import LaneRegistry
from benchmarks.sregym.pool.scheduler import PoolConfig, Scheduler, SchedulerBusy

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

NOW = datetime(2026, 10, 28, tzinfo=timezone.utc)


def row(spec: dict[str, object], index: int, **changes: object) -> str:
    values: dict[str, object] = {
        "run_id": str(spec["run_id"]),
        "index": index,
        "agent": "sdo",
        "problem_id": str(spec["problem"]),
        "model": "scripted",
        "incident_id": f"{spec['run_id']}-{index}",
        "injection": Injected(started_at=NOW, finished_at=NOW),
    }
    values.update(changes)
    return IncidentRecord.model_validate(values).model_dump_json()


NEVER_INJECTED = {"injection": NotInjected(stage="pre_injection"), "infra_error": "never_injected_retry"}

# problem -> per-attempt list of row overrides (one dict per encounter); default is two graded rows.
Script = dict[str, list[list[dict[str, object]]]]


@dataclass
class FakeHost:
    name: str
    cores: float
    script: Script
    load: float = 1.0
    files: dict[str, str] = field(default_factory=dict)
    running: dict[str, list[str]] = field(default_factory=dict)
    spawned: list[tuple[str, list[str]]] = field(default_factory=list)
    max_preparing_seen: int = 0
    attempts: dict[str, int] = field(default_factory=dict)

    def read_text(self, path: Path) -> str | None:
        return self.files.get(str(path))

    def write_text(self, path: Path, text: str) -> None:
        self.files[str(path)] = text

    def spawn(self, unit: str, argv: Sequence[str], log: Path) -> None:
        assert unit not in self.running
        self.running[unit] = list(argv)
        self.spawned.append((unit, list(argv)))
        preparing = sum(1 for command in self.running.values() if "prepare" in command)
        self.max_preparing_seen = max(self.max_preparing_seen, preparing)

    def active(self, unit: str) -> bool:
        argv = self.running.pop(unit, None)
        if argv is None:
            return False
        spec = json.loads(self.files[argv[argv.index("--spec") + 1]])
        if "prepare" in argv:
            self.files[spec["ready"]] = json.dumps(
                {
                    "app": spec["app"],
                    "source_digest": "sha256:" + spec["app"],
                    "build": spec["build"],
                    "kubeconfig": spec["run_dir"] + "/kubeconfig",
                }
            )
        else:
            problem = spec["problem"]
            attempt = self.attempts.get(problem, 0)
            self.attempts[problem] = attempt + 1
            plan = self.script.get(problem, [])
            overrides = plan[attempt] if attempt < len(plan) else [{}] * len(spec["encounters"])
            rows = [row(spec, index, **changes) for index, changes in enumerate(overrides)]
            self.files[spec["output"] + "/incidents.jsonl"] = "".join(line + "\n" for line in rows)
        return True  # still running at this observation; finished at the next one

    def load1(self) -> float:
        return self.load

    def lease_cores(self) -> float:
        return self.cores

    def gate(self) -> bool:
        return self.load < 40


def config(tmp_path: Path, problems: list[tuple[str, str]], **changes: object) -> PoolConfig:
    stream = tmp_path / "stream.json"
    stream.write_text(json.dumps([{"id": problem, "app": app} for problem, app in problems]))
    endpoints = tmp_path / "memory-endpoints.json"
    endpoints.write_text(
        json.dumps({app.replace("_", "-"): {"repo": f"o/{app}-sdo", "branch": "live/t"} for _, app in problems})
    )
    values: dict[str, object] = {
        "run_id": "pool-test",
        "registry": tmp_path / "lanes.json",
        "state_dir": tmp_path / "state",
        "stream": stream,
        "memory_endpoints": endpoints,
        "build": "h1",
        "hosts": {
            "chelan5": {"prefix": "c5", "lease": "pool-c5", "work_root": "/w5", "code_root": "/c", "python": "/p"},
            "chelan4": {
                "prefix": "c4",
                "address": "192.168.1.107",
                "lease": "pool-c4",
                "work_root": "/w4",
                "code_root": "/c",
                "python": "/p",
            },
        },
        "status_seconds": 0,
        "prepare_steps": [["true"]],
    }
    values.update(changes)
    return PoolConfig.model_validate(values)


def scheduler(
    tmp_path: Path, problems: list[tuple[str, str]], script: Script | None = None, **changes: object
) -> tuple[Scheduler, dict[str, FakeHost], list[str]]:
    hosts = {name: FakeHost(name, cores=8, script=script or {}) for name in ("chelan5", "chelan4")}
    posts: list[str] = []
    cfg = config(tmp_path, problems, **changes)
    return Scheduler(cfg, hosts, LaneRegistry(cfg.registry), posts.append), hosts, posts


FOUR = [("p0", "hotel_reservation"), ("p1", "social_network"), ("p2", "hotel_reservation"), ("p3", "social_network")]


def run(pool: Scheduler) -> dict[str, int]:
    with pool:
        return pool.run(sleep=lambda _: None, max_ticks=50)


def test_four_pairs_across_two_lanes_on_each_host(tmp_path: Path) -> None:
    pool, hosts, posts = scheduler(tmp_path, FOUR)
    summary = run(pool)
    assert summary["done"] == 4
    assert summary["held"] == 0
    lanes = LaneRegistry(tmp_path / "lanes.json").read().lanes
    assert sorted(lane.host for lane in lanes) == ["chelan4", "chelan4", "chelan5", "chelan5"]
    assert all(lane.state == "ready" for lane in lanes)
    assert all(host.max_preparing_seen <= 2 for host in hosts.values())
    pairs = [argv for host in hosts.values() for _, argv in host.spawned if "pair" in argv]
    assert len(pairs) == 4
    specs = [json.loads(hosts[h].files[a[a.index("--spec") + 1]]) for h in hosts for _, a in hosts[h].spawned]
    assert all(spec["memory"]["branch"] == "live/t" for spec in specs)
    assert all(spec["encounters"] == ["first", "repeat"] for spec in specs if "problem" in spec and "output" in spec)
    assert any(post.startswith("STATUS ") for post in posts)


def test_never_injected_first_is_retried_on_a_fresh_attempt(tmp_path: Path) -> None:
    pool, _, posts = scheduler(tmp_path, FOUR[:1], {"p0": [[NEVER_INJECTED], [{}, {}]]})
    summary = run(pool)
    assert summary["done"] == 1
    journal = json.loads((tmp_path / "state" / "pairs.json").read_text())
    assert journal["pairs"][0]["attempt"] == 1
    assert journal["pairs"][0]["history"][0]["disposition"] == "never_injected_retry"
    assert any("RETRY" in post for post in posts)


def test_never_injected_repeat_retries_only_the_repeat(tmp_path: Path) -> None:
    pool, hosts, _ = scheduler(tmp_path, FOUR[:1], {"p0": [[{}, NEVER_INJECTED], [{}]]})
    assert run(pool)["done"] == 1
    specs = [json.loads(h.files[a[a.index("--spec") + 1]]) for h in hosts.values() for _, a in h.spawned]
    assert [spec["encounters"] for spec in specs if "output" in spec] == [["first", "repeat"], ["repeat"]]


def test_unknown_failure_holds_lane_and_pair_and_others_continue(tmp_path: Path) -> None:
    pool, _, posts = scheduler(tmp_path, FOUR, {"p0": [[{"infra_error": "something new"}]]})
    summary = run(pool)
    assert summary == {**summary, "done": 3, "held": 1}
    held = [lane for lane in LaneRegistry(tmp_path / "lanes.json").read().lanes if lane.state == "held"]
    assert len(held) == 1
    assert "something new" in held[0].reason
    assert any(post.startswith("CASE ") for post in posts)


def test_known_class_continues_without_the_repeat(tmp_path: Path) -> None:
    pool, _, posts = scheduler(
        tmp_path, FOUR[:1], {"p0": [[{"infra_error": "injection_invalid"}]]}, known_continue=["injection_invalid"]
    )
    summary = run(pool)
    assert summary["done"] == 1
    journal = json.loads((tmp_path / "state" / "pairs.json").read_text())
    assert journal["pairs"][0]["history"][-1]["disposition"] == "known:injection_invalid"
    assert any("KNOWN CONTINUE" in post for post in posts)


def test_graded_oracle_failure_is_a_result_not_a_hold(tmp_path: Path) -> None:
    pool, _, _ = scheduler(tmp_path, FOUR[:1], {"p0": [[{"error": "oracle reported failure"}, {}]]})
    assert run(pool)["done"] == 1


def test_one_owner_per_state(tmp_path: Path) -> None:
    first, _, _ = scheduler(tmp_path, FOUR)
    second, _, _ = scheduler(tmp_path, FOUR)
    with first, pytest.raises(SchedulerBusy):
        second.__enter__()


def test_restart_resumes_the_journal_and_registry(tmp_path: Path) -> None:
    pool, hosts, _ = scheduler(tmp_path, FOUR)
    with pool:
        pool.run(sleep=lambda _: None, max_ticks=3)
    again = Scheduler(pool.config, hosts, LaneRegistry(tmp_path / "lanes.json"), lambda _: None)
    assert run(again)["done"] == 4
    assert sum(1 for host in hosts.values() for _, argv in host.spawned if "pair" in argv) == 4


def test_refuses_to_start_without_memory_for_every_stream_app(tmp_path: Path) -> None:
    endpoints = tmp_path / "partial.json"
    endpoints.write_text(json.dumps({"hotel-reservation": {"repo": "o/h-sdo", "branch": "live/t"}}))
    with pytest.raises(ValueError, match="social_network"):
        config(tmp_path, FOUR, memory_endpoints=endpoints)


def test_status_line_counts(tmp_path: Path) -> None:
    pool, _, posts = scheduler(tmp_path, FOUR)
    run(pool)
    final = [post for post in posts if post.startswith("STATUS ")][-1]
    assert "pairs done=4/4" in final
    assert "held=0" in final
    assert "chelan4" in final
