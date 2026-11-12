"""Benchmark operator configuration: the profile, launch variables, local worker variables and flag translation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops import config
from benchmarks.fuzzops.config import BENCH_PROFILE

if TYPE_CHECKING:
    from pathlib import Path


def test_a_benchmark_lane_resolves_to_the_values_benchmarks_ran_with(operator_binary: Path) -> None:
    # The flags every fuzzops lane passed before the central config (format-1 DEFAULT_CONTROLLER_FLAGS).
    lane = config.resolved([str(operator_binary)], (BENCH_PROFILE,), {"SDO_AGENT_MODEL": "synthetic"})
    assert lane["response"]["repair_policy"] == "recorded-actions"
    assert config.duration_seconds(lane["response"]["timeout"]) == 1800
    assert config.duration_seconds(lane["response"]["verification_timeout"]) == 300
    assert lane["response"]["max_loops"] == 10
    assert lane["learning"]["reflection_guidance"] == "baseline"
    production = config.resolved([str(operator_binary)], (), {"SDO_AGENT_MODEL": "synthetic"})
    assert production["response"]["repair_policy"] == "commit"
    # D2: the benchmark profile holds a condition for 60s before it opens an incident; production holds 5m.
    assert config.duration_seconds(lane["detection"]["self_heal_hold"]) == 60
    assert config.duration_seconds(production["detection"]["self_heal_hold"]) == 300
    assert {**production, "response": lane["response"], "detection": lane["detection"]} == lane


def test_local_workers_take_the_configured_limits(operator_binary: Path) -> None:
    environment = config.worker_environment([str(operator_binary)])
    assert environment["SDO_AGENT_EFFORT"] == "medium"
    assert int(environment["SDO_LIFECYCLE_TIMEOUT_SECONDS"]) > 0
    assert int(environment["SDO_MEMORY_CHECK_TIMEOUT_SECONDS"]) > 0


def test_launch_variables_name_only_what_the_launch_sets() -> None:
    assert config.operator_env() == {}
    assert config.operator_env(
        provider="claude",
        model="m",
        effort="high",
        evidence_host_path="/opt/sdo-evidence",
        evidence_run_id="run-1",
        mode="watch-only",
    ) == {
        "SDO_AGENT_PROVIDER": "claude",
        "SDO_AGENT_MODEL": "m",
        "SDO_AGENT_EFFORT": "high",
        "SDO_EVIDENCE_HOST_PATH": "/opt/sdo-evidence",
        "SDO_EVIDENCE_RUN_ID": "run-1",
        "SDO_MODE": "watch-only",
    }


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"mode": "paused"}, "mode must be one of"),
        ({"evidence_host_path": "relative"}, "absolute node path"),
        ({"evidence_run_id": "run-1"}, "requires an evidence host path"),
    ],
)
def test_launch_variables_refuse_a_malformed_value(values: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        config.operator_env(**values)


def test_config_files_are_passed_in_order_and_overlays_merge_deeply(tmp_path: Path) -> None:
    assert config.config_args((BENCH_PROFILE, tmp_path / "lane.yaml")) == [
        "--config",
        str(BENCH_PROFILE),
        "--config",
        str(tmp_path / "lane.yaml"),
    ]
    assert config.merge(
        {"response": {"max_loops": 3, "permissive_namespaces": ["a"]}},
        {"response": {"permissive_namespaces": ["b"]}, "mode": "frozen"},
    ) == {"response": {"max_loops": 3, "permissive_namespaces": ["b"]}, "mode": "frozen"}


@pytest.mark.parametrize(("value", "seconds"), [("15m0s", 900), ("1h0m0s", 3600), ("90s", 90), ("1.5s", 1.5)])
def test_go_durations_are_read_as_seconds(value: str, seconds: float) -> None:
    assert config.duration_seconds(value) == seconds


def test_a_non_go_duration_is_refused() -> None:
    with pytest.raises(ValueError, match="Go duration"):
        config.duration_seconds("15 minutes")


def test_pre_config_flags_translate_to_their_keys() -> None:
    assert config.translate_flags(
        [
            "--lifecycle-timeout",
            "900s",
            "--responder-env",
            "A=1",
            "--responder-standby=false",
            "--grant-cluster-kind",
            "ClusterRole",
            "--frozen-memory",
            "--external-incidents",
        ]
    ) == {
        "agent": {"lifecycle_timeout": "900s", "extra_env": ["A=1"]},
        "response": {"standby": False},
        "deployment": {"policy": {"granted_cluster_kinds": ["ClusterRole"]}},
        "mode": "external",
    }
