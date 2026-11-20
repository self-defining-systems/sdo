"""Host memory admission: MemAvailable >= lane cap + reserve, PSI below a threshold, lane caps within a budget."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import (
    HostMemoryPolicy,
    HostMemoryRefused,
    assert_host_memory_admission,
    assert_lane_budget,
    host_memory_refusal,
    memory_pressure_avg60,
    parse_memory_size,
    running_lane_caps,
)

if TYPE_CHECKING:
    from pathlib import Path

Gi = 1024**3


def _meminfo(tmp_path: Path, available_gib: float) -> Path:
    path = tmp_path / "meminfo"
    path.write_text(f"MemTotal: 999 kB\nMemAvailable: {int(available_gib * Gi // 1024)} kB\n")
    return path


def _psi(tmp_path: Path, avg60: float) -> Path:
    path = tmp_path / "pressure"
    path.write_text(
        f"some avg10=0.00 avg60={avg60:.2f} avg300=1.23 total=111\nfull avg10=0.00 avg60=0.00 avg300=0.45 total=1\n"
    )
    return path


def test_pressure_reads_the_some_line_avg60(tmp_path: Path) -> None:
    assert memory_pressure_avg60(_psi(tmp_path, 7.5)) == 7.5


@pytest.mark.parametrize("text", ["", "full avg10=0 avg60=1 avg300=0 total=0\n", "some avg10=0\n"])
def test_pressure_without_a_some_avg60_fails_closed(tmp_path: Path, text: str) -> None:
    path = tmp_path / "pressure"
    path.write_text(text)
    with pytest.raises(ValueError, match="memory pressure"):
        memory_pressure_avg60(path)
    with pytest.raises(ValueError, match="memory pressure"):
        memory_pressure_avg60(tmp_path / "missing")


def test_defaults_are_a_32gib_reserve_and_pressure_10() -> None:
    policy = HostMemoryPolicy()
    assert (policy.reserve_bytes, policy.max_pressure_avg60, policy.lane_budget_bytes) == (32 * Gi, 10.0, None)


@pytest.mark.parametrize(
    "kwargs",
    [{"reserve_bytes": -1}, {"max_pressure_avg60": 0.0}, {"max_pressure_avg60": 101.0}, {"lane_budget_bytes": 0}],
)
def test_policy_rejects_nonsense(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="host memory"):
        HostMemoryPolicy(**kwargs)
    with pytest.raises(TypeError, match="host memory"):
        HostMemoryPolicy(reserve_bytes="32g")  # type: ignore[arg-type]


def test_policy_from_env_is_configuration() -> None:
    policy = HostMemoryPolicy.from_env(
        {
            "SDO_HOST_MEMORY_RESERVE_GIB": "48",
            "SDO_HOST_MEMORY_PSI_MAX": "5",
            "SDO_HOST_LANE_BUDGET_GIB": "128",
        }
    )
    assert (policy.reserve_bytes, policy.max_pressure_avg60, policy.lane_budget_bytes) == (48 * Gi, 5.0, 128 * Gi)
    assert HostMemoryPolicy.from_env({}) == HostMemoryPolicy()


def test_available_must_cover_the_cap_plus_the_reserve() -> None:
    policy = HostMemoryPolicy()
    assert host_memory_refusal(16 * Gi, policy, available_bytes=48 * Gi, pressure_avg60=0.0) is None
    reason = host_memory_refusal(16 * Gi, policy, available_bytes=48 * Gi - 1, pressure_avg60=0.0)
    assert reason is not None
    assert "MemAvailable" in reason
    assert str(48 * Gi - 1) in reason
    assert str(48 * Gi) in reason


def test_pressure_at_or_above_the_threshold_refuses() -> None:
    policy = HostMemoryPolicy()
    assert host_memory_refusal(Gi, policy, available_bytes=200 * Gi, pressure_avg60=9.99) is None
    reason = host_memory_refusal(Gi, policy, available_bytes=200 * Gi, pressure_avg60=10.0)
    assert reason is not None
    assert "pressure" in reason
    assert "avg60=10.00" in reason


def test_assert_reads_proc_files_and_raises_a_refusal_naming_the_reason(tmp_path: Path) -> None:
    policy = HostMemoryPolicy()
    passed = assert_host_memory_admission(
        16 * Gi, policy, meminfo=_meminfo(tmp_path, 100), pressure=_psi(tmp_path, 1.0)
    )
    assert passed["passed"] is True
    assert passed["host_available_bytes"] == 100 * Gi
    with pytest.raises(HostMemoryRefused, match="MemAvailable"):
        assert_host_memory_admission(16 * Gi, policy, meminfo=_meminfo(tmp_path, 40), pressure=_psi(tmp_path, 1.0))
    with pytest.raises(HostMemoryRefused, match="pressure"):
        assert_host_memory_admission(16 * Gi, policy, meminfo=_meminfo(tmp_path, 100), pressure=_psi(tmp_path, 25.0))


def test_unreadable_measurements_refuse_instead_of_admitting(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="MemAvailable|memory pressure"):
        assert_host_memory_admission(Gi, HostMemoryPolicy(), meminfo=tmp_path / "missing", pressure=_psi(tmp_path, 0.0))
    with pytest.raises(ValueError, match="memory pressure"):
        assert_host_memory_admission(
            Gi, HostMemoryPolicy(), meminfo=_meminfo(tmp_path, 100), pressure=tmp_path / "missing"
        )


def test_memory_sizes_use_docker_units() -> None:
    assert parse_memory_size("16g") == 16 * Gi
    assert parse_memory_size("512M") == 512 * 1024**2
    assert parse_memory_size("1024") == 1024
    with pytest.raises(ValueError, match="memory size"):
        parse_memory_size("lots")


def test_lane_budget_is_the_sum_of_running_caps_plus_the_new_lane() -> None:
    running = {"c5-a": 32 * Gi, "c5-b": 32 * Gi}
    policy = HostMemoryPolicy(lane_budget_bytes=96 * Gi)
    assert assert_lane_budget("c5-c", 32 * Gi, running, policy)["lane_caps_after_bytes"] == 96 * Gi
    with pytest.raises(HostMemoryRefused, match="budget"):
        assert_lane_budget("c5-c", 32 * Gi + 1, running, policy)


def test_a_lane_that_is_already_running_is_not_counted_twice() -> None:
    running = {"c5-a": 32 * Gi, "c5-b": 32 * Gi}
    assert_lane_budget("c5-a", 32 * Gi, running, HostMemoryPolicy(lane_budget_bytes=64 * Gi))


def test_no_budget_configured_means_no_budget_check() -> None:
    assert assert_lane_budget("c5-c", 10_000 * Gi, {"c5-a": 1}, HostMemoryPolicy())["budget_bytes"] is None


def test_running_lane_caps_sums_node_caps_of_our_prefix_only() -> None:
    rows = "\n".join(
        [
            "c5-evl-hotel10 /c5-evl-hotel10-control-plane 17179869184",
            "c5-evl-hotel10 /c5-evl-hotel10-worker 17179869184",
            "irdet-ob /irdet-ob-worker 0",
            "c5-evl-hotel40 /c5-evl-hotel40-worker 0",
            " /fuzzops-registry 0",
        ]
    )
    seen: list[list[str]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        seen.append(argv)
        out = "ID1\nID2\n" if argv[1] == "ps" else rows
        return subprocess.CompletedProcess(argv, 0, out, "")

    caps = running_lane_caps("c5-", default_node_bytes=16 * Gi, run=run)
    # An uncapped node of ours counts as the default node cap, never as zero; foreign clusters are ignored.
    assert caps == {"c5-evl-hotel10": 32 * Gi, "c5-evl-hotel40": 16 * Gi}
    assert all(argv[1] in {"ps", "inspect"} for argv in seen), "read-only docker queries only"


def _docker(rows: str, inspect_code: int, inspect_err: str):
    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        code, out, err = (0, "ID1\nGONE\n", "") if argv[1] == "ps" else (inspect_code, rows, inspect_err)
        if kwargs.get("check") and code:
            raise subprocess.CalledProcessError(code, argv, out, err)
        return subprocess.CompletedProcess(argv, code, out, err)

    return run


def test_a_container_that_exits_between_ps_and_inspect_is_skipped() -> None:
    # Other users' containers come and go: docker inspect exits 1 for the vanished one and prints the others.
    run = _docker("c5-a /c5-a-worker 1073741824", 1, "Error: No such object: GONE\n")
    assert running_lane_caps("c5-", default_node_bytes=16 * Gi, run=run) == {"c5-a": Gi}


def test_any_other_inspect_failure_still_fails() -> None:
    run = _docker("", 1, "Cannot connect to the Docker daemon\n")
    with pytest.raises(subprocess.CalledProcessError):
        running_lane_caps("c5-", default_node_bytes=16 * Gi, run=run)
