from __future__ import annotations

from benchmarks.sregym.adapter.host_memory import HostMemoryPolicy, host_env

Gi = 1024**3


def test_host_guard_file_supplies_the_host_variables_and_the_environment_wins(tmp_path) -> None:
    file = tmp_path / "host.env"
    file.write_text(
        "OTHER=1\nSDO_HOST_LANE_PREFIX=c4-\nexport SDO_HOST_LANE_BUDGET_GIB=125\nSDO_HOST_MEMORY_PSI_MAX=7\n"
    )
    env = host_env({"SDO_HOST_ENV_FILE": str(file), "SDO_HOST_MEMORY_PSI_MAX": "5"})
    assert env["SDO_HOST_LANE_PREFIX"] == "c4-"
    assert "OTHER" not in env
    policy = HostMemoryPolicy.from_env(env)
    assert (policy.lane_budget_bytes, policy.max_pressure_avg60) == (125 * Gi, 5.0)


def test_a_missing_host_guard_file_changes_nothing(tmp_path) -> None:
    assert host_env({"SDO_HOST_ENV_FILE": str(tmp_path / "absent")}) == {"SDO_HOST_ENV_FILE": str(tmp_path / "absent")}
