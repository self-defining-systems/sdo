"""Opt-in lane fences reject foreign targets and Secret payload projections."""

from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "command",
    [
        ["kubectl", "--kubeconfig", "/foreign", "get", "pods"],
        ["docker", "exec", "luna-pilot-52-worker", "true"],
        ["kubectl", "get", "secrets", "-o", "json"],
        ["kubectl", "get", "pods,secrets", "-o", "yaml"],
        ["kubectl", "get", "--raw", "/api/v1/namespaces/default/secrets"],
        ["kubectl", "--context", "foreign", "get", "pods"],
    ],
)
def test_foreign_lane_or_secret_payload_is_rejected(command):
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard

    with pytest.raises(ValueError, match="lane guard"):
        LaneGuard("luna-pilot-50", Path("/owned"), "le-hu").command(command)


def test_secret_metadata_only_projection_is_explicit():
    from benchmarks.sregym.fastloop.lane_guard import LaneGuard

    guard = LaneGuard("luna-pilot-50", Path("/owned"), "le-hu")
    assert guard.command(["kubectl", "get", "secret", "credentials"])[-2:] == ["-o", "name"]
    assert guard.command(["kubectl", "get", "secrets", "-o", "name"])[-1] == "name"


def test_guarded_module_has_its_own_main_namespace_for_runtime_types(tmp_path):
    import json
    import os
    import subprocess
    import sys

    record = tmp_path / "launch.json"
    record.write_text(
        json.dumps(
            {
                "launcher": {
                    "pid": 99999999,
                    "start_ticks": 1,
                    "boot_id": "synthetic",
                    "uid": os.getuid(),
                    "name": "fixture",
                    "process_group": 99999999,
                },
                "started_at": "2020-01-01T00:00:00Z",
                "state_path": str(tmp_path / "state.json"),
                "command": [],
                "stdout_path": str(tmp_path / "out"),
                "stderr_path": str(tmp_path / "err"),
            }
        )
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "benchmarks.sregym.fastloop.lane_guard",
            "--cluster",
            "synthetic-50",
            "--kubeconfig",
            str(tmp_path / "config"),
            "--lease",
            "fixture",
            "--coord",
            "/bin/true",
            "--",
            "benchmarks.sregym.adapter.detached",
            "status",
            "--record",
            str(record),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1, result.stderr  # The synthetic launcher is dead.
    assert "Traceback" not in result.stderr
    assert "not fully defined" not in result.stderr
