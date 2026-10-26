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
