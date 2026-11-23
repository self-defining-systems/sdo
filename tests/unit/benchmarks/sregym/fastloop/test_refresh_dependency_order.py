"""A warm reset restarts the app's clients only when they are out of order with their startup dependency."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from benchmarks.sregym.fastloop import seed_recovery

NAMESPACE = "hotel-reservation"


def _listing(registry_at: str, clients_at: str) -> list[dict]:
    names = ["consul", *seed_recovery.RESTORE_CLIENTS[NAMESPACE]]
    rows: list[dict] = []
    for name in names:
        rows.append(
            {
                "kind": "Deployment",
                "metadata": {"name": name},
                "spec": {"selector": {"matchLabels": {"io/service": name}}},
            }
        )
        rows.append(
            {
                "kind": "Pod",
                "metadata": {
                    "name": f"{name}-0",
                    "labels": {"io/service": name},
                    "creationTimestamp": registry_at if name == "consul" else clients_at,
                },
            }
        )
    return rows


@pytest.mark.parametrize(
    ("registry_at", "restarted"),
    [("2026-11-23T08:15:18Z", False), ("2026-11-23T08:29:00Z", True)],
)
def test_clients_already_registered_after_the_registry_are_not_restarted(monkeypatch, registry_at, restarted):
    """Recorded on EKS (fast-loop row fl3 q2, lane s1): consul unchanged since 08:15, every client running since
    after it, yet the dependencies-only refresh restarted all eight clients and the next baseline check waited its
    whole 60 s sustained-health window. A registry the restore replaced still restarts its clients."""
    from benchmarks.sregym import adapter

    listing = _listing(registry_at, "2026-11-23T08:20:00Z")
    monkeypatch.setattr(adapter, "kubectl", lambda *a, **k: SimpleNamespace(stdout=json.dumps({"items": listing})))
    monkeypatch.setenv("KUBECONFIG", "/fixture/kubeconfig")
    exec_command = Mock(side_effect=RuntimeError("restarted"))
    problem = SimpleNamespace(namespace=NAMESPACE, kubectl=SimpleNamespace(exec_command=exec_command))
    if restarted:
        with pytest.raises(RuntimeError, match="restarted"):
            seed_recovery.refresh_baseline_clients(problem, dependencies_only=True)
    else:
        result = seed_recovery.refresh_baseline_clients(problem, dependencies_only=True)
        assert result["success"] is True and result["deployments"] == []
        exec_command.assert_not_called()
