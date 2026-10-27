"""Disposable eval workloads retire by ownership and actual absence."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest


def test_retirement_shares_deadline_and_requests_zero_grace(monkeypatch):
    from benchmarks.sregym.adapter.budgets import Deadline
    from benchmarks.sregym.fastloop import transient_cleanup as cleanup

    now = [0.0]
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        if args[0] == "get":
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {
                                    "namespace": "social-network",
                                    "name": "client-a",
                                    "uid": "a",
                                    "labels": {"service": "compose-post-service"},
                                    "ownerReferences": [{"kind": "ReplicaSet", "name": "compose-post-service-123"}],
                                }
                            }
                        ]
                    }
                ),
                "",
            )
        if args[0] == "delete":
            now[0] += 30
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(cleanup, "kubectl", command)
    cleanup.retire_pods(
        Path("config"),
        "social-network",
        "service=compose-post-service",
        owner_kind="ReplicaSet",
        deadline=Deadline(120, lambda: now[0]),
    )
    assert any("--grace-period=0" in args and "--wait=false" in args for args in calls)
    assert calls[-1][0] == "wait"
    assert "--timeout=90s" in calls[-1]


@pytest.mark.parametrize(("namespace", "owner"), [("foreign", "ReplicaSet"), ("social-network", "Job")])
def test_retirement_refuses_foreign_identity_before_any_write(monkeypatch, namespace, owner):
    from benchmarks.sregym.adapter.budgets import Deadline
    from benchmarks.sregym.fastloop import transient_cleanup as cleanup

    def command(args, **kwargs):
        assert args[0] == "get", "unknown identity must never be mutated"
        return subprocess.CompletedProcess(
            args,
            0,
            json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "namespace": namespace,
                                "name": "client",
                                "uid": "a",
                                "labels": {"service": "compose-post-service"},
                                "ownerReferences": [{"kind": owner, "name": "client-123"}],
                            }
                        }
                    ]
                }
            ),
            "",
        )

    monkeypatch.setattr(cleanup, "kubectl", command)
    with pytest.raises(ValueError, match="ownership"):
        cleanup.retire_pods(
            Path("config"),
            "social-network",
            "service=compose-post-service",
            owner_kind="ReplicaSet",
            deadline=Deadline(120, lambda: 0),
        )
