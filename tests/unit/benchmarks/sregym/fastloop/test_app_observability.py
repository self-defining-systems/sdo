"""A row starts with the app's in-memory trace stores empty, before SDO's install."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop import app_observability, cli

#: Bound at import: the package's autouse fixture replaces the attribute for every test.
REAL_WIPE = cli._wipe_sdo_and_harness

if TYPE_CHECKING:
    from pathlib import Path


def pod(name: str, image: str, *, deleting: bool = False) -> dict:
    metadata = {"name": name, **({"deletionTimestamp": "2026-11-23T00:00:00Z"} if deleting else {})}
    return {"metadata": metadata, "spec": {"containers": [{"name": "c", "image": image}]}}


def test_only_the_apps_in_memory_trace_stores_are_replaced_and_nothing_is_waited_for(tmp_path: Path, monkeypatch):
    calls: list[list[str]] = []
    pods = [
        pod("jaeger-1", "jaegertracing/all-in-one:1.57"),
        pod("jaeger-old", "jaegertracing/all-in-one:1.57", deleting=True),
        pod("frontend-1", "example/frontend:1"),
        pod("collector-1", "jaegertracing/jaeger:2.0"),  # a bounded, non-all-in-one store is not this class
    ]

    def kubectl(args, *, namespace, kubeconfig=None, **kwargs):
        calls.append([namespace, *args])
        return SimpleNamespace(stdout=json.dumps({"items": pods}) if args[0] == "get" else "")

    monkeypatch.setattr(app_observability, "kubectl", kubectl)
    assert app_observability.restart_in_memory_trace_stores(tmp_path / "kube", ["app"]) == ["app/jaeger-1"]
    assert ["app", "delete", "pod", "jaeger-1", "--ignore-not-found=true", "--wait=false"] in calls
    assert sum(call[1] == "delete" for call in calls) == 1


def test_an_app_without_one_is_left_alone(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        app_observability,
        "kubectl",
        lambda args, **kwargs: SimpleNamespace(stdout=json.dumps({"items": [pod("web", "example/web:1")]})),
    )
    assert app_observability.restart_in_memory_trace_stores(tmp_path / "kube", ["app"]) == []


def test_the_wait_returns_when_the_replacement_is_ready_and_names_one_that_never_is(tmp_path: Path, monkeypatch):
    states = iter([False, False, True])

    def kubectl(args, **kwargs):
        ready = next(states)
        item = pod("jaeger-2", "jaegertracing/all-in-one:1.57")
        item["status"] = {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}]}
        return SimpleNamespace(stdout=json.dumps({"items": [item]}))

    monkeypatch.setattr(app_observability, "kubectl", kubectl)
    app_observability.await_trace_stores_ready(tmp_path / "kube", ["app"], sleep=lambda _s: None)

    clock = iter(range(0, 100_000, 1000))
    monkeypatch.setattr(
        app_observability,
        "kubectl",
        lambda args, **kwargs: SimpleNamespace(
            stdout=json.dumps({"items": [pod("jaeger-3", "jaegertracing/all-in-one")]})
        ),
    )
    import pytest

    with pytest.raises(TimeoutError, match="app/jaeger-3"):
        app_observability.await_trace_stores_ready(
            tmp_path / "kube", ["app"], monotonic=lambda: float(next(clock)), sleep=lambda _s: None
        )


def test_the_row_start_wipe_replaces_the_trace_stores_before_it_and_waits_for_them_after(tmp_path: Path, monkeypatch):
    """SDO's install records the namespace's Pods by UID, so the replacement precedes it; the Tier-2 decision that
    follows reads the pods, so the wait precedes that."""
    from benchmarks.sregym.fastloop import wipe

    events: list[str] = []
    monkeypatch.setattr(
        app_observability, "restart_in_memory_trace_stores", lambda kube, ns, **kw: events.append("restart")
    )
    monkeypatch.setattr(app_observability, "await_trace_stores_ready", lambda kube, ns: events.append("await"))
    monkeypatch.setattr(wipe, "credentials_hook", lambda *a, **k: None)
    receipt = wipe.Tier1Receipt(0.0, (), False, (), ())
    monkeypatch.setattr(wipe, "wipe_environment", lambda *a, **k: events.append("wipe") or receipt)
    monkeypatch.setattr(cli, "KubectlLaneCluster", lambda *a, **k: None)
    environment = SimpleNamespace(
        kubeconfig=tmp_path / "kube",
        namespace="app",
        baseline_path=tmp_path / "baseline.json",
        seed_commit="a" * 40,
        run_dir=tmp_path,
        host_owned_kinds=(),
    )
    args = SimpleNamespace(credentials_hook=tmp_path / "hook", planned_commit=None, run_id="r")
    REAL_WIPE(args, environment)
    assert events == ["restart", "wipe", "await"]


def test_a_warm_start_replaces_only_a_trace_store_older_than_the_bound(tmp_path: Path, monkeypatch):
    """The earlier row's end replaced the store; replacing it again at the warm start made the baseline check's
    sustained-health window wait a whole minute for the fresh pod (fast-loop row: reset 114 s). A store left growing on
    an idle lane is older than the bound and is still replaced."""
    from datetime import UTC, datetime

    young = pod("jaeger-young", "jaegertracing/all-in-one:1.57")
    young["metadata"]["creationTimestamp"] = "2026-11-23T07:45:00Z"
    old = pod("jaeger-old", "jaegertracing/all-in-one:1.57")
    old["metadata"]["creationTimestamp"] = "2026-11-23T07:20:00Z"
    deleted: list[str] = []

    def kubectl(args, *, namespace, kubeconfig=None, **kwargs):
        if args[0] == "delete":
            deleted.append(args[2])
        return SimpleNamespace(stdout=json.dumps({"items": [young, old]}) if args[0] == "get" else "")

    monkeypatch.setattr(app_observability, "kubectl", kubectl)
    now = lambda: datetime(2026, 11, 23, 7, 48, tzinfo=UTC)  # noqa: E731
    restarted = app_observability.restart_in_memory_trace_stores(
        tmp_path / "kube", ["app"], older_than=app_observability.TRACE_STORE_MAX_AGE_SECONDS, now=now
    )
    assert restarted == ["app/jaeger-old"]
    assert deleted == ["jaeger-old"]


def test_a_reseeded_lane_row_end_replaces_the_trace_stores_for_the_next_row(tmp_path: Path, monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(cli, "application_namespaces", lambda *a: ["app"])
    monkeypatch.setattr(
        app_observability, "restart_in_memory_trace_stores", lambda kube, ns, **kw: calls.append(kw.get("reason"))
    )
    cli._replace_trace_stores_for_next_row(
        SimpleNamespace(baseline_path=tmp_path, kubeconfig=tmp_path / "kube", namespace="app")
    )
    assert calls == ["row end"]
