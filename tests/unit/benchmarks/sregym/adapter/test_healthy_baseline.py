from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter.healthy_baseline import (
    HealthyBaselineCaptureError,
    HealthyBaselineCaptureOps,
    capture_snapshots,
    publish_snapshots,
    snapshot_document,
)

if TYPE_CHECKING:
    from pathlib import Path


def _item(kind: str, name: str, **extra: object) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": kind,
        "metadata": {"name": name, "namespace": "app", "managedFields": [{"manager": "kubectl"}]},
        **extra,
    }


def _listing() -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            _item("Service", "frontend", spec={"selector": {"app": "frontend"}}),
            _item("Pod", "frontend-1"),
            _item("ConfigMap", "cfg"),
            _item("Deployment", "frontend"),
            _item("ReplicaSet", "frontend-rs"),
            _item("Endpoints", "frontend"),
            _item("EndpointSlice", "frontend-x"),
            _item("NetworkPolicy", "np"),
            _item("Event", "ev"),
            _item("Secret", "must-not-be-copied"),
        ],
    }


def test_snapshot_document_uses_the_sdktest_shape_and_drops_other_kinds_and_managed_fields() -> None:
    document = snapshot_document("app", _listing())

    assert document["namespace"] == "app"
    assert set(document) == {
        "namespace",
        "configMaps",
        "services",
        "pods",
        "deployments",
        "replicaSets",
        "endpoints",
        "endpointSlices",
        "networkPolicies",
        "events",
    }
    assert [item["metadata"]["name"] for item in document["services"]] == ["frontend"]  # type: ignore[index]
    assert "managedFields" not in document["services"][0]["metadata"]  # type: ignore[index]
    assert "Secret" not in json.dumps(document)


def test_snapshot_document_is_json_serialisable_with_empty_lists_for_absent_kinds() -> None:
    document = snapshot_document("app", {"items": []})

    assert document["pods"] == []
    json.dumps(document)


class _Kubectl:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[list[str], str | None, str | None]] = []
        self.fail = fail

    def __call__(
        self, args: list[str], *, namespace: str | None, input_text: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((args, namespace, input_text))
        if self.fail:
            return subprocess.CompletedProcess(args, 1, "", "boom")
        return subprocess.CompletedProcess(args, 0, json.dumps(_listing()), "")


def test_capture_takes_the_requested_number_of_spaced_snapshots_of_the_application_namespace() -> None:
    kubectl = _Kubectl()
    sleeps: list[float] = []

    snapshots = capture_snapshots("app", kubectl_runner=kubectl, count=3, interval_seconds=2.0, sleep=sleeps.append)

    assert sorted(snapshots) == ["healthy-0.json", "healthy-1.json", "healthy-2.json"]
    assert sleeps == [2.0, 2.0]
    assert all(namespace == "app" for _, namespace, _ in kubectl.calls)
    assert all(call[0][0] == "get" and "-o" in call[0] for call in kubectl.calls)
    assert snapshots["healthy-0.json"]["namespace"] == "app"


def test_capture_fails_loudly_when_kubectl_fails() -> None:
    with pytest.raises(HealthyBaselineCaptureError, match="boom"):
        capture_snapshots("app", kubectl_runner=_Kubectl(fail=True), count=1, interval_seconds=0, sleep=lambda _: None)


def test_capture_rejects_a_non_positive_count() -> None:
    with pytest.raises(ValueError, match="count"):
        capture_snapshots("app", kubectl_runner=_Kubectl(), count=0, interval_seconds=0, sleep=lambda _: None)


def test_publish_sends_snapshots_to_go_baseline_writer() -> None:
    seen: list[tuple[list[str], str | None]] = []

    def kubectl(
        args: list[str], *, namespace: str | None, input_text: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        seen.append((args, input_text))
        assert namespace == "ctl"
        return subprocess.CompletedProcess(args, 0, "", "")

    publish_snapshots({"healthy-0.json": {"namespace": "app"}}, control_namespace="ctl", kubectl_runner=kubectl)

    assert seen[0][0] == [
        "exec",
        "-i",
        "job/sdo-controller-run",
        "-c",
        "controller",
        "--",
        "/usr/local/bin/sdo-operator",
        "baseline-write",
    ]
    assert json.loads(seen[0][1] or "") == {"healthy-0.json": {"namespace": "app"}}


def test_publish_rejects_unsafe_file_names() -> None:
    with pytest.raises(ValueError, match="file name"):
        publish_snapshots({"../x.json": {}}, control_namespace="ctl", kubectl_runner=_Kubectl())


def test_publish_reports_a_failed_exec() -> None:
    with pytest.raises(HealthyBaselineCaptureError, match="boom"):
        publish_snapshots({"healthy-0.json": {}}, control_namespace="ctl", kubectl_runner=_Kubectl(fail=True))


class _Inner:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def inject_after_resume(self, control_namespace: str, generation: str, inject) -> dict[str, float]:  # type: ignore[no-untyped-def]
        self.events.append("gate")
        inject()
        return {"x": 1.0}

    def controller_pod(self, control_namespace: str) -> str:
        return "pod"


def test_ops_wrapper_captures_and_publishes_just_before_the_fault_is_injected() -> None:
    events: list[str] = []
    ops = HealthyBaselineCaptureOps(
        _Inner(events),  # type: ignore[arg-type]
        namespace="app",
        capture=lambda: {"healthy-0.json": {"namespace": "app"}},
        publish=lambda snapshots, control: events.append(f"publish:{control}:{sorted(snapshots)}"),
    )

    timings = ops.inject_after_resume("ctl", "g1", lambda: events.append("inject"))

    assert events == ["gate", "publish:ctl:['healthy-0.json']", "inject"]
    assert timings["x"] == 1.0
    assert "healthy_baseline_capture" in timings
    assert ops.controller_pod("ctl") == "pod"  # other calls pass through


def test_ops_wrapper_does_not_inject_when_capture_fails() -> None:
    events: list[str] = []

    def failing() -> dict[str, dict[str, object]]:
        raise HealthyBaselineCaptureError("cannot capture")

    ops = HealthyBaselineCaptureOps(
        _Inner(events),  # type: ignore[arg-type]
        namespace="app",
        capture=failing,
        publish=lambda snapshots, control: None,
    )

    with pytest.raises(HealthyBaselineCaptureError):
        ops.inject_after_resume("ctl", "g1", lambda: events.append("inject"))
    assert "inject" not in events


@pytest.mark.parametrize("count", [0, 17])
def test_publish_rejects_invalid_snapshot_count_before_remote_exec(count: int) -> None:
    kubectl = _Kubectl()
    snapshots = {f"healthy-{index}.json": {} for index in range(count)}
    with pytest.raises(HealthyBaselineCaptureError, match="1-16"):
        publish_snapshots(snapshots, control_namespace="ctl", kubectl_runner=kubectl)
    assert kubectl.calls == []


def test_publish_rejects_oversized_aggregate_before_remote_exec() -> None:
    kubectl = _Kubectl()
    snapshots = {f"healthy-{index}.json": {"padding": "x" * (3 << 20)} for index in range(3)}
    with pytest.raises(HealthyBaselineCaptureError, match="8 MiB"):
        publish_snapshots(snapshots, control_namespace="ctl", kubectl_runner=kubectl)
    assert kubectl.calls == []


@pytest.mark.parametrize("extra", [0, 1])
def test_publish_preserves_exact_transport_byte_boundary(extra: int) -> None:
    kubectl = _Kubectl()
    snapshots = {"healthy-0.json": {"padding": ""}}
    overhead = len(json.dumps(snapshots).encode("utf-8"))
    snapshots["healthy-0.json"]["padding"] = "x" * ((8 << 20) - overhead + extra)
    if extra:
        with pytest.raises(HealthyBaselineCaptureError, match="8 MiB"):
            publish_snapshots(snapshots, control_namespace="ctl", kubectl_runner=kubectl)
        assert kubectl.calls == []
    else:
        publish_snapshots(snapshots, control_namespace="ctl", kubectl_runner=kubectl)
        assert json.loads(kubectl.calls[0][2] or "") == snapshots


def test_publish_applies_limit_to_serialized_unicode_bytes() -> None:
    kubectl = _Kubectl()
    snapshots = {"healthy-0.json": {"padding": "\U0001f680" * (1 << 20)}}
    with pytest.raises(HealthyBaselineCaptureError, match="8 MiB"):
        publish_snapshots(snapshots, control_namespace="ctl", kubectl_runner=kubectl)
    assert kubectl.calls == []


@pytest.mark.parametrize("case", ["empty", "count", "bytes"])
def test_publish_admission_agrees_with_real_native_writer(operator_binary: Path, case: str) -> None:
    snapshots = (
        {"healthy-0.json": {"padding": "x" * (8 << 20)}}
        if case == "bytes"
        else {f"healthy-{index}.json": {} for index in range(17 if case == "count" else 0)}
    )
    # These inputs must fail before the native writer opens its fixed volume.
    native = subprocess.run(
        [str(operator_binary), "baseline-write"],
        input=json.dumps(snapshots),
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert native.returncode != 0
    kubectl = _Kubectl()
    with pytest.raises(HealthyBaselineCaptureError) as failure:
        publish_snapshots(snapshots, control_namespace="ctl", kubectl_runner=kubectl)
    assert native.stderr.strip() in str(failure.value)
    assert kubectl.calls == []
