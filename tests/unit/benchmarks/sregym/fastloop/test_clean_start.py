"""Clean-lane contracts against a synthetic API; no Kubernetes credentials."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.sregym.fastloop import clean_start


class API:
    def __init__(self, *, stuck: bool = False) -> None:
        self.stuck = stuck
        self.calls: list[tuple[list[str], Any]] = []
        self.objects = [
            {
                "apiVersion": "rbac.authorization.k8s.io/v1" if kind.startswith("ClusterRole") else "v1",
                "kind": kind,
                "metadata": {"name": name, **scope, "labels": {"platform.io/injected": "true"}},
            }
            for kind, name, scope in [
                ("ServiceAccount", "cleanup-controller", {"namespace": "hotel"}),
                ("ClusterRole", "configmap-cleanup-controller", {}),
                ("ClusterRoleBinding", "configmap-cleanup-controller", {}),
                ("ConfigMap", "fault-token", {"namespace": "hotel", "finalizers": ["platform.io/cleanup"]}),
            ]
        ]

    def __call__(self, args: list[str], **kwargs: Any) -> Any:
        self.calls.append((args, kwargs.get("namespace")))
        if args[:2] == ["get", "secrets"]:
            assert args[args.index("-o") + 1] == "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name"
            return SimpleNamespace(stdout="")
        assert not any("secret" in arg.lower() for arg in args)
        if args[0] == "api-resources":
            resources = (
                "configmaps\nserviceaccounts"
                if "--namespaced=true" in args
                else "clusterroles.rbac.authorization.k8s.io\nclusterrolebindings.rbac.authorization.k8s.io"
            )
            return SimpleNamespace(stdout=resources)
        if args[0] == "get":
            if args[args.index("-o") + 1] == "name":
                return SimpleNamespace(stdout="")
            if args[1] == "pods,jobs,configmaps":
                return SimpleNamespace(stdout=json.dumps({"items": []}))
            assert args[args.index("-l") + 1] == "platform.io/injected=true"
            namespaced = "--all-namespaces" in args
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "items": deepcopy(
                            [
                                v
                                for v in self.objects
                                if bool(v["metadata"].get("namespace")) == namespaced
                                and v["metadata"].get("labels", {}).get("platform.io/injected") == "true"
                            ]
                        )
                    }
                )
            )
        if args[0] == "delete":
            kind, name = args[1].split("/")
            if not self.stuck:
                self.objects = [
                    v for v in self.objects if (v["kind"].lower(), v["metadata"]["name"]) != (kind.split(".")[0], name)
                ]
        return SimpleNamespace(stdout="")


def test_restore_removes_labelled_namespaced_and_cluster_scoped_fault_objects(monkeypatch: Any) -> None:
    api = API()
    monkeypatch.setattr(clean_start, "kubectl", api)
    clean_start.remove_injected_objects(Path("synthetic-kubeconfig"))
    assert api.objects == []
    assert any(
        args[0] == "delete" and args[1].startswith("clusterrolebinding") and namespace is None
        for args, namespace in api.calls
    )
    assert any(args[0] == "patch" and namespace == "hotel" for args, namespace in api.calls)


def test_clean_baseline_admission_rejects_remaining_injected_objects(monkeypatch: Any) -> None:
    api = API(stuck=True)
    monkeypatch.setattr(clean_start, "kubectl", api)
    with pytest.raises(RuntimeError, match="injected objects remain"):
        clean_start.remove_injected_objects(Path("synthetic-kubeconfig"))
    with pytest.raises(RuntimeError, match="injected objects remain"):
        clean_start.assert_clean_baseline(Path("synthetic-kubeconfig"))


def test_wrk_job_and_payload_are_created_in_harness_namespace_with_service_dns(monkeypatch: Any) -> None:
    created: list[tuple[str, str, str]] = []
    manager = SimpleNamespace(namespace="hotel", wrk=SimpleNamespace(namespace="hotel"), url="http://10.0.0.1:5000")
    manager.start = lambda: created.extend(
        [(manager.namespace, "Job", manager.url), (manager.wrk.namespace, "ConfigMap", manager.url)]
    )
    app = SimpleNamespace(namespace="hotel-reservation", frontend_service="frontend", frontend_port=5000)
    app.create_workload = lambda: setattr(app, "wrk", manager)
    app.start_workload = lambda: pytest.fail("original app-namespace traffic must not start")
    monkeypatch.setattr(clean_start, "kubectl", lambda *_args, **_kwargs: SimpleNamespace(stdout=""))
    clean_start.prepare_harness_traffic(app, Path("synthetic-kubeconfig"))
    app.start_workload()
    assert created == [
        ("sregym", "Job", "http://frontend.hotel-reservation.svc.cluster.local:5000"),
        ("sregym", "ConfigMap", "http://frontend.hotel-reservation.svc.cluster.local:5000"),
    ]


def test_toy_reset_uses_the_same_injected_object_cleanup(monkeypatch: Any) -> None:
    from benchmarks.sregym.fastloop.toy import ToyWorker

    cleaned: list[Path] = []
    config = Path("synthetic-kubeconfig")
    monkeypatch.setattr(clean_start, "remove_injected_objects", cleaned.append)
    worker = ToyWorker(SimpleNamespace(kubeconfig=config))
    assert worker.request("clean_start", problem_id="toy-targetport") == {"success": True}
    assert cleaned == [config]


def test_existing_oracle_manager_keeps_identity_and_reads_harness_logs(monkeypatch: Any) -> None:
    manager = SimpleNamespace(namespace="hotel-reservation", wrk=SimpleNamespace(namespace="hotel-reservation"))
    app = SimpleNamespace(namespace="hotel-reservation", frontend_service="frontend", frontend_port=5000, wrk=manager)
    app.create_workload = lambda: pytest.fail("must preserve the oracle's existing manager")
    monkeypatch.setattr(
        clean_start, "kubectl", lambda *_args, **_kwargs: pytest.fail("configuration must not mutate the cluster")
    )
    assert clean_start.prepare_harness_traffic(app, Path("synthetic-kubeconfig"))
    assert app.wrk is manager
    assert manager.namespace == manager.wrk.namespace == "sregym"
    assert manager.url == "http://frontend.hotel-reservation.svc.cluster.local:5000"


def test_traffic_start_removes_orphan_legacy_pods_after_harness_creation(monkeypatch: Any) -> None:
    calls: list[tuple[list[str], str | None]] = []
    manager = SimpleNamespace(namespace="hotel-reservation", wrk=SimpleNamespace(namespace="hotel-reservation"))
    manager.start = lambda: calls.append((["create-traffic"], manager.namespace))
    app = SimpleNamespace(namespace="hotel-reservation", frontend_service="frontend", frontend_port=5000, wrk=manager)
    app.create_workload = lambda: None
    monkeypatch.setattr(clean_start, "kubectl", lambda args, **kw: calls.append((args, kw["namespace"])))
    clean_start.prepare_harness_traffic(app, Path("synthetic-kubeconfig"))
    app.start_workload()
    legacy_pods = next(
        i for i, (args, ns) in enumerate(calls) if args[:2] == ["delete", "pods"] and ns == "hotel-reservation"
    )
    assert "--selector=job-name=wrk2-job" in calls[legacy_pods][0]
    assert calls.index((["create-traffic"], "sregym")) < legacy_pods


def run_bridge(monkeypatch: Any, requests: list[dict[str, Any]], api: API) -> list[dict[str, Any]]:
    from benchmarks.sregym.fastloop import hotel_helpers, worker_bridge

    class Worker:
        def _problem_factory(self, _problem_id: str) -> Any:
            return SimpleNamespace(namespace="hotel-reservation")

        def handle(self, request: dict[str, Any]) -> dict[str, Any]:
            if request.get("op") == "deploy" and getattr(api, "inject_during_deploy", False):
                api.objects.append(
                    {
                        "kind": "ConfigMap",
                        "metadata": {
                            "namespace": "hotel-reservation",
                            "name": "late-fault",
                            "labels": {"platform.io/injected": "true"},
                        },
                    }
                )
            return {"success": True, "deployed": True}

        def _grade(self, _problem: Any) -> dict[str, Any]:
            assert api.objects == [], "grading must not admit injected leftovers"
            return {"success": True}

    # Existing cleanup fixtures isolate injected-object ownership. Helper policy
    # has its own worker-boundary fixtures in test_hotel_helpers.py.
    monkeypatch.setattr(hotel_helpers, "prepare_install", lambda *args: None)
    requests = [{**request, "baseline_path": "synthetic-baseline.json"} for request in requests]
    worker = Worker()
    module = SimpleNamespace(FastloopWorker=Worker, main=lambda: [worker.handle(request) for request in requests])
    monkeypatch.setenv("KUBECONFIG", "synthetic-kubeconfig")
    monkeypatch.setattr(clean_start, "kubectl", api)
    monkeypatch.setattr(
        worker_bridge.importlib,
        "import_module",
        lambda name: module
        if name.endswith(".worker")
        else SimpleNamespace(probe_pod_manifest=lambda *args: {}, _build_image=None),
    )
    return worker_bridge.main()


def test_worker_deploy_removes_previous_problem_objects_before_baseline(monkeypatch: Any) -> None:
    api = API()
    assert run_bridge(monkeypatch, [{"op": "deploy", "problem_id": "synthetic"}], api) == [
        {"success": True, "deployed": True}
    ]
    assert api.objects == []


def test_worker_full_recovery_cleans_injected_rbac(monkeypatch: Any) -> None:
    api = API()
    run_bridge(monkeypatch, [{"op": "recover"}], api)
    assert api.objects == []


def test_worker_partial_composite_recovery_preserves_other_faults(monkeypatch: Any) -> None:
    api = API()
    run_bridge(monkeypatch, [{"op": "recover", "fault": 0}], api)
    assert len(api.objects) == 4
    assert api.calls == []


def test_worker_baseline_oracle_rejects_residue_before_grading(monkeypatch: Any) -> None:
    with pytest.raises(RuntimeError, match="injected objects remain"):
        run_bridge(monkeypatch, [{"op": "baseline_oracle", "problem_id": "synthetic"}], API())


def test_injected_secret_cleanup_projects_metadata_without_reading_secret_data(monkeypatch: Any) -> None:
    api = API()
    remaining = True
    calls: list[list[str]] = []

    def metadata_api(args: list[str], **kwargs: Any) -> Any:
        nonlocal remaining
        calls.append(args)
        if args[:2] == ["get", "secrets"]:
            assert args[args.index("-o") + 1] == "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name"
            assert "--no-headers" in args
            return SimpleNamespace(stdout="hotel-reservation synthetic-tls\n" if remaining else "")
        if args[0] == "delete" and args[1] == "secret/synthetic-tls":
            remaining = False
            return SimpleNamespace(stdout="deleted")
        if args[0] == "patch" and args[1] == "secret/synthetic-tls":
            assert '"finalizers":[]' in args[-1]
            return SimpleNamespace(stdout="patched")
        return api(args, **kwargs)

    monkeypatch.setattr(clean_start, "kubectl", metadata_api)
    clean_start.remove_injected_objects(Path("synthetic-kubeconfig"))
    assert not remaining
    assert any(args[:2] == ["get", "secrets"] for args in calls)


def test_admission_rejects_legacy_harness_traffic_even_without_injection_label(monkeypatch: Any) -> None:
    api = API()
    api.objects.clear()

    def with_traffic(args: list[str], **kwargs: Any) -> Any:
        if args[0] == "get" and "job/wrk2-job" in args:
            return SimpleNamespace(stdout="job.batch/wrk2-job\n")
        return api(args, **kwargs)

    monkeypatch.setattr(clean_start, "kubectl", with_traffic)
    with pytest.raises(RuntimeError, match="harness traffic remains"):
        clean_start.assert_clean_baseline(Path("synthetic-kubeconfig"), namespace="hotel-reservation")


@pytest.mark.requires_sregym
def test_real_wrk2_creates_harness_objects_and_grader_reads_the_same_namespace() -> None:
    import os
    import subprocess

    root = Path(__file__).resolve().parents[5]
    reply = subprocess.run(
        [
            "uv",
            "run",
            "--directory",
            str(root / "third_party/sregym"),
            "python",
            "-m",
            "tests.unit.benchmarks.sregym.fastloop.clean_start_wrk2_worker",
        ],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert reply.returncode == 0, (reply.stdout + reply.stderr)[-4000:]
    assert "real Wrk2 Job, payload, settings and grader log namespace: pass" in reply.stdout


def test_worker_checks_admission_after_deployment_before_baseline_capture(monkeypatch: Any) -> None:
    api = API()
    api.inject_during_deploy = True
    with pytest.raises(RuntimeError, match="injected objects remain"):
        run_bridge(monkeypatch, [{"op": "deploy", "problem_id": "synthetic"}], api)


def test_cleanup_preserves_unlabelled_application_rbac_and_control_objects(monkeypatch: Any) -> None:
    api = API()
    accepted = [
        {
            "kind": "ClusterRoleBinding",
            "metadata": {"name": "accepted-app-binding"},
            "subjects": [{"kind": "ServiceAccount", "name": "default", "namespace": "hotel"}],
        },
        {"kind": "ConfigMap", "metadata": {"name": "sdo-controller-state", "namespace": "hotel-sdo"}},
    ]
    api.objects.extend(deepcopy(accepted))
    monkeypatch.setattr(clean_start, "kubectl", api)
    clean_start.remove_injected_objects(Path("synthetic-kubeconfig"))
    assert api.objects == accepted
