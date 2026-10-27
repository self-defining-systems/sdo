"""The lane guard keeps harness code off Secret data but lets the benchmark's own injector use its Secrets."""

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.lane_guard import LaneGuard, injector_secret_scope

READ_BACK = ["kubectl", "get", "secret", "product-catalog-db-conn", "-n", "astronomy-shop", "-o", "json"]
SCOPE = {"astronomy-shop": frozenset({"product-catalog-db-conn"})}


def guard() -> LaneGuard:
    return LaneGuard("luna-pilot-50", Path("/owned"), "le-hu")


def test_harness_code_cannot_read_the_secret_the_injector_may():
    with pytest.raises(ValueError, match="Secret namespace/name metadata"):
        guard().command(READ_BACK)


def test_injector_may_read_back_its_declared_secret_in_the_app_namespace():
    with injector_secret_scope(SCOPE):
        assert guard().command(READ_BACK) == READ_BACK
    with pytest.raises(ValueError, match="lane guard"):
        guard().command(READ_BACK)  # the exemption ends with the injector call


@pytest.mark.parametrize(
    "command",
    [
        ["kubectl", "get", "secret", "other", "-n", "astronomy-shop", "-o", "json"],
        ["kubectl", "get", "secret", "product-catalog-db-conn", "-n", "kube-system", "-o", "json"],
        ["kubectl", "get", "secret", "product-catalog-db-conn", "-o", "json"],
        ["kubectl", "get", "secrets", "-n", "astronomy-shop", "-o", "json"],
        ["kubectl", "get", "secret", "product-catalog-db-conn", "-A", "-o", "json"],
        ["kubectl", "get", "secret", "product-catalog-db-conn", "-n", "astronomy-shop", "-w"],
        ["kubectl", "get", "secret", "product-catalog-db-conn", "-n", "astronomy-shop", "-l", "a=b", "-o", "json"],
        ["kubectl", "get", "pods,secrets", "-n", "astronomy-shop", "-o", "json"],
        ["kubectl", "get", "--raw", "/api/v1/namespaces/astronomy-shop/secrets/product-catalog-db-conn"],
    ],
)
def test_exemption_is_one_declared_secret_in_one_namespace(command):
    with injector_secret_scope(SCOPE), pytest.raises(ValueError, match="lane guard"):
        guard().command(command)


def test_exemption_does_not_lift_foreign_lane_fences():
    with injector_secret_scope(SCOPE), pytest.raises(ValueError, match="foreign kubeconfig"):
        guard().command(["kubectl", "--kubeconfig", "/foreign", *READ_BACK[1:]])


def test_worker_scopes_the_injector_to_declared_secrets_of_the_incident_problems():
    from benchmarks.sregym.fastloop.worker_bridge import _injector_secrets

    class SecretRotationStaleEnvCredentialsAstronomyShop:
        namespace = "astronomy-shop"

    class Composite:
        namespace = "astronomy-shop"
        components = (SecretRotationStaleEnvCredentialsAstronomyShop(), object())

    class Plain:
        namespace = "astronomy-shop"

    class WorkerModule:
        @staticmethod
        def _app_namespace(problem):
            return problem.namespace

    assert _injector_secrets(WorkerModule, [Composite(), Plain()]) == SCOPE
    assert _injector_secrets(WorkerModule, [Plain()]) == {"astronomy-shop": frozenset()}


def _description(class_name: str):
    from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription

    return ProblemDescription(
        problem_id="mf__synthetic",
        composite=True,
        faults=(FaultSpec(kind="kafka_queue_problems", component="kafka", class_name=class_name),),
        injection_order=(0,),
    )


def test_row_start_accepts_declared_secret_fault_and_rejects_when_guard_refuses(monkeypatch):
    from benchmarks.sregym.fastloop import lane_guard
    from benchmarks.sregym.fastloop.lane_guard import FaultNotInstallableError, assert_faults_installable

    descriptions = {"mf__synthetic": _description("SecretRotationStaleEnvCredentialsAstronomyShop")}
    assert_faults_installable(descriptions, "astronomy-shop", guard())
    # A guard that no longer admits the injector's read-back must stop the row before onboarding.
    monkeypatch.setattr(lane_guard, "_reads_declared_secret", lambda *_args: False)
    with pytest.raises(FaultNotInstallableError, match="fault_not_installable.*mf__synthetic"):
        assert_faults_installable(descriptions, "astronomy-shop", guard())
    assert_faults_installable({"x": _description("SomeOtherFault")}, "astronomy-shop", guard())


def test_injector_subprocess_call_passes_the_installed_fence_and_harness_call_does_not(tmp_path, monkeypatch):
    """The SREGym injector shells out with ``subprocess.run``; the installed fence is the consumer."""
    import subprocess

    fake = tmp_path / "bin"
    fake.mkdir()
    kubectl = fake / "kubectl"
    kubectl.write_text("#!/bin/sh\necho synthetic-output\n")
    kubectl.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake}:/usr/bin:/bin")
    monkeypatch.setattr(subprocess, "Popen", subprocess.Popen)  # restored by monkeypatch after install()
    monkeypatch.setenv("KUBECONFIG", "unused")
    from benchmarks.sregym.fastloop.worker_client import SregymWorker

    monkeypatch.setattr(SregymWorker, "request", SregymWorker.request)
    LaneGuard("luna-pilot-50", tmp_path / "config", "le-hu", "/bin/true").install()

    with injector_secret_scope(SCOPE):
        assert subprocess.run(READ_BACK, capture_output=True, text=True).stdout == "synthetic-output\n"
    with pytest.raises(ValueError, match="Secret namespace/name metadata"):
        subprocess.run(READ_BACK, capture_output=True, text=True)
