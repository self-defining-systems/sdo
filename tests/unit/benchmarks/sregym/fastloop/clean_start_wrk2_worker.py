"""Exercise the pinned harness's actual Wrk2 code with a synthetic Kubernetes API."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from kubernetes.client.exceptions import ApiException
from sregym.generators.workload.wrk2 import Wrk2, Wrk2WorkloadManager

from benchmarks.sregym.fastloop import clean_start


class FakeAPI:
    def __init__(self) -> None:
        self.created: list[tuple[str, object]] = []
        self.log_namespaces: list[str] = []

    def delete_namespaced_config_map(self, **_kwargs: object) -> None:
        raise ApiException(status=404)

    def read_namespaced_job(self, **_kwargs: object) -> None:
        raise ApiException(status=404)

    def create_namespaced_config_map(self, *, namespace: str, body: object) -> None:
        self.created.append((namespace, body))

    def create_namespaced_job(self, *, namespace: str, body: dict, **_kwargs: object) -> SimpleNamespace:
        self.created.append((namespace, body))
        return SimpleNamespace(metadata=SimpleNamespace(name=body["metadata"]["name"]))

    def list_namespaced_pod(self, namespace: str, **_kwargs: object) -> SimpleNamespace:
        self.log_namespaces.append(namespace)
        return SimpleNamespace(items=[SimpleNamespace(metadata=SimpleNamespace(name="wrk2-job-synthetic"))])

    def read_namespaced_pod_log(self, _name: str, namespace: str, **_kwargs: object) -> str:
        self.log_namespaces.append(namespace)
        return ""


def main() -> None:
    api = FakeAPI()
    commands: list[tuple[list[str], str | None]] = []

    def kubectl(args: list[str], **kwargs: object) -> SimpleNamespace:
        commands.append((args, kwargs.get("namespace")))
        return SimpleNamespace(stdout="")

    with (
        TemporaryDirectory(prefix="synthetic-wrk2-") as directory,
        patch("sregym.generators.workload.wrk2.config.load_kube_config"),
        patch("sregym.generators.workload.wrk2.client.CoreV1Api", return_value=api),
        patch("sregym.generators.workload.wrk2.client.BatchV1Api", return_value=api),
        patch.object(clean_start, "kubectl", kubectl),
    ):
        script = Path(directory) / "synthetic.lua"
        script.write_text('request = function() return wrk.format("GET", "/hotels") end\n')
        wrk = Wrk2(rate=100, dist="exp", connections=100, duration=30, threads=3, namespace="hotel-reservation")
        manager = Wrk2WorkloadManager(wrk, script, "http://old-address:5000", namespace="hotel-reservation")
        app = SimpleNamespace(
            namespace="hotel-reservation", frontend_service="frontend", frontend_port=5000, wrk=manager
        )
        app.create_workload = lambda: None
        clean_start.prepare_harness_traffic(app, Path("synthetic-kubeconfig"))
        app.start_workload()
        assert app.wrk is manager
        assert [namespace for namespace, _ in api.created] == ["sregym", "sregym"]
        configmap, job = [body for _, body in api.created]
        assert configmap.metadata.name == "wrk2-payload-script"
        assert configmap.data[script.name] == script.read_text()
        shell = configmap.data["wrk2-workload.sh"]
        assert "http://frontend.hotel-reservation.svc.cluster.local:5000" in shell
        for setting in ("-R 100", "-D exp", "-t 3", "-c 100", "-d 30s"):
            assert setting in shell
        assert job["metadata"]["name"] == "wrk2-job"
        assert job["spec"]["template"]["spec"]["volumes"][0]["configMap"]["name"] == configmap.metadata.name
        manager.start(suspended=True)
        suspended_job = api.created[-1][1]
        assert suspended_job["spec"]["suspend"] is True
        manager.retrievelog()
        assert api.log_namespaces == ["sregym", "sregym"]
        assert any(args[:2] == ["delete", "pods"] and namespace == "hotel-reservation" for args, namespace in commands)
    print("real Wrk2 Job, payload, settings and grader log namespace: pass")


if __name__ == "__main__":
    main()
