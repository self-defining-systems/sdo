"""Run the pinned harness against a synthetic delayed Kubernetes garbage collector."""

from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import patch

from kubernetes.client.exceptions import ApiException
from sregym.generators.workload.wrk2 import Wrk2
from urllib3.exceptions import ReadTimeoutError


class FakeAPI:
    def __init__(self, scenario: str) -> None:
        self.scenario = scenario
        self.now = 0.0
        self.deleted = False
        self.policy = ""
        self.created = False
        self.grace = None

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def read_namespaced_job(self, **kwargs: object) -> SimpleNamespace:
        assert kwargs["namespace"] == "sregym"
        if self.deleted and self.scenario != "finalizer" and (self.policy == "Background" or self.now >= 65):
            raise ApiException(status=404)
        return SimpleNamespace(metadata=SimpleNamespace(uid="old-job-uid"))

    def delete_namespaced_job(self, *, body: object, **kwargs: object) -> None:
        assert kwargs["namespace"] == "sregym"
        self.deleted = True
        self.policy = body.propagation_policy
        self.grace = body.grace_period_seconds
        if self.policy == "Background":
            assert body.preconditions.uid == "old-job-uid"
        if self.scenario == "delete-timeout":
            self.now += 65
            raise ReadTimeoutError(None, "/synthetic", "synthetic timeout after accepted DELETE")

    def list_namespaced_pod(self, **kwargs: object) -> SimpleNamespace:
        assert kwargs["namespace"] == "sregym"
        assert kwargs["label_selector"] == "batch.kubernetes.io/controller-uid=old-job-uid"
        if self.scenario == "api-error":
            raise ApiException(status=403)
        if self.scenario == "gone" or self.now >= 65:
            return SimpleNamespace(items=[])
        pod = SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp=None if self.scenario == "active" else "synthetic-time"),
            status=SimpleNamespace(phase="Running"),
        )
        return SimpleNamespace(items=[pod])

    def create_namespaced_job(self, **kwargs: object) -> SimpleNamespace:
        assert kwargs["namespace"] == "sregym"
        self.created = True
        return SimpleNamespace(metadata=SimpleNamespace(name="wrk2-job"))


def main(scenario: str) -> None:
    api = FakeAPI(scenario)
    with (
        patch("sregym.generators.workload.wrk2.config.load_kube_config"),
        patch("sregym.generators.workload.wrk2.client.BatchV1Api", return_value=api),
        patch("sregym.generators.workload.wrk2.client.CoreV1Api", return_value=api),
        patch("sregym.generators.workload.wrk2.time.sleep", side_effect=api.sleep),
        patch("sregym.generators.workload.wrk2.time.monotonic", side_effect=lambda: api.now),
    ):
        wrk = Wrk2(rate=100, namespace="sregym")
        try:
            wrk.create_wrk_job("wrk2-job", "sregym", "synthetic.lua")
        except TimeoutError:
            assert scenario in {"active", "finalizer"}
        except ApiException as error:
            assert scenario == "api-error"
            api_error = error
        else:
            assert scenario in {"gone", "terminating", "delete-timeout"}
            assert api.created
        if scenario == "api-error":
            assert api_error.status == 403
        assert api.created == (scenario in {"gone", "terminating", "delete-timeout"})
        assert api.now <= (65 if scenario == "delete-timeout" else 60)
        assert api.grace == 1
    print(f"bounded wrk2 replacement {scenario}: pass")


if __name__ == "__main__":
    main(sys.argv[1])
