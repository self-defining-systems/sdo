"""Recovery consumes the accepted revision at the recovery boundary."""

from copy import deepcopy

import pytest

from benchmarks.sregym.fastloop.accepted_source import AcceptedSource
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_fault_driver import FakeWorker


class Cluster:
    def __init__(self, value):
        self.value = deepcopy(value)

    def list_objects(self, namespace):
        return [self.value]

    def apply(self, namespace, value):
        self.value = deepcopy(value)

    def delete(self, namespace, kind, name):
        pytest.fail("accepted object must remain")


def config(value):
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "settings", "namespace": "app"},
        "data": {"setting": value},
    }


def test_recovery_reads_newly_accepted_source_after_driver_creation(tmp_path):
    repo = tmp_path / "source"
    commit_accepted_source(repo, [config("old")])
    cluster = Cluster(config("old"))
    driver = SregymFaultDriver(
        FakeWorker(seed_contract=True),
        namespace="app",
        baseline=ApplicationBaseline("app", [config("old")]),
        cluster=cluster,
        accepted_source=lambda: AcceptedSource(repo, "app"),
    )
    commit_accepted_source(repo, [config("accepted repair")])
    (repo / "kubernetes/manifests.yaml").write_text("unaccepted local edits")
    driver.recover()
    assert cluster.value == config("accepted repair")


def test_captured_application_cannot_restore_without_an_accepted_source_provider():
    with pytest.raises(ValueError, match="accepted source"):
        SregymFaultDriver(
            FakeWorker(), namespace="app", baseline=ApplicationBaseline("app", []), cluster=Cluster(config("old"))
        )


def test_current_source_annotation_is_restored_even_when_spec_is_unchanged(tmp_path):
    old = config("same")
    new = deepcopy(old)
    new["metadata"]["annotations"] = {"source.example/policy": "accepted"}
    repo = tmp_path / "repo"
    commit_accepted_source(repo, [new])
    cluster = Cluster(old)
    driver = SregymFaultDriver(
        FakeWorker(seed_contract=True),
        namespace="app",
        baseline=ApplicationBaseline("app", [old]),
        cluster=cluster,
        accepted_source=lambda: AcceptedSource(repo, "app"),
    )
    driver.recover()
    assert cluster.value == new
