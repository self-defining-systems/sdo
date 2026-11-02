"""Captured fault infrastructure cannot become accepted application state."""

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, accepted_restore_baselines
from tests.unit.benchmarks.sregym.fastloop.accepted_source_fixture import commit_accepted_source


def test_capture_cannot_authorize_recreation_of_fault_namespaces(tmp_path):
    workload = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "webhook", "namespace": "policy-system"},
        "spec": {
            "template": {"spec": {"volumes": [{"name": "tls", "secret": {"secretName": "pod-policy-webhook-tls"}}]}}
        },
    }
    source = commit_accepted_source(tmp_path / "source", namespace="app")
    restored = accepted_restore_baselines(source, (ApplicationBaseline("policy-system", [workload]),))
    assert [(baseline.namespace, baseline.objects) for baseline in restored] == [("app", [])]
