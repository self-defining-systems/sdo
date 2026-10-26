"""Namespace recovery must retain prerequisites, independent of application names."""
from __future__ import annotations

from copy import deepcopy

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import capture_application_baseline, reconcile_application
from tests.unit.benchmarks.sregym.fastloop.test_baseline_recovery import FakeCluster, obj


@pytest.mark.parametrize("namespace,accounts", [
    ("astronomy-shop", ["astronomy-shop", "grafana", "jaeger", "otel-collector"]),
    ("hotel-reservation", ["reservation-reader"]),
    ("social-network", ["social-reader"]),
])
def test_namespace_recovery_capture_contains_referenced_nondefault_service_accounts(namespace, accounts):
    # AST3's sealed-baseline reproducer: all four account names come from real
    # pod-template references, not recovery configuration.
    cluster = FakeCluster()
    for account in accounts:
        cluster.add(obj("Deployment", account, namespace, spec={"template": {"spec": {"serviceAccountName": account}}}))
        cluster.add(obj("ServiceAccount", account, namespace))
    role = obj("Role", "reader", namespace, rules=[{"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}])
    binding = obj("RoleBinding", "reader", namespace, roleRef={"kind": "Role", "name": "reader"},
                  subjects=[{"kind": "ServiceAccount", "name": accounts[0], "namespace": namespace}])
    cluster.add(role)
    cluster.add(binding)
    original = deepcopy(cluster.objects)
    baseline = capture_application_baseline(cluster, namespace)
    captured_accounts = {v["metadata"]["name"] for v in baseline.objects if v["kind"] == "ServiceAccount"}
    assert set(accounts) <= captured_accounts
    cluster.objects.clear()
    reconcile_application(cluster, baseline, namespace)
    assert cluster.objects == original
    order = [kind for _, kind, _ in cluster.touched]
    assert order.index("ServiceAccount") < order.index("Deployment")
