"""Kubernetes optional lists may be absent or explicitly null."""

import pytest

from benchmarks.sregym.fastloop.recovery_closure import prerequisites


@pytest.mark.parametrize("kind", ["Pod", "StatefulSet", "ServiceAccount", "RoleBinding"])
def test_null_reference_lists_are_empty(kind):
    pod = {
        "imagePullSecrets": None,
        "volumes": None,
        "containers": None,
        "initContainers": None,
        "ephemeralContainers": None,
    }
    spec = pod if kind == "Pod" else {"template": {"spec": pod}, "volumeClaimTemplates": None}
    assert prerequisites({"kind": kind, "spec": spec, "imagePullSecrets": None, "subjects": None}, "test") == set()


def test_nested_null_reference_lists():
    pod = {"volumes": [{"projected": {"sources": None}}], "containers": [{"env": None, "envFrom": None}]}
    assert prerequisites({"kind": "Pod", "spec": pod}, "test") == set()
