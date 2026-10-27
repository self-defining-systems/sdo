"""Admission must compare the exact grants requested from the real installer."""

from copy import deepcopy
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.native_operator import NativeInstallConfig
from benchmarks.sregym.fastloop.rbac import installer_rbac, require_rbac_equality


def config(tmp_path: Path, permissive: bool) -> NativeInstallConfig:
    return NativeInstallConfig(
        repository=tmp_path,
        namespace="app",
        control_namespace="app-sdo",
        application="app",
        controller_image="controller:test",
        responder_image="responder:test",
        validator_image="validator:test",
        repository_pvc="repository",
        credentials_secret="unused",
        model="scripted",
        permissive_responder=permissive,
    )


def listing(plan):
    rows = []
    for key, kind in (("ClusterRoles", "ClusterRole"), ("ClusterBindings", "ClusterRoleBinding")):
        for value in plan[key] or []:
            row = deepcopy(value)
            row["kind"] = kind
            rows.append(row)
    return rows


def test_default_configuration_remains_exact(tmp_path):
    requested = config(tmp_path, False)
    plan = installer_rbac("app", requested)
    rows = listing(plan)
    assert require_rbac_equality(plan, rows, "app")["passed"]
    rows.append(
        {
            "kind": "RoleBinding",
            "metadata": {"namespace": "default", "name": "unrequested"},
            "subjects": [{"kind": "ServiceAccount", "namespace": "app-sdo", "name": "sdo-responder"}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "cluster-admin"},
        }
    )
    with pytest.raises(ValueError, match="unexpected grant"):
        require_rbac_equality(plan, rows, "app")
