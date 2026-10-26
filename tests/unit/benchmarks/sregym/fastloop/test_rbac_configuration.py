"""Admission must compare the exact grants requested from the real installer."""

from copy import deepcopy
from dataclasses import replace
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
    for key, kind in (
        ("ControlRoles", "Role"),
        ("AppRoles", "Role"),
        ("ClusterRoles", "ClusterRole"),
        ("ControlBindings", "RoleBinding"),
        ("AppBindings", "RoleBinding"),
        ("ClusterBindings", "ClusterRoleBinding"),
    ):
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


@pytest.fixture
def discovery_kubeconfig(tmp_path):
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import urlsplit

    resources = {
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": "v1",
        "resources": [
            {
                "name": name,
                "kind": kind,
                "namespaced": namespaced,
                "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"],
            }
            for name, kind, namespaced in [
                ("services", "Service", True),
                ("pods", "Pod", True),
                ("configmaps", "ConfigMap", True),
                ("secrets", "Secret", True),
                ("namespaces", "Namespace", False),
                ("nodes", "Node", False),
            ]
        ],
    }
    replies = {
        "/api": {"kind": "APIVersions", "versions": ["v1"]},
        "/apis": {"kind": "APIGroupList", "groups": []},
        "/api/v1": resources,
        "/api/v1/namespaces": {
            "kind": "NamespaceList",
            "apiVersion": "v1",
            "items": [
                {"metadata": {"name": name}}
                for name in ("app", "app-sdo", "default", "observe", "kube-system", "policy-system")
            ],
        },
    }

    class API(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path).path
            self.send_response(200 if path in replies else 404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(replies.get(path, {"kind": "Status", "status": "Failure", "reason": "NotFound"})).encode()
            )

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), API)
    thread = Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    path = tmp_path / "kubeconfig"
    path.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [{"name": "fixture", "cluster": {"server": f"http://127.0.0.1:{server.server_port}"}}],
                "contexts": [{"name": "fixture", "context": {"cluster": "fixture", "user": "fixture"}}],
                "current-context": "fixture",
                "users": [{"name": "fixture", "user": {}}],
            }
        )
    )
    try:
        yield path
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def test_permissive_configuration_matches_installer_and_rejects_extra_grant(tmp_path, discovery_kubeconfig):
    requested = replace(
        config(tmp_path, True), kubeconfig=discovery_kubeconfig, responder_namespaces=("policy-system",)
    )
    plan = installer_rbac("app", requested)
    rows = listing(plan)
    assert require_rbac_equality(plan, rows, "app")["passed"]
    # The exact grant from the live admission failure is legitimate only here.
    read = next(
        row
        for row in rows
        if row["kind"] == "RoleBinding"
        and row["metadata"]["namespace"] == "default"
        and row["roleRef"]["kind"] == "ClusterRole"
    )
    assert read["metadata"]["name"] == "app-sdo-responder-permissive-read"
    extra = deepcopy(read)
    extra["metadata"]["name"] = "unrequested"
    with pytest.raises(ValueError, match="unexpected grant"):
        require_rbac_equality(plan, [*rows, extra], "app")
    role = next(
        row for row in rows if row["kind"] == "ClusterRole" and row["metadata"]["name"] == read["roleRef"]["name"]
    )
    role["rules"].append({"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]})
    with pytest.raises(ValueError, match="rules for"):
        require_rbac_equality(plan, rows, "app")
