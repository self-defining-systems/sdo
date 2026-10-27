"""Exact responder grants compared to the production installer's resource plan."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import NativeInstallConfig, WaitKind, kubectl, wait_for


def canonical_rules(rules: list[dict[str, Any]]) -> list[str]:
    return sorted(
        json.dumps({key: sorted(value) for key, value in rule.items() if value}, sort_keys=True) for rule in rules
    )


def require_rbac_equality(
    expected: dict[str, Any], listing: list[dict[str, Any]], namespace: str, control_namespace: str | None = None
) -> dict[str, Any]:
    control = control_namespace or namespace + "-sdo"

    def bound(row: dict[str, Any]) -> bool:
        return any(
            subject.get("kind") == "ServiceAccount"
            and subject.get("name") == "sdo-responder"
            and subject.get("namespace", row["metadata"].get("namespace")) == control
            for subject in row.get("subjects", [])
        )

    def identity(row: dict[str, Any], kind: str) -> tuple[str, str, str]:
        return kind, row["metadata"].get("namespace", ""), row["metadata"]["name"]

    def binding_content(row: dict[str, Any]) -> tuple[Any, list[str]]:
        subjects = sorted(
            json.dumps({key: value for key, value in subject.items() if value}, sort_keys=True)
            for subject in row.get("subjects", [])
        )
        return row["roleRef"], subjects

    bindings = {}
    roles = {}
    for key, kind in (
        ("ControlBindings", "RoleBinding"),
        ("AppBindings", "RoleBinding"),
        ("ClusterBindings", "ClusterRoleBinding"),
        ("ObservationBindings", "RoleBinding"),
        ("ObservationClusterBindings", "ClusterRoleBinding"),
    ):
        bindings.update({identity(row, kind): row for row in expected.get(key, []) or [] if bound(row)})
    for key, kind in (
        ("ControlRoles", "Role"),
        ("AppRoles", "Role"),
        ("ClusterRoles", "ClusterRole"),
        ("ObservationRoles", "Role"),
        ("ObservationClusterRoles", "ClusterRole"),
    ):
        roles.update({identity(row, kind): row for row in expected.get(key, []) or []})
    actual_bindings = {
        identity(row, row["kind"]): row
        for row in listing
        if row["kind"] in {"RoleBinding", "ClusterRoleBinding"} and bound(row)
    }
    for key, row in actual_bindings.items():
        if key not in bindings or binding_content(row) != binding_content(bindings[key]):
            raise ValueError(f"eval: responder RBAC differs from installer: unexpected grant {key}")
    if actual_bindings.keys() != bindings.keys():
        raise ValueError("eval: responder RBAC differs from installer: missing binding")
    actual_roles = {identity(row, row["kind"]): row for row in listing if row["kind"] in {"Role", "ClusterRole"}}
    selected = set()
    for key, binding in bindings.items():
        ref = binding["roleRef"]
        role_key = (ref["kind"], key[1] if ref["kind"] == "Role" else "", ref["name"])
        if (
            role_key not in roles
            or role_key not in actual_roles
            or canonical_rules(actual_roles[role_key].get("rules", []))
            != canonical_rules(roles[role_key].get("rules", []))
        ):
            raise ValueError(f"eval: responder RBAC differs from installer: rules for {role_key}")
        selected.add(role_key)
    return {
        "passed": True,
        "scope": "exact responder roles and all bound grants",
        "roles": [[ns, name] for _, ns, name in sorted(selected)],
    }


def installer_rbac(namespace: str, config: NativeInstallConfig | None = None) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[3]
    if config is None:
        config = NativeInstallConfig(
            repository=root,
            namespace=namespace,
            control_namespace=namespace + "-sdo",
            application="eval-dry-run",
            controller_image="controller:test",
            responder_image="responder:test",
            validator_image="validator:test",
            repository_pvc="sdo-application-repository",
            credentials_secret="unused",
            model="scripted",
        )
    if config.namespace != namespace:
        raise ValueError("RBAC installer configuration namespace differs from admission namespace")
    result = subprocess.run(
        ["go", "run", str(Path(__file__).with_name("rbac_go") / "main.go"), "install-existing", *config.common_args()],
        cwd=root / "sdo/operator",
        capture_output=True,
        text=True,
        check=True,
        timeout=wait_for(WaitKind.COMMAND),
    )
    return json.loads(result.stdout)


def verify_rbac(kubeconfig: Path, namespace: str, config: NativeInstallConfig | None = None) -> dict[str, Any]:
    if config is not None and config.kubeconfig != kubeconfig:
        raise ValueError("RBAC installer configuration kubeconfig differs from admission kubeconfig")
    listing = []
    for resources, scope in (("roles,rolebindings", ["--all-namespaces"]), ("clusterroles,clusterrolebindings", [])):
        listing.extend(
            json.loads(kubectl(["get", resources, *scope, "-o", "json"], namespace=None, kubeconfig=kubeconfig).stdout)[
                "items"
            ]
        )
    return require_rbac_equality(
        installer_rbac(namespace, config), listing, namespace, config.control_namespace if config is not None else None
    )


def reset_responder_grants(kubeconfig: Path, namespace: str) -> None:
    """Fresh warm-lane reset retires previous run authority before reinstall.

    Grant identity is the exact lane responder ServiceAccount, never a name
    prefix alone. The installer recreates the current run's declared grants.
    """
    control = namespace + "-sdo"
    seen = set()
    for resources, scope in (("rolebindings", ["--all-namespaces"]), ("clusterrolebindings", [])):
        rows = json.loads(
            kubectl(["get", resources, *scope, "-o", "json"], namespace=None, kubeconfig=kubeconfig).stdout
        )["items"]
        for row in rows:
            if not any(
                subject.get("kind") == "ServiceAccount"
                and subject.get("name") == "sdo-responder"
                and subject.get("namespace", row["metadata"].get("namespace")) == control
                for subject in row.get("subjects", [])
            ):
                continue
            key = (row["kind"], row["metadata"].get("namespace"), row["metadata"]["name"])
            if key in seen:
                continue
            seen.add(key)
            kubectl(
                ["delete", f"{key[0]}/{key[2]}", "--ignore-not-found=true", "--wait=true", "--timeout=20s"],
                namespace=key[1],
                kubeconfig=kubeconfig,
            )
