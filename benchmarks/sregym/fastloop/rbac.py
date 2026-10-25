"""Exact responder grants compared to the production installer's resource plan."""

from __future__ import annotations

import json
import subprocess
from functools import lru_cache
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import kubectl


def canonical_rules(rules: list[dict[str, Any]]) -> list[str]:
    return sorted(
        json.dumps({key: sorted(value) for key, value in rule.items() if value}, sort_keys=True) for rule in rules
    )


def require_rbac_equality(expected: dict[str, Any], listing: list[dict[str, Any]], namespace: str) -> dict[str, Any]:
    control = namespace + "-sdo"
    roles = {
        (row["metadata"]["namespace"], row["metadata"]["name"]): row
        for key in ("ControlRoles", "AppRoles")
        for row in expected[key]
        if row["metadata"]["name"] == "sdo-responder"
    }
    bindings = {}
    for row in listing:
        if row["kind"] not in {"RoleBinding", "ClusterRoleBinding"}:
            continue
        if not any(
            subject.get("kind") == "ServiceAccount"
            and subject.get("name") == "sdo-responder"
            and subject.get("namespace", row["metadata"].get("namespace")) == control
            for subject in row.get("subjects", [])
        ):
            continue
        metadata = row["metadata"]
        key = (metadata.get("namespace", ""), metadata["name"])
        ref = row["roleRef"]
        if key not in roles or ref != {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": "sdo-responder",
        }:
            raise ValueError(f"eval: responder RBAC differs from installer: unexpected grant {key}")
        bindings[key] = row
    if set(bindings) != set(roles):
        raise ValueError("eval: responder RBAC differs from installer: missing binding")
    actual = {
        (row["metadata"].get("namespace"), row["metadata"]["name"]): row for row in listing if row["kind"] == "Role"
    }
    for key, role in roles.items():
        if key not in actual or canonical_rules(actual[key].get("rules", [])) != canonical_rules(role.get("rules", [])):
            raise ValueError(f"eval: responder RBAC differs from installer: rules for {key}")
    return {
        "passed": True,
        "scope": "exact responder roles and all bound grants",
        "roles": [list(key) for key in roles],
    }


@lru_cache(maxsize=8)
def installer_rbac(namespace: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        ["go", "run", str(Path(__file__).with_name("rbac_go") / "main.go"), namespace],
        cwd=root / "sdo/operator",
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return json.loads(result.stdout)


def verify_rbac(kubeconfig: Path, namespace: str) -> dict[str, Any]:
    listing = []
    for resources, scope in (("roles,rolebindings", ["--all-namespaces"]), ("clusterrolebindings", [])):
        listing.extend(
            json.loads(kubectl(["get", resources, *scope, "-o", "json"], namespace=None, kubeconfig=kubeconfig).stdout)[
                "items"
            ]
        )
    return require_rbac_equality(installer_rbac(namespace), listing, namespace)


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
