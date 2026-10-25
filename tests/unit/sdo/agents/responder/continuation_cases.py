"""Hand-written synthetic continuation with the Gate 2 receipt shape."""

from typing import Any


def action_id(index: int) -> str:
    return f"sdo-act-12345678-1234-4321-8123-{index:012d}"


def ten_receipt_continuation() -> dict[str, Any]:
    resources = [
        {"api_version": "v1", "kind": "ServiceAccount", "namespace": "hotel", "name": "cleanup"},
        {"api_version": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "name": "cleanup"},
        {"api_version": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "name": "cleanup"},
    ]
    findings = [
        {
            "primary_resource": resource,
            "fingerprint": f"source-drift/unexplained/cleanup-{index}-" + "a" * 64,
            "detector_id": "source-drift",
            "rule_id": "unexplained",
            "summary": "source-absent cleanup object",
            "evidence": "fresh inventory; " * 40,
        }
        for index, resource in enumerate(resources)
    ]
    actions = [
        ("repair-0", "restore route", True),
        ("repair-1", "restore database user", True),
        ("repair-2", "remove stale deployment", True),
        ("repair-3", "remove script", True),
        ("repair-4", "restore labels", True),
        ("repair-5", "restore annotations", True),
        ("repair-6", "restore replicas", True),
        ("repair-7", "restore service port", True),
        ("repair-8", "remove cleanup account", False),
        ("repair-9", "remove cleanup role", False),
    ]
    return {
        "attempt": 4,
        "healthy": False,
        "summary": "three source inventory findings remain",
        "still_broken": findings,
        "already_done": [
            {
                "kind": "program-application" if index < 4 else "recorded-action",
                "action_id": action_id(index),
                **(
                    {"application_id": f"application-{index}", "program_ids": [f"restore-component-{index}"]}
                    if index < 4
                    else {}
                ),
                "summary": summary,
                "error": "Forbidden" if not success else "",
                "receipt": {
                    "action_id": action_id(index),
                    "kind": "program-plan" if index < 4 else "kubectl",
                    "summary": summary,
                    "resources": [resources[index - 8]]
                    if index >= 8
                    else [{"api_version": "v1", "kind": "Service", "namespace": "hotel", "name": f"route-{index}"}],
                    "details": "synthetic typed mutation and readback evidence; " * 25,
                    "target": "Service/route",
                    "started_at": f"2026-01-01T00:00:{index:02d}Z",
                    "completed_at": f"2026-01-01T00:01:{index:02d}Z",
                    "success": success,
                    "reversible": False,
                },
            }
            for index, (_, summary, success) in enumerate(actions)
        ],
    }
