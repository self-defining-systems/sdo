"""The responder write class check: a fault object the responder cannot write is a gap."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import responder_write_check as check

PLAN = [
    {
        "id": "row",
        "draws": [
            "FaultDraw(key='namespace_memory_limit', app='hotel_reservation', params=(), footprints=("
            "Footprint(kind='ResourceQuota', name='memory-limit-quota', field=None), "
            "Footprint(kind='Deployment', name='frontend', field=None)), tier=<Tier.ORDINARY: 1>)",
            "FaultDraw(key='webhook_tls', app='astronomy_shop', params=(), footprints=("
            "Footprint(kind='MutatingWebhookConfiguration', name='pod-policy', field=None), "
            "Footprint(kind='Namespace', name='policy-system', field=None)), tier=<Tier.BLOCKS_ADMISSION: 2>)",
        ],
    }
]


def api_server(rules: list[dict]):
    """A kubectl stand-in whose SubjectAccessReview answers come from Role rules."""

    def run(args, *, namespace, kubeconfig, input_text):
        assert args[:2] == ["create", "-f"]
        assert namespace is None
        spec = json.loads(input_text)["spec"]
        assert spec["user"] == "system:serviceaccount:app-sdo:sdo-responder"
        attrs = spec["resourceAttributes"]
        allowed = any(
            attrs["group"] in rule["groups"]
            and attrs["resource"] in rule["resources"]
            and attrs["verb"] in rule["verbs"]
            and (not rule.get("names") or attrs.get("name") in rule["names"])
            for rule in rules
        )
        return SimpleNamespace(stdout=json.dumps({"status": {"allowed": allowed}}))

    return run


def rules(resources: list[tuple[str, str]], verbs=check.WRITE_VERBS, names: list[str] | None = None) -> list[dict]:
    return [
        {"groups": [group], "resources": [resource], "verbs": list(verbs), "names": names or []}
        for group, resource in resources
    ]


def plan_file(tmp_path: Path) -> Path:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(PLAN))
    return path


def asks(rule_set: list[dict]) -> check.Allowed:
    return check.subject_access_review(Path("/dev/null"), control_namespace="app-sdo", run=api_server(rule_set))


NAMESPACE_EDITS = rules([("", "namespaces")], verbs=("update", "patch"), names=["policy-system"])


def granted_without_quota() -> list[dict]:
    granted = rules([("apps", "deployments"), ("admissionregistration.k8s.io", "mutatingwebhookconfigurations")])
    return granted + NAMESPACE_EDITS + rules([("", "resourcequotas")], verbs=("get", "list", "watch"))


def test_missing_quota_write_is_a_reported_gap(tmp_path):
    rows = check.check(check.fault_objects(plan_file(tmp_path)), asks(granted_without_quota()))
    assert [(row.kind, row.namespace, row.denied_verbs) for row in check.gaps(rows)] == [
        ("ResourceQuota", "hotel-reservation", check.WRITE_VERBS)
    ]
    assert "namespace_memory_limit" in check.render(rows)


def test_granting_quota_writes_clears_the_gap(tmp_path):
    granted = granted_without_quota() + rules([("", "resourcequotas")])
    assert check.gaps(check.check(check.fault_objects(plan_file(tmp_path)), asks(granted))) == []


def test_namespace_edit_grant_is_checked_by_name(tmp_path):
    objects = check.fault_objects(plan_file(tmp_path))
    full = rules(
        [
            ("apps", "deployments"),
            ("", "resourcequotas"),
            ("admissionregistration.k8s.io", "mutatingwebhookconfigurations"),
        ]
    )
    elsewhere = rules([("", "namespaces")], verbs=("update", "patch"), names=["other"])
    gap = check.gaps(check.check(objects, asks(full + elsewhere)))
    assert [(row.kind, row.name, row.denied_verbs) for row in gap] == [
        ("Namespace", "policy-system", ("update", "patch"))
    ]


def test_withheld_kinds_are_listed_apart_from_gaps(tmp_path):
    path = tmp_path / "plan.json"
    path.write_text(
        json.dumps(
            [
                {
                    "draws": [
                        "FaultDraw(key='rotation', app='astronomy_shop', params=(), "
                        "footprints=(Footprint(kind='Secret', name='s', field=None),))"
                    ]
                }
            ]
        )
    )
    rows = check.check(check.fault_objects(path), asks([]))
    assert [(row.kind, row.status) for row in rows] == [("Secret", "withheld")]
    assert check.gaps(rows) == []


def test_cluster_scoped_kinds_are_asked_without_a_namespace(tmp_path):
    seen = []

    def run(args, *, namespace, kubeconfig, input_text):
        seen.append(json.loads(input_text)["spec"]["resourceAttributes"])
        return SimpleNamespace(stdout=json.dumps({"status": {"allowed": True}}))

    allowed = check.subject_access_review(Path("/dev/null"), control_namespace="app-sdo", run=run)
    check.check(check.fault_objects(plan_file(tmp_path)), allowed)
    webhook = [a for a in seen if a["resource"] == "mutatingwebhookconfigurations"]
    assert webhook
    assert all("namespace" not in a for a in webhook)


def test_fault_kind_without_a_grant_decision_is_refused():
    with pytest.raises(ValueError, match="no responder grant decision"):
        check.FaultObject("f", "hotel_reservation", "PodDisruptionBudgetX", "x")
