from benchmarks.sregym.adapter.fault_reversion import grade_faults
from benchmarks.sregym.protocol.injection import FaultComponent


def component(field=None, kind="Deployment", name="app", cls="LivenessProbeMisconfiguration"):
    return FaultComponent(
        fault=0,
        incident=0,
        problem_id="synthetic",
        injection_order=0,
        identity={"class": cls, "footprints": [{"kind": kind, "name": name, "field": field}]},
    )


def deployment(path):
    return {
        "kind": "Deployment",
        "metadata": {"name": "app"},
        "spec": {"template": {"spec": {"containers": [{"name": "app", "livenessProbe": {"httpGet": {"path": path}}}]}}},
    }


def test_probe_is_not_reverted_just_because_pods_are_ready():
    grade = grade_faults([component()], [deployment("/healthy")], [deployment("/bad")])
    assert grade[0].status == "not_reverted"


def test_unknown_and_absence_are_distinct():
    fault = component(kind="ConfigMap", cls="MissingConfigMap")
    baseline = [{"kind": "ConfigMap", "metadata": {"name": "app"}, "data": {"x": "y"}}]
    assert grade_faults([fault], baseline, None)[0].status == "unknown"
    assert grade_faults([fault], baseline, [])[0].status == "not_reverted"
    assert grade_faults([fault], baseline, baseline)[0].status == "reverted"


def test_target_port_ignores_server_metadata_and_unrelated_fields():
    fault = component(field="spec.ports.targetPort", kind="Service")
    baseline = [
        {
            "kind": "Service",
            "metadata": {"name": "app"},
            "spec": {"ports": [{"name": "http", "port": 80, "targetPort": 8080}]},
        }
    ]
    live = [
        {
            "kind": "Service",
            "metadata": {"name": "app", "resourceVersion": "99"},
            "spec": {"ports": [{"name": "http", "port": 80, "targetPort": 8080}], "clusterIP": "1.2.3.4"},
        }
    ]
    assert grade_faults([fault], baseline, live)[0].status == "reverted"
    live[0]["spec"]["ports"][0]["targetPort"] = 8888
    assert grade_faults([fault], baseline, live)[0].status == "not_reverted"


def test_unknown_footprint_is_never_credit():
    assert grade_faults([component(field="unrecognized")], [], [])[0].status == "unknown"


def test_injected_webhook_must_be_removed_even_when_existing_pods_are_ready():
    fault = component(kind="MutatingWebhookConfiguration", name="injected", cls="MutatingWebhookResourceLimits")
    live = [{"kind": "MutatingWebhookConfiguration", "metadata": {"name": "injected"}, "webhooks": []}]
    assert grade_faults([fault], [], live)[0].status == "not_reverted"
    assert grade_faults([fault], [], [])[0].status == "reverted"


def test_unlisted_kinds_cannot_be_proven_absent():
    assert grade_faults([component(kind="UnsupportedKind")], [], [])[0].status == "unknown"


def test_service_recreation_ignores_allocations_but_preserves_headless_and_ports():
    from copy import deepcopy

    fault = component(kind="Service", cls="MissingService")
    baseline = {
        "kind": "Service",
        "metadata": {"name": "app", "uid": "old"},
        "spec": {
            "clusterIP": "10.0.0.1",
            "clusterIPs": ["10.0.0.1"],
            "ports": [{"port": 80, "targetPort": 8080, "nodePort": 30001}],
        },
    }
    live = deepcopy(baseline)
    live["metadata"].update(uid="new", resourceVersion="3")
    live["spec"].update(clusterIP="10.0.0.2", clusterIPs=["10.0.0.2"])
    live["spec"]["ports"][0]["nodePort"] = 30002
    assert grade_faults([fault], [baseline], [live])[0].status == "reverted"
    live["spec"]["ports"][0]["targetPort"] = 9000
    assert grade_faults([fault], [baseline], [live])[0].status == "not_reverted"
    baseline["spec"]["clusterIP"] = "None"
    assert grade_faults([fault], [baseline], [live])[0].status == "not_reverted"
