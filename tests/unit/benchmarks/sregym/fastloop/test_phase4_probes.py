"""Object-state regressions for Phase 4, using hand-written API responses."""

from copy import deepcopy

import pytest

from benchmarks.sregym.fastloop.fault_tracker import probes_for
from benchmarks.sregym.faults.description import FaultSpec, validate_suite
from benchmarks.sregym.faults.worker import describe_problem


def deployment(**pod_fields):
    return {
        "metadata": {"generation": 2},
        "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "app", "image": "good"}], **pod_fields}}},
        "status": {"observedGeneration": 2, "readyReplicas": 1, "updatedReplicas": 1, "replicas": 1},
    }


# Each example names the real injected surface, independently of the probe registry.
SPEC_CASES = [
    ("AssignNonExistentNode", {"nodeSelector": {"kubernetes.io/hostname": "extra-node"}}),
    ("WrongDNSPolicy", {"dnsPolicy": "Default", "dnsConfig": {"nameservers": ["8.8.8.8"]}}),
    (
        "InitContainerDependencyHang",
        {"initContainers": [{"name": "dependency-check", "command": ["sleep", "infinity"]}]},
    ),
    ("PVCClaimMismatch", {"volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "missing"}}]}),
    ("DuplicatePVCMounts", {"volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "shared"}}]}),
    (
        "SidecarPortConflict",
        {"containers": [{"name": "app", "image": "good"}, {"name": "sidecar", "command": ["nc", "-lk", "-p", "8080"]}]},
    ),
    (
        "ServicePortConflict",
        {"containers": [{"name": "app", "image": "good", "ports": [{"containerPort": 8080, "hostPort": 9100}]}]},
    ),
    ("WrongBinUsage", {"containers": [{"name": "app", "image": "good", "command": ["/missing/binary"]}]}),
    (
        "EnvVariableShadowing",
        {"containers": [{"name": "app", "image": "good", "env": [{"name": "PORT", "value": "9999"}]}]},
    ),
    (
        "MissingEnvVariable",
        {"containers": [{"name": "app", "image": "good", "env": [{"name": "RECOMMENDATION_ADDR", "value": ""}]}]},
    ),
    (
        "IncorrectPortAssignment",
        {
            "containers": [
                {"name": "app", "image": "good", "env": [{"name": "CURRENCY_ADDR", "value": "currency:9090"}]}
            ]
        },
    ),
    (
        "LivenessProbeMisconfiguration",
        {"containers": [{"name": "app", "image": "good", "livenessProbe": {"httpGet": {"port": 9999}}}]},
    ),
    (
        "LivenessProbeTooAggressive",
        {"containers": [{"name": "app", "image": "good", "livenessProbe": {"periodSeconds": 1}}]},
    ),
    ("MisconfigAppHotelRes", {"containers": [{"name": "app", "image": "buggy"}]}),
    ("UpdateIncompatibleCorrelated", {"containers": [{"name": "app", "image": "incompatible"}]}),
    (
        "PodAntiAffinityDeadlock",
        {"affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{}]}}},
    ),
    ("RollingUpdateMisconfigured", {"initContainers": [{"name": "hang-init", "command": ["sleep", "infinity"]}]}),
]


@pytest.mark.parametrize(("class_name", "patch"), SPEC_CASES)
def test_spec_probe_observes_fault_then_restoration(class_name, patch):
    part = type(class_name, (), {"faulty_service": "app"})()
    description = describe_problem("fixture", part)
    assert description.tracking_supported
    fault = description.faults[0]
    good = deployment()
    if class_name == "MissingEnvVariable":
        good["spec"]["template"]["spec"]["containers"][0]["env"] = [
            {"name": "RECOMMENDATION_ADDR", "value": "recommendation:8080"}
        ]
        patch = {"containers": [{"name": "app", "image": "good", "env": []}]}
    cluster = {("deployment", "app"): deepcopy(good)}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    bad = deployment(**patch)
    cluster[("deployment", "app")] = bad
    assert probe() is False  # Ready alone cannot prove a field fault is repaired.
    cluster[("deployment", "app")] = deepcopy(good)
    assert probe() is True
    cluster[("deployment", "app")] = None
    assert probe() is False


@pytest.mark.parametrize(
    "class_name",
    ["AdmissionWebhookTLSMismatch", "AdmissionWebhookOutage", "PSARestrictedBlocksRecreation", "NamespaceMemoryLimit"],
)
def test_admission_probe_waits_for_current_generation_replicas(class_name):
    fault = describe_problem("fixture", type(class_name, (), {"faulty_service": "app"})()).faults[0]
    assert fault.tracking_supported
    obj = deployment()
    obj["status"]["readyReplicas"] = 0
    probe = probes_for((fault,), lambda kind, name: obj)[fault.name]
    assert probe() is False
    obj["status"]["readyReplicas"] = 1
    obj["status"]["observedGeneration"] = 1
    assert probe() is False
    obj["status"]["observedGeneration"] = 2
    assert probe() is True
    obj["spec"]["replicas"] = 0
    assert probe() is False


FLAG_CASES = [
    ("LoadGeneratorFloodHomepage", "loadGeneratorFloodHomepage"),
    ("AdServiceManualGc", "adManualGc"),
    ("CartServiceFailure", "cartFailure"),
    ("ImageSlowLoad", "imageSlowLoad"),
    ("PaymentServiceFailure", "paymentFailure"),
    ("PaymentServiceUnreachable", "paymentUnreachable"),
    ("ProductCatalogServiceFailure", "productCatalogFailure"),
]


@pytest.mark.parametrize(("class_name", "flag"), FLAG_CASES)
def test_feature_flags_are_probed_independently(class_name, flag):
    import json

    fault = describe_problem("fixture", type(class_name, (), {"faulty_service": "app"})()).faults[0]
    assert fault.tracking_supported
    flags = {flag: {"defaultVariant": "on"}, "unrelated": {"defaultVariant": "on"}}

    def read(kind, name):
        assert (kind, name) == ("configmap", "flagd-config")
        return {"data": {"demo.flagd.json": json.dumps({"flags": flags})}}

    probe = probes_for((fault,), read)[fault.name]
    assert probe() is False
    flags[flag]["defaultVariant"] = "off"
    assert probe() is True
    del flags[flag]
    assert probe() is False


def test_multi_target_fault_is_one_probe_and_requires_every_target():
    part = type("UpdateIncompatibleCorrelated", (), {"faulty_services": ["db-a", "db-b"]})()
    description = describe_problem("pair", part)
    assert description.tracking_supported
    fault = description.faults[0]
    assert fault.target_components == ("db-a", "db-b")
    cluster = {("deployment", name): deployment() for name in fault.target_components}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    cluster[("deployment", "db-b")] = deployment(containers=[{"name": "app", "image": "bad"}])
    assert probe() is False
    cluster[("deployment", "db-b")] = deployment()
    assert probe() is True
    assert len(probes_for((fault,), lambda kind, name: cluster.get((kind, name)))) == 1


def test_spec_capture_missing_baseline_refuses_before_injection():
    fault = describe_problem("fixture", type("AssignNonExistentNode", (), {"faulty_service": "app"})()).faults[0]
    with pytest.raises(ValueError, match="baseline.*app"):
        probes_for((fault,), lambda kind, name: None)


def test_tracking_validation_rechecks_kind_even_if_metadata_claims_support():
    fault = FaultSpec("future_fault", "app", tracking_supported=False, tracking_reason="not implemented")
    object.__setattr__(fault, "tracking_supported", True)
    from benchmarks.sregym.faults.description import ProblemDescription

    description = ProblemDescription(problem_id="future", faults=(fault,), injection_order=(0,))
    with pytest.raises(ValueError, match="future_fault"):
        validate_suite({"future": description}, require_tracking=True, capability_mode="permissive")


def test_missing_service_needs_the_object_and_ready_endpoints():
    fault = describe_problem("fixture", type("MissingService", (), {"faulty_service": "app"})()).faults[0]
    cluster = {("service", "app"): None, ("endpoints", "app"): {"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]}}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    cluster[("service", "app")] = {"spec": {"ports": [{"port": 80}]}}
    assert probe() is True
    cluster[("endpoints", "app")] = {"subsets": []}
    assert probe() is False


@pytest.mark.parametrize(
    ("class_name", "flag", "component", "field"),
    [
        ("AdServiceHighCpu", "adHighCpu", "ad", "resources"),
        ("FailedReadinessProbe", "failedReadinessProbe", "cart", "readinessProbe"),
    ],
)
def test_flag_and_extra_spec_fault_both_need_repair(class_name, flag, component, field):
    import json

    fault = describe_problem("fixture", type(class_name, (), {"faulty_service": component})()).faults[0]
    assert fault.tracking_supported
    obj = deployment()
    flags = {flag: {"defaultVariant": "off"}}

    def read(kind, name):
        if (kind, name) == ("deployment", component):
            return obj
        assert (kind, name) == ("configmap", "flagd-config")
        return {"data": {"demo.flagd.json": json.dumps({"flags": flags})}}

    probe = probes_for((fault,), read)[fault.name]
    obj["spec"]["template"]["spec"]["containers"][0][field] = (
        {"limits": {"cpu": "100m"}} if field == "resources" else {"grpc": {"port": 9999}}
    )
    flags[flag]["defaultVariant"] = "on"
    assert probe() is False
    flags[flag]["defaultVariant"] = "off"
    assert probe() is False
    del obj["spec"]["template"]["spec"]["containers"][0][field]
    assert probe() is True


def test_internal_traffic_policy_cannot_use_ready_endpoints_as_proof():
    fault = describe_problem(
        "fixture", type("InternalTrafficPolicyLocalAstronomyShop", (), {"faulty_service": "app"})()
    ).faults[0]
    assert fault.tracking_supported
    obj = {"spec": {"internalTrafficPolicy": "Local"}}
    cluster = {("service", "app"): obj, ("endpoints", "app"): {"subsets": [{"addresses": [{"ip": "1.2.3.4"}]}]}}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    obj["spec"]["internalTrafficPolicy"] = "Cluster"
    assert probe() is True


def test_dns_egress_policy_needs_both_udp_and_tcp_53_without_destination_restrictions():
    part = type(
        "ServiceDNSResolutionFailure", (), {"faulty_service": "app", "dns_failure_layer": "network_policy_block_dns"}
    )()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    policy = {"spec": {"policyTypes": ["Egress"], "egress": []}}
    cluster = {("networkpolicy", "deny-dns-egress-app"): policy}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    policy["spec"]["egress"] = [{"ports": [{"port": 53, "protocol": "UDP"}]}]
    assert probe() is False
    policy["spec"]["egress"][0]["ports"].append({"port": 53, "protocol": "TCP"})
    assert probe() is True
    policy["spec"]["egress"][0]["to"] = [{"ipBlock": {"cidr": "127.0.0.1/32"}}]
    assert probe() is False
    cluster[("networkpolicy", "deny-dns-egress-app")] = None
    assert probe() is True


def test_coredns_template_variant_remains_explicitly_unsupported():
    part = type("ServiceDNSResolutionFailure", (), {"faulty_service": "app", "dns_failure_layer": "coredns_template"})()
    assert not describe_problem("fixture", part).tracking_supported


def test_ingress_probe_checks_the_selected_route_and_all_matching_rules():
    part = type(
        "IngressMisroute",
        (),
        {
            "faulty_service": ["frontend", "wrong"],
            "ingress_name": "gateway",
            "path": "/api",
            "correct_service": "frontend",
        },
    )()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    route = {"path": "/api(/|$)(.*)", "backend": {"service": {"name": "wrong", "port": {"number": 80}}}}
    obj = {"spec": {"rules": [{"http": {"paths": [route]}}]}}

    def read(kind, name):
        assert (kind, name) == ("ingress", "gateway")
        return obj

    probe = probes_for((fault,), read)[fault.name]
    assert probe() is False
    route["backend"]["service"]["name"] = "frontend"
    assert probe() is True
    obj["spec"]["rules"].append({"http": {"paths": [{"path": "/api", "backend": {"service": {"name": "wrong"}}}]}})
    assert probe() is False
    obj["spec"]["rules"] = []
    assert probe() is False


def test_offline_plan_check_rejects_unprobed_fault_without_any_lane_access(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import cli

    good = describe_problem("good", type("IncorrectImage", (), {"faulty_service": "app"})())
    bad = describe_problem("future", type("FutureFault", (), {"faulty_service": "app"})())
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.probe_coverage.describe_sregym_problems",
        lambda *args, **kwargs: {"good": good, "future": bad},
    )
    monkeypatch.setattr(cli, "up_preflight", lambda *_: pytest.fail("must reject before host or lane access"))
    plan = tmp_path / "plan.json"
    plan.write_text('[{"sequence":0,"problem_id":"good"},{"sequence":1,"problem_id":"future"}]')
    with pytest.raises(ValueError, match="future.*FutureFault"):
        cli.main(["check-plan", "--plan", str(plan)])
    with pytest.raises(ValueError, match="future.*FutureFault"):
        cli.main(["preflight", "--plan", str(plan)])


def test_offline_plan_check_accepts_supported_stream(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import cli

    good = describe_problem("good", type("IncorrectImage", (), {"faulty_service": "app"})())
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.probe_coverage.describe_sregym_problems", lambda *args, **kwargs: {"good": good}
    )
    stream = tmp_path / "stream.json"
    stream.write_text('{"incidents":["good","good"],"capability_mode":"permissive"}')
    assert cli.main(["check-plan", "--plan", str(stream)]) == 0


def test_hpa_needs_fresh_computable_cpu_metric_not_just_ready_workload():
    part = type("HPAMissingEffectiveCPURequest", (), {"faulty_service": "app", "HPA_NAME": "capacity"})()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    obj = deployment(containers=[{"name": "app", "resources": {"requests": {}}}])
    hpa = {
        "metadata": {"generation": 2},
        "spec": {"scaleTargetRef": {"kind": "Deployment", "name": "app"}},
        "status": {
            "observedGeneration": 2,
            "conditions": [{"type": "ScalingActive", "status": "False", "reason": "FailedGetResourceMetric"}],
        },
    }
    cluster = {("deployment", "app"): obj, ("hpa", "capacity"): hpa}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    obj["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "100m"
    assert probe() is False
    hpa["status"]["conditions"][0]["status"] = "True"
    hpa["status"]["currentMetrics"] = [
        {"type": "Resource", "resource": {"name": "cpu", "current": {"averageUtilization": 10}}}
    ]
    assert probe() is True
    hpa["status"]["observedGeneration"] = 1
    assert probe() is False
    hpa["status"]["observedGeneration"] = 2
    del hpa["status"]["currentMetrics"]
    assert probe() is False


def test_shared_memory_probe_tracks_the_worker_and_rejects_undersized_mount():
    part = type("DevShmExhaustionHotelReservation", (), {"worker_name": "media-processor", "scratch_mib": 128})()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.component == "media-processor"
    assert fault.tracking_supported
    obj = deployment(containers=[{"name": "worker", "image": "busybox:1.36"}])
    cluster = {("deployment", "media-processor"): obj}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    spec = obj["spec"]["template"]["spec"]
    spec["containers"][0]["volumeMounts"] = [{"name": "shm", "mountPath": "/dev/shm"}]
    spec["volumes"] = [{"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"}}]
    assert probe() is False
    spec["volumes"][0]["emptyDir"]["sizeLimit"] = "256Mi"
    assert probe() is True
    cluster[("deployment", "media-processor")] = None
    assert probe() is False  # Removing the worker is not a repair.


def test_edge_filter_restoration_is_not_confused_by_ready_replicas():
    part = type("EdgeRequestFilterCPUSaturation", (), {"faulty_service": "proxy"})()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    obj = deployment()
    probe = probes_for((fault,), lambda kind, name: obj)[fault.name]
    obj["spec"]["template"]["spec"]["containers"][0]["command"] = ["perl", "-e", "bad filter"]
    assert probe() is False
    del obj["spec"]["template"]["spec"]["containers"][0]["command"]
    assert probe() is True


def test_finalizer_requires_live_controller_and_repaired_authority_even_after_token_deletion():
    part = type(
        "FinalizerDeadlockController",
        (),
        {
            "faulty_service": "token",
            "namespace": "app-ns",
            "controller_name": "cleanup",
            "sa_name": "cleanup-sa",
            "clusterrole_name": "cleanup-role",
            "clusterrolebinding_name": "cleanup-binding",
        },
    )()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    cm = {"metadata": {"deletionTimestamp": "now", "finalizers": ["cleanup"]}}
    role = {"rules": [{"apiGroups": [""], "resources": ["configmaps"], "verbs": ["get", "patch", "update"]}]}
    binding = {
        "roleRef": {"kind": "ClusterRole", "name": "cleanup-role"},
        "subjects": [{"kind": "ServiceAccount", "name": "cleanup-sa", "namespace": "app-ns"}],
    }
    cluster = {
        ("deployment", "cleanup"): deployment(serviceAccountName="cleanup-sa"),
        ("serviceaccount", "cleanup-sa"): {"metadata": {"name": "cleanup-sa"}},
        ("configmap", "token"): cm,
        ("clusterrole", "cleanup-role"): role,
        ("clusterrolebinding", "cleanup-binding"): binding,
    }
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    cluster[("configmap", "token")] = None
    assert probe() is False
    role["rules"].append({"apiGroups": [""], "resources": ["configmaps/finalizers"], "verbs": ["patch", "update"]})
    assert probe() is True
    cluster[("deployment", "cleanup")]["spec"]["template"]["spec"]["serviceAccountName"] = "other-sa"
    assert probe() is False
    cluster[("deployment", "cleanup")]["spec"]["template"]["spec"]["serviceAccountName"] = "cleanup-sa"
    cluster[("serviceaccount", "cleanup-sa")] = None
    assert probe() is False
    cluster[("serviceaccount", "cleanup-sa")] = {"metadata": {"name": "cleanup-sa"}}
    role["rules"][1]["resourceNames"] = ["other-token"]
    assert probe() is False
    role["rules"][1]["resourceNames"] = ["token"]
    assert probe() is True
    cluster[("deployment", "cleanup")] = None
    assert probe() is False


def test_cronjob_requires_native_sidecar_and_completion_evidence_without_stuck_legacy_jobs():
    part = type(
        "CronJobSidecarBlocksCompletionHotelReservation",
        (),
        {
            "faulty_service": "archiver",
            "cronjob_name": "archiver",
            "PRIMARY_CONTAINER": "main",
            "SIDECAR_CONTAINER": "shipper",
        },
    )()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    podspec = {"containers": [{"name": "main"}, {"name": "shipper", "command": ["sleep", "infinity"]}]}
    cronjob = {"metadata": {"uid": "cj1"}, "spec": {"jobTemplate": {"spec": {"template": {"spec": podspec}}}}}
    legacy_job = {
        "metadata": {"ownerReferences": [{"kind": "CronJob", "name": "archiver", "uid": "cj1"}]},
        "spec": {"template": {"spec": deepcopy(podspec)}},
        "status": {"active": 1},
    }
    jobs = {"items": [legacy_job]}
    cluster = {("cronjob", "archiver"): cronjob, ("jobs", ""): jobs}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    assert probe() is False
    podspec["containers"] = [{"name": "main"}]
    podspec["initContainers"] = [{"name": "shipper", "restartPolicy": "Always"}]
    assert probe() is False
    completed = {
        "metadata": {"ownerReferences": [{"kind": "CronJob", "name": "archiver", "uid": "cj1"}]},
        "spec": {"template": {"spec": deepcopy(podspec)}},
        "status": {"conditions": [{"type": "Complete", "status": "True"}]},
    }
    jobs["items"] = [completed, legacy_job]
    assert probe() is False
    jobs["items"] = [completed]
    assert probe() is True
    cronjob["spec"]["suspend"] = True
    assert probe() is False


def test_webhook_mutation_needs_actual_admitted_pod_resources_restored():
    part = type("MutatingWebhookResourceLimits", (), {"faulty_service": "app"})()
    fault = describe_problem("fixture", part).faults[0]
    assert fault.tracking_supported
    obj = deployment(
        containers=[{"name": "app", "resources": {"requests": {"memory": "128Mi"}, "limits": {"memory": "256Mi"}}}]
    )
    obj["spec"]["selector"] = {"matchLabels": {"app": "app"}}
    pod = {
        "metadata": {"labels": {"app": "app"}},
        "spec": deepcopy(obj["spec"]["template"]["spec"]),
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }
    pods = {"items": [pod]}
    cluster = {("deployment", "app"): obj, ("pods", ""): pods}
    probe = probes_for((fault,), lambda kind, name: cluster.get((kind, name)))[fault.name]
    pod["spec"]["containers"][0]["resources"]["limits"]["memory"] = "16Mi"
    assert probe() is False  # Deployment template alone still looks healthy.
    pod["spec"]["containers"][0]["resources"]["limits"]["memory"] = "256Mi"
    assert probe() is True
    pod["metadata"]["labels"] = {"app": "other"}
    assert probe() is False


@pytest.mark.parametrize("kind", ["Secret", "secret", "secrets", "secrets.v1"])
def test_kubectl_reader_never_reads_secret_data(kind, monkeypatch):
    from benchmarks.sregym.fastloop.fault_tracker import KubectlReader

    monkeypatch.setattr("subprocess.run", lambda *_args, **_kwargs: pytest.fail("Secret read attempted"))
    with pytest.raises(ValueError, match="outside.*inventory"):
        KubectlReader(namespace="app")(kind, "credential")


@pytest.mark.parametrize("kind", ["pods", "jobs"])
def test_kubectl_reader_lists_only_explicit_nonsecret_inventories(kind, monkeypatch):
    import subprocess

    from benchmarks.sregym.fastloop.fault_tracker import KubectlReader

    def run(command, **_kwargs):
        assert "" not in command
        assert command == ["kubectl", "-n", "app", "get", kind, "-o", "json", "--ignore-not-found"]
        return subprocess.CompletedProcess(command, 0, '{"items":[]}')

    monkeypatch.setattr("subprocess.run", run)
    assert KubectlReader(namespace="app")(kind, "") == {"items": []}


def test_capture_is_immutable_and_does_not_recapture_the_fault():
    fault = describe_problem("fixture", type("WrongDNSPolicy", (), {"faulty_service": "app"})()).faults[0]
    obj = deployment(dnsConfig={"nameservers": ["10.96.0.10"]})
    probe = probes_for((fault,), lambda *_: obj)[fault.name]
    obj["spec"]["template"]["spec"]["dnsConfig"]["nameservers"][0] = "8.8.8.8"
    assert probe() is False
    assert probe() is False
    obj["spec"]["template"]["spec"]["dnsConfig"]["nameservers"][0] = "10.96.0.10"
    assert probe() is True


@pytest.mark.parametrize("value", ["0", "64Mi", "127Mi", "invalid", "NaN", "-1Mi"])
def test_shared_memory_does_not_count_insufficient_or_invalid_capacity(value):
    fault = describe_problem(
        "fixture", type("DevShmExhaustionHotelReservation", (), {"worker_name": "worker", "scratch_mib": 128})()
    ).faults[0]
    obj = deployment(
        volumes=[{"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": value}}],
        containers=[{"name": "worker", "volumeMounts": [{"name": "shm", "mountPath": "/dev/shm"}]}],
    )
    assert probes_for((fault,), lambda *_: obj)[fault.name]() is False


def test_real_phase4_inventory_is_offline_and_remaining_gaps_are_explicit():
    import os
    import subprocess
    from pathlib import Path

    root = Path(__file__).resolve().parents[5]
    script = """
import json, socket, subprocess
from pathlib import Path
from unittest.mock import patch
from kubernetes.client.rest import RESTClientObject
from benchmarks.sregym.faults.worker import describe
from benchmarks.sregym.faults.description import ProblemDescription, validate_suite

def forbidden(*args, **kwargs):
    raise AssertionError('offline discovery attempted a command or network/API call')

ids = json.loads(Path('../../tests/fixtures/sregym/fastloop/phase4_seed42.json').read_text())['incidents']
with (
    patch.object(subprocess, 'Popen', forbidden),
    patch.object(socket.socket, 'connect', forbidden),
    patch.object(RESTClientObject, 'request', forbidden),
):
    catalog = {key: ProblemDescription.model_validate(value) for key, value in describe(ids).items()}
    assert len(catalog) == 50
    assert sum(len(d.faults) for d in catalog.values()) == 170
    unsupported = {f.class_name for d in catalog.values() for f in d.faults if not f.tracking_supported}
    assert unsupported == set(), unsupported
    for key, item in catalog.items():
        if item.tracking_supported:
            validate_suite({key: item}, require_tracking=True, capability_mode='permissive')
        else:
            try:
                validate_suite({key: item}, require_tracking=True, capability_mode='permissive')
            except ValueError as exc:
                assert 'tracking unsupported' in str(exc)
            else:
                raise AssertionError('unprobed fault passed the tracking gate')
"""
    reply = subprocess.run(
        ["uv", "run", "--directory", str(root / "third_party/sregym"), "python", "-c", script],
        env={**os.environ, "PYTHONPATH": str(root), "LITELLM_LOCAL_MODEL_COST_MAP": "True"},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert reply.returncode == 0, (reply.stdout + reply.stderr)[-4000:]


def test_unknown_fault_cannot_borrow_a_supported_draw_key():
    from types import SimpleNamespace

    part = type("UnknownFault", (), {"faulty_service": "app"})()
    problem = SimpleNamespace(
        problems=[part], draws=[SimpleNamespace(key="target_port", param_dict=dict, footprints=())], injection_order=[0]
    )
    description = describe_problem("future", problem)
    assert description.faults[0].kind == "target_port"  # Preserve the benchmark identity.
    assert not description.tracking_supported
    with pytest.raises(ValueError, match="tracking unsupported.*UnknownFault"):
        validate_suite({"future": description}, require_tracking=True, capability_mode="permissive")


def test_shared_memory_mount_on_unrelated_sidecar_does_not_repair_worker():
    fault = describe_problem(
        "fixture",
        type("DevShmExhaustionHotelReservation", (), {"worker_name": "media-processor", "scratch_mib": 128})(),
    ).faults[0]
    obj = deployment(
        volumes=[{"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": "256Mi"}}],
        containers=[
            {"name": "worker"},
            {"name": "sidecar", "volumeMounts": [{"name": "shm", "mountPath": "/dev/shm"}]},
        ],
    )
    assert probes_for((fault,), lambda *_: obj)[fault.name]() is False


@pytest.mark.parametrize("targets", [("",), ("a", "a"), ("a", "b")])
def test_target_metadata_cannot_claim_an_ambiguous_probe_identity(targets):
    with pytest.raises(ValueError, match="targets"):
        FaultSpec("incorrect_image", "a", targets=targets)


@pytest.mark.parametrize(
    "kind",
    [
        "deployment",
        "service",
        "configmap",
        "networkpolicy",
        "ingress",
        "hpa",
        "cronjob",
        "clusterrole",
        "clusterrolebinding",
        "serviceaccount",
    ],
)
def test_named_nonsecret_probe_inventory_is_read_only(kind, monkeypatch):
    import subprocess

    from benchmarks.sregym.fastloop.fault_tracker import KubectlReader

    def run(command, **kwargs):
        assert command == ["kubectl", "-n", "app", "get", kind, "target", "-o", "json", "--ignore-not-found"]
        return subprocess.CompletedProcess(command, 0, '{"metadata":{"name":"target"}}')

    monkeypatch.setattr("subprocess.run", run)
    assert KubectlReader(namespace="app")(kind, "target") == {"metadata": {"name": "target"}}


def test_plan_gate_checks_probe_identity_across_composed_problem_ids(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop.probe_coverage import check_plan

    first = describe_problem("first", type("IncorrectImage", (), {"faulty_service": "app"})())
    second = describe_problem("second", type("IncorrectImage", (), {"faulty_service": "app"})())
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.probe_coverage.describe_sregym_problems",
        lambda *args, **kwargs: {"first": first, "second": second},
    )
    plan = tmp_path / "stream.json"
    plan.write_text('[["first","second"]]')
    with pytest.raises(ValueError, match="duplicate probe identit"):
        check_plan(plan, sregym_dir=tmp_path)
