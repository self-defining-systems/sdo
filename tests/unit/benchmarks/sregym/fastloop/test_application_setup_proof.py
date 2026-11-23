"""A lane that passed its pre-injection admission keeps its application deploy until something invalidates it.

Each eval row wipes SDO's install (Tier-1) and reinstalls it, so the deploy skip must be bound to what the deploy
establishes, the application and its retained cluster scope, never to the SDO install the wipe deliberately removes.
"""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import setup_proof as proof

CONTROL = "app-sdo"


def service(port=80, **metadata):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"namespace": "app", "name": "web", "uid": "web-uid", "resourceVersion": "1", **metadata},
        "spec": {"ports": [{"port": port}]},
    }


def deployment(revision="1", generation=1, uid="deploy-uid", image="web:1"):
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "namespace": "app",
            "name": "web",
            "uid": uid,
            "generation": generation,
            "annotations": {"deployment.kubernetes.io/revision": revision},
        },
        "spec": {"template": {"spec": {"containers": [{"name": "web", "image": image}]}}},
        "status": {"readyReplicas": 1},
    }


def sdo_install():
    """What the SDO install adds: its namespace, its cluster RBAC and labelled helpers in the app namespace."""
    return [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": CONTROL, "uid": "ns-sdo"}},
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"namespace": CONTROL, "name": "sdo-controller-state", "uid": "s"},
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {"name": f"sdo-admin-{CONTROL}", "uid": "cr"},
            "rules": [],
        },
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "namespace": "app",
                "name": "sdo-probe",
                "uid": "sa",
                "labels": {"sdo.dev/application-namespace": "app"},
            },
        },
    ]


def node(uid="node-uid"):
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node", "uid": uid},
        "spec": {"podCIDR": "10.0.0.0/24"},
    }


def runtime_pod(container="c1", restarts=0):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "app", "name": "web-1", "uid": container},
        "status": {"containerStatuses": [{"name": "web", "containerID": container, "restartCount": restarts}]},
    }


def identities(objects):
    return proof.application_identities(objects, CONTROL)


def test_application_identity_ignores_the_sdo_install_a_tier1_wipe_removes():
    sealed = identities([service(), deployment(), node(), *sdo_install()])
    after_wipe = identities([service(), deployment(), node()])
    assert sealed == after_wipe
    assert "app/Service/web" in sealed


def install_volume():
    """The PersistentVolume behind the SDO repository claim: cluster-scoped, named after its claim's UID."""
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": "pvc-0123", "uid": "pv"},
        "spec": {"claimRef": {"namespace": CONTROL, "name": "sdo-application-repository"}},
    }


def restarted(deploy, at):
    deploy["spec"]["template"].setdefault("metadata", {})["annotations"] = {"kubectl.kubernetes.io/restartedAt": at}
    return deploy


def test_live_row_start_churn_keeps_the_identity():
    """Seen on live lanes (2026-11-19): the wipe deletes the install's volumes, and row start restarts Jaeger."""
    sealed = identities([service(), deployment(), install_volume()])
    after_wipe = identities([service(), restarted(deployment(revision="2"), "2026-11-19T22:47:59Z")])
    assert sealed == after_wipe


def test_application_identity_ignores_runtime_and_a_restored_rollout():
    sealed = identities([service(), deployment(), runtime_pod()])
    # A fault rolled the Deployment and recovery rolled it back: same configuration, new revision and generation,
    # restarted Pods. The admission that follows re-proves health; the deploy would change nothing.
    restored = identities([service(resourceVersion="9"), deployment(revision="3", generation=3), runtime_pod("c2", 4)])
    assert sealed == restored


@pytest.mark.parametrize(
    "changed",
    [
        [service(port=81), deployment(), node()],  # configuration drift
        [service(), deployment(image="web:2"), node()],  # another application source
        [deployment(), node()],  # a deleted object
        [
            service(),
            deployment(),
            node(),
            {**service(), "metadata": {"namespace": "app", "name": "injected", "uid": "i"}},
        ],
        [service(), deployment(), node(uid="rebuilt-node")],  # a rebuilt lane
    ],
)
def test_application_identity_changes_with_what_the_deploy_establishes(changed):
    assert identities(changed) != identities([service(), deployment(), node()])


def test_application_identity_refuses_secrets():
    with pytest.raises(ValueError, match="Secrets"):
        identities([{"apiVersion": "v1", "kind": "Secret", "metadata": {"namespace": "app", "name": "s", "uid": "s"}}])


@pytest.fixture
def fingerprint():
    return proof.ApplicationFingerprint(
        lane="fixture",
        code_revision="a" * 40,
        images={"controller": {"node": ["sha256:" + "1" * 64]}},
        app_manifests="manifest-hash",
        live_objects={"app/Service/web": "spec-hash"},
        tools={"cli.py": "tool-hash"},
    )


ADMISSION = {"passed": True, "qualified_scenarios": True}


def test_a_passing_admission_seals_a_receipt_the_next_row_reuses(tmp_path, fingerprint):
    path = proof.seal_application_receipt(tmp_path, fingerprint, ADMISSION)
    assert proof.application_reuse_decision(tmp_path, fingerprint) == {
        "reused": True,
        "receipt": str(path),
        "changed": [],
    }
    document = json.loads(path.read_text())
    assert document["proof"]["fault_injected"] is False
    assert document["proof"]["admission"] == ADMISSION


@pytest.mark.parametrize("admission", [{"passed": False}, {}, {"passed": "yes"}])
def test_only_a_passing_admission_seals(tmp_path, fingerprint, admission):
    with pytest.raises(ValueError, match="passing"):
        proof.seal_application_receipt(tmp_path, fingerprint, admission)
    assert not (tmp_path / "latest.json").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("lane", "other"),
        ("code_revision", "c" * 40),
        ("images", {"controller": {"node": ["sha256:" + "2" * 64]}}),
        ("app_manifests", "changed"),
        ("live_objects", {"app/Service/web": "drift"}),
        ("tools", {"cli.py": "changed"}),
    ],
)
def test_any_change_to_what_the_deploy_establishes_invalidates_the_receipt(tmp_path, fingerprint, field, value):
    proof.seal_application_receipt(tmp_path, fingerprint, ADMISSION)
    decision = proof.application_reuse_decision(tmp_path, replace(fingerprint, **{field: value}))
    assert decision["reused"] is False
    assert decision["changed"] == [field]


def test_a_changed_object_set_names_the_objects_that_differ(tmp_path, fingerprint):
    """A refused reuse on a live lane must say which objects moved, or the next run learns nothing."""
    sealed = replace(
        fingerprint, live_objects={"app/Service/web": "a", "app/Service/gone": "b", "<cluster>/Node/n": "c"}
    )
    proof.seal_application_receipt(tmp_path, sealed, ADMISSION)
    now = replace(sealed, live_objects={"app/Service/web": "a2", "<cluster>/Node/n": "c", "app/Service/new": "d"})

    decision = proof.application_reuse_decision(tmp_path, now)

    assert decision["changed"] == ["live_objects"]
    assert decision["differing"] == {"live_objects": ["app/Service/gone", "app/Service/new", "app/Service/web"]}


def test_a_tampered_receipt_never_reuses(tmp_path, fingerprint):
    path = proof.seal_application_receipt(tmp_path, fingerprint, ADMISSION)
    document = json.loads(path.read_text())
    document["proof"]["admission"]["passed"] = False
    path.write_text(json.dumps(document))
    assert proof.application_reuse_decision(tmp_path, fingerprint)["reused"] is False


def test_row_deploy_reuses_the_application_receipt_after_the_tier1_wipe(tmp_path, fingerprint, monkeypatch):
    """The row path: no SDO install exists when the deploy is decided, so the full-lane fingerprint is never read."""
    from benchmarks.sregym.fastloop import cli

    environment = SimpleNamespace(run_dir=tmp_path, deploy_problem="fixture", baseline_path=tmp_path / "baseline")
    calls = []
    worker = SimpleNamespace(request=lambda *args, **kwargs: calls.append(args[0]))
    monkeypatch.setattr(proof, "collect_fingerprint", lambda env: pytest.fail("the SDO install is wiped at row start"))
    monkeypatch.setattr(proof, "collect_application_fingerprint", lambda env: fingerprint)

    first = cli.deploy_with_setup_proof(environment, worker, tmp_path / "first")
    assert first["changed"] == ["no_passing_receipt"]
    assert calls == ["deploy"]

    proof.seal_application_receipt(tmp_path / proof.APPLICATION_RECEIPTS, fingerprint, ADMISSION)
    second = cli.deploy_with_setup_proof(environment, worker, tmp_path / "second")
    assert second["reused"] is True
    assert calls == ["deploy"]

    monkeypatch.setattr(
        proof,
        "collect_application_fingerprint",
        lambda env: replace(fingerprint, live_objects={"app/Service/web": "x"}),
    )
    third = cli.deploy_with_setup_proof(environment, worker, tmp_path / "third")
    assert third["changed"] == ["live_objects"]
    assert calls == ["deploy", "deploy"]


def test_admission_seals_only_after_its_proof_passes():
    from benchmarks.sregym.fastloop.admission import EvalAdmissionOps

    order = []
    inner = SimpleNamespace(
        set_maintenance=lambda *args, **kwargs: order.append("resume"),
        inject_after_resume=lambda control, generation, inject: order.append("inject") or {},
    )
    admission = SimpleNamespace(prove=lambda control, fresh=False: order.append("prove") or dict(ADMISSION))
    ops = EvalAdmissionOps(inner, admission, on_proven=lambda result: order.append(("seal", result["passed"])))
    ops.inject_after_resume("app-sdo", "g", lambda: None)
    assert order == ["resume", "prove", ("seal", True), "inject"]


def test_a_failed_seal_never_fails_the_row():
    from benchmarks.sregym.fastloop.admission import EvalAdmissionOps

    def broken(_):
        raise TimeoutError("fixture deadline")

    inner = SimpleNamespace(set_maintenance=lambda *a, **k: None, inject_after_resume=lambda *a: {"x": 1.0})
    admission = SimpleNamespace(prove=lambda control, fresh=False: dict(ADMISSION))
    result = EvalAdmissionOps(inner, admission, on_proven=broken).inject_after_resume("app-sdo", "g", lambda: None)
    assert result["x"] == 1.0


def test_collector_needs_no_sdo_install_and_reads_only_bounded_gets(tmp_path):
    from benchmarks.sregym.fastloop.environment import Images

    workspace = tmp_path / "repo"
    (workspace / "kubernetes").mkdir(parents=True)
    (workspace / "kubernetes/app.yaml").write_text("kind: Service\n")
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{}")
    images = Images()
    digests = [
        {"names": [f"docker.io/library/{name}", f"{name.split(':')[0]}@sha256:{'1' * 64}"]}
        for name in images.model_dump().values()
    ]
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[0] == "git":
            stdout = "a" * 40 if ("describe" in command or "rev-parse" in command) else ""
        else:
            assert command[0] == "kubectl"
            assert "get" in command
            assert "exec" not in command
            items = [{**node(), "status": {"images": digests}}] if proof.CLUSTER_RESOURCES in command else [service()]
            stdout = json.dumps({"items": items})
        return SimpleNamespace(stdout=stdout)

    environment = SimpleNamespace(
        cluster="fixture",
        namespace="app",
        workspace=workspace,
        kubeconfig=tmp_path / "kube",
        baseline_path=baseline,
        images=images,
        dev_mount=None,
        application="fixture",
        problem_application="fixture",
        app="sregym",
        deploy_problem="fixture-problem",
        seed_commit="b" * 40,
        worker_env={},
        dev_mount_snapshot=False,
    )
    measured = proof.collect_application_fingerprint(environment, run=run)
    assert set(measured.live_objects) == {"app/Service/web", "<cluster>/Node/node"}
    assert all(0 < kwargs["timeout"] <= 15 for _, kwargs in calls)


@pytest.mark.parametrize(
    ("running_uid", "repaired", "proved_again"),
    [("warm-uid", False, False), ("other-uid", False, True), ("warm-uid", True, True), (None, False, True)],
)
def test_the_injection_gate_stands_on_the_warm_proof_of_the_same_unchanged_controller(
    running_uid, repaired, proved_again
):
    """Recorded on EKS (fast-loop fl2 q2): the injection gate re-proved the controller ~25 s after the warm transaction
    proved it, with no pause or change in between (~16 s). The warm proof is accepted only while the proven controller
    still runs and no rehearsal repaired the app; the application setup is still sealed."""
    from benchmarks.sregym.fastloop.admission import CarriedProof, EvalAdmissionOps

    order = []
    inner = SimpleNamespace(
        set_maintenance=lambda *args, **kwargs: None,
        inject_after_resume=lambda control, generation, inject: order.append("inject") or {},
        controller_pod=lambda control: SimpleNamespace(uid=running_uid) if running_uid else None,
    )
    admission = SimpleNamespace(prove=lambda control, fresh=False: order.append("prove") or dict(ADMISSION))
    ops = EvalAdmissionOps(
        inner,
        admission,
        on_proven=lambda result: order.append("seal"),
        carried=CarriedProof("warm-uid", unchanged=lambda: not repaired),
    )
    ops.inject_after_resume("app-sdo", "g", lambda: None)
    assert order == (["prove", "seal", "inject"] if proved_again else ["seal", "inject"])
    ops.inject_after_resume("app-sdo", "g2", lambda: None)  # a later injection always proves
    assert order[-3:] == ["prove", "seal", "inject"]
