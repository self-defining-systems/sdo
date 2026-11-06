"""End-to-end scripted fastloop run over a real learned Go program, nightly tier.

PR CI ignores this file: the stand-in controller really `go build`s the learned program (2.4 s locally,
5-9 s on a hosted runner) so the second incident runs the compiled binary, the property under test. The
cheaper hermetic contracts stay in test_fastloop_hermetic_run.py.
"""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from benchmarks.sregym import adapter
from benchmarks.sregym.adapter import driver
from benchmarks.sregym.adapter.deployment_source import kustomize_source
from benchmarks.sregym.adapter.driver import DeployedLifecycle, DeployedLifecycleContext
from benchmarks.sregym.adapter.persistent import PersistentState
from benchmarks.sregym.fastloop import cli, composite, fault_tracker
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, save_application_baseline
from benchmarks.sregym.fastloop.composite import CompositeSettings
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
from benchmarks.sregym.fastloop.records import load_records
from benchmarks.sregym.faults.description import ProblemDescription
from tests.unit.benchmarks.sregym.fastloop import script_harness as scripts
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_hermetic_run import (  # noqa: F401  (control_api is a fixture)
    FIXTURE_SEED,
    HEALTH_DETECTORS,
    NAMESPACE,
    REGISTRATIONS,
    ROOT,
    STREAM,
    Application,
    Controller,
    Worker,
    _patch_kubectl,
    component_for,
    control_api,
)

pytestmark = pytest.mark.go_integration


def test_scripted_fastloop_reuses_reflected_target_port_program(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    control_api: SimpleNamespace,  # noqa: F811
) -> None:
    workspace = tmp_path / "application_workspace"
    shutil.copytree(FIXTURE_SEED, workspace)
    for path in (workspace / "kubernetes").rglob("*.yaml"):
        documents = list(yaml.safe_load_all(path.read_text()))
        for item in documents:
            if item and item.get("kind") == "Deployment" and item.get("metadata", {}).get("name") in {"rate", "search"}:
                item["spec"]["template"]["spec"]["containers"][0]["image"] = (
                    "yinfangchen/hotelreservation@sha256:" + "a" * 64
                )
        path.write_text(yaml.safe_dump_all(documents, sort_keys=False))
    # The hotel application's ``kubernetes/`` is a kustomize root: list the synthetic manifests.
    manifests = sorted(
        str(path.relative_to(workspace / "kubernetes")) for path in (workspace / "kubernetes").rglob("*.yaml")
    )
    (workspace / "kubernetes/kustomization.yaml").write_text(yaml.safe_dump({"resources": manifests}))
    diagnostics = workspace / ".sdo/diagnostics"
    diagnostics.mkdir(parents=True)
    (diagnostics / "manifest.yaml").write_text("detectors: []\n")
    (diagnostics / "go.mod").write_text(
        "module hermetic-diagnostics\n\ngo 1.24\n\nrequire sdo.dev/operator/sdk v0.0.0\n"
    )
    (workspace / ".sdo/lifecycle-provenance.yaml").write_text("{}\n")
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
    subprocess.run(["git", "add", "."], cwd=workspace, check=True)
    subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed"],
        cwd=workspace,
        check=True,
    )
    # Reproduce a loaded-host compiler stall deterministically. Diagnosis must
    # execute the learned binary, never compile on the incident's hot path.
    run = subprocess.run

    def loaded_host_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if argv[0] == "go" and controller.incidents == 2 and kwargs.get("cwd") == controller.sweep:
            raise subprocess.TimeoutExpired(argv, 90, stderr="injected compiler stall")
        return run(argv, **kwargs)

    monkeypatch.setattr(subprocess, "run", loaded_host_run)
    app = Application(workspace)
    worker = Worker(app)
    controller = Controller(app, tmp_path / "capture")
    (tmp_path / "cluster_baseline_state.json").write_text("{}\n")
    save_application_baseline(
        tmp_path / "cluster_baseline_state.json",
        (ApplicationBaseline(NAMESPACE, app.list_objects(NAMESPACE)),),
    )
    FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fake",
        kubeconfig=control_api.kubeconfig,
        namespace=NAMESPACE,
        application=NAMESPACE,
        workspace=workspace,
        sregym_dir=ROOT / "third_party/sregym",
        private_tmp=None,
    ).save()
    # Model the newly required admission I/O from this simulated application's
    # actual readiness and routing state. The pure production predicates still
    # run; only the Kubernetes/prober subprocess boundaries are replaced.
    from benchmarks.sregym.fastloop import admission, baseline_recovery, clean_start, rbac

    traffic = workspace / ".sdo/diagnostics/traffic/workloads"
    traffic.mkdir(parents=True)
    (traffic / "health.yaml").write_text("purpose: health-probe\nscenarios:\n- id: frontend\n")
    for detector in HEALTH_DETECTORS:
        (workspace / ".sdo/diagnostics/detectors/health" / detector).mkdir(parents=True)
        (workspace / ".sdo/diagnostics/detectors/health" / detector / "manifest.yaml").write_text("id: fixture\n")
    plan = rbac.installer_rbac(NAMESPACE)
    grants = []
    for key in ("ClusterRoles", "ClusterBindings"):
        for original in plan[key]:
            row = deepcopy(original)
            row["kind"] = "ClusterRoleBinding" if key.endswith("Bindings") else "ClusterRole"
            grants.append(row)

    def admission_api(args, **_kwargs):
        result = {"items": []}
        if args[:2] == ["get", "clusterroles,clusterrolebindings"]:
            result = {"items": grants}
        elif args[:2] == ["get", "job/sdo-controller-run"]:
            result = {
                "metadata": {"uid": "synthetic-controller"},
                "status": {"active": 1},
                "spec": {
                    "template": {
                        "spec": {"containers": [{"name": "controller", "args": ["operate", "--app", str(workspace)]}]}
                    }
                },
            }
        elif args[:2] == ["get", "configmap/sdo-namespace-onboarding"]:
            # The fake controller's install is already onboarded when admission starts.
            record = {"acceptedCommit": "a" * 40, "objects": [{"kind": "Deployment"}]}
            result = {"data": {"onboarding.json": json.dumps(record)}}
        elif args[:2] in (
            ["get", "configmap/sdo-namespace-onboarding-pending"],
            ["get", "configmap/sdo-namespace-onboarding-rejected"],
        ):
            return SimpleNamespace(stdout="")
        elif args[0] == "exec":
            assert args[5:] == ["sdo-operator", "observe-traffic", "--app", str(workspace)]
            result = {
                "windows": {
                    "health": {
                        "scenarios": [
                            {
                                "scenario": {"id": "frontend"},
                                "qualified": app.healthy(),
                                "samples": [{"success": app.healthy()}],
                            }
                        ]
                    }
                }
            }
        elif args[:2] == ["get", "deployments,statefulsets,daemonsets,pods"]:
            objects = [row for row in app.list_objects(NAMESPACE) if row["kind"] == "Deployment"]
            for pod in app.snapshot()["pods"]:
                pod.update(
                    kind="Pod",
                    status={"conditions": [{"type": "Ready", "status": "True" if app.healthy() else "False"}]},
                )
                pod["metadata"]["uid"] = pod["metadata"]["name"]
                objects.append(pod)
            result = {"items": objects}
        if args[:2] == ["get", "secrets"]:
            return SimpleNamespace(stdout="")
        if args[0] == "get" and "-o" in args and args[args.index("-o") + 1] == "name":
            return SimpleNamespace(stdout="")
        if args[0] == "api-resources":
            return SimpleNamespace(stdout="")
        return SimpleNamespace(stdout=json.dumps(result))

    original_state = controller.runtime_state

    def admission_state(control):
        state = original_state(control)
        state["history"] = [
            {
                "detector_id": key,
                "status": "clear" if app.healthy() else "firing",
                "evaluated_at": datetime.now(timezone.utc).isoformat(),
            }
            for key in HEALTH_DETECTORS
        ]
        return state

    monkeypatch.setattr(controller, "runtime_state", admission_state)
    from benchmarks.sregym.adapter import lane_resources

    def resource_snapshot(kubeconfig, *, app_namespaces):
        assert kubeconfig == control_api.kubeconfig
        now = datetime.now(timezone.utc)
        pods = deepcopy(app.snapshot()["pods"])
        measurements = []
        for pod in pods:
            pod["metadata"]["uid"] = pod["metadata"]["name"]
            pod["status"] = {
                "containerStatuses": [
                    {"name": row["name"], "state": {"running": {}}} for row in pod["spec"]["containers"]
                ]
            }
            measurements.append(
                {
                    "podRef": {key: pod["metadata"][key] for key in ("namespace", "name", "uid")},
                    "containers": [
                        {"name": row["name"], "memory": {"time": now.isoformat(), "workingSetBytes": 1024**2}}
                        for row in pod["spec"]["containers"]
                    ],
                }
            )
        return lane_resources.evaluate_lane_resources(
            namespaces={"items": [{"metadata": {"name": NAMESPACE}}]},
            pods={"items": pods},
            summaries=[{"pods": measurements}],
            app_namespaces=app_namespaces,
            host_available_bytes=32 * 1024**3,
            now=now,
        )

    monkeypatch.setattr(admission, "assert_lane_resources", resource_snapshot)
    monkeypatch.setattr(
        admission,
        "assert_host_disk_headroom",
        lambda: lane_resources.evaluate_host_disk(
            mount="/simulated-kind-storage",
            total_bytes=1000 * 1024**3,
            free_bytes=200 * 1024**3,
        ),
    )
    _patch_kubectl(monkeypatch, admission_api)
    monkeypatch.setattr(clean_start, "kubectl", admission_api)
    monkeypatch.setattr(rbac, "kubectl", admission_api)
    monkeypatch.setattr(cli, "_worker", lambda _environment: worker)
    monkeypatch.setattr(cli, "kubectl", lambda *_args, **_kwargs: SimpleNamespace(returncode=0))
    monkeypatch.setattr(cli, "_cluster_ops", lambda _args, _namespace: controller)
    monkeypatch.setattr(cli, "KubectlApplicationCluster", lambda _kubeconfig: app)
    monkeypatch.setattr(baseline_recovery, "KubectlApplicationCluster", lambda _kubeconfig: app)
    monkeypatch.setattr(cli, "_composite_settings", lambda _args: CompositeSettings(20, 1, 0.001, 0.001))
    monkeypatch.setattr(
        adapter,
        "deployed_lifecycle",
        lambda _namespace: DeployedLifecycle(DeployedLifecycleContext("healthy", []), "seed"),
    )
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(control_api.kubeconfig))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(driver, "_publish_agent_registrations", lambda *_args: None)
    monkeypatch.setattr(driver, "inspect_existing", lambda *_args: object())

    def install(config: object) -> object:
        assert config.kubeconfig == control_api.kubeconfig
        assert config.deployment_source == kustomize_source("kubernetes")
        return SimpleNamespace(reused=False, accepted_commit="a" * 40)

    monkeypatch.setattr(driver, "install_existing", install)
    from benchmarks.sregym.adapter import native_operator

    def synchronize(config):
        # This fixture's broker writes the actual accepted Git repository in
        # place. The transport edge needs no PVC copy, but must bind that repo.
        assert config.repository.resolve() == workspace.resolve()
        assert (config.repository / ".git").exists()

    monkeypatch.setattr(native_operator, "sync_existing", synchronize)
    monkeypatch.setattr(adapter, "sync_existing", synchronize)
    monkeypatch.setattr(
        adapter,
        "launch_native_lifecycle",
        lambda config, *_args: controller.install(replace(config, persistent=True, wait_for_completion=False)),
    )
    monkeypatch.setattr(
        adapter,
        "kubectl",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=json.dumps({"items": controller.configmaps()})),
    )
    monkeypatch.setattr(composite, "reader_for", lambda *_args: app.read)
    monkeypatch.setattr(
        fault_tracker,
        "describe_sregym_problems",
        lambda ids, **_kwargs: {
            problem: ProblemDescription(
                problem_id=problem,
                faults=(
                    FaultSpec("target_port" if "target_port" in problem else "incorrect_image", component_for(problem)),
                ),
                injection_order=(0,),
            )
            for problem in ids
        },
    )
    monkeypatch.setattr(scripts, "PROGRAM_SOURCE", ROOT / "benchmarks/sregym/fastloop/scripted_program")
    monkeypatch.setattr(scripts, "_command", controller.command)
    monkeypatch.setattr(scripts, "_tool", controller.tool)
    monkeypatch.setattr(scripts.time, "sleep", lambda _seconds: None)
    monkeypatch.setenv("SDO_CAPTURED_CORPUS_DIR", str(controller.capture))

    assert (
        cli.main(
            [
                "run",
                "--run-dir",
                str(tmp_path),
                "--agent",
                "sdo",
                "--run-id",
                "hermetic",
                "--agent-registrations",
                str(REGISTRATIONS),
                "--stream",
                str(STREAM),
                "--diagnosis-grading",
                "off",
                "--health-timeout",
                "2",
            ]
        )
        == 0
    )

    helper_proof = json.loads((tmp_path / "results/hermetic/admission/hotel-script-profile.json").read_text())
    assert helper_proof == {"profile": "hotel-script-free/v1", "passed": True, "objects_checked": len(app.objects)}
    planned = json.loads(STREAM.read_text())["incidents"]
    assert worker.rehearsed == list(dict.fromkeys(problem for row in planned for problem in row))
    records = load_records(tmp_path / "results/hermetic/incidents.jsonl")
    with (tmp_path / "results/hermetic/incidents.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(records) == len(rows) == 2
    assert all(
        row.oracle and row.oracle.success and row.recovery_oracle and row.recovery_oracle.success for row in records
    )
    assert all(row.error is None and row.injection_to_mitigation_seconds is not None for row in records)
    assert all("responder_tokens" in row and "reflection_tokens" in row for row in rows)
    # Every scripted row is bound to the incident its own injection caused (INV1): no negative timings.
    assert all(row.detected_at is None or row.detected_at >= row.injection_started_at for row in records)
    assert all((row.injection_to_detection_seconds or 0) >= 0 for row in records)
    assert all(row.driver_phase_timings_seconds["injection_to_verified_recovery"] > 0 for row in records)
    assert all(Path(str(row.artifacts_dir), "sdo_production_receipt_strict.json").is_file() for row in records)
    # The row-start wipe (test_wipe.py) owns control-state removal; no selective reset runs on a non-toy lane.
    assert control_api.deleted == []
    assert set(control_api.objects) == set(control_api.names)
    assert controller.installs == 1
    assert controller.registrations == [REGISTRATIONS, REGISTRATIONS]
    assert records[0].run_prelude_phase_seconds["total_before_first_incident"] >= 0
    assert records[1].run_prelude_phase_seconds == {}
    assert records[1].driver_phase_timings_seconds["controller_install_inspection"] >= 0
    assert records[1].fault_gate_timings_seconds["controller_baseline_wait"] >= 0
    assert (workspace / ".sdo/playbooks/service-targetport-mismatch/diagnose.go").is_file()
    corpus = next((workspace / ".sdo/corpus").iterdir())
    assert (corpus / "open.json").is_file()
    assert (tmp_path / "results/hermetic/checkpoints/000/responder/checkpoint.json").is_file()
    committed = subprocess.run(
        ["git", "show", "--pretty=", "--name-only", "HEAD"],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert ".sdo/playbooks/service-targetport-mismatch/diagnose.go" in committed
    assert str((corpus / "open.json").relative_to(workspace)) in committed
    assert records[1].program_verdicts.get("Match") == 1
    assert records[1].tool_commands.get("apply") == 1
    assert app.objects == app.baseline
    saved = PersistentState.load(tmp_path / "sdo_persistent_controller.json").controllers[NAMESPACE]
    assert saved.served_stages == ["fastloop-000", "fastloop-001"]
    assert saved.pending is None
