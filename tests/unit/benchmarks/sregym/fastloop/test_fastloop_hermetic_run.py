"""A two-incident scripted run through the CLI and persistent controller adapter."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
import subprocess
import threading
import time
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import pytest
import yaml
from google.protobuf import json_format
from google.protobuf.any_pb2 import Any as ProtoAny

from benchmarks.sregym import adapter
from benchmarks.sregym.adapter import driver, scripted_worker
from benchmarks.sregym.adapter.driver import DeployedLifecycle, DeployedLifecycleContext
from benchmarks.sregym.adapter.persistent import PersistentState
from benchmarks.sregym.fastloop import cli, composite, fault_tracker
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, save_application_baseline
from benchmarks.sregym.fastloop.composite import CompositeSettings
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
from benchmarks.sregym.fastloop.records import load_records
from benchmarks.sregym.faults.description import ProblemDescription
from sdo.contracts import AgentResult, AgentTask, IncidentRequest, IncidentResult, to_canonical_json
from tests.proto_messages import from_dict
from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = Path(__file__).resolve().parents[5]
FIXTURE_SEED = ROOT / "tests/fixtures/sdo/scripted_seed"
STREAM = ROOT / "tests/fixtures/sregym/fastloop/pbprog_explicit.json"
REGISTRATIONS = ROOT / "benchmarks/sregym/adapter/scripted_registrations"
NAMESPACE = "hotel-reservation"
COMPONENTS = ("reservation", "rate", "geo", "search")
SWEEP_MAIN = """
package main

import (
    "context"
    "encoding/json"
    "os"

    targetport "hermetic/programsrc"
    "sdo.dev/operator/sdk"
    "sdo.dev/operator/sdk/program"
    "sdo.dev/operator/sdk/sdktest"
)

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		panic(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: snapshot}
	bindings := program.Bindings{Objects: program.ObjectBindings{
		"service": sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "hotel-reservation", Name: "geo"},
	}}
	candidate := targetport.Program{}
	verdict, err := candidate.Diagnose(context.Background(), view, bindings)
	if err != nil {
		panic(err)
	}
	plan, err := candidate.Mitigate(context.Background(), view, bindings)
	if err != nil {
		panic(err)
	}
	if len(plan.Actions) != 1 {
		panic("expected one targetPort action")
	}
	action, ok := plan.Actions[0].(program.SetField)
	if !ok {
		panic("expected SetField")
	}
	value, _, err := program.ResolveValue(action.Value, program.ValueInputs{
        Observed: func(ref sdk.ObjectRef) (map[string]any, error) {
        for _, pod := range snapshot.Pods() {
            if ref.Kind == "Pod" && pod.Namespace == ref.Namespace && pod.Name == ref.Name {
                raw, err := json.Marshal(pod)
                if err != nil { return nil, err }
                var object map[string]any
                err = json.Unmarshal(raw, &object)
                return object, err
            }
        }
        return nil, os.ErrNotExist
        },
    })
    if err != nil { panic(err) }
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"result": verdict.Result, "path": action.Path, "value": value,
	}); err != nil {
		panic(err)
	}
}
"""


@pytest.fixture
def control_api(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """Run the real stop/reset CLI against prior-install control leftovers."""
    names = [
        "sdo-namespace-onboarding",
        "sdo-namespace-onboarding-pending",
        "sdo-namespace-onboarding-attempt",
        "sdo-namespace-onboarding-window",
        "sdo-namespace-onboarding-rejected",
        "sdo-controller-state",
        "sdo-controller-maintenance",
    ]
    objects = {
        name: {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": NAMESPACE + "-sdo",
                "uid": "uid-" + name,
                "resourceVersion": "1",
                "labels": {"app.kubernetes.io/managed-by": "sdo"},
            },
            "data": {},
        }
        for name in names
    }
    deleted: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            pass

        def reply(self, status: int, value: dict[str, Any]) -> None:
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            parts = urlsplit(self.path).path.split("/")
            assert NAMESPACE + "-sdo" in parts, self.path
            plural = parts[parts.index(NAMESPACE + "-sdo") + 1]
            if plural == "configmaps":
                name = parts[-1]
                if name in objects:
                    self.reply(200, objects[name])
                else:
                    self.reply(404, {"apiVersion": "v1", "kind": "Status", "reason": "NotFound", "code": 404})
            else:
                kinds = {
                    "jobs": "JobList",
                    "pods": "PodList",
                    "services": "ServiceList",
                    "networkpolicies": "NetworkPolicyList",
                }
                assert plural in kinds, self.path
                self.reply(
                    200,
                    {
                        "apiVersion": "/".join(parts[2:4]) if parts[1] == "apis" else "v1",
                        "kind": kinds[plural],
                        "items": [],
                    },
                )

        def do_DELETE(self) -> None:
            name = urlsplit(self.path).path.split("/")[-1]
            assert name in objects, self.path
            options = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            assert options["preconditions"] == {"uid": "uid-" + name, "resourceVersion": "1"}
            objects.pop(name)
            deleted.append(name)
            self.reply(200, {"apiVersion": "v1", "kind": "Status", "status": "Success"})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    kubeconfig = tmp_path / "control-kubeconfig"
    kubeconfig.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [{"name": "test", "cluster": {"server": f"http://127.0.0.1:{server.server_port}"}}],
                "users": [{"name": "test", "user": {}}],
                "contexts": [{"name": "test", "context": {"cluster": "test", "user": "test"}}],
                "current-context": "test",
            }
        )
    )
    try:
        yield SimpleNamespace(kubeconfig=kubeconfig, objects=objects, deleted=deleted, names=names)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def component_for(problem: str) -> str:
    if "target_port" in problem:
        return problem.split("__v_", 1)[1].split("_", 1)[0]
    return next(name for name in COMPONENTS if f"_{name}_nonexistent_tag" in problem)


class Application:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        for name in COMPONENTS:
            for kind in ("Service", "Deployment"):
                item = next(
                    item
                    for path in (workspace / "kubernetes").rglob("*.yaml")
                    for item in yaml.safe_load_all(path.read_text(encoding="utf-8"))
                    if item and item.get("kind") == kind and item.get("metadata", {}).get("name") == name
                )
                item.setdefault("metadata", {})["namespace"] = NAMESPACE
                if kind == "Deployment" and name in {"rate", "search"}:
                    item["spec"]["template"]["spec"]["containers"][0]["image"] = (
                        "yinfangchen/hotelreservation@sha256:" + "a" * 64
                    )
                self.objects[kind, name] = item
        self.baseline = deepcopy(self.objects)

    def list_objects(self, _namespace: str) -> list[dict[str, Any]]:
        return [
            self.read("deployment", name) if kind == "Deployment" else deepcopy(item)
            for (kind, name), item in self.objects.items()
        ]

    def apply(self, _namespace: str, item: dict[str, Any]) -> None:
        self.objects[item["kind"], item["metadata"]["name"]] = deepcopy(item)

    def delete(self, _namespace: str, kind: str, name: str) -> None:
        self.objects.pop((kind, name), None)

    def read(self, kind: str, name: str) -> dict[str, Any] | None:
        if kind == "deployment":
            item = deepcopy(self.objects.get(("Deployment", name)))
            if item is not None:
                good_image = (
                    item["spec"]["template"]["spec"]["containers"][0]["image"]
                    == self.baseline["Deployment", name]["spec"]["template"]["spec"]["containers"][0]["image"]
                )
                ready = int(good_image)
                item["metadata"]["generation"] = 1
                item["status"] = {
                    "observedGeneration": 1,
                    "replicas": 1,
                    "readyReplicas": ready,
                    "updatedReplicas": ready,
                }
            return item
        return deepcopy(self.objects.get((kind.title(), name)))

    def healthy(self) -> bool:
        return self.objects == self.baseline

    def snapshot(self) -> dict[str, Any]:
        services = [deepcopy(self.objects["Service", name]) for name in COMPONENTS]
        pods = [
            {
                "metadata": {
                    "name": f"{name}-pod",
                    "resourceVersion": "hermetic-1",
                    "namespace": NAMESPACE,
                    "labels": service["spec"]["selector"],
                },
                "spec": {"containers": self.objects["Deployment", name]["spec"]["template"]["spec"]["containers"]},
            }
            for name, service in zip(COMPONENTS, services, strict=True)
        ]
        return {"namespace": NAMESPACE, "services": services, "pods": pods}


class Worker:
    def __init__(self, app: Application) -> None:
        self.app = app
        self.faults: list[tuple[str, str]] = []

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
        if op == "deploy":
            return {"deployed": True}
        if op == "inject":
            problem = str(kwargs["problem_id"])
            name = component_for(problem)
            kind = "Service" if "target_port" in problem else "Deployment"
            if not kwargs.get("compose"):
                self.faults.clear()
            self.faults.append((kind, name))
            if kind == "Service":
                self.app.objects[kind, name]["spec"]["ports"][0]["targetPort"] = 9999
            else:
                self.app.objects[kind, name]["spec"]["template"]["spec"]["containers"][0]["image"] = "missing:tag"
            at = time.time()
            return {"fault": len(self.faults) - 1, "started_at": at - 0.01, "finished_at": at}
        if op == "oracle":
            selected = self.faults if "fault" not in kwargs else [self.faults[kwargs["fault"]]]
            return {
                "kind": "sregym-mitigation-oracle",
                "success": all(self.app.objects[key] == self.app.baseline[key] for key in selected),
            }
        if op == "baseline_oracle":
            assert kwargs["verify_traffic"] is True
            return {
                "kind": "health-check",
                "success": self.app.healthy(),
                "details": {"scope": "seed-baseline", "traffic_ok": self.app.healthy()},
            }
        if op == "recover":
            return {
                "seconds": 0.001,
                "protocol_version": 2,
                "recovery_contract": "seed-compatible/v1",
                "success": True,
            }
        if op == "refresh":
            return {"protocol_version": 2, "recovery_contract": "seed-compatible/v1", "success": True}
        if op == "health":
            return {"healthy": self.app.healthy()}
        raise AssertionError(op)


class Controller(FakeOps):
    def __init__(self, app: Application, capture: Path) -> None:
        super().__init__()
        self.app = app
        self.capture = capture
        self.commands: list[str] = []
        self.commands_by_incident: dict[str, list[str]] = {}
        self.program_runs_by_incident: dict[str, list[dict[str, Any]]] = {}
        self.results: dict[str, dict[str, Any]] = {}
        self.registrations: list[Path | None] = []
        self.sweep: Path | None = None
        self.sweep_result: dict[str, Any] | None = None

    def install(self, config: Any) -> bool:
        self.registrations.append(config.agent_registrations)
        assert config.agent_registrations == REGISTRATIONS
        for role, command in (("responder", "responder"), ("reflector", "reflection")):
            registration = json.loads((REGISTRATIONS / f"{role}.json").read_text())
            assert registration["kubernetes_job"]["command"][-1] == command
        return super().install(config)

    def inject_after_resume(self, control: str, generation: str, inject: Any) -> dict[str, float]:
        inject()
        self.incidents += 1
        incident = f"incident-{self.incidents}"
        self.capture.mkdir(exist_ok=True)
        (self.capture / "open.json").write_text(json.dumps(self.app.snapshot()))
        (self.capture / "open@detect.json").write_text(json.dumps(self.app.snapshot()))
        self.commands.clear()
        self.program_runs_by_incident[incident] = []
        raw = json.loads((ROOT / "tests/fixtures/sdo/contracts/incident_request.json").read_text())
        raw.update(
            incident_id=incident,
            namespace=NAMESPACE,
            repository_worktree=str(self.app.workspace),
            repair_policy="recorded-actions",
            surfaced_playbooks=[],
        )
        changed = [key for key in self.app.baseline if self.app.objects[key] != self.app.baseline[key]]
        raw["findings"] = [
            {
                "detector_id": "availability",
                "rule_id": "availability",
                "status": "active",
                "severity": "critical",
                "summary": f"Service {name} unavailable",
                "evidence": "No ready endpoints",
                "primary_resource": {"api_version": "v1", "kind": "Service", "namespace": NAMESPACE, "name": name},
            }
            for kind, name in changed
            if kind == "Service"
        ]
        changes = []
        for kind, name in changed:
            fields = []
            if kind == "Deployment":
                before = self.app.baseline[kind, name]["spec"]["template"]["spec"]["containers"][0]
                after = self.app.objects[kind, name]["spec"]["template"]["spec"]["containers"][0]
                fields.append(
                    {"field": f"container[{before['name']}].image", "before": before["image"], "after": after["image"]}
                )
            changes.append({"kind": kind, "name": name, "change": "modified", "fields": fields})
        raw["state_changes"] = {"baseline_at": now(), "observed_at": now(), "changes": changes}
        request = from_dict(IncidentRequest, raw)
        result = scripted_worker.scripted_responder(request)
        assert result.status == "completed"
        if incident == "incident-1":
            image = self.app.objects["Deployment", "rate"]["spec"]["template"]["spec"]["containers"][0]["image"]
            source_image = self.app.baseline["Deployment", "rate"]["spec"]["template"]["spec"]["containers"][0]["image"]
            assert source_image.endswith("a" * 64)
            assert image == source_image
        (self.capture / "fixed.json").write_text(json.dumps(self.app.snapshot()))
        (self.capture / "open@detect-recordings.json").write_text("[]\n")
        data = json_format.MessageToDict(result, preserving_proto_field_name=True)
        self.results[incident] = data
        self.commands_by_incident[incident] = list(self.commands)
        at = now()
        self.states[control]["pending_closure"] = {
            "request": {"incident_id": incident},
            "result": data,
            "detected_at": at,
            "dispatched_at": at,
            "responder_completed_at": at,
            "verified_at": at,
        }
        return {"controller_baseline_wait": 0.001}

    def export_runtime_artifacts(self, config: Any, directory: Path) -> dict[str, str | None]:
        incident = f"incident-{self.incidents}"
        self.reflectable.add(incident)
        return super().export_runtime_artifacts(config, directory)

    def _finish_reflection(self, control: str, incident: str) -> None:
        if incident == "incident-1":
            turn = scripted_worker.ScriptedReflector().resume(
                worktree=self.app.workspace,
                incident_id=incident,
                closure=SimpleNamespace(request=SimpleNamespace(namespace=NAMESPACE, drift_events=[])),
            )
            assert turn.playbook_ids == [scripted_worker.PROGRAM_ID]
            directory = self.app.workspace / ".sdo/playbooks" / scripted_worker.PROGRAM_ID
            corpus = next((self.app.workspace / ".sdo/corpus").iterdir())
            labels = json.loads((corpus / "labels.json").read_text())
            assert labels["provenance"]["open_at"] == "detect"
            assert (
                labels["provenance"]["open_sha256"] == hashlib.sha256((corpus / "open.json").read_bytes()).hexdigest()
            )
            assert (
                labels["provenance"]["fixed_sha256"] == hashlib.sha256((corpus / "fixed.json").read_bytes()).hexdigest()
            )
            assert (corpus / "recordings.json").read_bytes() == (
                self.capture / "open@detect-recordings.json"
            ).read_bytes()
            subprocess.run(["git", "add", ".sdo/playbooks", ".sdo/corpus"], cwd=self.app.workspace, check=True)
            subprocess.run(
                ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "reflection"],
                cwd=self.app.workspace,
                check=True,
            )
            self.sweep = self.capture.parent / "sweep"
            source = self.sweep / "programsrc"
            source.mkdir(parents=True)
            for name in ("diagnose.go", "mitigate.go"):
                shutil.copyfile(directory / name, source / name)
            (self.sweep / "go.mod").write_text(
                "module hermetic\n\ngo 1.24\n\nrequire sdo.dev/operator/sdk v0.0.0\n"
                f"replace sdo.dev/operator/sdk => {ROOT / 'sdo/operator/sdk'}\n"
            )
            (self.sweep / "main.go").write_text(SWEEP_MAIN)
        super()._finish_reflection(control, incident)

    def tool(self, *args: str) -> dict[str, Any]:
        self.commands.append(args[0])
        if args[0] == "source":
            kind, namespace, name = args[1].split("/", 2)
            assert namespace == NAMESPACE
            return {"source": deepcopy(self.app.baseline[kind, name])}
        if args[0] == "diagnose":
            if self.incidents == 2:
                assert (self.app.workspace / ".sdo/playbooks/service-targetport-mismatch/diagnose.go").is_file()
                assert self.sweep is not None
                snapshot = self.capture / "open@detect.json"
                completed = subprocess.run(
                    ["go", "run", "-mod=mod", ".", str(snapshot)],
                    cwd=self.sweep,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=90,
                )
                assert completed.returncode == 0, completed.stderr
                self.sweep_result = json.loads(completed.stdout)
                runs = [
                    {
                        "program_id": scripted_worker.PROGRAM_ID,
                        "bindings": {
                            "objects": {"service": {"kind": "Service", "namespace": NAMESPACE, "name": "geo"}}
                        },
                        "verdict": {"result": self.sweep_result["result"]},
                    }
                ]
                self.program_runs_by_incident[f"incident-{self.incidents}"] = runs
                return {"program_runs": runs}
            return {"program_runs": []}
        if args[0] == "plan":
            assert args[1] == scripted_worker.PROGRAM_ID
            assert self.sweep_result is not None
            assert self.sweep_result["path"] == "spec.ports[0].targetPort"
            return {"plan_id": "targetport-plan"}
        if args[0] == "apply":
            assert args[1:] == ("targetport-plan",)
            assert self.sweep_result is not None
            self.app.objects["Service", "geo"]["spec"]["ports"][0]["targetPort"] = self.sweep_result["value"]
            return {"healthy": False}
        if args[0] == "verify":
            return {"healthy": self.app.healthy(), "summary": "fake detector sweep"}
        raise AssertionError(args)

    def command(self, argv: list[str], **_kwargs: Any) -> str:
        if argv[0] == "kubectl":
            if argv[3] == "rollout":
                return ""
            assert argv[3] == "patch"
            kind, name = argv[4].title(), argv[5]
            patch = json.loads(argv[-1])["spec"]
            if kind == "Service":
                self.app.objects[kind, name]["spec"]["ports"] = patch["ports"]
            else:
                current = self.app.objects[kind, name]["spec"]["template"]["spec"]["containers"]
                for old, new in zip(current, patch["template"]["spec"]["containers"], strict=True):
                    old["image"] = new["image"]
            return ""
        if argv[1:3] == ["program", "scaffold"]:
            target = Path(argv[argv.index("--out") + 1])
            target.mkdir()
            (target / "diagnose_test.go").write_text("package targetport\n")
            return ""
        assert argv[1:3] == ["memory", "check"]
        assert (self.app.workspace / ".sdo/playbooks/service-targetport-mismatch/diagnose.go").is_file()
        return ""

    def collect_receipt(self, config: Any, incident_id: str, directory: Path) -> dict[str, Any]:
        assert isinstance(config.repository, Path)
        assert isinstance(config.agent_registrations, Path)
        receipt = super().collect_receipt(config, incident_id, directory)
        commands = self.commands_by_incident[incident_id]
        receipt.update(
            {
                "repair_actions": self.results[incident_id]["repair_actions"],
                "usage": {"input_tokens": 0, "output_tokens": 0},
                "reflection_usage": {"input_tokens": 0, "output_tokens": 0},
                "reflection_attempts": 1,
                "recorded_at": now(),
                "native_ledger": {
                    "outcome_document": {
                        "program_runs": self.program_runs_by_incident[incident_id],
                        "program_tool_calls": [{"command": command} for command in commands],
                        "response_loops_used": 1,
                    }
                },
            }
        )
        return receipt

    def configmaps(self) -> list[dict[str, Any]]:
        incident = f"incident-{self.incidents}"
        request, result = ProtoAny(), ProtoAny()
        request.Pack(IncidentRequest(schema_version="sdo.dev/v1alpha1", incident_id=incident))
        result.Pack(IncidentResult(schema_version="sdo.dev/v1alpha1", incident_id=incident))
        task = AgentTask(
            schema_version="sdo.dev/v1alpha1",
            task_id=incident,
            agent_id="sdo-responder",
            task_type="incident-response",
            request=request,
            workspace={"proposal_id": incident},
        )
        response = AgentResult(
            schema_version="sdo.dev/v1alpha1",
            task_id=incident,
            proposal_id=incident,
            status="completed",
            response=result,
        )
        return [
            {"data": {"agent-task.json": to_canonical_json(task)}},
            {"data": {"agent-result.json": to_canonical_json(response)}},
        ]


def test_scripted_fastloop_reuses_reflected_target_port_program(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, control_api: SimpleNamespace
) -> None:
    workspace = tmp_path / "application_workspace"
    shutil.copytree(FIXTURE_SEED, workspace)
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
    monkeypatch.setattr(cli, "_worker", lambda _environment: worker)
    monkeypatch.setattr(cli, "kubectl", lambda *_args, **_kwargs: SimpleNamespace(returncode=0))
    monkeypatch.setattr(cli, "_cluster_ops", lambda _args, _namespace: controller)
    monkeypatch.setattr(cli, "KubectlApplicationCluster", lambda _kubeconfig: app)
    monkeypatch.setattr(cli, "_composite_settings", lambda _args: CompositeSettings(20, 1, 0.001, 0.001))
    monkeypatch.setattr(
        adapter,
        "deployed_lifecycle",
        lambda _namespace: DeployedLifecycle(DeployedLifecycleContext("healthy", []), "seed"),
    )
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", str(tmp_path / "unused-kubeconfig"))
    monkeypatch.setattr(driver, "prepare_benchmark_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(driver, "_publish_agent_registrations", lambda *_args: None)
    monkeypatch.setattr(driver, "inspect_existing", lambda *_args: object())

    def install(config: object) -> object:
        assert config.deployed_manifest_paths == ("kubernetes",)
        return SimpleNamespace(reused=False, accepted_commit="a" * 40)

    monkeypatch.setattr(driver, "install_existing", install)
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
    monkeypatch.setattr(scripted_worker, "PROGRAM_SOURCE", ROOT / "benchmarks/sregym/fastloop/scripted_program")
    monkeypatch.setattr(scripted_worker, "_command", controller.command)
    monkeypatch.setattr(scripted_worker, "_tool", controller.tool)
    monkeypatch.setattr(scripted_worker.time, "sleep", lambda _seconds: None)
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
                "--time-scale",
                "0.01",
                "--health-timeout",
                "2",
            ]
        )
        == 0
    )

    records = load_records(tmp_path / "results/hermetic/incidents.jsonl")
    with (tmp_path / "results/hermetic/incidents.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(records) == len(rows) == 2
    assert all(
        row.oracle and row.oracle.success and row.recovery_oracle and row.recovery_oracle.success for row in records
    )
    assert all(row.error is None and row.injection_to_mitigation_seconds is not None for row in records)
    assert all("responder_tokens" in row and "reflection_tokens" in row for row in rows)
    assert all(Path(str(row.artifacts_dir), "sdo_production_receipt_strict.json").is_file() for row in records)
    assert control_api.deleted == control_api.names
    assert control_api.objects == {}
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
