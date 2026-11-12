"""A two-incident scripted run through the CLI and persistent controller adapter."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import threading
import time
from copy import deepcopy
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

from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from sdo.contracts import AgentResult, AgentTask, IncidentRequest, IncidentResult, to_canonical_json
from tests.proto_messages import from_dict
from tests.unit.benchmarks.sregym.adapter.test_persistent import FakeOps
from tests.unit.benchmarks.sregym.fastloop import script_harness as scripts

pytestmark = pytest.mark.go_integration

if TYPE_CHECKING:
    from collections.abc import Iterator

ROOT = Path(__file__).resolve().parents[5]
FIXTURE_SEED = ROOT / "tests/fixtures/sdo/scripted_seed"
STREAM = ROOT / "tests/fixtures/sregym/fastloop/pbprog_explicit.json"
REGISTRATIONS = ROOT / "benchmarks/sregym/fastloop/agent_registrations"
NAMESPACE = "hotel-reservation"
HEALTH_DETECTORS = ("workload-health", "service-endpoints", "service-reachability", "traffic-health")
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
	var calls []sdktest.PrimitiveCall
	recorded, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(recorded, &calls); err != nil {
		panic(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
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
        SourceCommit: "accepted",
        Source: func(ref sdk.ObjectRef) (map[string]any, error) {
            raw, err := os.ReadFile(os.Args[3])
            if err != nil { return nil, err }
            var object map[string]any
            err = json.Unmarshal(raw, &object)
            return object, err
        },
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


def _patch_kubectl(monkeypatch: pytest.MonkeyPatch, api: object) -> None:
    """Admission and its worker-safe readiness module both read the API."""
    from benchmarks.sregym.fastloop import admission as admission_module
    from benchmarks.sregym.fastloop import workload_readiness

    monkeypatch.setattr(admission_module, "kubectl", api)
    monkeypatch.setattr(workload_readiness, "kubectl", api)


@pytest.fixture
def control_api(tmp_path: Path) -> Iterator[SimpleNamespace]:
    """A control namespace API holding prior-install leftovers that a non-toy row must not delete selectively."""
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
                self.apply(NAMESPACE, item)
        self.baseline = deepcopy(self.objects)

    def list_objects(self, _namespace: str) -> list[dict[str, Any]]:
        return [
            self.read("deployment", name) if kind == "Deployment" else deepcopy(item)
            for (kind, name), item in self.objects.items()
        ]

    def apply(self, _namespace: str, item: dict[str, Any]) -> None:
        stored = deepcopy(item)
        stored.pop("status", None)
        stored["metadata"].pop("generation", None)
        self.objects[stored["kind"], stored["metadata"]["name"]] = stored

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


def test_fake_application_recovers_after_interrupted_responder() -> None:
    """Restoring a captured live object must not retain server-owned status."""
    from benchmarks.sregym.fastloop.baseline_recovery import reconcile_application

    app = Application(FIXTURE_SEED)
    baseline = ApplicationBaseline(NAMESPACE, app.list_objects(NAMESPACE))
    app.objects["Deployment", "search"]["spec"]["template"]["spec"]["containers"][0]["image"] = "missing:tag"
    app.objects["Service", "geo"]["spec"]["ports"][0]["targetPort"] = 9999
    assert not app.healthy()
    cleanup = reconcile_application(app, baseline, NAMESPACE)
    assert set(cleanup.restored) == {"Deployment/search", "Service/geo"}
    assert app.healthy()


class Worker:
    def __init__(self, app: Application) -> None:
        self.app = app
        self.faults: list[tuple[str, str]] = []
        self.rehearsed: list[str] = []

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
        if op == "deploy":
            return {"deployed": True}
        if op == "rehearse_oracles":
            assert not self.faults
            assert self.app.healthy()
            self.rehearsed.append(str(kwargs["problem_id"]))
            return {"success": True}
        if op == "inject":
            problem = str(kwargs["problem_id"])
            assert problem in self.rehearsed
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
        self.reflection_lock = threading.Lock()

    def install(self, config: Any) -> bool:
        self.registrations.append(config.agent_registrations)
        assert config.agent_registrations == REGISTRATIONS
        for role, command in (
            ("responder", "sdo.agents.responder.worker"),
            ("reflector", "sdo.agents.responder.reflection_worker"),
        ):
            registration = json.loads((REGISTRATIONS / f"{role}.json").read_text())
            assert registration["kubernetes_job"]["command"][-1] == command
        return super().install(config)

    def _write_recordings(self, comparisons: list[tuple[str, str, bool]], *, fixed: bool) -> None:
        recordings = [
            {
                "method": "DiffAgainstSource",
                "arguments": [
                    {
                        "api_version": "apps/v1" if kind == "Deployment" else "v1",
                        "kind": kind,
                        "namespace": NAMESPACE,
                        "name": name,
                    }
                ],
                "result": {"changed": differs and phase == "open", "text": "accepted: {}\nobserved: {}"},
                **({"phase": "fixed"} if phase == "fixed" else {}),
            }
            for phase in (("open", "fixed") if fixed else ("open",))
            for kind, name, differs in comparisons
            if differs or phase == "open"
        ]
        for phase in ("detect", "pre-apply"):
            (self.capture / f"open@{phase}-recordings.json").write_text(json.dumps(recordings) + "\n")

    def inject_after_resume(self, control: str, generation: str, inject: Any) -> dict[str, float]:
        inject()
        self.incidents += 1
        incident = f"incident-{self.incidents}"
        self.capture.mkdir(exist_ok=True)
        (self.capture / "open.json").write_text(json.dumps(self.app.snapshot()))
        for phase in ("detect", "pre-apply"):
            (self.capture / f"open@{phase}.json").write_text(json.dumps(self.app.snapshot()))
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
        # The controller records every object's source comparison at each capture boundary, then the same
        # comparison again at the verified fixed boundary. The program under test reads them back.
        comparisons = [
            (kind, name, (kind, name) in changed)
            for kind, name in self.app.objects
            if kind in ("Service", "Deployment")
        ]
        self._write_recordings(comparisons, fixed=False)
        result = scripts.respond(request)
        assert result.status == "completed"
        if incident == "incident-1":
            image = self.app.objects["Deployment", "rate"]["spec"]["template"]["spec"]["containers"][0]["image"]
            source_image = self.app.baseline["Deployment", "rate"]["spec"]["template"]["spec"]["containers"][0]["image"]
            assert source_image.endswith("a" * 64)
            assert image == source_image
        (self.capture / "fixed.json").write_text(json.dumps(self.app.snapshot()))
        self._write_recordings(comparisons, fixed=True)
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
        # Telemetry and receipt polling share this fake. Only one observer may
        # materialize/compile the learned program and acknowledge the closure.
        with self.reflection_lock:
            closure = self.states[control].get("pending_closure")
            if not closure or closure["request"]["incident_id"] != incident:
                return
            if incident == "incident-1":
                turn = scripts.Reflector().learn(
                    worktree=self.app.workspace,
                    incident_id=incident,
                    closure=SimpleNamespace(request=SimpleNamespace(namespace=NAMESPACE, drift_events=[])),
                )
                assert turn.note
                directory = self.app.workspace / ".sdo/playbooks" / scripts.PROGRAM_ID
                corpus = next((self.app.workspace / ".sdo/corpus").iterdir())
                labels = json.loads((corpus / "labels.json").read_text())
                assert labels["provenance"]["open_at"] == "pre-apply"
                assert (
                    labels["provenance"]["open_sha256"]
                    == hashlib.sha256((corpus / "open.json").read_bytes()).hexdigest()
                )
                assert (
                    labels["provenance"]["fixed_sha256"]
                    == hashlib.sha256((corpus / "fixed.json").read_bytes()).hexdigest()
                )
                assert (corpus / "recordings.json").read_bytes() == (
                    self.capture / "open@pre-apply-recordings.json"
                ).read_bytes()
                subprocess.run(["git", "add", ".sdo/playbooks", ".sdo/corpus"], cwd=self.app.workspace, check=True)
                subprocess.run(
                    [
                        "git",
                        "-c",
                        "user.name=Test",
                        "-c",
                        "user.email=test@example.invalid",
                        "commit",
                        "-qm",
                        "reflection",
                    ],
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
                # Compilation belongs to preparation, like the real builder. Keep
                # host compiler scheduling outside the recurrence response window.
                subprocess.run(
                    ["go", "build", "-mod=mod", "-o", "diagnose", "."],
                    cwd=self.sweep,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=180,
                )
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
                accepted = self.sweep / "accepted-service.json"
                accepted.write_text(json.dumps(self.app.baseline["Service", "geo"]))
                completed = subprocess.run(
                    [
                        str(self.sweep / "diagnose"),
                        str(snapshot),
                        str(self.capture / "open@detect-recordings.json"),
                        str(accepted),
                    ],
                    cwd=self.sweep,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                assert completed.returncode == 0, completed.stderr
                self.sweep_result = json.loads(completed.stdout)
                runs = [
                    {
                        "program_id": scripts.PROGRAM_ID,
                        "bindings": {"service": {"kind": "Service", "namespace": NAMESPACE, "name": "geo"}},
                        "verdict": {"result": self.sweep_result["result"]},
                    }
                ]
                self.program_runs_by_incident[f"incident-{self.incidents}"] = runs
                return {"program_runs": runs}
            return {"program_runs": []}
        if args[0] == "plan":
            assert args[1] == scripts.PROGRAM_ID
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
            target.mkdir(parents=True)
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


def test_fake_controller_finishes_each_reflection_once() -> None:
    """The telemetry sampler and receipt poller may observe the same closure."""
    from concurrent.futures import ThreadPoolExecutor

    controller = Controller(Application(FIXTURE_SEED), FIXTURE_SEED)
    control, incident = "hotel-reservation-sdo", "incident-2"
    controller.states[control] = {"pending_closure": {"request": {"incident_id": incident}}}
    controller.logs[control] = []
    with ThreadPoolExecutor(max_workers=2) as readers:
        list(readers.map(lambda _: controller._finish_reflection(control, incident), range(2)))
    assert controller.events == [("reflected", incident)]
    assert controller.states[control] == {"last_acknowledged_incident_id": incident}
