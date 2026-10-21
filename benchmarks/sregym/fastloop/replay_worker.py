"""Re-execute a checkpointed scripted worker against captured Kubernetes objects."""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import subprocess
import tempfile
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

from google.protobuf import json_format

from benchmarks.sregym.adapter import scripted_worker
from benchmarks.sregym.fastloop.fake_kube import FakeKubernetesAPIServer
from benchmarks.sregym.fastloop.file_edits import apply_file_edits, record_file_edits
from benchmarks.sregym.fastloop.model_recording import RecordingExecutor
from benchmarks.sregym.fastloop.replay import CheckpointError, CheckpointStore, StageCheckpoint, replay_broker_result
from sdo.agents.responder import (
    CodexSessionBackend,
    SessionReflector,
    execute_incident,
    run_reflection_task,
    run_worker_task,
)
from sdo.contracts import (
    AgentResult,
    AgentTask,
    IncidentRequest,
    ReflectionRequest,
    ReflectionResult,
    parse_json,
    to_canonical_json,
)


class _Publisher:
    def __init__(self) -> None:
        self.payload: str | None = None

    def publish_payload(self, _name: str, key: str, payload: str) -> None:
        if key != "agent-result.json":
            raise CheckpointError(f"unexpected worker result key {key!r}")
        self.payload = payload


class CapturedKubernetesAPI:
    def __init__(self, captures: dict[str, bytes]) -> None:
        open_data = (
            captures.get("open@pre-apply.json") or captures.get("open@settle.json") or captures.get("open@detect.json")
        )
        fixed_data = captures.get("fixed.json")
        if open_data is None or fixed_data is None:
            raise CheckpointError("responder replay requires captured open and fixed Kubernetes snapshots")
        opened, fixed = json.loads(open_data), json.loads(fixed_data)
        self.objects = _objects(opened)
        self.fixed = _objects(fixed)
        self.plans: dict[str, list[tuple[str, str]]] = {}
        self.runs: list[dict[str, Any]] = []

    def command(self, argv: list[str], **_kwargs: Any) -> str:
        if len(argv) >= 9 and argv[0] == "kubectl" and argv[3] == "patch":
            kind = "Service" if argv[4].lower() == "service" else "Deployment"
            key = (kind, argv[5])
            if key not in self.objects:
                raise CheckpointError(f"uncaptured Kubernetes object {kind}/{argv[5]}")
            patch_value = json.loads(argv[-1])
            if kind == "Service":
                self.objects[key]["spec"]["ports"] = patch_value["spec"]["ports"]
            else:
                current = self.objects[key]["spec"]["template"]["spec"]["containers"]
                for update in patch_value["spec"]["template"]["spec"]["containers"]:
                    match = next((item for item in current if item["name"] == update["name"]), None)
                    if match is None:
                        raise CheckpointError(f"uncaptured container {update['name']!r}")
                    match["image"] = update["image"]
            return ""
        if len(argv) >= 4 and argv[0] == "kubectl" and argv[3] == "rollout":
            return ""
        raise CheckpointError(f"uncaptured Kubernetes command: {argv[:6]}")

    def tool(self, *args: str) -> dict[str, Any]:
        if args[0] == "source":
            if len(args) != 2:
                raise CheckpointError(f"invalid source reference: {args[1:]}")
            parts = args[1].split("/", 2)
            if len(parts) != 3 or (parts[0], parts[2]) not in self.fixed:
                raise CheckpointError(f"uncaptured source {args[1]!r}")
            return {"source": json.loads(json.dumps(self.fixed[parts[0], parts[2]]))}
        if args[0] == "diagnose":
            return {"program_runs": self.runs}
        if args[0] == "plan":
            plan_id = f"replay-{len(self.plans) + 1}"
            bindings = [
                value.split("=", 1)[1].split("/", 2)
                for flag, value in zip(args[2::2], args[3::2], strict=False)
                if flag == "--bind"
            ]
            self.plans[plan_id] = [(parts[0], parts[2]) for parts in bindings if len(parts) == 3]
            return {"plan_id": plan_id}
        if args[0] == "apply":
            for plan_id in args[1:]:
                for key in self.plans.get(plan_id, []):
                    if key in self.fixed:
                        self.objects[key] = json.loads(json.dumps(self.fixed[key]))
            return {"applied": True}
        if args[0] == "verify":
            healthy = all(
                _relevant(self.objects.get(key), key[0]) == _relevant(value, key[0])
                for key, value in self.fixed.items()
                if key[0] in {"Service", "Deployment"}
            )
            return {"healthy": healthy, "summary": "captured fake Kubernetes API"}
        raise CheckpointError(f"uncaptured responder tool {args[0]!r}")


def _objects(snapshot: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for kind, collection in (("Service", "services"), ("Deployment", "deployments")):
        for item in snapshot.get(collection, []):
            result[(kind, item["metadata"]["name"])] = item
    return result


def _relevant(item: dict[str, Any] | None, kind: str) -> Any:
    if item is None:
        return None
    if kind == "Service":
        return item["spec"].get("ports")
    return [(container["name"], container["image"]) for container in item["spec"]["template"]["spec"]["containers"]]


def replay_scripted_responder(store: CheckpointStore, incident: int, *, source: Path) -> dict[str, Any]:
    checkpoint = store.load(incident, "responder")
    task = parse_json(checkpoint.request, AgentTask())
    typed = IncidentRequest()
    if not task.request.Unpack(typed):
        raise CheckpointError("responder checkpoint has no typed incident request")
    api = CapturedKubernetesAPI(checkpoint.captures)
    api.runs = json_format.MessageToDict(typed, preserving_proto_field_name=True).get("initial_program_runs", [])
    with tempfile.TemporaryDirectory(prefix="sdo-fastloop-responder-") as temporary:
        checkout = Path(temporary) / "accepted"
        _clone_at(source, checkout, typed.repository_base_commit)
        typed.repository_worktree = str(checkout)
        task.workspace.checkout_path = str(checkout)
        task.request.Pack(typed)
        task_path = Path(temporary) / "request.json"
        task_path.write_text(to_canonical_json(task), encoding="utf-8")
        publisher = _Publisher()
        with ExitStack() as stack:
            stack.enter_context(patch.object(scripted_worker, "_command", api.command))
            stack.enter_context(patch.object(scripted_worker, "_tool", api.tool))
            result = run_worker_task(task_path, "replay-result", publisher, executor=scripted_worker.scripted_responder)
        if publisher.payload is None:
            raise CheckpointError("scripted responder did not publish a result")
    expected = parse_json(checkpoint.result, AgentResult()) if checkpoint.result is not None else None
    actual = json_format.MessageToDict(result.response, preserving_proto_field_name=True)
    recorded = json_format.MessageToDict(expected.response, preserving_proto_field_name=True) if expected else None
    for value in (actual, recorded):
        if isinstance(value, dict):
            value.pop("timing", None)
            for repair in value.get("repair_actions", []):
                repair.pop("started_at", None)
                repair.pop("completed_at", None)
            for verification in value.get("verification_evidence", []):
                verification.pop("observed_at", None)
    difference = list(
        difflib.unified_diff(
            json.dumps(recorded, indent=2, sort_keys=True).splitlines(),
            json.dumps(actual, indent=2, sort_keys=True).splitlines(),
            fromfile="recorded",
            tofile="current",
            lineterm="",
        )
    )
    return {
        "status": result.status,
        "identical": actual == recorded,
        "difference": difference[:80],
        "result": json.loads(publisher.payload),
    }


def replay_scripted_reflection(
    store: CheckpointStore, incident: int, *, source: Path, operator_root: Path
) -> dict[str, Any]:
    checkpoint = store.load(incident, "reflection")
    task = parse_json(checkpoint.request, AgentTask())
    typed = ReflectionRequest()
    if not task.request.Unpack(typed):
        raise CheckpointError("reflection checkpoint has no typed reflection request")
    with tempfile.TemporaryDirectory(prefix="sdo-fastloop-reflection-") as temporary:
        root = Path(temporary)
        checkout = root / "accepted"
        _clone_at(source, checkout, typed.outcome_commit)
        task.workspace.checkout_path = str(checkout)
        task.request.Pack(typed)
        task_path = root / "request.json"
        task_path.write_text(to_canonical_json(task), encoding="utf-8")
        captures = root / "corpus"
        captures.mkdir()
        for name, data in checkpoint.captures.items():
            (captures / name).write_bytes(data)
        publisher = _Publisher()
        binary = root / "sdo-operator"

        def command(argv: list[str], *, timeout: int = 180) -> str:
            if argv[0] != "sdo-operator":
                raise CheckpointError(f"unexpected reflection command: {argv[:3]}")
            if not binary.is_file():
                built = subprocess.run(
                    ["go", "build", "-o", str(binary), "./cmd/sdo-operator"],
                    cwd=operator_root,
                    capture_output=True,
                    text=True,
                    timeout=180,
                    check=False,
                )
                if built.returncode:
                    raise CheckpointError(f"build replay operator: {built.stderr.strip()[-4096:]}")
            local_args = [str(operator_root) if item == "/opt/sdo/sdo/operator" else item for item in argv[1:]]
            completed = subprocess.run(
                [str(binary), *local_args],
                cwd=checkout,
                env={**os.environ, "SDO_OPERATOR_ROOT": str(operator_root)},
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            if completed.returncode:
                raise CheckpointError(f"reflection command failed: {completed.stderr.strip()[-4096:]}")
            return completed.stdout

        with ExitStack() as stack:
            stack.enter_context(patch.object(scripted_worker, "_command", command))
            stack.enter_context(
                patch.object(scripted_worker, "PROGRAM_SOURCE", Path(__file__).with_name("scripted_program"))
            )
            stack.enter_context(patch.dict(os.environ, {"SDO_CAPTURED_CORPUS_DIR": str(captures)}))
            result = run_reflection_task(
                task_path, "replay-result", publisher, reflector=scripted_worker.ScriptedReflector()
            )
        if publisher.payload is None:
            raise CheckpointError("scripted reflection did not publish a result")
        edits = record_file_edits(checkout, typed.outcome_commit)
        verdict = replay_broker_result(
            StageCheckpoint(checkpoint.request, publisher.payload, captures=checkpoint.captures, file_edits=edits),
            source=source,
            operator_root=operator_root,
        )
    expected = parse_json(checkpoint.result, AgentResult()) if checkpoint.result is not None else None
    actual = json_format.MessageToDict(result.response, preserving_proto_field_name=True)
    recorded = json_format.MessageToDict(expected.response, preserving_proto_field_name=True) if expected else None
    difference = list(
        difflib.unified_diff(
            json.dumps(recorded, indent=2, sort_keys=True).splitlines(),
            json.dumps(actual, indent=2, sort_keys=True).splitlines(),
            fromfile="recorded",
            tofile="current",
            lineterm="",
        )
    )
    return {
        "status": result.status,
        "identical": actual == recorded,
        "difference": difference[:80],
        "result": json.loads(publisher.payload),
        "broker": verdict,
    }


def _clone_at(source: Path, destination: Path, commit: str) -> None:
    for command in (
        ["git", "clone", "--quiet", "--no-hardlinks", "--no-checkout", "--", str(source), str(destination)],
        ["git", "-C", str(destination), "reset", "--quiet", "--hard", commit],
    ):
        completed = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
        if completed.returncode:
            raise CheckpointError(f"prepare accepted replay source: {completed.stderr.strip()[-4096:]}")


def replay_live_worker(
    store: CheckpointStore,
    incident: int,
    stage: str,
    *,
    source: Path,
    live_on_cache_miss: bool,
    operator_root: Path | None = None,
) -> dict[str, Any]:
    checkpoint = store.load(incident, stage)
    operator_root = operator_root or Path(__file__).resolve().parents[3] / "sdo/operator"
    proposals: list[dict[str, Any]] = []
    task = parse_json(checkpoint.request, AgentTask())
    if stage not in {"responder", "reflection"}:
        raise CheckpointError(f"no live worker contract for stage {stage!r}")
    with tempfile.TemporaryDirectory(prefix="sdo-fastloop-live-") as temporary:
        root = Path(temporary)
        checkout = root / "accepted"
        original_checkout = task.workspace.checkout_path
        if stage == "responder":
            typed = IncidentRequest()
            if not task.request.Unpack(typed):
                raise CheckpointError("responder checkpoint has no typed incident request")
            commit = typed.repository_base_commit
            namespace = typed.namespace
            typed.repository_worktree = str(checkout)
        else:
            typed = ReflectionRequest()
            if not task.request.Unpack(typed):
                raise CheckpointError("reflection checkpoint has no typed reflection request")
            commit = typed.outcome_commit
            namespace = typed.closure.request.namespace
        _clone_at(source, checkout, commit)
        task.workspace.checkout_path = str(checkout)
        task.request.Pack(typed)
        task_path = root / "request.json"
        task_path.write_text(to_canonical_json(task), encoding="utf-8")
        session_id = typed.incident_id if stage == "responder" else typed.outcome.incident_id
        session = hashlib.sha256(session_id.encode()).hexdigest()[:32]
        incident_dirs = list(store.root.parent.glob(f"{incident:03d}_*"))
        if len(incident_dirs) != 1:
            raise CheckpointError(f"expected one incident artifact directory for {incident:03d}")
        model_dir = incident_dirs[0] / "sdo_runtime" / "sessions" / session / "model_io"
        recording = RecordingExecutor(
            model_dir,
            request_json=checkpoint.request,
            live_on_miss=live_on_cache_miss,
            prompt_replacements={str(checkout): original_checkout},
            ordered=stage == "reflection",
        )
        opened = (
            checkpoint.captures.get("open@pre-apply.json")
            or checkpoint.captures.get("open@settle.json")
            or checkpoint.captures.get("open@detect.json")
        )
        snapshot = json.loads(opened) if opened is not None else {"namespace": namespace}
        publisher = _Publisher()
        captures = root / "corpus"
        captures.mkdir()
        for name, data in checkpoint.captures.items():
            (captures / name).write_bytes(data)
        with (
            FakeKubernetesAPIServer(snapshot) as server,
            patch.dict(
                os.environ,
                {
                    "KUBECONFIG": str(server.write_kubeconfig(root / "kubeconfig")),
                    "KUBERNETES_SERVICE_HOST": "",
                    "SDO_CAPTURED_CORPUS_DIR": str(captures),
                },
            ),
        ):
            if stage == "responder":
                result = run_worker_task(
                    task_path,
                    "replay-result",
                    publisher,
                    executor=lambda request: execute_incident(
                        request, model="gpt-6-luna", provider="codex", executor=recording
                    ),
                )
            else:
                guidance = typed.guidance or "baseline"
                reflector = SessionReflector(
                    CodexSessionBackend(model="gpt-6-luna", executor=recording), guidance=guidance
                )
                result = run_reflection_task(task_path, "replay-result", publisher, reflector=reflector)
        if publisher.payload is None:
            raise CheckpointError("live worker did not publish a result")
        if stage == "reflection" and result.status == "completed":
            response = ReflectionResult()
            if not result.response.Unpack(response):
                raise CheckpointError("reflection worker returned the wrong result type")
            if response.HasField("artifact_proposal"):
                raise CheckpointError("proposal-only reflection recordings are unsupported; record direct file edits")
            if checkpoint.file_edits is None:
                raise CheckpointError("reflection recording has no direct file edits; record a new dispatch")
            apply_file_edits(checkout, checkpoint.file_edits)
            edits = record_file_edits(checkout, commit)
            verdict = replay_broker_result(
                StageCheckpoint(checkpoint.request, publisher.payload, captures=checkpoint.captures, file_edits=edits),
                source=source,
                operator_root=operator_root,
            )
            proposals.append({"artifact_ids": list(response.proposed_artifact_ids), **verdict})

    expected = parse_json(checkpoint.result, AgentResult()) if checkpoint.result is not None else None
    actual = json_format.MessageToDict(result.response, preserving_proto_field_name=True)
    recorded = json_format.MessageToDict(expected.response, preserving_proto_field_name=True) if expected else None
    difference = list(
        difflib.unified_diff(
            json.dumps(recorded, indent=2, sort_keys=True).splitlines(),
            json.dumps(actual, indent=2, sort_keys=True).splitlines(),
            fromfile="recorded",
            tofile="current",
            lineterm="",
        )
    )
    return {
        "status": "failed" if proposals and proposals[-1].get("status") == "rejected" else result.status,
        "identical": actual == recorded,
        "difference": difference[:80],
        "result": json.loads(publisher.payload),
        "proposals": proposals,
        "prompt_changes": recording.prompt_changes,
    }
