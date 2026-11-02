"""Re-execute a checkpointed scripted worker against captured Kubernetes objects."""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

from google.protobuf import json_format

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.agent_script.cli import script_environment
from benchmarks.sregym.agent_script.converter import convert_recording
from benchmarks.sregym.fastloop.fake_kube import FakeKubernetesAPIServer
from benchmarks.sregym.fastloop.file_edits import encode_file_edits, record_file_edits
from benchmarks.sregym.fastloop.reflection_script import reflection_script
from benchmarks.sregym.fastloop.replay import (
    CheckpointError,
    CheckpointStore,
    StageCheckpoint,
    materialize_captures,
    replay_broker_result,
)
from benchmarks.sregym.fastloop.replay_transport import captured_commands
from benchmarks.sregym.fastloop.responder_script import responder_script
from benchmarks.sregym.repository_git import host_clone_command, host_git_command
from sdo.agents.responder import (
    CodexSessionBackend,
    SessionReflector,
    capture_sections,
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

    def publish_payload(self, name: str, key: str, payload: str) -> None:
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
    sections = capture_sections(snapshot)
    for kind, collection in (("Service", "services"), ("Deployment", "deployments")):
        for item in sections.get(collection, []):
            result[(kind, item["metadata"]["name"])] = item
    return result


def _relevant(item: dict[str, Any] | None, kind: str) -> Any:
    if item is None:
        return None
    if kind == "Service":
        return item["spec"].get("ports")
    return [(container["name"], container["image"]) for container in item["spec"]["template"]["spec"]["containers"]]


def replay_generated_responder(store: CheckpointStore, incident: int, *, source: Path) -> dict[str, Any]:
    checkpoint = store.load(incident, "responder")
    checkpoint.require_captures()
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
        script_path = Path(temporary) / "script.json"
        script_path.write_text(responder_script(typed).model_dump_json())
        tools = Path(temporary) / "tools"
        tools.mkdir()

        def command(argv):
            return json.dumps(api.tool(*argv[1:])) if argv[0] == "sdo" else api.command(argv)

        with captured_commands(Path(temporary), command) as env, patch.dict(os.environ, env):
            with (
                patch.dict(
                    os.environ,
                    {
                        "SDO_AGENT_SCRIPT": str(script_path),
                        "SDO_TASK_PATH": str(task_path),
                        "SDO_TOOLS_DIR": str(tools),
                    },
                ),
                script_environment() as worker_env,
            ):
                with patch.dict(os.environ, worker_env):

                    def execute(request):
                        return execute_incident(request, provider="codex", model="gpt-6-luna")

                    result = run_worker_task(task_path, "replay-result", publisher, executor=execute)
        if publisher.payload is None:
            raise CheckpointError("scripted responder did not publish a result")
    expected = parse_json(checkpoint.result, AgentResult()) if checkpoint.result is not None else None
    actual = json_format.MessageToDict(result.response, preserving_proto_field_name=True)
    recorded = json_format.MessageToDict(expected.response, preserving_proto_field_name=True) if expected else None
    for value in (actual, recorded):
        if isinstance(value, dict):
            value.pop("timing", None)
            value.pop("usage", None)
            value.pop("responder_session_id", None)
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


def _read_only_git(args: list[str]) -> bool:
    """Whether ``git`` arguments only inspect a checkout (configuration overrides, then diff or ls-files)."""

    while args[:1] == ["-c"]:
        args = args[2:]
    return args[:1] in (["diff"], ["ls-files"])


def replay_generated_reflection(
    store: CheckpointStore, incident: int, *, source: Path, operator_root: Path
) -> dict[str, Any]:
    checkpoint = store.load(incident, "reflection")
    checkpoint.require_captures()
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
        materialize_captures(captures, checkpoint.captures)
        publisher = _Publisher()
        binary = root / "sdo-operator"

        def command(argv: list[str], *, timeout: int = int(wait_for(WaitKind.ROLLOUT))) -> str:
            git = host_git_command(checkout)
            if argv[: len(git)] == git and _read_only_git(argv[len(git) :]):
                # The reflector decides from its own checkout's diff; inspection never writes.
                inspected = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
                if inspected.returncode:
                    raise CheckpointError(f"reflection git inspection failed: {inspected.stderr.strip()[-4096:]}")
                return inspected.stdout
            if argv[0] != "sdo-operator":
                raise CheckpointError(f"unexpected reflection command: {argv[:3]}")
            if not binary.is_file():
                built = subprocess.run(
                    ["go", "build", "-o", str(binary), "./cmd/sdo-operator"],
                    cwd=operator_root,
                    capture_output=True,
                    text=True,
                    timeout=wait_for(WaitKind.ROLLOUT),
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

        tools = root / "tools"
        tools.mkdir()
        script_path = root / "script.json"
        with patch.dict(os.environ, {"SDO_TOOLS_DIR": str(tools)}):
            script_path.write_text(
                reflection_script(
                    worktree=checkout, incident_id=typed.outcome.incident_id, closure=typed.closure, capture=captures
                ).model_dump_json()
            )
        with captured_commands(root, command) as env, patch.dict(os.environ, env):
            with (
                patch.dict(
                    os.environ,
                    {
                        "SDO_AGENT_SCRIPT": str(script_path),
                        "SDO_TASK_PATH": str(task_path),
                        "SDO_TOOLS_DIR": str(tools),
                        "SDO_CAPTURED_CORPUS_DIR": str(captures),
                    },
                ),
                script_environment() as worker_env,
            ):
                with patch.dict(os.environ, worker_env):
                    result = run_reflection_task(
                        task_path,
                        "replay-result",
                        publisher,
                        reflector=SessionReflector(
                            CodexSessionBackend(model="gpt-6-luna"), guidance=typed.guidance or "baseline"
                        ),
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
        host_clone_command(source, destination, "--quiet", "--no-hardlinks", "--no-checkout"),
        host_git_command(destination, "reset", "--quiet", "--hard", commit),
    ):
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=wait_for(WaitKind.SLOW_COMMAND), check=False
        )
        if completed.returncode:
            raise CheckpointError(f"prepare accepted replay source: {completed.stderr.strip()[-4096:]}")


def replay_script_worker(
    store: CheckpointStore,
    incident: int,
    stage: str,
    *,
    source: Path,
    operator_root: Path | None = None,
    script: Path | None = None,
) -> dict[str, Any]:
    checkpoint = store.load(incident, stage)
    checkpoint.require_captures()
    operator_root = operator_root or Path(__file__).resolve().parents[3] / "sdo/operator"
    proposals: list[dict[str, Any]] = []
    task = parse_json(checkpoint.request, AgentTask())
    if stage not in {"responder", "reflection"}:
        raise CheckpointError(f"no worker script contract for stage {stage!r}")
    with tempfile.TemporaryDirectory(prefix="sdo-fastloop-script-") as temporary:
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
            session_id = typed.incident_id
        else:
            typed = ReflectionRequest()
            if not task.request.Unpack(typed):
                raise CheckpointError("reflection checkpoint has no typed reflection request")
            commit = typed.outcome_commit
            namespace = typed.closure.request.namespace
            session_id = typed.outcome.incident_id
        _clone_at(source, checkout, commit)
        task.workspace.checkout_path = str(checkout)
        task.request.Pack(typed)
        task_path = root / "request.json"
        task_path.write_text(to_canonical_json(task), encoding="utf-8")
        session = hashlib.sha256(session_id.encode()).hexdigest()[:32]
        incident_dirs = list(store.root.parent.glob(f"{incident:03d}_*"))
        if len(incident_dirs) != 1 and script is None:
            raise CheckpointError(f"expected one incident artifact directory for {incident:03d}")
        model_dir = (incident_dirs[0] if incident_dirs else root) / "sdo_runtime" / "sessions" / session / "model_io"
        paths = list((model_dir / hashlib.sha256(checkpoint.request.encode()).hexdigest()).glob("*.json"))
        edits_path = root / "file_edits.json"
        if checkpoint.file_edits is not None:
            edits_path.write_bytes(encode_file_edits(checkpoint.file_edits))
        exported = (
            model_dir.parent / "checkpoints" / hashlib.sha256(checkpoint.request.encode()).hexdigest() / "script.json"
        )
        script_path = root / "script.json"
        if script is not None:
            script_path.write_text(script.read_text().replace(original_checkout, str(checkout)))
        elif exported.is_file():
            script_path.write_text(exported.read_text().replace(original_checkout, str(checkout)))
        else:
            if not paths:
                raise CheckpointError("missing agent script and model recordings; no model fallback")
            plan = convert_recording(
                paths,
                role="reflector" if stage == "reflection" else "responder",
                file_edits=edits_path if edits_path.exists() else None,
            )
            script_path.write_text(plan.model_dump_json().replace(original_checkout, str(checkout)))
        # Relocate recorded worker paths into this isolated replay session.
        relocated = script_path.read_text()
        for old, new in {
            "/sdo/session/tools": str(root / "tools"),
            "/sdo/session/corpus": str(root / "corpus"),
            "/opt/sdo/sdo/operator": str(operator_root),
        }.items():
            relocated = relocated.replace(old, new)
        script_path.write_text(relocated)
        opened = (
            checkpoint.captures.get("open@pre-apply.json")
            or checkpoint.captures.get("open@settle.json")
            or checkpoint.captures.get("open@detect.json")
        )
        snapshot = json.loads(opened) if opened is not None else {}
        snapshot.setdefault("namespace", namespace)
        publisher = _Publisher()
        captures = root / "corpus"
        captures.mkdir()
        materialize_captures(captures, checkpoint.captures)
        api = CapturedKubernetesAPI(checkpoint.captures) if stage == "responder" else None
        if api is not None:
            api.runs = json_format.MessageToDict(typed, preserving_proto_field_name=True).get(
                "initial_program_runs", []
            )
        kubectl = shutil.which("kubectl")
        binary = root / "sdo-operator"

        def sync_captured_objects() -> None:
            if api is not None:
                for (kind, _), obj in api.objects.items():
                    server.set_object(
                        "services" if kind == "Service" else "deployments",
                        dict(obj, kind=kind, apiVersion="v1" if kind == "Service" else "apps/v1"),
                    )

        def command(argv: list[str]) -> str:
            if argv[0] == "sdo" and api is not None:
                value = api.tool(*argv[1:])
                sync_captured_objects()
                return json.dumps(value)
            if argv[0] == "kubectl":
                if api is not None:
                    try:
                        value = api.command(argv)
                        sync_captured_objects()
                        return value
                    except CheckpointError:
                        pass
                if kubectl is None:
                    raise CheckpointError("kubectl is required for captured Kubernetes reads")
                argv = [kubectl, *argv[1:]]
            elif argv[0] == "sdo-operator":
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
                        raise CheckpointError(f"build replay operator: {built.stderr[-4096:]}")
                argv = [str(binary), *argv[1:]]
            else:
                raise CheckpointError(f"uncaptured worker command: {argv[:3]}")
            completed = subprocess.run(
                argv,
                cwd=checkout,
                env={**os.environ, "SDO_OPERATOR_ROOT": str(operator_root)},
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            if completed.returncode:
                raise CheckpointError(f"replay command failed: {completed.stderr[-4096:]}")
            if api is not None and kubectl is not None and argv[0] == kubectl:
                api.objects = {
                    (obj["kind"], obj["metadata"]["name"]): obj
                    for obj in server.objects.values()
                    if obj.get("kind") in {"Service", "Deployment"}
                }
            return completed.stdout

        (root / "tools").mkdir()
        with (
            captured_commands(root, command) as tool_env,
            patch.dict(os.environ, tool_env),
            patch.dict(
                os.environ,
                {
                    "SDO_AGENT_SCRIPT": str(script_path),
                    "SDO_TASK_PATH": str(task_path),
                    "SDO_TOOLS_DIR": str(root / "tools"),
                },
            ),
            script_environment() as script_env,
            patch.dict(os.environ, script_env),
            FakeKubernetesAPIServer(snapshot) as server,
            patch.dict(
                os.environ,
                {
                    "KUBECONFIG": str(server.write_kubeconfig(root / "kubeconfig")),
                    "KUBERNETES_SERVICE_HOST": "",
                    "SDO_CAPTURED_CORPUS_DIR": str(captures),
                    "SDO_OPERATOR_ROOT": str(operator_root),
                    "SDO_HEALTHY_BASELINE": "",
                    "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
                },
            ),
        ):
            if stage == "responder":
                result = run_worker_task(
                    task_path,
                    "replay-result",
                    publisher,
                    executor=lambda request: execute_incident(request, model="gpt-6-luna", provider="codex"),
                )
            else:
                if not isinstance(typed, ReflectionRequest):
                    raise CheckpointError("reflection checkpoint requires a reflection request")
                guidance = typed.guidance or "baseline"
                reflector = SessionReflector(CodexSessionBackend(model="gpt-6-luna"), guidance=guidance)
                result = run_reflection_task(task_path, "replay-result", publisher, reflector=reflector)
        if publisher.payload is None:
            raise CheckpointError("script worker did not publish a result")
        if stage == "reflection" and result.status == "completed":
            response = ReflectionResult()
            if not result.response.Unpack(response):
                raise CheckpointError("reflection worker returned the wrong result type")
            if response.HasField("artifact_proposal"):
                raise CheckpointError("proposal-only reflection recordings are unsupported; record direct file edits")
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
    }
