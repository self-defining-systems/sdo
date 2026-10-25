"""Hand-written controller rehearsal directives use the production tool boundary."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.scripted_replay import replay_recorded_repair as replay
from benchmarks.sregym.assurance.scripted_codex.mixed10 import SLOTS, directive_for


class Tools:
    def __init__(self, *, failures: bool = False) -> None:
        self.commands: list[str] = []
        self.failures = failures
        self.verifies = 0

    def __call__(self, command: str, *, timeout: float = 300) -> tuple[int, str]:
        self.commands.append(command)
        args = shlex.split(command)
        if args[:2] == ["sdo", "source"]:
            kind, namespace, name = args[2].split("/")
            source = {
                "apiVersion": "apps/v1" if kind == "Deployment" else "v1",
                "kind": kind,
                "metadata": {"name": name, "namespace": namespace},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": name}},
                    "template": {"spec": {"containers": [{"name": name, "image": "seed@sha256:abc"}]}},
                },
                "data": {"test": "baseline"},
            }
            return 0, json.dumps({"source": source})
        if args[:2] == ["sdo", "verify"]:
            self.verifies += 1
            return 0, json.dumps({"healthy": self.verifies > 40, "summary": "independent verification"})
        if args[:2] == ["sdo", "act"]:
            return (1, "mutation failed") if self.failures else (0, "recorded mutation")
        if "logs" in args:
            return 0, "record validation failed at offset=20"
        if "pods" in args:
            return 0, json.dumps({"items": []})
        if "deployments" in args:
            return 0, json.dumps({"items": []})
        return 0, "{}"


@pytest.mark.parametrize(("slot", "passes"), [(1, 2), (4, 2), (2, 1)])
def test_credential_rehearsals_require_two_cleanup_passes(tmp_path: Path, slot: int, passes: int) -> None:
    from benchmarks.sregym.assurance.mixed10_rehearsal import runner_arguments
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(runner_arguments(slot, tmp_path, tmp_path / "output"))
    assert args.cleanup_passes == passes


def test_fresh_scripted_lane_prepares_empty_credentials_without_reading_secrets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import subprocess
    from types import SimpleNamespace

    from benchmarks.sregym.assurance import mixed10_rehearsal

    created = []

    def create(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command == ["kubectl", "--kubeconfig", str(tmp_path / "kubeconfig"), "create", "-f", "-"]
        created.append(json.loads(str(kwargs["input"])))
        return subprocess.CompletedProcess(command, 0, "created", "")

    monkeypatch.setattr(mixed10_rehearsal.subprocess, "run", create)
    environment = SimpleNamespace(namespace="social-network", kubeconfig=tmp_path / "kubeconfig")
    mixed10_rehearsal.prepare_scripted_control(environment)
    assert created == [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "social-network-sdo"}},
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "Opaque",
            "metadata": {"name": "sdo-agent-credentials", "namespace": "social-network-sdo"},
        },
    ]


@pytest.mark.parametrize("exists", [True, False])
def test_scripted_control_only_tolerates_already_existing_resources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exists: bool
) -> None:
    import subprocess
    from types import SimpleNamespace

    from benchmarks.sregym.assurance import mixed10_rehearsal

    def create(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, "", "(AlreadyExists)" if exists else "(Forbidden)")

    monkeypatch.setattr(mixed10_rehearsal.subprocess, "run", create)
    environment = SimpleNamespace(namespace="social-network", kubeconfig=tmp_path / "kubeconfig")
    if exists:
        mixed10_rehearsal.prepare_scripted_control(environment)
    else:
        with pytest.raises(RuntimeError, match="scripted Namespace"):
            mixed10_rehearsal.prepare_scripted_control(environment)


@pytest.mark.parametrize("change", ["none", "unqualified-source", "topology", "objective", "missing-detector"])
def test_scripted_health_refresh_retains_complete_bundle_only_for_same_objective_and_topology(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from benchmarks.sregym.adapter import scripted_worker
    from sdo.contracts import HealthJudgeRequest, SourceResource

    monkeypatch.setenv("SDO_SCRIPTED_REPLAY_SLOT", "2")
    sdo = tmp_path / ".sdo"
    (sdo / "rendered").mkdir(parents=True)
    goal_document = "---\nschema_version: 1\nowner: human\n---\n# Health objective\n\nhealthy\n"
    (sdo / "goal.md").write_text(goal_document)
    names = {"health-objective", "service-endpoints", "service-reachability", "workload-health"}
    for name in names:
        if change == "missing-detector" and name == "workload-health":
            continue
        path = sdo / "diagnostics/detectors/health" / name / "manifest.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(f"id: {name}\nclass: health\nowner: health_judge\n")
    ref = {"kind": "Deployment", "namespace": "social-network", "name": "synthetic"}
    (sdo / "rendered/lock.json").write_text(json.dumps({"manifest_paths": ["kubernetes"], "objects": [{"ref": ref}]}))
    request = HealthJudgeRequest(
        namespace="social-network",
        health_objective="changed" if change == "objective" else "healthy",
        accepted_source_commit="a" * 40,
        source_resources=[SourceResource(**ref, source="kubernetes/app.yaml")],
    )
    if change == "topology":
        request.source_resources.add(
            kind="Service", namespace="social-network", name="new", source="kubernetes/new.yaml"
        )
    if change == "unqualified-source":
        request.source_resources[0].namespace = "default"
    if change not in ("none", "unqualified-source"):
        with pytest.raises(ValueError, match="scripted health refresh"):
            scripted_worker.scripted_health_judge(request, tmp_path)
    else:
        result = scripted_worker.scripted_health_judge(request, tmp_path)
        assert result.authored
        assert set(result.detector_ids) == names
        assert not result.HasField("artifact_proposal")
        assert (sdo / "goal.md").read_text() == goal_document

        from sdo.contracts import (
            SCHEMA_VERSION,
            AgentResult,
            AgentTask,
            HealthJudgeResult,
            parse_json,
            to_canonical_json,
        )

        request.application = "synthetic"
        request.attempt = request.validation_attempt = 1
        request.architecture_summary_markdown = "Synthetic accepted architecture"
        request.source_topology_fingerprint = "b" * 64
        task = AgentTask(
            schema_version=SCHEMA_VERSION,
            task_id="refresh",
            agent_id="sdo-health-judge",
            task_type="health-judgment-direct-v1",
        )
        task.workspace.proposal_id = "refresh"
        task.workspace.checkout_path = str(tmp_path)
        task.workspace.base_commit = "a" * 40
        task.request.Pack(request)
        task_path = tmp_path / "task.json"
        task_path.write_text(to_canonical_json(task))
        published = []

        class Publisher:
            def publish_payload(self, name: str, key: str, payload: str) -> None:
                published.append((name, key, payload))

        monkeypatch.setattr(scripted_worker, "KubernetesResultPublisher", lambda **kwargs: Publisher())
        monkeypatch.setenv("SDO_NAMESPACE", "social-network-sdo")
        monkeypatch.setenv("SDO_RESULT_CONFIGMAP", "refresh-result")
        monkeypatch.setenv("SDO_TASK_PATH", str(task_path))
        assert scripted_worker.main(["health-judge"]) == 0
        assert published[0][:2] == ("refresh-result", "agent-result.json")
        envelope = parse_json(published[0][2], AgentResult())
        typed = HealthJudgeResult()
        assert envelope.response.Unpack(typed)
        assert envelope.status == "completed"
        assert set(typed.detector_ids) == names


def test_exact_stream_directives() -> None:
    assert len(SLOTS) == 10
    assert [slot.problem_id.split("__")[3] for slot in SLOTS] == [
        "s1416840650",
        "s64878673",
        "s3455414167",
        "s2571351228",
        "s1913342792",
        "s2092627323",
        "s1471960113",
        "s1969073207",
        "s2139970257",
        "s2045972234",
    ]
    for index in range(10):
        directive = directive_for(index)
        assert directive.extra["slot"] == str(index)
        assert directive.status_attempts * 5 >= 600
        assert directive.reflection == "no_change"


@pytest.mark.parametrize("index", range(10))
def test_replay_uses_recorded_mutations_and_waits_for_slow_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    index: int,
) -> None:
    monkeypatch.setattr("benchmarks.sregym.adapter.scripted_replay.time.sleep", lambda _: None)
    tools = Tools()
    manifests = tmp_path / "kubernetes"
    manifests.mkdir()
    for step in SLOTS[index].steps:
        if step.operation in {"restore", "credentials"}:
            _, output = tools(f"sdo source {step.kind}/{SLOTS[index].namespace}/{step.name}")
            source = json.loads(output)["source"]
            (manifests / f"{step.kind}-{step.name}.yaml").write_text(json.dumps(source))
    result = replay(tools, index, {"incident_id": "test", "namespace": SLOTS[index].namespace}, tmp_path)
    assert result["status"] == "completed"
    assert tools.verifies == 41  # More than the old 24 * 5s polling window.
    assert any(command.startswith("sdo act ") for command in tools.commands)
    assert not result["repair_actions"]  # Real sdo act receipts are merged by the real worker.
    assert all("get secret" not in command.lower() for command in tools.commands)
    assert all(
        not command.startswith("kubectl") or any(f" {verb} " in command for verb in ("get", "logs", "wait"))
        for command in tools.commands
    )


def test_failed_mutation_never_claims_completion(tmp_path: Path) -> None:
    result = replay(Tools(failures=True), 2, {"incident_id": "test", "namespace": "social-network"}, tmp_path)
    assert result["status"] == "failed"
    assert not result["confirmed_root_causes"]


def test_wrong_namespace_rejected_before_any_command(tmp_path: Path) -> None:
    tools = Tools()
    with pytest.raises(ValueError, match="namespace"):
        replay(tools, 2, {"incident_id": "test", "namespace": "other"}, tmp_path)
    assert not tools.commands


@pytest.mark.parametrize("slot", [3, 9])
def test_real_sdo_cli_records_and_merges_replay_actions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slot: int
) -> None:
    """Exercise real source/act/verify file transport and the adapter result codec."""
    import os
    import sys
    import threading
    import time

    from google.protobuf import json_format

    from benchmarks.sregym.adapter import scripted_worker
    from sdo.contracts import IncidentRequest

    root = Path(__file__).resolve().parents[5]
    manifests = tmp_path / "kubernetes"
    manifests.mkdir()
    source = {}
    tools = Tools()
    for step in SLOTS[slot].steps:
        if step.operation == "restore":
            ref = f"{step.kind}/{SLOTS[slot].namespace}/{step.name}"
            _, output = tools(f"sdo source {ref}")
            source[ref] = json.loads(output)["source"]
            (manifests / f"{step.kind}-{step.name}.yaml").write_text(json.dumps(source[ref]))
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"source": source, "commands": []}))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    sdo = bin_dir / "sdo"
    sdo.write_text(f"#!{sys.executable}\nfrom sdo.agents.responder.tools_cli import main\nraise SystemExit(main())\n")
    sdo.chmod(0o755)
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(
        f"#!{sys.executable}\n"
        + """import json, os, sys
from pathlib import Path
p = Path(os.environ['REPLAY_TEST_STATE'])
state = json.loads(p.read_text())
args = sys.argv[1:]
state['commands'].append(args)
p.write_text(json.dumps(state))
if 'get' in args and 'deployments' in args:
    print(json.dumps({'items': []}))
elif 'get' in args and 'pods' in args:
    print(json.dumps({'items': [{'metadata': {'name': 'url-shorten-pending'}, 'status': {'phase': 'Pending'}}]}))
else:
    print('mutation succeeded')
"""
    )
    kubectl.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PYTHONPATH", str(root))
    monkeypatch.setenv("REPLAY_TEST_STATE", str(state))
    monkeypatch.setenv("SDO_SCRIPTED_REPLAY_SLOT", str(slot))
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path / "scratch"))
    session = tmp_path / "tools"
    for child in ("requests", "responses"):
        (session / child).mkdir(parents=True)
    monkeypatch.setenv("SDO_TOOLS_DIR", str(session))
    stop = threading.Event()
    observed = []

    def server() -> None:
        while not stop.is_set():
            for path in (session / "requests").glob("*.json"):
                if path.name.startswith("."):
                    continue
                request = json.loads(path.read_text())
                observed.append(request["command"])
                response = {"source": source[request["ref"]]} if request["command"] == "source" else {"healthy": True}
                # Match Go's atomic response publication: readers may observe
                # the file as soon as its final name exists.
                temporary = session / "responses" / ("." + path.name)
                temporary.write_text(json.dumps(response))
                temporary.replace(session / "responses" / path.name)
                path.unlink()
            time.sleep(0.005)

    thread = threading.Thread(target=server, daemon=True)
    thread.start()
    raw = json.loads((root / "tests/fixtures/sdo/contracts/incident_request.json").read_text())
    raw.update(namespace=SLOTS[slot].namespace, repository_worktree=str(tmp_path))
    request = json_format.ParseDict(raw, IncidentRequest())
    try:
        result = scripted_worker.scripted_responder(request)
    finally:
        stop.set()
        thread.join(timeout=5)
    assert result.status == "completed", result
    expected = 2 if slot == 3 else 9
    assert len(result.repair_actions) == expected
    assert all(action.success for action in result.repair_actions)
    assert observed.count("pre-apply") == expected
    assert observed.count("verify") == 1
    assert not any(".sdo/" in command for command in observed)
    receipts = [json.loads(line) for line in (tmp_path / "scratch/repair-actions.jsonl").read_text().splitlines()]
    assert len(receipts) == expected * 2
    assert all("sdo.dev/source-restoration" in path.read_text() for path in manifests.iterdir())
    if slot == 9:
        commands = json.loads(state.read_text())["commands"]
        assert any("url-shorten-pending" in command for command in commands)
        # The strategy restoration follows both Recreate and pod replacement.
        assert commands[-2][2:4] == ["patch", "deployment"]


def test_phase_b_command_uses_exact_slot_and_no_model_registrations(tmp_path: Path) -> None:
    from benchmarks.sregym.assurance.mixed10_rehearsal import prepare, runner_arguments
    from benchmarks.sregym.fastloop.cli import build_parser
    from benchmarks.sregym.fastloop.stream import load_stream_config

    output = tmp_path / "slot-9"
    prepare(9, output)
    stream = load_stream_config(output / "stream.json")
    assert stream.incidents == ((SLOTS[9].problem_id,),)
    assert stream.capability_mode == "permissive"
    assert stream.permissive_responder
    args = build_parser().parse_args(runner_arguments(9, tmp_path, output))
    assert args.slot_offset == 9
    assert args.require_faulty_oracle
    assert args.inject_before_resume
    assert args.require_full_cleanup
    for path in (output / "registrations").iterdir():
        command = json.loads(path.read_text())["kubernetes_job"]["command"]
        assert "SDO_SCRIPTED_REPLAY_SLOT=9" in command
        assert "benchmarks.sregym.adapter.scripted_worker" in command
    from sdo.contracts import HealthJudgeRequest, HealthJudgeResult

    judge = json.loads((output / "registrations/health-judge.json").read_text())
    capability = judge["capabilities"][0]
    assert capability["request_type_url"] == "type.googleapis.com/" + HealthJudgeRequest.DESCRIPTOR.full_name
    assert capability["result_type_url"] == "type.googleapis.com/" + HealthJudgeResult.DESCRIPTOR.full_name


def test_service_restoration_preserves_cluster_allocated_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("benchmarks.sregym.adapter.scripted_replay.time.sleep", lambda _: None)
    tools = Tools()
    manifests = tmp_path / "kubernetes"
    manifests.mkdir()
    for step in SLOTS[7].steps:
        _, output = tools(f"sdo source {step.kind}/{SLOTS[7].namespace}/{step.name}")
        (manifests / f"{step.kind}-{step.name}.yaml").write_text(json.dumps(json.loads(output)["source"]))

    def allocated(command: str, *, timeout: float = 300) -> tuple[int, str]:
        args = shlex.split(command)
        if args[:1] == ["kubectl"] and "get" in args and "Service" in args:
            return 0, json.dumps({"spec": {"clusterIP": "10.0.0.7", "clusterIPs": ["10.0.0.7"]}})
        if args[:2] == ["sdo", "act"] and "patch" in args and "Service" in args:
            patch = json.loads(Path(args[args.index("--patch-file") + 1]).read_text())
            if patch[0]["value"].get("clusterIP") != "10.0.0.7":
                return 1, "Service clusterIP is immutable"
        return tools(command, timeout=timeout)

    result = replay(allocated, 7, {"incident_id": "test", "namespace": "social-network"}, tmp_path)
    assert result["status"] == "completed"


def test_phase_b_rejects_zero_gap_lock_from_before_source_repair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import subprocess
    from types import SimpleNamespace

    from benchmarks.sregym.assurance import mixed10_rehearsal

    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(tmp_path), *args], text=True).strip()

    git("init", "-q")
    source = tmp_path / "app.yaml"
    source.write_text("before\n")
    git("add", "app.yaml")
    git("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "seed")
    seed = git("rev-parse", "HEAD")
    source.write_text("repaired\n")
    git("add", "app.yaml")
    git("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-qm", "repair")
    repaired = git("rev-parse", "HEAD")
    output = tmp_path / "results"
    output.mkdir()
    (output / "sdo_production_receipt_strict.json").write_text(
        json.dumps(
            {
                "proposal_commit": repaired,
                "outcome_commit": repaired,
                "native_ledger": {"outcome_document": {"classification": "success"}},
            }
        )
    )
    rendered = tmp_path / ".sdo/rendered"
    rendered.mkdir(parents=True)
    (rendered / "lock.json").write_text(json.dumps({"source_commit": seed, "objects": [{"file": "object.yaml"}]}))
    monkeypatch.setattr(
        mixed10_rehearsal,
        "load_records",
        lambda _: [
            SimpleNamespace(index=2, error=None, full_cleanup_verified=True, oracle=SimpleNamespace(success=True))
        ],
    )
    monkeypatch.setattr(mixed10_rehearsal, "validate_native_production_receipt", lambda _: None)
    with pytest.raises(ValueError, match="source repair"):
        mixed10_rehearsal.verify_results(output, SimpleNamespace(workspace=tmp_path))


def test_slot_nine_retries_pending_pod_only_after_old_affinity_pod_is_gone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("benchmarks.sregym.adapter.scripted_replay.time.sleep", lambda _: None)
    tools = Tools()
    manifests = tmp_path / "kubernetes"
    manifests.mkdir()
    for step in SLOTS[9].steps:
        if step.operation == "restore":
            _, output = tools(f"sdo source {step.kind}/social-network/{step.name}")
            (manifests / f"{step.kind}-{step.name}.yaml").write_text(json.dumps(json.loads(output)["source"]))
    reads = 0

    def scheduling(command: str, *, timeout: float = 300) -> tuple[int, str]:
        nonlocal reads
        args = shlex.split(command)
        if args[:1] == ["kubectl"] and "pods" in args:
            reads += 1
            old = (
                [
                    {
                        "metadata": {"name": "old-serving"},
                        "spec": {
                            "affinity": {
                                "podAntiAffinity": {
                                    "requiredDuringSchedulingIgnoredDuringExecution": [
                                        {"topologyKey": "kubernetes.io/hostname"}
                                    ]
                                }
                            }
                        },
                    }
                ]
                if reads < 3
                else []
            )
            return 0, json.dumps(
                {"items": [*old, {"metadata": {"name": "replacement"}, "status": {"phase": "Pending"}}]}
            )
        if args[:2] == ["sdo", "act"] and "pod" in args and "replacement" in args and reads < 3:
            return 1, "old anti-affinity pod still blocks the fresh scheduling attempt"
        return tools(command, timeout=timeout)

    result = replay(scheduling, 9, {"incident_id": "test", "namespace": "social-network"}, tmp_path)
    assert result["status"] == "completed"
    assert reads == 3


def test_as3_slot_six_requires_valkey_fault_before_repair(tmp_path: Path) -> None:
    from benchmarks.sregym.assurance.mixed10_rehearsal import runner_arguments
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(runner_arguments(6, tmp_path, tmp_path / "output"))
    assert args.require_faulty_component == ["fault_1:ValkeyMemoryDisruption"]
