from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from benchmarks.sregym.fastloop.baseline_recovery import load_application_baseline
from benchmarks.sregym.fastloop.cli import build_parser, format_incidents, format_summary, main
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import IncidentRecord, OracleVerdict, append_record
from benchmarks.sregym.fastloop.stream import load_stream, load_stream_config

T0 = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


def _record(index: int, agent: str) -> IncidentRecord:
    return IncidentRecord(
        run_id=f"run-{agent}",
        index=index,
        agent=agent,
        problem_id="missing_configmap_hotel_reservation",
        model="gpt-6-luna",
        injection={"state": "injected", "started_at": T0, "finished_at": T0 + timedelta(seconds=6)},
        detected_at=T0 + timedelta(seconds=9) if agent == "sdo" else None,
        mitigation_applied_at=T0 + timedelta(seconds=45),
        oracle=OracleVerdict(kind="sregym-mitigation-oracle", success=True),
        incident_wall_seconds=150.0,
        warm_path=True if agent == "sdo" else None,
    )


def test_summary_reads_every_incidents_file_under_a_run_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    append_record(tmp_path / "results" / "a" / "incidents.jsonl", _record(0, "sdo"))
    append_record(tmp_path / "results" / "a" / "incidents.jsonl", _record(1, "sdo"))
    append_record(tmp_path / "results" / "b" / "incidents.jsonl", _record(0, "codex"))

    assert main(["summary", str(tmp_path)]) == 0

    output = capsys.readouterr().out
    assert "run-sdo" in output
    assert "run-codex" in output
    assert "inj->det_s" in output
    assert "9.0" in output


def test_summary_of_nothing_fails(tmp_path: Path) -> None:
    assert main(["summary", str(tmp_path)]) == 1


def test_tables_have_one_row_per_incident_and_per_agent() -> None:
    records = [_record(0, "sdo"), _record(1, "sdo"), _record(0, "codex")]

    assert len(format_incidents(records).splitlines()) == 4
    assert len(format_summary(records).splitlines()) == 3


def test_run_requires_an_environment_from_up(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="fastloop up"):
        FastloopEnvironment.load(tmp_path)


def test_environment_round_trips_all_paths_through_saved_file(tmp_path: Path) -> None:
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=tmp_path / "private",
    )

    environment.save()

    assert FastloopEnvironment.load(tmp_path) == environment


@pytest.mark.parametrize("attach_only", [False, True])
def test_up_persists_a_dns_application_name_and_loads_legacy_display_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, attach_only: bool
) -> None:
    from benchmarks.sregym.fastloop import cli

    monkeypatch.setattr(cli, "up_preflight", lambda _args: None)
    (tmp_path / "application_workspace").mkdir()
    fenced_config = tmp_path / "existing.kubeconfig"
    fenced_config.write_text("existing lane")
    monkeypatch.setenv("KUBECONFIG", str(fenced_config))

    class Worker:
        def __init__(self, *_args: object, **kwargs: Any) -> None:
            # Kubernetes caches KUBECONFIG at import time, before the cluster op.
            assert kwargs["env"]["KUBECONFIG"] == str(
                fenced_config if attach_only else tmp_path / "kubeconfigs/worker_0.kubeconfig"
            )

        def __enter__(self) -> Worker:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def request(self, op: str, **_kwargs: object) -> dict[str, object]:
            if op in {"cluster", "attach_existing"}:
                assert op == ("attach_existing" if attach_only else "cluster")
                return {"kubeconfig": str(fenced_config if attach_only else tmp_path / "worker.kubeconfig")}
            if op == "deploy":
                (tmp_path / "cluster_baseline_state.json").write_text('{"namespaces": ["observe"]}', encoding="utf-8")
                return {"deployed": True}
            assert op == "app_info"
            return {"app_name": "Hotel Reservation", "namespace": "hotel-reservation", "descriptions": "hotel"}

    monkeypatch.setattr(cli, "SregymWorker", Worker)
    monkeypatch.setattr(cli, "application_namespaces", lambda *_args: ("hotel-reservation",))

    class Cluster:
        def __init__(self, _kubeconfig: Path) -> None:
            pass

        def list_objects(self, namespace: str) -> list[dict[str, Any]]:
            return [{"kind": "Deployment", "metadata": {"name": "mongo", "namespace": namespace}, "spec": {}}]

    monkeypatch.setattr(cli, "KubectlApplicationCluster", Cluster)
    extra = ["--reuse-cluster-only"] if attach_only else []
    assert main(["up", "--no-dev-mount", "--run-dir", str(tmp_path), "--builder", "", "--no-sandbox", *extra]) == 0
    assert FastloopEnvironment.load(tmp_path).application == "hotel-reservation"
    baseline = load_application_baseline(tmp_path / "cluster_baseline_state.json", "hotel-reservation")
    assert baseline is not None
    assert baseline[0].objects[0]["metadata"]["name"] == "mongo"

    path = tmp_path / "fastloop.json"
    old = json.loads(path.read_text(encoding="utf-8"))
    old["application"] = "Hotel Reservation"
    path.write_text(json.dumps(old), encoding="utf-8")
    assert FastloopEnvironment.load(tmp_path).application == "hotel-reservation"


def test_parser_defaults_match_the_benchmark_configuration() -> None:
    args = build_parser().parse_args(["run", "--run-dir", "x", "--agent", "sdo"])

    assert (args.model, args.provider, args.reflection_session, args.incidents) == ("gpt-6-luna", "codex", "resume", 1)
    up = build_parser().parse_args(["up", "--run-dir", "x"])
    assert up.cpu_limit == "3"
    assert up.cluster_prefix == "fastloop-w"
    assert up.no_sandbox is False
    assert up.kind_worker_nodes == 1


def test_run_checks_sdo_credentials_before_worker_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import cli

    FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=tmp_path / "private",
    ).save()
    checked: list[list[str]] = []

    def missing_secret(args: list[str], **_kwargs: object) -> SimpleNamespace:
        checked.append(args)
        return SimpleNamespace(
            returncode=1, stderr='Error from server (NotFound): secrets "sdo-agent-credentials" not found'
        )

    monkeypatch.setattr(cli, "kubectl", missing_secret)
    monkeypatch.setattr(cli, "_worker", lambda _environment: pytest.fail("worker started before credential preflight"))

    with pytest.raises(SystemExit, match="credentials Secret.*sdo-agent-credentials.*missing"):
        main(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])

    assert checked == [["get", "secret", "sdo-agent-credentials"]]


def test_detection_timeout_reaches_single_and_composite_sdo_stages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym.fastloop import cli, composite

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fastloop-w0",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel-reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    monkeypatch.setattr(cli, "_cluster_ops", lambda *_args: object())
    monkeypatch.setattr(composite, "reader_for", lambda *_args: lambda *_read: None)
    default = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
    selected = build_parser().parse_args(
        ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--detection-timeout", "12"]
    )

    assert (
        cli._sdo_agent(default, environment, tmp_path).stage_inputs("stage", tmp_path, "fp").detection_timeout_seconds
        is None
    )
    for composite_mode in (False, True):
        agent = cli._sdo_agent(selected, environment, tmp_path, composite=composite_mode)
        assert agent.stage_inputs("stage", tmp_path, "fp").detection_timeout_seconds == 12


def test_default_response_budget_reaches_fastloop_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import cli

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fastloop-w0",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel-reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    monkeypatch.setattr(cli, "_cluster_ops", lambda *_args: object())
    args = build_parser().parse_args(
        [
            "run",
            "--run-dir",
            str(tmp_path),
            "--agent",
            "sdo",
        ]
    )
    agent = cli._sdo_agent(args, environment, tmp_path)
    assert agent.stage_inputs("stage", tmp_path, "fp").runtime_config.max_response_loops == 10


def test_sdo_repair_policy_defaults_to_recorded_actions_and_is_configurable(tmp_path: Path) -> None:
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
    assert args.repair_policy == "recorded-actions"
    selected = build_parser().parse_args(
        [
            "run",
            "--run-dir",
            str(tmp_path),
            "--agent",
            "sdo",
            "--repair-policy",
            "commit",
        ]
    )
    assert selected.repair_policy == "commit"


def test_sdo_accepts_scripted_worker_registration_override(tmp_path: Path) -> None:
    registrations = tmp_path / "registrations"
    registrations.mkdir()
    args = build_parser().parse_args(
        ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--agent-registrations", str(registrations)]
    )
    assert args.agent_registrations == registrations


def test_up_builds_the_same_one_worker_kind_lane_as_the_experiments(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import up_worker_environment

    args = build_parser().parse_args(
        ["up", "--run-dir", str(tmp_path), "--cluster-prefix", "assure-s", "--worker-id", "1"]
    )
    environment = up_worker_environment(args, workspace=tmp_path / "workspace")

    assert environment["SREGYM_KIND_CLUSTER_NAME"] == "assure-s1"
    assert environment["SREGYM_KIND_WORKER_NODES"] == "1"
    assert environment["SREGYM_APP_SOURCE_DIR"] == str(tmp_path / "workspace")
    assert environment["SREGYM_KIND_REQUIRE_NETWORK_POLICY"] == "1"


def test_dev_kind_config_mounts_the_worktree_on_every_node(tmp_path: Path) -> None:
    import yaml

    from benchmarks.sregym.fastloop.dev import write_kind_config

    source = tmp_path / "kind.yaml"
    source.write_text("kind: Cluster\nnodes:\n  - role: control-plane\n  - role: worker\n", encoding="utf-8")
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    result = write_kind_config(source, tmp_path / "dev.yaml", worktree)

    nodes = yaml.safe_load(result.read_text(encoding="utf-8"))["nodes"]
    assert all(node["extraMounts"] == [{"hostPath": str(worktree), "containerPath": "/opt/sdo-dev"}] for node in nodes)


def test_up_cache_requires_every_node_to_have_the_requested_image_digests(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import Images

    calls: list[list[str]] = []

    def run(argv: list[str], **_kwargs: object) -> object:
        calls.append(argv)
        value = "node-a\nnode-b\n" if argv[:3] == ["kind", "get", "nodes"] else "sha256:one\n"
        if argv[:3] == ["docker", "exec", "node-b"]:
            value = '{"status":{"id":"sha256:other"}}'
        elif argv[:2] == ["docker", "exec"]:
            value = '{"status":{"id":"sha256:one"}}'
        return type("Result", (), {"returncode": 0, "stdout": value})()

    monkeypatch.setattr(cli.subprocess, "run", run)
    images = Images(controller="controller:pbprog", responder="responder:pbprog", validator="validator:pbprog")

    assert cli.node_images_match("fastloop-w0", images) is False
    assert any(call[:3] == ["docker", "exec", "node-b"] for call in calls)


def test_up_rejects_a_negative_worker_count(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="kind-worker-nodes"):
        main(["up", "--run-dir", str(tmp_path), "--kind-worker-nodes", "-1"])


def test_stream_accepts_ordered_single_and_composed_incidents(tmp_path: Path) -> None:
    stream = tmp_path / "stream.json"
    stream.write_text('["single", ["target-port", "incorrect-image"]]', encoding="utf-8")

    assert load_stream(stream) == (("single",), ("target-port", "incorrect-image"))


def test_stream_config_sets_grading_mode_and_cli_can_override(tmp_path: Path) -> None:
    stream = tmp_path / "stream.json"
    stream.write_text(
        '{"diagnosis_grading":"deferred","judge_model_id":"codex-gpt-6-luna","incidents":["one",["two","three"]]}',
        encoding="utf-8",
    )

    config = load_stream_config(stream)
    assert config.incidents == (("one",), ("two", "three"))
    assert config.diagnosis_grading == "deferred"
    args = build_parser().parse_args(["run", "--run-dir", "x", "--agent", "sdo", "--diagnosis-grading", "inline"])
    assert args.diagnosis_grading == "inline"
    assert build_parser().parse_args(["run", "--run-dir", "x", "--agent", "sdo"]).diagnosis_grading is None


def test_stream_config_rejects_unknown_grading_mode(tmp_path: Path) -> None:
    stream = tmp_path / "stream.json"
    stream.write_text('{"diagnosis_grading":"later","incidents":["one"]}', encoding="utf-8")
    with pytest.raises(ValueError, match="diagnosis_grading"):
        load_stream_config(stream)


def test_playbook_experiment_streams_defer_diagnosis_grading() -> None:
    root = Path(__file__).resolve().parents[5]
    for name in ("pbprog_smoke.json", "pbprog_explicit.json"):
        config = load_stream_config(root / "tests" / "fixtures" / "sregym" / "fastloop" / name)
        assert config.diagnosis_grading == "deferred"
        assert config.judge_model_id == "codex-gpt-6-luna"


def test_mf_stream_launches_with_both_component_probes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from benchmarks.sregym.fastloop import cli

    root = Path(__file__).resolve().parents[5]
    stream = root / "tests" / "fixtures" / "sregym" / "fastloop" / "pbprog_smoke.json"
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fastloop-w0",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="Hotel Reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=root / "third_party" / "sregym",
        private_tmp=None,
    )
    monkeypatch.setattr(FastloopEnvironment, "load", lambda _path: environment)

    # A legacy capture must carry its recovered reference closure into every
    # subsequent driver's cleanup, rather than deleting the restored account.
    import hashlib

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, save_application_baseline

    workload = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "reservation", "namespace": environment.namespace},
        "spec": {"template": {"spec": {"serviceAccountName": "reservation-reader"}}},
    }
    account = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "reservation-reader", "namespace": environment.namespace},
    }
    environment.baseline_path.write_text("{}")
    save_application_baseline(environment.baseline_path, (ApplicationBaseline(environment.namespace, [workload]),))
    rendered = environment.workspace / ".sdo/rendered"
    rendered.mkdir(parents=True)
    payload = json.dumps(account)
    (rendered / "account.yaml").write_text(payload)
    (rendered / "lock.json").write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "ref": {
                            "api_version": "v1",
                            "kind": "ServiceAccount",
                            "namespace": environment.namespace,
                            "name": "reservation-reader",
                        },
                        "file": "account.yaml",
                        "sha256": hashlib.sha256(payload.encode()).hexdigest(),
                    }
                ]
            }
        )
    )

    class Worker:
        def __enter__(self) -> Worker:
            return self

        def __exit__(self, *_args: object) -> None:
            pass

        def request(self, op: str, **_kwargs: object) -> dict[str, object]:
            assert op == "deploy"
            return {}

    captured: dict[str, object] = {}

    def agent(*_args: object, **kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(cli, "_worker", lambda _env: Worker())
    monkeypatch.setattr(cli, "kubectl", lambda *_args, **_kwargs: SimpleNamespace(returncode=0))

    def driver(*_args, **kwargs):
        captured["driver_baseline"] = kwargs["baseline"]
        return object()

    monkeypatch.setattr(cli, "SregymFaultDriver", driver)
    monkeypatch.setattr(cli, "_sdo_agent", agent)
    monkeypatch.setattr(cli, "run_incidents", lambda *_args, **_kwargs: [])
    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
    from benchmarks.sregym.faults.description import ProblemDescription

    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.fault_tracker.describe_sregym_problems",
        lambda ids, **_kwargs: {
            problem_id: ProblemDescription(
                problem_id=problem_id,
                composite=True,
                faults=(
                    FaultSpec("target_port", "reservation"),
                    FaultSpec("incorrect_image", "rate", declared_index=1),
                ),
                injection_order=(0, 1),
            )
            for problem_id in ids
        },
    )

    assert (
        main(
            ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--stream", str(stream), "--diagnosis-grading", "off"]
        )
        == 1
    )
    assert captured["composite"] is True
    faults = captured["stream_faults"]
    assert {key: len(value) for key, value in faults.items()} == {
        "mf__hotel_reservation__n2__s1773744812__v_": 2,
        "mf__hotel_reservation__n2__s1085951852__v_": 2,
    }

    assert any(
        value["kind"] == "ServiceAccount" and value["metadata"]["name"] == "reservation-reader"
        for baseline in captured["driver_baseline"]
        for value in baseline.objects
    )


@pytest.mark.parametrize("content", ["[]", "[[]]", '["a", ["a", "a"]]', "[4]"])
def test_stream_rejects_invalid_incidents(tmp_path: Path, content: str) -> None:
    stream = tmp_path / "stream.json"
    stream.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="stream"):
        load_stream(stream)


class _StopAtProxy(Exception):
    pass


def test_codex_run_hands_the_worker_an_agent_kubeconfig_path_it_can_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym.fastloop import cli

    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    (codex_home / "auth.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    requested: list[Path] = []

    class Worker:
        def request(self, op: str, **args: object) -> dict[str, object]:
            assert op == "proxy"
            requested.append(Path(str(args["kubeconfig"])))
            raise _StopAtProxy

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fastloop-w0",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="Hotel Reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "codex"])
    results_dir = tmp_path / "results" / "fresh-run"

    with pytest.raises(_StopAtProxy):
        cli._run_codex(args, environment, results_dir, Worker(), None, None)  # type: ignore[arg-type]

    assert requested == [results_dir / "agent.kubeconfig"]
    assert results_dir.is_dir()


@pytest.mark.parametrize("stream_name", ["pbprog_repeat.json", "pbprog_compose.json"])
def test_codex_prompt_and_oracle_cover_every_playbook_stream_incident(
    stream_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym.fastloop import cli, codex_agent, composite
    from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
    from benchmarks.sregym.fastloop.fault_tracker import FaultSpec
    from benchmarks.sregym.fastloop.loop import LoopConfig

    root = Path(__file__).resolve().parents[5]
    incidents = load_stream(root / "tests" / "fixtures" / "sregym" / "fastloop" / stream_name)
    registered = {problem for ids in incidents for problem in ids}
    auth_home = tmp_path / "codex-home"
    auth_home.mkdir()
    (auth_home / "auth.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CODEX_HOME", str(auth_home))

    class Worker:
        def __init__(self) -> None:
            self.prompts: list[str] = []
            self.injected: list[str] = []
            self.oracles: list[tuple[tuple[str, ...], int | None, bool]] = []
            self.recoveries: list[tuple[str, ...]] = []
            self.recovered = False

        def request(self, op: str, **args: Any) -> dict[str, Any]:
            if op == "proxy":
                return {"kubeconfig": args["kubeconfig"]}
            if op == "codex_prompt":
                problem_id = args["problem_id"]
                if problem_id not in registered:
                    raise ValueError(f"Problem ID {problem_id} not found in registry")
                self.prompts.append(problem_id)
                return {
                    "prompt": "You are an SRE agent tasked with diagnosing and fixing issues in a Kubernetes "
                    "application.\nApplication: Hotel Reservation\nNamespace: hotel-reservation\n"
                    "Investigate the incident.\nThe submission endpoint is: " + args["api_base"] + "/submit\n"
                }
            if op == "inject":
                if not args.get("compose"):
                    self.injected = []
                    self.recovered = False
                self.injected.append(args["problem_id"])
                return {"started_at": 1790000000.0, "finished_at": 1790000001.0, "fault": len(self.injected) - 1}
            if op == "oracle":
                self.oracles.append((tuple(self.injected), args.get("fault"), self.recovered))
                return {"kind": "sregym-mitigation-oracle", "success": True}
            if op == "recover":
                self.recoveries.append(tuple(self.injected))
                self.recovered = True
                return {"seconds": 0.0}
            if op == "health":
                return {"healthy": True}
            raise AssertionError(op)

    def run_codex(command: list[str], **kwargs: Any) -> SimpleNamespace:
        prompt = command[-1]
        endpoint = prompt.split("The submission endpoint is: ", 1)[1].splitlines()[0]
        httpx.post(endpoint, json={"solution": "application incident"})
        httpx.post(endpoint, json={"solution": ""})
        kwargs["stdout"].write('{"type":"turn.completed","usage":{"input_tokens":1}}\n')
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(codex_agent.subprocess, "run", run_codex)
    monkeypatch.setattr(composite, "reader_for", lambda *_args, **_kwargs: lambda *_read: None)
    worker = Worker()
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fastloop-w0",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="Hotel Reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=root / "third_party" / "sregym",
        private_tmp=None,
    )
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "codex"])
    config = LoopConfig(
        run_id="codex-test",
        problems=tuple(ids[0] for ids in incidents),
        incidents=len(incidents),
        results_path=tmp_path / "results" / "incidents.jsonl",
        stream=incidents,
    )
    tracked = {
        "+".join(ids): tuple(FaultSpec("incorrect_image", f"component-{index}") for index in range(len(ids)))
        for ids in incidents
        if len(ids) > 1
    }
    driver = SregymFaultDriver(worker, namespace=environment.namespace)

    records = cli._run_codex(args, environment, tmp_path / "results", worker, driver, config, stream_faults=tracked)

    assert len(records) == len(incidents)
    assert all(record.error is None and record.oracle and record.oracle.success for record in records)
    assert worker.prompts == [ids[0] for ids in incidents]
    assert worker.recoveries == list(incidents)
    for index, ids in enumerate(incidents):
        prompt = (tmp_path / "results" / f"{index:03d}_{'+'.join(ids)}" / "prompt.txt").read_text(encoding="utf-8")
        assert "Application: Hotel Reservation" in prompt
        assert all(problem not in prompt for problem in ids)
        expected_faults = [None]  # official aggregate oracle, including nested composites
        assert [
            fault for injected, fault, recovered in worker.oracles if injected == ids and not recovered
        ] == expected_faults
        assert [
            fault for injected, fault, recovered in worker.oracles if injected == ids and recovered
        ] == expected_faults


def test_sdo_agent_preserves_accepted_seed_manifest_selection(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop import cli

    workspace = tmp_path / "workspace"
    rendered = workspace / ".sdo/rendered"
    rendered.mkdir(parents=True)
    (rendered / "lock.json").write_text(json.dumps({"manifest_paths": ["kubernetes/astronomy-shop.yaml"]}))
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="astronomy-shop",
        application="astronomy-shop",
        workspace=workspace,
        sregym_dir=tmp_path / "sregym",
        private_tmp=tmp_path / "private",
    )
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"])
    agent = cli._sdo_agent(args, environment, tmp_path / "results")
    assert agent.stage_inputs("stage", tmp_path, "fp").runtime_config.deployed_manifest_paths == (
        "kubernetes/astronomy-shop.yaml",
    )


def test_fresh_memory_resets_control_state_before_install_but_warm_repeat_keeps_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from benchmarks.sregym import adapter
    from benchmarks.sregym.adapter import native_operator
    from benchmarks.sregym.fastloop import cli
    from tests.unit.benchmarks.sregym.fastloop.test_fastloop_sdo_agent import FakeOps, _clock, _inject

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel-reservation",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    ops = FakeOps()
    calls: list[str] = []
    monkeypatch.setattr(cli, "_cluster_ops", lambda *_args: ops)

    real_execute = native_operator.execute_operator

    def execute(command: str, args: list[str], **_kwargs: Any) -> str:
        if command not in {"stop", "reset-control-state"}:
            return real_execute(command, args, **_kwargs)
        assert args[args.index("--control-namespace") + 1] == "hotel-reservation-sdo"
        assert args[args.index("--kubeconfig") + 1] == str(environment.kubeconfig)
        calls.append(command)
        return json.dumps(
            {"removed": [], "leftovers": []}
            if command == "stop"
            else {"control_namespace": "hotel-reservation-sdo", "deleted": ["sdo-namespace-onboarding"]}
        )

    def install(config: Any, *_args: Any) -> None:
        calls.append("install")
        ops.install(config)

    monkeypatch.setattr(native_operator, "execute_operator", execute)
    monkeypatch.setattr(adapter, "launch_native_lifecycle", install)
    from benchmarks.sregym.adapter.driver import DeployedLifecycle, DeployedLifecycleContext

    monkeypatch.setattr(
        adapter,
        "deployed_lifecycle",
        lambda _namespace: DeployedLifecycle(
            context=DeployedLifecycleContext(health_objective="objective", active_resources=[]),
            fingerprint="test-topology",
        ),
    )
    args = build_parser().parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--no-checkpoints"])
    for fresh in range(2):
        if fresh:
            environment.state_path.unlink()
        agent = cli._sdo_agent(args, environment, tmp_path / "results")
        agent._clock = _clock()
        agent.learn(agent.resolve(0, "p", _inject))
        assert calls == ["stop", "reset-control-state", "install"] * (fresh + 1)
        # A new CLI invocation and the warm reset/reinstall path retain memory.
        state = adapter.PersistentState.load(environment.state_path)
        record = state.controllers[environment.namespace]
        record.controller_pod_uid = ""
        record.controller_pod_name = ""
        state.save(environment.state_path)
        ops.delete_controller("hotel-reservation-sdo")
        ops.namespace_exists = lambda _namespace: True
        warm = cli._sdo_agent(args, environment, tmp_path / "results")
        warm._clock = _clock()
        warm.learn(warm.resolve(1, "p", _inject))
        assert calls == ["stop", "reset-control-state", "install"] * (fresh + 1)
