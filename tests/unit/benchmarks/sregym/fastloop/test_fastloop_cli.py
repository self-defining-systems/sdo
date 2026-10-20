from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.cli import build_parser, format_incidents, format_summary, main
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import IncidentRecord, OracleVerdict, append_record
from benchmarks.sregym.fastloop.stream import load_stream

T0 = datetime(2026, 9, 27, 12, 0, 0, tzinfo=timezone.utc)


def _record(index: int, agent: str) -> IncidentRecord:
    return IncidentRecord(
        run_id=f"run-{agent}",
        index=index,
        agent=agent,
        problem_id="missing_configmap_hotel_reservation",
        model="gpt-6-luna",
        injection_started_at=T0,
        injection_finished_at=T0 + timedelta(seconds=6),
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


def test_parser_defaults_match_the_benchmark_configuration() -> None:
    args = build_parser().parse_args(["run", "--run-dir", "x", "--agent", "sdo"])

    assert (args.model, args.provider, args.reflection_session, args.incidents) == ("gpt-6-luna", "codex", "resume", 1)
    up = build_parser().parse_args(["up", "--run-dir", "x"])
    assert up.cpu_limit == "3"
    assert up.cluster_prefix == "fastloop-w"
    assert up.no_sandbox is False
    assert up.kind_worker_nodes == 1


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
