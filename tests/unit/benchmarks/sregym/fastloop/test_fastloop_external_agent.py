"""The external arm: any agent command, gated on the result file and request logs it writes."""

from __future__ import annotations

import json
import re
import stat
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop import external_agent
from benchmarks.sregym.fastloop.codex_agent import SubmissionStub
from benchmarks.sregym.fastloop.external_agent import (
    DEFAULT_CLOSING_PROMPT,
    ExternalAgentSettings,
    ExternalBaselineAgent,
    run_external_cell,
)
from benchmarks.sregym.fastloop.loop import InjectionWindow

REPO_ROOT = Path(__file__).resolve().parents[5]
T0 = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)

# A stand-in agent command speaking the contract: it records what it was given, writes the request logs, the
# evidence and the result file, and misbehaves the way FAKE_MODE says.
FAKE_COMMAND = textwrap.dedent(
    f"""\
    #!{sys.executable}
    import argparse, json, os, subprocess, sys, time, urllib.request
    from pathlib import Path

    parser = argparse.ArgumentParser()
    for flag in ("flag", "phase", "cell-dir", "prompt-file", "kubeconfig", "deadline-epoch", "state-in"):
        parser.add_argument("--" + flag)
    args = parser.parse_args()
    mode = os.environ.get("FAKE_MODE", "ok")
    cell = Path(args.cell_dir)
    phase = args.phase
    out = cell / phase
    out.mkdir(parents=True, exist_ok=True)
    Path(os.environ["FAKE_PROBE"] + "." + phase).write_text(json.dumps({{
        "argv": sys.argv[1:], "prompt": Path(args.prompt_file).read_text(),
        "kubeconfig_text": Path(args.kubeconfig).read_text(), "cwd": os.getcwd(),
        "env": {{k: os.environ.get(k) for k in (
            "KUBECONFIG", "SDO_EXTERNAL_PHASE", "SDO_EXTERNAL_SUBMIT_URL", "SDO_EXTERNAL_STATE_OUT",
            "SREGYM_SUMMARY_FILE", "FAKE_EXTRA")}},
        "state_in_exists": bool(args.state_in) and Path(args.state_in).is_dir()}}))
    if mode == "hang":
        subprocess.Popen(["sleep", "300"])
        time.sleep(300)
    if mode == "noresult":
        sys.exit(0)
    if mode == "badresult":
        (out / "agent-result.json").write_text("{{not json")
        sys.exit(0)

    def record(i, output, extra=None):
        usage = {{"input_tokens": 10, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 50,
                  "cache_creation": {{"ephemeral_1h_input_tokens": 20, "ephemeral_5m_input_tokens": 30}},
                  "output_tokens": output}}
        line = {{"type": "assistant", "timestamp": "2026-11-18T12:00:0%d.000Z" % i, "phase": phase,
                 "message": {{"id": f"msg_{{phase}}_{{i}}", "model": "claude-sonnet-5-5", "usage": usage}}}}
        return {{**line, **(extra or {{}})}}

    own = [record(1, 5), record(1, 7), record(2, 9)]  # response 1 is logged twice: one request
    independent = [record(1, 7, {{"http_status": 200, "complete": True}}),
                   record(2, 9, {{"http_status": 200, "complete": True}}),
                   {{"type": "assistant", "http_status": 429, "message": {{}}}}]
    own.append({{"type": "request_error", "status": 429}})
    timed_out = False
    if mode == "mismatch":
        independent[1] = record(2, 10, {{"http_status": 200, "complete": True}})
    if mode in ("cut", "cut_untimed"):
        independent.append(record(3, 4, {{"http_status": 200, "complete": False}}))
        timed_out = mode == "cut"
    if mode == "missing_independent_record":
        independent.pop(1)
    (out / "requests.jsonl").write_text("".join(json.dumps(x) + chr(10) for x in own))
    (out / "requests-independent.jsonl").write_text("".join(json.dumps(x) + chr(10) for x in independent))
    if mode != "no_evidence":
        (out / "events.json").write_text("" if mode == "empty_evidence" else "[1]")
    (cell / "agent").mkdir(exist_ok=True)
    (cell / "agent" / "memory.txt").write_text("kept " + phase)
    if phase == "incident" and os.environ.get("SDO_EXTERNAL_SUBMIT_URL") and mode != "no_submit":
        for text in ("the diagnosis", "the fix"):
            req = urllib.request.Request(os.environ["SDO_EXTERNAL_SUBMIT_URL"] + "/submit",
                                         json.dumps({{"solution": text}}).encode(),
                                         {{"Content-Type": "application/json"}})
            urllib.request.urlopen(req).read()
    (out / "agent-result.json").write_text(json.dumps({{
        "schema": "sdo.external-agent-result/v1", "phase": phase, "label": "fake", "agent_version": "9.9",
        "started_at": "2026-11-18T12:00:00Z", "ended_at": "2026-11-18T12:00:09Z", "timed_out": timed_out,
        "requests": phase + "/requests.jsonl", "independent_requests": phase + "/requests-independent.jsonl",
        "required_evidence": [phase + "/events.json"],
        "problems": ["events export failed"] if mode == "problems" else [], "state_dir": "agent"}}))
    sys.exit(3 if mode == "crash" else 0)
    """
)


@pytest.fixture
def fake_command(tmp_path: Path) -> Path:
    path = tmp_path / "bin" / "fake-agent"
    path.parent.mkdir()
    path.write_text(FAKE_COMMAND)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _settings(tmp_path: Path, command: Path, mode: str = "ok", **overrides: object) -> ExternalAgentSettings:
    kubeconfig = tmp_path / "agent.kubeconfig"
    kubeconfig.write_text("kubeconfig-body")
    values: dict[str, object] = {
        "command": (str(command), "--flag", "value"),
        "label": "fake agent (patched, abc123)",
        "results_dir": tmp_path / "results",
        "kubeconfig": kubeconfig,
        "incident_timeout_seconds": 60.0,
        "closing_timeout_seconds": 60.0,
        "evidence_grace_seconds": 5.0,
        "environ": {"PATH": "/usr/bin:/bin", "SREGYM_SUMMARY_FILE": "/x", "FAKE_PROBE": str(tmp_path / "probe")},
        "extra_env": {"FAKE_MODE": mode, "FAKE_EXTRA": "configured"},
    }
    values.update(overrides)
    return ExternalAgentSettings(**values)  # type: ignore[arg-type]


def _incident(settings: ExternalAgentSettings, tmp_path: Path, **kwargs: object):
    return run_external_cell(
        settings,
        cell_dir=tmp_path / "cells" / "000_problem",
        prompt="NOTICE",
        phase="incident",
        **kwargs,  # type: ignore[arg-type]
    )


def _probe(tmp_path: Path, phase: str) -> dict:
    return json.loads((tmp_path / f"probe.{phase}").read_text())


def test_an_incident_phase_runs_the_command_with_the_contract_and_gates_on_the_result(
    tmp_path: Path, fake_command: Path
) -> None:
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        result = _incident(_settings(tmp_path, fake_command), tmp_path, submit_url=stub.url)

    cell = result.cell_dir
    assert result.error is None and result.transcripts_complete, result.transcript_problems
    assert result.result is not None and result.result.agent_version == "9.9"
    probe = _probe(tmp_path, "incident")
    argv = probe["argv"]
    assert argv[:3] == ["--flag", "value", "--phase"] or "--phase" in argv
    assert argv[argv.index("--phase") + 1] == "incident"
    assert argv[argv.index("--cell-dir") + 1] == str(cell)
    assert argv[argv.index("--deadline-epoch") + 1]
    assert "--state-in" not in argv
    assert probe["prompt"] == "NOTICE" and (cell / "incident" / "prompt.txt").read_text() == "NOTICE"
    assert probe["kubeconfig_text"] == "kubeconfig-body"
    env = probe["env"]
    assert env["SDO_EXTERNAL_PHASE"] == "incident"
    assert env["SDO_EXTERNAL_SUBMIT_URL"] == stub.url
    assert env["SDO_EXTERNAL_STATE_OUT"] == str(cell / "agent")
    assert env["FAKE_EXTRA"] == "configured"  # the arm config's environment reaches the command
    assert env["SREGYM_SUMMARY_FILE"] is None  # the harness's summary variable does not
    assert env["KUBECONFIG"] == argv[argv.index("--kubeconfig") + 1]
    assert not Path(probe["cwd"]).exists()  # the scratch directory is gone afterwards
    assert (cell / "incident" / "command.stdout").exists() and (cell / "incident" / "command.stderr").exists()


def test_the_clock_records_notice_start_and_end_for_each_phase(tmp_path: Path, fake_command: Path) -> None:
    settings = _settings(tmp_path, fake_command)
    order: list[str] = []
    result = _incident(settings, tmp_path, on_notice=lambda: order.append("notice"))
    closing = run_external_cell(settings, cell_dir=result.cell_dir, prompt="CLOSE", phase="closing")

    assert order == ["notice"]
    assert result.clock.notice_at <= result.clock.started_at <= result.clock.ended_at
    clock = json.loads((result.cell_dir / "clock.json").read_text())
    assert {"notice_at", "started_at", "ended_at"} <= clock.keys()
    assert clock["label"] == "fake agent (patched, abc123)"
    assert clock["phases"]["incident"]["returncode"] == 0 and clock["phases"]["incident"]["backstop_killed"] is False
    assert clock["phases"]["closing"]["started_at"] == closing.clock.started_at.isoformat()
    assert clock["notice_at"] == result.clock.notice_at.isoformat()  # the top is the incident's


def test_accounting_has_one_row_per_phase_online_then_offline_with_the_cache_split(
    tmp_path: Path, fake_command: Path
) -> None:
    settings = _settings(tmp_path, fake_command)
    incident = _incident(settings, tmp_path)
    run_external_cell(settings, cell_dir=incident.cell_dir, prompt="CLOSE", phase="closing")

    accounting = incident.accounting
    assert accounting is not None and accounting.online and accounting.requests == 2  # three records, two ids
    tokens = accounting.tokens
    assert (tokens.cached_input_tokens, tokens.cache_write_input_tokens, tokens.output_tokens) == (200, 100, 7 + 9)
    assert tokens.cache_write_1h_input_tokens == 40 and tokens.input_tokens == 2 * 160
    assert accounting.usd is not None and accounting.usd > 0
    assert (accounting.request_errors, accounting.independent_errors) == (1, 1)  # counted, never priced
    assert accounting.crosscheck == "passed" and not accounting.partial
    document = json.loads((incident.cell_dir / "accounting.json").read_text())
    rows = {row["phase"]: row for row in document["phases"]}
    assert list(rows) == ["incident", "closing"]
    assert rows["incident"]["online"] is True and rows["closing"]["online"] is False
    assert rows["incident"]["requests"] == 2 and rows["closing"]["requests"] == 2
    assert rows["closing"]["tokens"]["cache_write_input_tokens"] == 100


def test_a_log_shared_by_phases_is_split_by_its_phase_tag(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.external_accounting import account_phase

    def line(phase: str, ident: str) -> str:
        usage = {"input_tokens": 1, "output_tokens": 2}
        message = {"id": ident, "model": "claude-sonnet-5-5", "usage": usage}
        return json.dumps({"type": "assistant", "phase": phase, "message": message}) + "\n"

    log = tmp_path / "shared.jsonl"
    log.write_text(line("incident", "a") + line("closing", "b") + line("closing", "c"))

    assert account_phase("incident", log, None, timed_out=False).requests == 1
    assert account_phase("closing", log, None, timed_out=False).requests == 2


@pytest.mark.parametrize("mode", ["no_evidence", "empty_evidence", "problems", "noresult", "badresult"])
def test_missing_or_empty_evidence_and_a_missing_or_bad_result_make_the_cell_not_gradable(
    tmp_path: Path, fake_command: Path, mode: str
) -> None:
    result = _incident(_settings(tmp_path, fake_command, mode), tmp_path)

    assert not result.transcripts_complete
    joined = "; ".join(result.transcript_problems)
    expected = {
        "no_evidence": "events.json is missing or empty",
        "empty_evidence": "events.json is missing or empty",
        "problems": "events export failed",
        "noresult": "wrote no incident/agent-result.json",
        "badresult": "is unreadable",
    }[mode]
    assert expected in joined


def test_a_crash_is_an_error_but_keeps_its_evidence(tmp_path: Path, fake_command: Path) -> None:
    result = _incident(_settings(tmp_path, fake_command, "crash"), tmp_path)

    assert result.returncode == 3 and result.error == "fake agent (patched, abc123) incident exited with code 3"
    assert result.transcripts_complete and result.accounting is not None and result.accounting.requests == 2


def test_a_command_that_never_returns_is_killed_as_a_backstop(tmp_path: Path, fake_command: Path) -> None:
    settings = _settings(tmp_path, fake_command, "hang", incident_timeout_seconds=1.0, evidence_grace_seconds=1.0)
    started = time.monotonic()
    result = _incident(settings, tmp_path)

    assert time.monotonic() - started < 60
    assert result.backstop_killed and result.timed_out and "group killed" in (result.error or "")
    assert not result.transcripts_complete  # it wrote no result, so the row is not gradable
    clock = json.loads((result.cell_dir / "clock.json").read_text())
    assert clock["phases"]["incident"]["backstop_killed"] is True and clock["phases"]["incident"]["timed_out"] is True


def test_a_cross_check_mismatch_is_not_gradable(tmp_path: Path, fake_command: Path) -> None:
    result = _incident(_settings(tmp_path, fake_command, "mismatch"), tmp_path)

    assert not result.transcripts_complete
    assert result.accounting is not None and result.accounting.crosscheck == "failed"
    assert any(
        "accounting cross-check failed" in item and "token fields differ" in item for item in result.transcript_problems
    )


def test_a_request_the_independent_log_never_saw_is_not_gradable(tmp_path: Path, fake_command: Path) -> None:
    result = _incident(_settings(tmp_path, fake_command, "missing_independent_record"), tmp_path)

    assert not result.transcripts_complete
    assert any("only in the command's log" in item for item in result.transcript_problems)


def test_a_timed_out_phase_prices_a_cut_response_from_the_independent_log_and_flags_it_partial(
    tmp_path: Path, fake_command: Path
) -> None:
    result = _incident(_settings(tmp_path, fake_command, "cut"), tmp_path)

    accounting = result.accounting
    assert result.transcripts_complete, result.transcript_problems
    assert result.timed_out and "timed out" in (result.error or "")
    assert accounting is not None and accounting.partial and accounting.partial_requests == 1
    assert accounting.requests == 3 and accounting.tokens.output_tokens == 7 + 9 + 4
    assert accounting.crosscheck == "passed"
    row = json.loads((result.cell_dir / "accounting.json").read_text())["phases"][0]
    assert row["partial"] is True


def test_an_incomplete_response_is_a_mismatch_when_the_phase_did_not_time_out(
    tmp_path: Path, fake_command: Path
) -> None:
    result = _incident(_settings(tmp_path, fake_command, "cut_untimed"), tmp_path)

    assert not result.transcripts_complete
    assert any("incomplete independent record" in item for item in result.transcript_problems)


def test_the_closing_phase_gets_an_empty_kubeconfig_and_the_fixed_text_only(tmp_path: Path, fake_command: Path) -> None:
    settings = _settings(tmp_path, fake_command)
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        agent = ExternalBaselineAgent(settings, stub=stub, prompt_for=lambda _problem, _api: "NOTICE")
        outcome = agent.resolve(0, "problem-x", lambda: InjectionWindow(T0, T0))
        learned = agent.learn(outcome)

    closing = _probe(tmp_path, "closing")
    assert closing["kubeconfig_text"] == ""
    assert closing["prompt"] == DEFAULT_CLOSING_PROMPT
    assert closing["env"]["SDO_EXTERNAL_SUBMIT_URL"] is None
    assert closing["env"]["SDO_EXTERNAL_PHASE"] == "closing"
    assert "--state-in" not in closing["argv"]
    # nothing about the outcome, the grade or the incident reaches the command
    for word in ("problem-x", "the fix", "the diagnosis", "success"):
        assert word not in closing["prompt"]
    assert not any("outcome" in item for item in closing["argv"])
    assert learned.error is None
    assert learned.reflection_tokens.cache_write_input_tokens == 100
    assert learned.reflection_started_at is not None and learned.reflection_finished_at is not None
    assert learned.responder_tokens == outcome.responder_tokens


def test_no_closing_phase_runs_unless_configured(tmp_path: Path, fake_command: Path) -> None:
    settings = _settings(tmp_path, fake_command, closing_timeout_seconds=None)
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        agent = ExternalBaselineAgent(settings, stub=stub, prompt_for=lambda _problem, _api: "NOTICE")
        outcome = agent.resolve(0, "problem-x", lambda: InjectionWindow(T0, T0))
        assert agent.learn(outcome) is outcome

    assert not (tmp_path / "probe.closing").exists()


def test_a_closing_phase_with_problems_marks_the_row_without_touching_the_incident_numbers(
    tmp_path: Path, fake_command: Path
) -> None:
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        agent = ExternalBaselineAgent(_settings(tmp_path, fake_command), stub=stub, prompt_for=lambda _p, _a: "NOTICE")
        outcome = agent.resolve(0, "problem-x", lambda: InjectionWindow(T0, T0))
        broken = ExternalBaselineAgent(
            _settings(tmp_path, fake_command, "no_evidence"), stub=stub, prompt_for=lambda _p, _a: "NOTICE"
        )
        learned = broken.learn(outcome)

    assert learned.error is not None and "external transcripts incomplete" in learned.error
    assert learned.responder_tokens == outcome.responder_tokens


def test_a_state_map_entry_is_passed_as_state_in_and_a_bad_one_is_refused_up_front(
    tmp_path: Path, fake_command: Path
) -> None:
    state = tmp_path / "pass1" / "agent"
    state.mkdir(parents=True)
    settings = _settings(tmp_path, fake_command, state_map={"problem-x": state})
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        agent = ExternalBaselineAgent(settings, stub=stub, prompt_for=lambda _problem, _api: "NOTICE")
        agent.resolve(0, "problem-x", lambda: InjectionWindow(T0, T0))

    argv = _probe(tmp_path, "incident")["argv"]
    assert argv[argv.index("--state-in") + 1] == str(state)
    assert settings.state_for("problem-x+other") == state and settings.state_for("problem-y") is None
    with pytest.raises(ValueError, match="is not a directory"):
        _settings(tmp_path, fake_command, state_map={"p": tmp_path / "missing"})


def test_state_in_reaches_the_command_and_the_source_is_not_modified(tmp_path: Path, fake_command: Path) -> None:
    state = tmp_path / "pass1" / "agent"
    state.mkdir(parents=True)
    (state / "memory.txt").write_text("pass 1")
    result = _incident(_settings(tmp_path, fake_command), tmp_path, state_in=state)

    probe = _probe(tmp_path, "incident")
    assert probe["argv"][probe["argv"].index("--state-in") + 1] == str(state)
    assert probe["state_in_exists"] is True
    assert (state / "memory.txt").read_text() == "pass 1"
    assert result.result is not None and result.result.state_dir == result.cell_dir / "agent"


def test_the_fast_loop_agent_maps_the_cell_to_an_outcome(tmp_path: Path, fake_command: Path) -> None:
    settings = _settings(tmp_path, fake_command, model="some-model")
    window = InjectionWindow(T0, T0)
    with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
        agent = ExternalBaselineAgent(settings, stub=stub, prompt_for=lambda _problem, _api: "NOTICE")
        outcome = agent.resolve(0, "problem-x", lambda: window)

    assert (agent.name, agent.provider, agent.model) == ("external", "external", "some-model")
    assert outcome.injection is window and outcome.error is None
    assert (outcome.diagnosis, outcome.mitigation) == ("the diagnosis", "the fix")
    assert outcome.dispatched_at is not None and outcome.responder_started_at is not None
    assert outcome.responder_tokens.cache_write_input_tokens == 100
    assert outcome.agent_label == "fake agent (patched, abc123)"
    assert outcome.agent_state_dir is not None and outcome.agent_state_dir.endswith("000_problem-x/agent")
    assert outcome.artifacts_dir is not None and outcome.artifacts_dir.endswith("000_problem-x")


def test_a_row_whose_command_submitted_nothing_or_lost_evidence_says_so(tmp_path: Path, fake_command: Path) -> None:
    for index, (mode, expected) in enumerate(
        (("no_submit", "without submitting a mitigation"), ("no_evidence", "transcripts incomplete"))
    ):
        with SubmissionStub(app_info={"app_name": "a", "namespace": "n"}) as stub:
            agent = ExternalBaselineAgent(
                _settings(tmp_path, fake_command, mode), stub=stub, prompt_for=lambda _problem, _api: "NOTICE"
            )
            outcome = agent.resolve(index, "problem-x", lambda: InjectionWindow(T0, T0))
        assert expected in (outcome.error or "")


@pytest.mark.parametrize(
    "overrides",
    [
        {"command": ()},
        {"command": ("",)},
        {"label": " "},
        {"incident_timeout_seconds": 0},
        {"closing_timeout_seconds": -1},
        {"closing_prompt": ""},
        {"evidence_grace_seconds": -1},
        {"model": ""},
    ],
)
def test_settings_validate_in_post_init(tmp_path: Path, fake_command: Path, overrides: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        _settings(tmp_path, fake_command, **overrides)  # type: ignore[arg-type]


def test_a_result_path_cannot_leave_the_cell_directory(tmp_path: Path) -> None:
    cell = tmp_path / "cell"
    (cell / "incident").mkdir(parents=True)
    (cell / "incident" / "agent-result.json").write_text(
        json.dumps(
            {
                "phase": "incident",
                "timed_out": False,
                "requests": "../outside.jsonl",
                "required_evidence": [],
                "problems": [],
            }
        )
    )

    with pytest.raises(external_agent.ExternalResultError, match="inside the cell directory"):
        external_agent.load_result(cell, "incident")


@pytest.mark.parametrize(
    ("argv", "model", "agent_command"),
    [
        (["--agent", "external", "--agent-command", "run-it --x"], "unspecified", "run-it --x"),
        (["--agent", "external", "--agent-command", "c", "--model", "claude-opus-5-5"], "claude-opus-5-5", "c"),
    ],
)
def test_fastloop_run_takes_the_external_agent_and_records_the_declared_model(
    argv: list[str], model: str, agent_command: str
) -> None:
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(
        [
            "run",
            "--run-dir",
            "x",
            *argv,
            "--agent-label",
            "L",
            "--agent-env",
            "A=b",
            "--agent-closing-timeout",
            "120",
            "--agent-state-map",
            "map.json",
        ]
    )

    assert (args.agent, args.model, args.agent_command, args.agent_label) == ("external", model, agent_command, "L")
    assert (
        args.agent_env == ["A=b"] and args.agent_closing_timeout == 120.0 and args.agent_state_map == Path("map.json")
    )


def test_the_agent_flags_need_the_external_agent_and_a_command() -> None:
    from benchmarks.sregym.fastloop.cli import _check_external_args, build_parser

    def parsed(*argv: str):
        return build_parser().parse_args(["run", "--run-dir", "x", *argv])

    with pytest.raises(SystemExit, match="requires --agent-command"):
        _check_external_args(parsed("--agent", "external"))
    with pytest.raises(SystemExit, match="apply to --agent external only"):
        _check_external_args(parsed("--agent", "codex", "--agent-command", "c"))
    with pytest.raises(SystemExit, match="KEY=VALUE"):
        _check_external_args(parsed("--agent", "external", "--agent-command", "c", "--agent-env", "oops"))
    _check_external_args(parsed("--agent", "external", "--agent-command", "c", "--agent-env", "A=b"))
    _check_external_args(parsed("--agent", "codex"))


def test_the_cli_builds_the_settings_from_the_flags(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import _external_settings, build_parser

    state = tmp_path / "s"
    state.mkdir()
    mapping = tmp_path / "map.json"
    mapping.write_text(json.dumps({"p": str(state)}))
    closing = tmp_path / "closing.txt"
    closing.write_text("Close it.")
    args = build_parser().parse_args(
        [
            "run",
            "--run-dir",
            "x",
            "--agent",
            "external",
            "--agent-command",
            "bin/tool --opt 'a b'",
            "--agent-env",
            "K=v=w",
            "--agent-state-map",
            str(mapping),
            "--agent-closing-timeout",
            "30",
            "--agent-closing-prompt-file",
            str(closing),
            "--timeout",
            "600",
            "--model",
            "m",
        ]
    )
    kubeconfig = tmp_path / "k"
    kubeconfig.write_text("")

    settings = _external_settings(args, kubeconfig, tmp_path / "results")

    assert settings.command == ("bin/tool", "--opt", "a b") and settings.label == "tool"
    assert settings.extra_env == {"K": "v=w"} and settings.state_map == {"p": state}
    assert (settings.incident_timeout_seconds, settings.closing_timeout_seconds) == (600.0, 30.0)
    assert settings.closing_prompt == "Close it." and settings.model == "m"


def test_the_sources_name_no_particular_agent() -> None:
    # The arm is agent-agnostic: the sources of this arm never name the agent it was first built for.
    word = re.compile(r"\b" + "e" + "xo" + r"\b", re.IGNORECASE)
    root = REPO_ROOT / "benchmarks" / "sregym" / "fastloop"
    sources = [
        root / "external_agent.py",
        root / "external_accounting.py",
        root / "README.md",
        Path(__file__),
    ]
    for source in sources:
        assert not word.search(source.read_text()), source
