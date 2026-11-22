"""The SREGym lanes adapter's real-agent mode: operator settings -> ``fastloop run`` flags, the credentials hook, the
subject's description and the per-run columns a report is built from."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.lanes import agent, subject, workload
from benchmarks.sregym.lanes.workload import AdapterError
from tests.unit.benchmarks.sregym.lanes.test_lanes_workload import (
    PACK,
    RUN_ARGS,
    FakePageCluster,
    FakeRunner,
    accepted_repo,
    call,
    git,
    incident,
    lane_env,
    write_environment,
)

REAL_SETTINGS = {
    "auth_file": "/creds/adc.json",
    "config": {
        "agent": {
            "extra_env": [
                "CLAUDE_CODE_USE_VERTEX=1",
                "CLOUD_ML_REGION=global",
                "ANTHROPIC_VERTEX_PROJECT_ID=some-project",
                "GOOGLE_APPLICATION_CREDENTIALS=/sdo/credentials/.credentials.json",
            ]
        },
        "response": {"after_playbook": "agent"},
    },
    "effort": "medium",
    "model": "claude-sonnet-5-5",
    "provider": "claude",
}


def real_settings(tmp_path: Path) -> dict:
    """The batch settings with a credential file that exists (a stand-in: its content is never read)."""
    credential = tmp_path / "adc.json"
    credential.write_text("{}")
    return {**REAL_SETTINGS, "auth_file": str(credential)}


def operator_file(tmp_path: Path, settings: dict, **block: str) -> Path:
    path = tmp_path / "operator.json"
    path.write_text(json.dumps({"hooks": "sregym", "revision": "abc", "settings": settings, **block}))
    return path


# ----- settings -----


@pytest.mark.parametrize("settings", [{}, {"startup_bound": "0s"}, {"provider": "claude", "model": "stub"}])
def test_settings_without_a_model_are_the_scripted_agent(tmp_path, settings):
    assert agent.AgentSettings.from_operator(operator_file(tmp_path, settings)) is None
    assert agent.AgentSettings.from_operator(None) is None


def test_the_batch_settings_are_a_real_agent(tmp_path):
    settings = agent.AgentSettings.from_operator(operator_file(tmp_path, REAL_SETTINGS))
    assert settings is not None
    assert (settings.provider, settings.model, settings.effort) == ("claude", "claude-sonnet-5-5", "medium")
    assert settings.auth_file == Path("/creds/adc.json")
    assert settings.config["response"] == {"after_playbook": "agent"}
    assert settings.credential_key == ".credentials.json"


def test_a_codex_agent_keys_its_credential_as_auth_json(tmp_path):
    settings = agent.AgentSettings.from_operator(
        operator_file(tmp_path, {"provider": "codex", "model": "m", "auth_file": "/a/auth.json"})
    )
    assert settings is not None
    assert settings.credential_key == "auth.json"


@pytest.mark.parametrize(
    ("settings", "message"),
    [
        ({"provider": "gemini", "model": "m"}, "provider"),
        ({"provider": "claude"}, "model"),
        ({"model": "m", "provider": "claude", "config": "x"}, "config"),
        ({"model": "m", "provider": "claude", "effort": "turbo"}, "effort"),
    ],
)
def test_bad_agent_settings_are_refused_with_the_setting_named(tmp_path, settings, message):
    with pytest.raises((TypeError, ValueError), match=message):
        agent.AgentSettings.from_operator(operator_file(tmp_path, settings))


def test_run_flags_name_the_agent_the_overlay_and_the_credentials_hook(tmp_path):
    settings = agent.AgentSettings.from_operator(operator_file(tmp_path, REAL_SETTINGS))
    assert settings is not None
    overlay = tmp_path / "operator-config.yaml"
    flags = settings.run_flags(overlay)
    assert flags[flags.index("--provider") + 1] == "claude"
    assert flags[flags.index("--model") + 1] == "claude-sonnet-5-5"
    assert flags[flags.index("--reasoning-effort") + 1] == "medium"
    assert flags[flags.index("--config") + 1] == str(overlay)
    assert flags[flags.index("--credentials-hook") + 1] == str(agent.CREDENTIALS_HOOK)
    assert "--agent-registrations" not in flags
    assert json.loads(overlay.read_text()) == REAL_SETTINGS["config"]


def test_the_hook_environment_references_the_credential_file_and_never_reads_it(tmp_path):
    settings = agent.AgentSettings.from_operator(operator_file(tmp_path, REAL_SETTINGS))
    assert settings is not None
    assert settings.hook_env() == {
        "SREGYM_LANES_AUTH_FILE": "/creds/adc.json",
        "SREGYM_LANES_AUTH_KEY": ".credentials.json",
    }


# ----- the workload's run -----


def fast_run(tmp_path: Path, env_dir: Path, repo: Path):
    seen: list[tuple[list[str], dict[str, str]]] = []

    def fast(argv, *, env=None, cwd=None, tick=None):
        argv = list(argv)
        seen.append((argv, dict(env or {})))
        run_id = argv[argv.index("--run-id") + 1]
        (env_dir / "results" / run_id).mkdir(parents=True)
        (env_dir / "results" / run_id / "incidents.jsonl").write_text(
            json.dumps(incident(run_id, before="", after=git(repo, "rev-parse", "HEAD"))) + "\n"
        )
        return ""

    return fast, seen


def run_with(tmp_path: Path, settings: dict | None = None, **extra_env: str):
    settings = real_settings(tmp_path) if settings is None else settings
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)
    fast, seen = fast_run(tmp_path, env_dir, repo)
    env = lane_env(
        tmp_path,
        FUZZOPS_EXEC_OUT=str(tmp_path / "exec"),
        BENCH_OPERATOR=str(operator_file(tmp_path, settings)),
        **extra_env,
    )
    code, record = call(
        "run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=fast, page_cluster=FakePageCluster(True)
    )
    assert code == 0, record
    return seen[0], tmp_path / "exec", record


def test_a_real_agent_run_drives_the_real_responder_with_the_overlay_and_credentials(tmp_path):
    (argv, env), exec_out, _ = run_with(tmp_path)
    assert argv[argv.index("--provider") + 1] == "claude"
    assert argv[argv.index("--model") + 1] == "claude-sonnet-5-5"
    assert "--agent-registrations" not in argv
    assert argv[argv.index("--credentials-hook") + 1] == str(agent.CREDENTIALS_HOOK)
    assert json.loads(Path(argv[argv.index("--config") + 1]).read_text()) == REAL_SETTINGS["config"]
    assert Path(argv[argv.index("--config") + 1]).is_relative_to(exec_out)
    assert env["SREGYM_LANES_AUTH_FILE"] == str(tmp_path / "adc.json")
    assert env["SREGYM_LANES_AUTH_KEY"] == ".credentials.json"
    for flag in ("--healthy-baseline", "--inject-before-resume", "--require-full-cleanup"):
        assert flag in argv  # the scripted row's flags stay


def test_the_hosts_placeholder_credentials_hook_does_not_replace_the_real_one(tmp_path):
    # hosts.yaml's SREGYM_LANES_RUN_ARGS carries the model-free hook for scripted plans.
    (argv, _), _, _ = run_with(
        tmp_path, SREGYM_LANES_RUN_ARGS="--credentials-hook /host/dummy-credentials-hook.sh --foo bar"
    )
    hooks = [argv[i + 1] for i, item in enumerate(argv) if item == "--credentials-hook"]
    assert hooks[-1] == str(agent.CREDENTIALS_HOOK)
    assert "/host/dummy-credentials-hook.sh" not in argv
    assert "--foo" in argv  # other host flags survive


def test_a_scripted_run_keeps_the_scripted_registrations_and_the_hosts_hook(tmp_path):
    (argv, env), _, _ = run_with(
        tmp_path, {"startup_bound": "0s"}, SREGYM_LANES_RUN_ARGS="--credentials-hook /host/dummy.sh"
    )
    assert argv[argv.index("--agent-registrations") + 1].endswith("fastloop/agent_registrations")
    assert "--provider" not in argv
    assert argv[argv.index("--credentials-hook") + 1] == "/host/dummy.sh"
    assert "SREGYM_LANES_AUTH_FILE" not in env


def test_a_missing_credential_file_fails_the_run_before_fastloop(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "nohome"))
    settings = {k: v for k, v in REAL_SETTINGS.items() if k != "auth_file"}
    repo, base = accepted_repo(tmp_path)
    write_environment(tmp_path, repo, seed_commit=base)

    def never(argv, **_):
        raise AssertionError("fastloop run must not start")

    env = lane_env(tmp_path, BENCH_OPERATOR=str(operator_file(tmp_path, settings)))
    code, result = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=never)
    assert code == 1
    assert "credential file" in result["error"]


def test_a_lane_built_with_another_responder_image_is_refused_before_fastloop(tmp_path):
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)
    document = json.loads((env_dir / "fastloop.json").read_text())
    document["images"] = {"controller": "c:1", "responder": "sdo-sregym-responder:scripted"}
    (env_dir / "fastloop.json").write_text(json.dumps(document))
    settings = {**real_settings(tmp_path), "responder_image": "sdo-sregym-responder:plain"}

    def never(argv, **_):
        raise AssertionError("fastloop run must not start")

    env = lane_env(tmp_path, BENCH_OPERATOR=str(operator_file(tmp_path, settings)))
    code, result = call("run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=never)
    assert code == 1
    assert "responder image" in result["error"]
    assert "sdo-sregym-responder:plain" in result["error"]


def test_app_up_gives_a_real_agents_images_to_fastloop_up(tmp_path):
    seen: list[list[str]] = []

    def up(argv, *, env=None, cwd=None, tick=None):
        seen.append(list(argv))
        write_environment(tmp_path, tmp_path / "ws")
        return ""

    settings = {**real_settings(tmp_path), "responder_image": "sdo-sregym-responder:plain"}
    env = lane_env(
        tmp_path,
        BENCH_OPERATOR=str(operator_file(tmp_path, settings)),
        SREGYM_LANES_CLUSTER_UP="1",
        SREGYM_LANES_UP_ARGS="--responder-image sdo-sregym-responder:scripted",
    )
    code, result = call(
        "app-up",
        "up",
        ["--pack", str(PACK), "--namespace", "hotel-reservation"],
        env,
        runner=FakeRunner({"kind get clusters": "other\n"}),
        long_runner=up,
    )
    assert code == 0, result
    argv = seen[0]
    images = [argv[i + 1] for i, a in enumerate(argv) if a == "--responder-image"]
    assert images[-1] == "sdo-sregym-responder:plain"  # fastloop takes the last


# ----- the credentials hook -----


def run_hook(tmp_path: Path, env: dict[str, str], namespace: str = "hotel-reservation-sdo", has_ns: bool = False):
    fake = tmp_path / "bin"
    fake.mkdir(exist_ok=True)
    log = tmp_path / "kubectl.log"
    kubectl = fake / "kubectl"
    kubectl.write_text(
        '#!/bin/sh\necho "$*" >> "$KUBECTL_LOG"\n'
        'case "$*" in\n'
        '  *"get ns"*|*"get namespace"*) [ -n "$HAS_NS" ] || exit 1;;\n'
        '  *"--dry-run=client"*) echo "kind: Secret";;\n'
        "esac\n"
    )
    kubectl.chmod(0o755)
    result = subprocess.run(
        ["sh", str(agent.CREDENTIALS_HOOK), namespace],
        env={
            "PATH": f"{fake}:{os.environ['PATH']}",
            "KUBECTL_LOG": str(log),
            "KUBECONFIG": "/lane/kubeconfig",
            **({"HAS_NS": "1"} if has_ns else {}),
            **env,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    return result, log.read_text().splitlines() if log.exists() else []


def test_the_hook_puts_the_credential_file_into_the_secret_by_reference(tmp_path):
    credential = tmp_path / "adc.json"
    credential.write_text('{"secret-value": "do-not-print"}')
    result, calls = run_hook(
        tmp_path, {"SREGYM_LANES_AUTH_FILE": str(credential), "SREGYM_LANES_AUTH_KEY": ".credentials.json"}
    )
    assert result.returncode == 0, result.stderr
    secret = [line for line in calls if "create secret generic sdo-agent-credentials" in line]
    assert secret
    assert f"--from-file=.credentials.json={credential}" in secret[0]
    assert "-n hotel-reservation-sdo" in secret[0]
    assert any("create ns hotel-reservation-sdo" in line or "create namespace" in line for line in calls)
    assert any(line.startswith("-n hotel-reservation-sdo apply") or " apply -f -" in line for line in calls)
    assert "do-not-print" not in result.stdout + result.stderr


def test_the_hook_leaves_an_existing_namespace_alone(tmp_path):
    credential = tmp_path / "adc.json"
    credential.write_text("{}")
    result, calls = run_hook(
        tmp_path,
        {"SREGYM_LANES_AUTH_FILE": str(credential), "SREGYM_LANES_AUTH_KEY": "auth.json"},
        has_ns=True,
    )
    assert result.returncode == 0, result.stderr
    assert not any("create ns" in line or "create namespace" in line for line in calls)
    assert any("--from-file=auth.json=" in line for line in calls)


@pytest.mark.parametrize("env", [{}, {"SREGYM_LANES_AUTH_FILE": "/does/not/exist", "SREGYM_LANES_AUTH_KEY": "k"}])
def test_the_hook_fails_loudly_without_a_credential_file(tmp_path, env):
    result, calls = run_hook(tmp_path, env)
    assert result.returncode != 0
    assert "SREGYM_LANES_AUTH_FILE" in result.stderr
    assert not any("secret" in line for line in calls)


# ----- the subject -----


def test_the_subject_of_a_real_agent_needs_credentials(tmp_path):
    env = {"BENCH_OPERATOR": str(operator_file(tmp_path, REAL_SETTINGS))}
    assert subject.dispatch("describe", env) == (0, {"max_startup": "0s", "needs_credentials": True})
    scripted = {"BENCH_OPERATOR": str(operator_file(tmp_path, {"startup_bound": "0s"}))}
    assert subject.dispatch("describe", scripted) == (0, {"max_startup": "0s", "needs_credentials": False})
    assert subject.dispatch("describe", {}) == (0, {"max_startup": "0s", "needs_credentials": False})


def test_the_subject_passes_the_agent_settings_through_without_the_credential_path(tmp_path):
    env = {"BENCH_PACK": "/pack", "BENCH_OPERATOR": str(operator_file(tmp_path, REAL_SETTINGS))}
    for verb in ("prepare", "swap"):
        code, result = subject.dispatch(verb, env, version=lambda: "rev")
        assert code == 0
        assert result["agent"] == {"provider": "claude", "model": "claude-sonnet-5-5", "effort": "medium"}
        assert "/creds/adc.json" not in json.dumps(result)
    scripted = {"BENCH_PACK": "/pack"}
    assert "agent" not in subject.dispatch("prepare", scripted, version=lambda: "rev")[1]


# ----- what the grade exposes per problem -----


def priced_incident(run_id: str) -> dict:
    row = incident(run_id)
    row.update(
        {
            "provider": "claude",
            "model": "claude-sonnet-5-5",
            "reasoning_effort": "medium",
            "detected_at": "2026-11-20T10:01:20Z",
            "dispatched_at": "2026-11-20T10:01:30Z",
            "resolved_at": "2026-11-20T10:04:20Z",
            "responder_started_at": "2026-11-20T10:01:31Z",
            "responder_finished_at": "2026-11-20T10:04:00Z",
            "reflection_started_at": "2026-11-20T10:04:30Z",
            "reflection_finished_at": "2026-11-20T10:05:30Z",
            "resolution_path": "agent",
            "warm_path": False,
            "match_reasons": [],
            "responder_tokens": {"input_tokens": 1_000_000, "output_tokens": 100_000, "model_requests": 12},
            "reflection_tokens": {
                "input_tokens": 500_000,
                "cached_input_tokens": 400_000,
                "output_tokens": 10_000,
                "model_requests": 3,
            },
        }
    )
    return row


def test_summarize_carries_the_dispatch_the_playbook_path_and_the_responder_cost():
    summary = workload.summarize(priced_incident("r"))
    assert summary["dispatched_at"] == "2026-11-20T10:01:30Z"
    assert summary["resolution_path"] == "agent"
    assert summary["warm_path"] is False
    assert summary["match_reasons"] == []
    assert summary["responder_tokens"]["output_tokens"] == 100_000
    # claude-sonnet-5-5: $2/M input, $10/M output (agentshim's table): 1M in + 0.1M out
    assert summary["responder_usd"] == pytest.approx(3.0)
    # reflection: 100k uncached * $2/M + 400k cache read * $0.10/M + 10k output * $10/M
    assert summary["reflection_usd"] == pytest.approx(0.34)
    assert summary["responder_requests"] == 12
    assert summary["reflection_started_at"] == "2026-11-20T10:04:30Z"
    assert summary["reflection_finished_at"] == "2026-11-20T10:05:30Z"


def test_a_row_the_automatic_stage_closed_has_no_responder_cost():
    row = incident("r")
    row.update({"provider": "claude", "model": "claude-sonnet-5-5", "resolution_path": "deterministic"})
    summary = workload.summarize(row)
    assert summary["dispatched_at"] is None
    assert summary["responder_usd"] == 0.0
    assert summary["responder_requests"] == 0


def test_a_playbook_closure_dispatched_the_incident_but_not_the_agent():
    row = incident("r")
    row.update({"provider": "claude", "model": "claude-sonnet-5-5", "resolution_path": "deterministic"})
    row["dispatched_at"] = row["responder_started_at"] = "2026-11-20T10:01:30Z"  # the stage's, as recorded
    grade = workload.grade_line({"operator": "o", "valid": True, "fault": "f", "sregym": workload.summarize(row)})
    assert grade["dispatched_at"] == "2026-11-20T10:01:30Z"
    assert grade["agent_dispatched"] is False
    assert grade["responder_usd"] == 0.0


def test_an_unpriced_model_has_unknown_cost_never_zero():
    row = priced_incident("r")
    row["model"] = "no-such-model"
    summary = workload.summarize(row)
    assert summary["responder_usd"] is None
    assert summary["reflection_usd"] is None


def test_the_grade_has_one_column_per_fact_of_the_final_report():
    record = {
        "operator": "exec:p1-r1",
        "valid": True,
        "fault": "network_policy_block",
        "sregym": {
            **workload.summarize(priced_incident("r")),
            "page_source": "own",
            "paged_at": None,
        },
        "memory": {"seeded": True, "commit": "c" * 40, "playbook_files_changed": ["a", "b"]},
    }
    row = workload.grade_line(record)
    assert row["page_source"] == "own"
    assert row["agent_dispatched"] is True
    assert row["dispatched_at"] == "2026-11-20T10:01:30Z"
    assert row["detected_at"] == "2026-11-20T10:01:20Z"
    assert row["resolved_at"] == "2026-11-20T10:04:20Z"
    assert row["page_to_healthy_seconds"] == pytest.approx(180.0)  # own page: detected_at -> resolved_at
    assert row["responder_usd"] == pytest.approx(3.0)
    assert row["warm_path"] is False
    assert row["match_reasons"] == []
    assert row["resolution_path"] == "agent"
    assert row["learned"] is True
    assert row["closing_finished"] is True


def test_a_fallback_page_is_the_clock_of_page_to_healthy_time():
    summary = {
        **workload.summarize(priced_incident("r")),
        "page_source": "fallback",
        "paged_at": "2026-11-20T10:03:00Z",
    }
    row = workload.grade_line({"operator": "o", "valid": True, "fault": "f", "sregym": summary, "memory": {}})
    assert row["page_to_healthy_seconds"] == pytest.approx(80.0)


def test_a_run_records_the_page_instant_and_exports_the_memory_after_the_closing_turn(tmp_path):
    repo, base = accepted_repo(tmp_path)
    env_dir = write_environment(tmp_path, repo, seed_commit=base)

    def fast(argv, *, env=None, cwd=None, tick=None):
        argv = list(argv)
        run_id = argv[argv.index("--run-id") + 1]
        results = env_dir / "results" / run_id
        results.mkdir(parents=True)
        # the incident record is appended before the closing turn ends; the closing turn's commit lands after
        (results / "incidents.jsonl").write_text(json.dumps(priced_incident(run_id)) + "\n")
        book = repo / ".sdo" / "playbooks" / "netpol" / "README.md"
        book.parent.mkdir(parents=True)
        book.write_text("learned by the closing turn\n")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "closing turn")
        return ""

    exec_out = tmp_path / "exec"
    env = lane_env(
        tmp_path,
        FUZZOPS_EXEC_OUT=str(exec_out),
        BENCH_OPERATOR=str(operator_file(tmp_path, real_settings(tmp_path))),
    )
    code, record = call(
        "run", "run", RUN_ARGS, env, runner=workload.run_process, long_runner=fast, page_cluster=FakePageCluster(True)
    )
    assert code == 0, record
    memory = json.loads((exec_out / "memory" / "memory.json").read_text())
    assert memory["commit"] == git(repo, "rev-parse", "HEAD")
    assert memory["playbook_files_changed"] == [".sdo/playbooks/netpol/README.md"]


def test_an_adapter_error_names_the_unreadable_overlay_setting(tmp_path):
    with pytest.raises(AdapterError):
        agent.settings_for(
            {"BENCH_OPERATOR": str(operator_file(tmp_path, {"provider": "claude", "model": "m", "config": "x"}))}
        )
