"""Lanes subject for any external agent command, under the plain-agent arm's hook (:mod:`benchmarks.fuzzops.vanilla`).

Same page text, page time, scoped credentials, deadline, lane lock, isolation proof and golden reset as ``vanilla``;
only the process that answers the page differs. SDO knows nothing about the agent behind the command. It is called
with the external-agent contract of ``benchmarks/sregym/fastloop/README.md``::

    COMMAND... --phase incident --cell-dir D --prompt-file F --kubeconfig K --deadline-epoch T

``D`` is fresh and randomly named: no fault, lane or previous state. The command confines its own agent (it gets a
minimal environment, never the lane's admin credentials), stops it at ``T`` and then exports evidence. Besides
``D/incident/agent-result.json`` (schema ``sdo.external-agent-result/v1``) it writes ``transcript.jsonl`` (Claude Code
JSONL records with timestamps and ``Bash`` tool calls, so one row tool reads every arm) and ``usage.json`` there.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops import eks, vanilla
from benchmarks.fuzzops.cellevidence import atomic_write_json, cell_problems, read_json_object
from benchmarks.fuzzops.hooks import HookError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

MODULE = "benchmarks.fuzzops.external"
RESULT_SCHEMA = "sdo.external-agent-result/v1"
#: Time after the agent's deadline for the command to stop it and export evidence, before the group is killed.
EVIDENCE_GRACE_SECONDS = 90
TEXT_EVIDENCE = (".json", ".jsonl", ".txt", ".log", ".stdout", ".stderr")


@dataclass(frozen=True)
class Settings:
    command: tuple[str, ...]
    label: str
    model: str

    def __post_init__(self) -> None:
        if not self.command or not all(isinstance(part, str) and part for part in self.command):
            raise ValueError("settings.external.command must be a non-empty list of strings")
        if not Path(self.command[0]).is_absolute():
            raise ValueError("settings.external.command must start with an absolute path")
        for name, value in (("label", self.label), ("model", self.model)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"settings.external.{name} must be a non-empty string")


def settings(env: Mapping[str, str]) -> Settings:
    raw = vanilla.operator_settings(env).get("external")
    if not isinstance(raw, dict):
        raise HookError("settings.external (command, label, model) is required")
    try:
        return Settings(tuple(raw.get("command") or ()), raw.get("label", ""), raw.get("model", ""))
    except (TypeError, ValueError) as exc:
        raise HookError(str(exc)) from exc


def validate(env: Mapping[str, str]) -> None:
    vanilla.vertex(env)
    command = Path(settings(env).command[0])
    if not command.is_file() or not os.access(command, os.X_OK):
        raise HookError("settings.external.command is not an executable on this host")


def preflight(_env: Mapping[str, str]) -> None:
    # Host preflight carries no spec, so there is no command to probe yet; ``check`` probes it.
    detail = "the command is checked per spec"
    print(json.dumps({"checks": [{"check": "external-sandbox", "ok": True, "detail": detail}]}))


def command_env(
    env: Mapping[str, str], home: Path, config: Path, auth: Path, extra: Mapping[str, str]
) -> dict[str, str]:
    """Nothing of the lane reaches the command: no admin kubeconfig, cloud credentials, fault, run or lane names."""
    return {
        **extra,
        "HOME": str(home),
        "KUBECONFIG": str(config),
        "GOOGLE_APPLICATION_CREDENTIALS": str(auth),
        "PATH": env.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "LANG": "C.UTF-8",
    }


def run_command(argv: Sequence[str], env: Mapping[str, str], cwd: Path, deadline: float) -> tuple[int | None, bool]:
    """Run to the end; past the deadline plus the evidence grace kill the group: (exit code, killed)."""
    with (cwd / "command.log").open("w") as log:
        proc = subprocess.Popen(
            argv, cwd=cwd, env=dict(env), stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True
        )
        try:
            return proc.wait(timeout=max(0.01, deadline + EVIDENCE_GRACE_SECONDS - time.time())), False
        except subprocess.TimeoutExpired:
            return None, True
        finally:
            # The session may outlive its leader; nothing the agent started stays on the host.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()


def keep_evidence(cell: Path, target: Path, secrets: Sequence[str]) -> None:
    """Redacted copies of the command's text evidence; the agent's own state directory is not kept (no memory)."""
    for path in sorted(cell.rglob("*")):
        relative = path.relative_to(cell)
        if relative.parts[0] in ("agent", "home") or path.suffix not in TEXT_EVIDENCE:
            continue
        # Resolve before opening: an agent-planted link must not pull a host file into evidence.
        if not path.resolve().is_relative_to(cell.resolve()) or not path.is_file():
            continue
        dest = target / "session-dir" / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        vanilla.copy_log(path, dest, secrets)


def result_problems(result: Mapping[str, Any], phase: Path) -> list[str]:
    if result.get("schema") != RESULT_SCHEMA:
        return ["agent-result.json missing or not " + RESULT_SCHEMA]
    problems = [str(item) for item in result.get("problems") or []]
    missing = [
        f"required evidence missing: {item}"
        for item in result.get("required_evidence") or []
        if not (phase.parent / str(item)).is_file()
    ]
    if not (phase / "usage.json").is_file():
        missing.append("required evidence missing: usage.json")
    return problems + missing


def capture(env: Mapping[str, str], target: Path, injected: float) -> None:
    spec = settings(env)
    auth, extra = vanilla.vertex(env)
    config = vanilla.state(env) / "kubeconfig"
    token = json.loads(config.read_text())["users"][0]["user"]["token"]
    secrets = [token, *vanilla.auth_secrets(auth)]
    deadline = injected + vanilla.AGENT_SECONDS
    with tempfile.TemporaryDirectory(prefix="agent-") as temp:
        cell = Path(temp)
        home = cell / "home"
        home.mkdir()
        # The command's kubeconfig is its own copy: the lane directory's path names the lane.
        scoped = cell / "home" / "kubeconfig"
        scoped.touch(mode=0o600)
        scoped.write_bytes(config.read_bytes())
        prompt = cell / "prompt.txt"
        prompt.write_text(vanilla.page_text(env["FUZZOPS_NAMESPACE"]))
        argv = [*spec.command, "--phase", "incident", "--cell-dir", str(cell), "--prompt-file", str(prompt)]
        argv += ["--kubeconfig", str(scoped), "--deadline-epoch", str(deadline)]
        started = time.time()
        exit_code, killed = run_command(argv, command_env(env, home, scoped, auth, extra), cell, deadline)
        phase = cell / "incident"
        result = read_json_object(phase / "agent-result.json")
        problems = result_problems(result, phase)
        if killed:
            problems.append("the command outlived the deadline and its evidence grace; its group was killed")
        transcript = phase / "transcript.jsonl"
        if transcript.is_file():
            vanilla.copy_log(transcript, target / "transcript.jsonl", secrets)
        vanilla.copy_log(cell / "command.log", target / "stderr.log", secrets)
        keep_evidence(cell, target, secrets)
        usage = read_json_object(phase / "usage.json")
        atomic_write_json(target / "usage.json", usage)
        summary = result.get("summary")
        if isinstance(summary, str) and env.get("FUZZOPS_REPORT"):
            atomic_write_json(Path(env["FUZZOPS_REPORT"]), {"summary": summary[:600]})
        atomic_write_json(
            target / "done.json",
            {
                # The page, as for the plain agent: the command's own setup is the arm's time.
                "started_at": vanilla.timestamp(started),
                "ended_at": result.get("ended_at") or vanilla.timestamp(time.time()),
                "capped": bool(result.get("timed_out")) or killed,
                "exit": exit_code,
                "model": spec.model,
                "label": result.get("label") or spec.label,
                "agent_version": result.get("agent_version"),
                "result_event": bool(result),
                "is_error": False,
                "problems": problems,
            },
        )


def supervise(env: Mapping[str, str], injected: float, lock_fd: int) -> None:
    os.set_inheritable(lock_fd, False)
    capture(env, vanilla.run_dir(env), injected)


def problems_of(dest: Path, done: Mapping[str, Any]) -> list[str]:
    problems = cell_problems(dest, "vanilla") + list(done.get("problems") or [])
    if not done.get("result_event"):
        problems.append("agent result missing" + (f" ({done['error']})" if done.get("error") else ""))
    elif done.get("exit") not in (None, 0):
        problems.append("the agent command exited with an error")
    return problems


def collect(env: Mapping[str, str]) -> int:
    with vanilla.lane_lock(env):
        return vanilla.publish(env, "external", vanilla.EVIDENCE_FILES, problems_of)


def prepare(env: Mapping[str, str]) -> None:
    vanilla.prepare(env, settings(env).label)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    environ = dict(os.environ if env is None else env)
    eks.export_entry_host(environ)
    verb = args[0] if args else ""
    try:
        if verb == "describe":
            validate(environ)
            print(json.dumps({"max_startup": "0s", "needs_credentials": True}))
        elif verb == "check":
            validate(environ)
            vanilla.binary(environ, "kubectl")
        elif verb == "preflight":
            preflight(environ)
        elif verb == "watch":
            return vanilla.watch(environ, MODULE, "the external agent command")
        elif verb == "supervise":
            supervise(environ, float(args[1]), int(args[2]))
        elif verb == "collect":
            return collect(environ)
        elif verb == "transcripts":
            print("row transcripts are verified by collect")
        elif verb in vanilla.LANE_VERBS:
            vanilla.lifecycle(environ, verb, prepare)
        else:
            return 64
    except (HookError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        why = str(exc) if isinstance(exc, HookError) else f"{verb} failed ({type(exc).__name__})"
        if verb == "supervise":
            atomic_write_json(vanilla.run_dir(environ) / "done.json", {"error": why})
        print(why, file=sys.stderr if verb in ("watch", "supervise") else sys.stdout)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
