#!/usr/bin/env python3
"""Run standalone onboarding against a bootstrapped SDO checkout.

Keep the conversation open until its committed export passes the broker's
initial health-judge memory check. The agent's final reply is a report, never
a gate: a committed export that validates is kept whatever the reply says. A
successful exit identifies the commit to export.

`--session-out FILE` records the session id with the validated commit. After
install the controller soaks the probes; when `sdo-operator onboarding soak`
reports red it prints the one follow-up turn, and `--resume ID` sends exactly
that message to the same session, validating the export again. Give the
follow-up its own transcript so its time and tokens stay apart. This launcher
uses only the standard library so it can run in a kit.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import IO

GOAL = ".sdo/goal.md"


class ExportRejected(Exception):
    """A problem the agent can fix in its checkout."""


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


def memory_environment(base: str) -> dict[str, str]:
    return {**os.environ, "SDO_ACTOR": "health_judge", "SDO_PHASE": "initial", "SDO_BASE_COMMIT": base}


def committed_head(repo: Path) -> str:
    status = git(repo, "status", "--porcelain", "--untracked-files=all")
    if status:
        raise ExportRejected(f"Commit your onboarding edits and remove uncommitted scratch files.\n{status}")
    return git(repo, "rev-parse", "HEAD")


def check_export(repo: Path, base: str, operator: str) -> str:
    commit = committed_head(repo)
    ancestry = subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", base, commit], check=False)
    if ancestry.returncode == 1:
        raise ExportRejected(f"The export must descend from the original onboarding commit {base}.")
    ancestry.check_returncode()
    if git(repo, "diff", "--name-only", base, commit, "--", GOAL):
        raise ExportRejected(f"Restore {GOAL} exactly as supplied in {base} and commit the restoration.")

    # memory check prepares generic detectors and normalizes authored files.
    # Give it the precise export tree in a disposable clone so those writes
    # cannot alter the agent's checkout or become part of the export.
    with TemporaryDirectory(prefix="sdo-onboarding-export-") as temp:
        git(repo, "clone", "--quiet", "--shared", "--no-checkout", ".", temp)
        snapshot = Path(temp)
        git(snapshot, "checkout", "--quiet", "--detach", commit)
        checked = subprocess.run(
            [operator, "memory", "check", "--root", temp],
            cwd=snapshot,
            env=memory_environment(base),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        if checked.returncode == 1:
            raise ExportRejected(checked.stdout.strip() or "sdo-operator memory check rejected the export.")
        if checked.returncode:
            raise RuntimeError(f"sdo-operator memory check exited {checked.returncode}:\n{checked.stdout.strip()}")

    if committed_head(repo) != commit:
        raise ExportRejected("The export commit changed during validation. Finish editing before completing.")
    return commit


def send_message(stream: IO[str], message: str) -> None:
    frame = {"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": message}]}}
    stream.write(json.dumps(frame) + "\n")
    stream.flush()


def finish_turn(
    stdin: IO[str],
    stdout: IO[str],
    transcript: IO[str],
    repo: Path,
    base: str,
    operator: str,
) -> tuple[str, str | None]:
    """Wait for a terminal result and drain registered background work, then validate.

    The reply is printed on stderr and never decides the outcome; only
    `check_export` does. An unsuccessful result event still raises.
    """
    state = None
    session_id: str | None = None
    pending: set[str] = set()
    reported_tasks = False
    result: dict[str, Any] | None = None
    for line in stdout:
        transcript.write(line)
        transcript.flush()
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict):
            continue
        kind, subtype = event.get("type"), event.get("subtype")
        if isinstance(event.get("session_id"), str) and event["session_id"]:
            session_id = event["session_id"]
        if kind == "result":
            if subtype != "success" or event.get("is_error") is not False:
                raise RuntimeError("Claude returned an unsuccessful result. See the transcript.")
            result = event
        elif kind == "assistant":
            result = None
        elif kind == "system":
            if subtype == "session_state_changed":
                state = event.get("state")
            elif subtype == "background_tasks_changed":
                reported_tasks = bool(event.get("tasks"))
            elif subtype == "task_started":
                pending.add(event["task_id"])
                result = None
            elif subtype == "task_notification":
                pending.discard(event["task_id"])
                # The notification can wake another turn even after a result.
                result = None
        if result is None or state not in (None, "idle") or pending or reported_tasks:
            continue

        answer = result.get("result")
        if isinstance(answer, str) and answer.strip():
            print(f"Onboarding reply: {answer.strip().splitlines()[0]}", file=sys.stderr)
        try:
            return check_export(repo, base, operator), session_id
        except ExportRejected as error:
            print(f"Onboarding export rejected:\n{error}", file=sys.stderr)
            send_message(
                stdin,
                "The onboarding export was rejected. Fix the issues below, commit your changes, and finish again."
                f"\n\n{error}",
            )
            result = None
    raise RuntimeError("Claude exited before an onboarding export passed validation. See the transcript.")


def run_session(
    repo: Path,
    base: str,
    operator: str,
    prompt: str,
    transcript: IO[str],
    cli_args: list[str],
    resume: str | None = None,
) -> tuple[str, str | None]:
    command = [
        "claude",
        "-p",
        "--dangerously-skip-permissions",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        *(["--resume", resume] if resume else []),
        *cli_args,
    ]
    with subprocess.Popen(
        command,
        cwd=repo,
        env=memory_environment(base),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        assert process.stdin is not None
        assert process.stdout is not None
        try:
            send_message(process.stdin, prompt)
            commit, session_id = finish_turn(process.stdin, process.stdout, transcript, repo, base, operator)
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.returncode:
            raise RuntimeError(f"Claude exited {process.returncode} after validation. See the transcript.")
    if committed_head(repo) != commit:
        raise RuntimeError("The checkout changed after validation.")
    return commit, session_id or resume


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base", required=True, help="frozen pre-agent commit containing the supplied goal and Go bootstrap"
    )
    parser.add_argument(
        "--operator", default="sdo-operator", help="pinned operator binary on PATH or at an absolute path"
    )
    parser.add_argument("--session-out", type=Path, help="write the session id and validated commit here as JSON")
    parser.add_argument("--resume", help="session id to resume with the prompt as its one follow-up message")
    parser.add_argument("prompt", type=Path)
    parser.add_argument("transcript", type=Path)
    parser.add_argument("repo", type=Path)
    parser.add_argument("claude_args", nargs=argparse.REMAINDER, help="Claude CLI settings after --")
    args = parser.parse_args()
    repo = args.repo.resolve()
    try:
        if Path(git(repo, "rev-parse", "--show-toplevel")).resolve() != repo:
            raise RuntimeError("Pass the repository's Git root.")
        base = git(repo, "rev-parse", "--verify", "--end-of-options", f"{args.base}^{{commit}}")
        if git(repo, "cat-file", "-t", f"{base}:{GOAL}") != "blob":
            raise RuntimeError(f"The frozen base must contain {GOAL}.")
        prompt = args.prompt.read_text()
        with args.transcript.open("w") as transcript:
            cli_args = args.claude_args
            if cli_args[:1] == ["--"]:
                cli_args = cli_args[1:]
            commit, session_id = run_session(repo, base, args.operator, prompt, transcript, cli_args, args.resume)
        if args.session_out is not None:
            args.session_out.write_text(json.dumps({"session_id": session_id, "commit": commit}) + "\n")
    except (ExportRejected, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1
    print(f"Onboarding export validated: {commit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
