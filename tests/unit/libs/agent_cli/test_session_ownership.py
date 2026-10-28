"""Real structured-turn producers and consumers share persisted writer ownership."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from libs.agent_cli.structured import run_structured_turn
from tests.structured_turns import ScriptedAgent, reply


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_resume_waits_for_writer_release_and_uses_same_session(tmp_path: Path, provider: str) -> None:
    announced, close_writer, resumed = threading.Event(), threading.Event(), threading.Event()

    def checkpoint(_session_id: str) -> None:
        announced.set()
        assert close_writer.wait(3)

    writer = ScriptedAgent(lambda _: reply(provider, {"ok": True}, session_id="session-1"))

    def reflect(_request):
        resumed.set()
        return reply(provider, {"ok": True}, session_id="session-1")

    reader = ScriptedAgent(reflect)
    options = {
        "output_schema": {
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": False,
        },
        "cwd": tmp_path,
        "access": "danger-full-access",
        "session_ownership_directory": tmp_path / "ownership",
        "timeout_seconds": 2,
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            run_structured_turn, provider, "respond", executor=writer.executor, on_session_started=checkpoint, **options
        )
        try:
            assert announced.wait(1)
            second = pool.submit(
                run_structured_turn,
                provider,
                "reflect",
                executor=reader.executor,
                resume_session_id="session-1",
                **options,
            )
            assert not resumed.wait(0.05), "reflection resumed an open writer"
        finally:
            close_writer.set()
        assert first.result().session_id == "session-1"
        assert second.result().session_id == "session-1"
    assert resumed.is_set()
    assert "session-1" in reader.argvs[0]


def test_unreleased_writer_fails_within_bound_without_starting_resume(tmp_path: Path) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffTimeout, SessionOwnership

    # Crash recovery: a new owner reads the active record left by the old
    # process, even though its OS lock is no longer held.
    owner = SessionOwnership(tmp_path / "ownership", "codex", timeout_seconds=0.1)
    owner.claim("session-1", resume=False)
    owner.close(writer_closed=False)
    reader = ScriptedAgent(lambda _: reply("codex", {"ok": True}, session_id="session-1"))
    started = time.monotonic()
    with pytest.raises(SessionHandoffTimeout, match="session-1"):
        run_structured_turn(
            "codex",
            "reflect",
            output_schema={"type": "object", "properties": {}, "required": [], "additionalProperties": False},
            cwd=tmp_path,
            access="danger-full-access",
            executor=reader.executor,
            resume_session_id="session-1",
            session_ownership_directory=tmp_path / "ownership",
            timeout_seconds=0.1,
        )
    assert time.monotonic() - started < 2
    assert not reader.requests


def test_handoff_is_persisted_and_cannot_return_to_responder(tmp_path: Path) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffError, SessionOwnership

    responder = SessionOwnership(tmp_path, "codex", timeout_seconds=1)
    responder.claim("session-1", resume=False)
    responder.close(writer_closed=True)
    reflection = SessionOwnership(tmp_path, "codex", timeout_seconds=1, role="reflection")
    reflection.claim("session-1", resume=True)
    reflection.close(writer_closed=True)
    # New objects model independent workers and a controller restart. Further
    # reflection corrections can resume; a stale responder cannot reclaim it.
    correction = SessionOwnership(tmp_path, "codex", timeout_seconds=1, role="reflection")
    correction.claim("session-1", resume=True)
    correction.close(writer_closed=True)
    with pytest.raises(SessionHandoffError, match="transferred to reflection"):
        SessionOwnership(tmp_path, "codex", timeout_seconds=1).claim("session-1", resume=True)


@pytest.mark.parametrize("contents", [None, "{}", "[]", "null", '{"version":2}'])
def test_missing_or_invalid_release_evidence_never_authorizes_resume(tmp_path: Path, contents: str | None) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffError, SessionOwnership

    writer = SessionOwnership(tmp_path, "codex", timeout_seconds=1)
    writer.claim("session-1", resume=False)
    writer.close(writer_closed=True)
    (record,) = tmp_path.glob("*.json")
    if contents is None:
        record.unlink()
    else:
        record.write_text(contents)
    with pytest.raises(SessionHandoffError, match="no valid persisted writer ownership"):
        SessionOwnership(tmp_path, "codex", timeout_seconds=1, role="reflection").claim("session-1", resume=True)


def test_terminal_output_does_not_release_a_real_process_writer(tmp_path: Path) -> None:
    """The provider can emit its final response while its writer is still open."""
    announced, resumed = threading.Event(), threading.Event()
    release = tmp_path / "release"
    binary = tmp_path / "codex"
    output = "".join(reply("codex", {"ok": True}, session_id="session-1").stdout)
    binary.write_text(
        f"#!{sys.executable}\n"
        "import pathlib, sys, time\n"
        "if '--help' in sys.argv: sys.exit(0)\n"
        f"root = pathlib.Path({str(tmp_path)!r})\n"
        "if 'resume' in sys.argv:\n"
        "    if (root / 'writer-open').exists(): sys.exit(91)\n"
        "else:\n"
        "    (root / 'writer-open').touch()\n"
        f"print({output!r}, end='', flush=True)\n"
        "if 'resume' not in sys.argv:\n"
        "    deadline = time.monotonic() + 10\n"
        "    while not (root / 'release').exists():\n"
        "        if time.monotonic() >= deadline: sys.exit(92)\n"
        "        time.sleep(0.01)\n"
        "    (root / 'writer-open').unlink()\n"
    )
    binary.chmod(0o700)
    options = {
        "output_schema": {
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": False,
        },
        "cwd": tmp_path,
        "access": "danger-full-access",
        "environment": {"PATH": str(tmp_path) + os.pathsep + os.environ["PATH"]},
        "session_ownership_directory": tmp_path / "ownership",
        "timeout_seconds": 5,
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(
            run_structured_turn, "codex", "respond", on_session_started=lambda _: announced.set(), **options
        )
        try:
            assert announced.wait(3)
            reader = pool.submit(
                run_structured_turn,
                "codex",
                "reflect",
                resume_session_id="session-1",
                session_ownership_role="reflection",
                on_session_started=lambda _: resumed.set(),
                **options,
            )
            assert not resumed.wait(0.05)
            assert not writer.done(), "test did not hold the actual writer open"
        finally:
            release.touch()
        assert writer.result().session_id == reader.result().session_id == "session-1"
    assert resumed.is_set()


@pytest.mark.parametrize(
    ("field", "value"),
    [("version", 2), ("provider", "claude"), ("session_id", "other"), ("state", "closed"), ("role", "other")],
)
def test_release_must_match_the_exact_versioned_session_identity(tmp_path: Path, field: str, value: object) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffError, SessionOwnership

    producer = SessionOwnership(tmp_path, "codex", timeout_seconds=1)
    producer.claim("session-1", resume=False)
    producer.close(writer_closed=True)
    (record,) = tmp_path.glob("*.json")
    document = json.loads(record.read_text())
    document[field] = value
    record.write_text(json.dumps(document))
    with pytest.raises(SessionHandoffError, match="identity or state"):
        SessionOwnership(tmp_path, "codex", timeout_seconds=1, role="reflection").claim("session-1", resume=True)


def test_process_death_does_not_manufacture_writer_release(tmp_path: Path) -> None:
    from libs.agent_cli.session_ownership import SessionHandoffTimeout, SessionOwnership

    # An independent process dies without its release callback. flock is dropped
    # by the OS; the next worker must still see the durable active record.
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import os, sys; from pathlib import Path; "
            "from libs.agent_cli.session_ownership import SessionOwnership; "
            "owner = SessionOwnership(Path(sys.argv[1]), 'codex', timeout_seconds=1); "
            "owner.claim('session-1', resume=False); os._exit(0)",
            str(tmp_path),
        ],
        check=True,
        timeout=5,
    )
    with pytest.raises(SessionHandoffTimeout):
        SessionOwnership(tmp_path, "codex", timeout_seconds=0.1, role="reflection").claim("session-1", resume=True)
