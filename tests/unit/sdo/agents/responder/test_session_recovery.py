from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from sdo.agents.responder.session_recovery import SessionRecovery, SessionRecoveryError
from sdo.contracts import MitigationReport, MitigationSession


def controller_session() -> MitigationSession:
    session = MitigationSession(version=1, incident_id="incident", session_id="logical-session", dispatch_epoch=1)
    session.original_deadline.FromDatetime(datetime(2030, 1, 1, tzinfo=UTC))
    return session


def transcript(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str) -> Path:
    home = tmp_path / "provider"
    monkeypatch.setenv("CODEX_HOME" if provider == "codex" else "CLAUDE_CONFIG_DIR", str(home))
    path = home / (
        "sessions/2030/01/01/rollout-date-provider-session.jsonl"
        if provider == "codex"
        else "projects/app/provider-session.jsonl"
    )
    path.parent.mkdir(parents=True)
    path.write_text('{"synthetic":"persisted transcript"}\n')
    return path


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_checkpoint_is_durable_before_controller_accepts_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, provider: str
) -> None:
    path = transcript(tmp_path, monkeypatch, provider)
    seen = []

    def persist(checkpoint: object) -> None:
        assert (tmp_path / "scratch/provider-checkpoint.json").is_file()
        seen.append(checkpoint)

    recovery = SessionRecovery(controller_session(), provider, tmp_path / "scratch", persist)
    recovery.checkpoint("provider-session")
    assert seen[0].provider_session_id == "provider-session"  # type: ignore[attr-defined]
    assert seen[0].checkpoint_digest == hashlib.sha256(path.read_bytes()).hexdigest()  # type: ignore[attr-defined]
    recovery.session.provider = provider
    recovery.session.provider_session_id = "provider-session"
    recovery.session.checkpoint_digest = seen[0].checkpoint_digest  # type: ignore[attr-defined]
    path.write_text(path.read_text() + '{"synthetic":"later event"}\n')
    replacement = SessionRecovery(recovery.session, provider, tmp_path / "scratch", persist)
    assert replacement.resume_session_id() == "provider-session"


@pytest.mark.parametrize("failure", ["missing", "changed", "wrong-provider", "wrong-incident"])
def test_missing_or_unresumable_checkpoint_never_starts_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    path = transcript(tmp_path, monkeypatch, "codex")
    session = controller_session()
    recovery = SessionRecovery(session, "codex", tmp_path / "scratch", lambda _: None)
    recovery.checkpoint("provider-session")
    session.provider = "codex"
    session.provider_session_id = "provider-session"
    session.checkpoint_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if failure == "missing":
        path.unlink()
    elif failure == "changed":
        path.write_text('{"synthetic":"different transcript"}\n')
    elif failure == "wrong-provider":
        session.provider = "claude"
    else:
        session.incident_id = "different"
    with pytest.raises(SessionRecoveryError, match="interrupted"):
        SessionRecovery(session, "codex", tmp_path / "scratch", lambda _: None).resume_session_id()


def test_new_controller_cannot_hide_lost_checkpoint_acknowledgment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    transcript(tmp_path, monkeypatch, "codex")
    session = controller_session()
    recovery = SessionRecovery(session, "codex", tmp_path / "scratch", lambda _: None)
    recovery.checkpoint("provider-session")
    # Controller never received the checkpoint, but the original provider did start.
    recovered = []
    replacement = SessionRecovery(session, "codex", tmp_path / "scratch", recovered.append)
    assert replacement.resume_session_id() == "provider-session"
    assert len(recovered) == 1


@pytest.mark.parametrize("initially_empty", [False, True])
def test_first_checkpoint_waits_for_provider_transcript_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, initially_empty: bool
) -> None:
    home = tmp_path / "provider"
    monkeypatch.setenv("CODEX_HOME", str(home))
    path = home / "sessions/2030/01/01/rollout-date-provider-session.jsonl"
    if initially_empty:
        path.parent.mkdir(parents=True)
        path.touch()

    def flush_transcript(_seconds: float) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"synthetic":"provider asynchronously flushed its session"}\n')

    monkeypatch.setattr(time, "sleep", flush_transcript)
    seen = []
    recovery = SessionRecovery(controller_session(), "codex", tmp_path / "scratch", seen.append)
    recovery.checkpoint("provider-session")
    assert seen[0].checkpoint_digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert recovery.resume_session_id() == "provider-session"


def test_pending_reports_enter_same_session_prompt_and_ack_only_after_delivery(tmp_path: Path) -> None:
    session = controller_session()
    session.pending_reports.append(
        MitigationReport(
            version=1,
            incident_id="incident",
            session_id="logical-session",
            request_id="request",
            attempt_id="attempt",
            acceptance_epoch=1,
            status=MitigationReport.STATUS_INTERRUPTED,
        )
    )
    seen = []
    recovery = SessionRecovery(session, "codex", tmp_path, seen.append)
    prompt = recovery.recovered_prompt()
    assert '"attempt_id":"attempt"' in prompt
    assert '"status":"STATUS_INTERRUPTED"' in prompt
    assert not seen
    # A missing provider identity cannot acknowledge reports in a fresh session.
    with pytest.raises(SessionRecoveryError):
        recovery.acknowledge_reports()
    assert not seen


def test_original_deadline_bounds_every_resumed_turn(tmp_path: Path) -> None:
    session = controller_session()
    session.original_deadline.FromDatetime(datetime.now(UTC))
    recovery = SessionRecovery(session, "codex", tmp_path, lambda _: None)
    with pytest.raises(SessionRecoveryError, match="original deadline"):
        recovery.remaining_seconds()


def test_expired_session_does_not_launch_a_provider(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.responder import codex
    from tests.unit.sdo.agents.responder.test_codex_responder import _request

    session = controller_session()
    session.original_deadline.FromDatetime(datetime(1970, 1, 1, tzinfo=UTC))
    recovery = SessionRecovery(session, "codex", tmp_path, lambda _: None)
    monkeypatch.setattr(codex, "open_recovery", lambda *_: recovery)
    monkeypatch.setattr(
        codex, "run_structured_turn", lambda *_args, **_kwargs: pytest.fail("expired provider launched")
    )
    with pytest.raises(codex.ResponderExecutionError, match="original deadline"):
        codex.execute_incident(_request(), provider="codex")
