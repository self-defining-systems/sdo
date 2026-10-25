"""Resume a menu participant's original provider session from its persisted transcript.

The controller owns epochs, budget and report settlement. This worker keeps only
provider checkpoint identity in the existing incident PVC and refuses recovery
when the transcript prefix is unavailable. It never starts a replacement session
as a fallback for an accepted session.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt
from sdo.contracts import MitigationSession, MitigationSessionCheckpoint, to_canonical_json

if TYPE_CHECKING:
    from collections.abc import Callable

_TOKEN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")


class SessionRecoveryError(RuntimeError):
    """An interrupted session cannot safely be resumed."""


@dataclass(frozen=True)
class _Checkpoint:
    incident_id: str
    logical_session_id: str
    provider: str
    provider_session_id: str
    transcript: str
    prefix_bytes: int
    digest: str


@dataclass
class SessionRecovery:
    session: MitigationSession
    provider: str
    scratch: Path
    persist: Callable[[MitigationSessionCheckpoint], None]

    @property
    def checkpoint_path(self) -> Path:
        return self.scratch / "provider-checkpoint.json"

    def _home(self) -> Path:
        variable = "CODEX_HOME" if self.provider == "codex" else "CLAUDE_CONFIG_DIR"
        value = os.environ.get(variable)
        if self.provider not in {"codex", "claude"} or not value:
            raise SessionRecoveryError("interrupted: provider checkpoint home unavailable")
        return Path(value).resolve()

    def _transcript(self, session_id: str, *, wait_seconds: float = 0) -> Path:
        if not _TOKEN.fullmatch(session_id):
            raise SessionRecoveryError("interrupted: invalid provider session identity")
        home = self._home()
        pattern = (
            f"sessions/**/rollout-*{session_id}.jsonl"
            if self.provider == "codex"
            else f"projects/**/{session_id}.jsonl"
        )
        deadline = time.monotonic() + wait_seconds
        matches = list(home.glob(pattern))
        # Codex can emit thread.started before its asynchronous rollout writer
        # creates the file. Only a fresh checkpoint waits; recovery still fails
        # immediately if an already accepted transcript has disappeared.
        while time.monotonic() < deadline and (
            not matches
            or (
                len(matches) == 1
                and matches[0].resolve().is_relative_to(home)
                and matches[0].is_file()
                and matches[0].stat().st_size == 0
            )
        ):
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            matches = list(home.glob(pattern))
        if len(matches) != 1 or not matches[0].resolve().is_relative_to(home):
            raise SessionRecoveryError("interrupted: persisted provider transcript unavailable")
        return matches[0]

    def _read(self) -> _Checkpoint:
        try:
            checkpoint = _Checkpoint(**json.loads(self.checkpoint_path.read_text(encoding="utf-8")))
            if (
                checkpoint.incident_id != self.session.incident_id
                or checkpoint.logical_session_id != self.session.session_id
                or checkpoint.provider != self.provider
                or not isinstance(checkpoint.prefix_bytes, int)
                or checkpoint.prefix_bytes <= 0
            ):
                raise ValueError("identity mismatch")
            transcript = self._transcript(checkpoint.provider_session_id)
            if str(transcript.relative_to(self._home())) != checkpoint.transcript:
                raise ValueError("transcript identity changed")
            with transcript.open("rb") as stream:
                prefix = stream.read(checkpoint.prefix_bytes)
            if len(prefix) != checkpoint.prefix_bytes or hashlib.sha256(prefix).hexdigest() != checkpoint.digest:
                raise ValueError("checkpoint prefix changed")
            return checkpoint
        except (OSError, TypeError, ValueError) as exc:
            raise SessionRecoveryError("interrupted: provider checkpoint missing or changed") from exc

    def _wire(self, checkpoint: _Checkpoint, *, acknowledge: bool = False) -> MitigationSessionCheckpoint:
        message = MitigationSessionCheckpoint(
            version=1,
            incident_id=self.session.incident_id,
            session_id=self.session.session_id,
            dispatch_epoch=self.session.dispatch_epoch,
            provider=self.provider,
            provider_session_id=checkpoint.provider_session_id,
            checkpoint_digest=checkpoint.digest,
        )
        if acknowledge:
            message.acknowledged_attempt_ids.extend(report.attempt_id for report in self.session.pending_reports)
        return message

    def resume_session_id(self) -> str | None:
        if not self.session.provider_session_id and not self.checkpoint_path.exists():
            if self.session.pending_reports or self.session.writes_frozen:
                raise SessionRecoveryError("interrupted: missing checkpoint for recovered responder")
            return None
        checkpoint = self._read()
        if self.session.provider_session_id:
            if (
                self.session.provider != self.provider
                or self.session.provider_session_id != checkpoint.provider_session_id
                or self.session.checkpoint_digest != checkpoint.digest
            ):
                raise SessionRecoveryError("interrupted: controller/provider checkpoint identity mismatch")
        else:
            # Crash after local checkpoint persistence, before controller reply.
            # Resume that conversation; never allocate a fresh provider session.
            self.persist(self._wire(checkpoint))
        return checkpoint.provider_session_id

    def checkpoint(self, session_id: str) -> None:
        try:
            if self.checkpoint_path.exists():
                checkpoint = self._read()
                if checkpoint.provider_session_id != session_id:
                    raise SessionRecoveryError("interrupted: provider session changed")
            else:
                transcript = self._transcript(session_id, wait_seconds=min(5.0, self.remaining_seconds()))
                with transcript.open("rb") as stream:
                    prefix = stream.read()
                    os.fsync(stream.fileno())
                if not prefix:
                    raise SessionRecoveryError("interrupted: empty provider transcript")
                checkpoint = _Checkpoint(
                    self.session.incident_id,
                    self.session.session_id,
                    self.provider,
                    session_id,
                    str(transcript.relative_to(self._home())),
                    len(prefix),
                    hashlib.sha256(prefix).hexdigest(),
                )
                self.scratch.mkdir(parents=True, exist_ok=True)
                fd, temporary = tempfile.mkstemp(prefix=".checkpoint-", dir=self.scratch)
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as stream:
                        json.dump(asdict(checkpoint), stream, separators=(",", ":"))
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, self.checkpoint_path)
                    directory_fd = os.open(self.scratch, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                finally:
                    Path(temporary).unlink(missing_ok=True)
            self.persist(self._wire(checkpoint))
        except OSError as exc:
            raise SessionRecoveryError("interrupted: provider checkpoint persistence failed") from exc

    def remaining_seconds(self) -> float:
        remaining = (self.session.original_deadline.ToDatetime(tzinfo=UTC) - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise SessionRecoveryError("interrupted: original deadline expired")
        return remaining

    def recovered_prompt(self) -> RenderedPrompt:
        if not self.session.pending_reports:
            return named_prompt("recovery.reports", "")
        reports = ",".join(
            json.dumps(json.loads(to_canonical_json(report)), separators=(",", ":"))
            for report in self.session.pending_reports
        )
        return render_prompt(
            "responder",
            "session_recovery",
            part="recovery.instructions",
            reports=named_prompt("recovery.reports", reports),
        )

    def acknowledge_reports(self) -> None:
        if not self.session.pending_reports:
            return
        checkpoint = self._read()
        if checkpoint.provider_session_id != self.session.provider_session_id:
            raise SessionRecoveryError("interrupted: report acknowledgment has no matching provider session")
        self.persist(self._wire(checkpoint, acknowledge=True))


def open_recovery(incident_id: str, provider: str) -> SessionRecovery | None:
    """Join the controller session only when the dispatcher enables the protocol."""
    if os.environ.get("SDO_RESPONDER_SESSION") != "1" and os.environ.get("SDO_MITIGATION_SESSION") != "1":
        return None
    from sdo.agents.responder import recorded_actions
    from sdo.agents.responder.tools_cli import _exchange
    from sdo.contracts import parse_json, validate

    directory = Path(os.environ.get("SDO_TOOLS_DIR", "/sdo/session/tools"))
    try:
        response = _exchange({"command": "session-open"}, directory)
        if response.get("error") or not isinstance(response.get("session"), dict):
            raise ValueError("controller session unavailable")
        session = parse_json(json.dumps(response["session"]), MitigationSession())
        validate(session)
        if session.incident_id != incident_id or session.writes_frozen:
            raise ValueError("controller session frozen or mismatched")
    except (OSError, ValueError, TimeoutError) as exc:
        raise SessionRecoveryError("interrupted: durable responder session unavailable or writes frozen") from exc

    def persist(checkpoint: MitigationSessionCheckpoint) -> None:
        response = _exchange(
            {"command": "session-checkpoint", "checkpoint": json.loads(to_canonical_json(checkpoint))}, directory
        )
        if response.get("error"):
            raise SessionRecoveryError("interrupted: controller refused provider checkpoint")

    return SessionRecovery(session, provider, recorded_actions.scratch_directory(), persist)
