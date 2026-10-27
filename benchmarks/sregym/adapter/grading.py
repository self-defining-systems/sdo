"""Eval-only grading at durable native closure, never a production input."""

from __future__ import annotations

import fcntl
import json
import logging
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter.budgets import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

logger = logging.getLogger(__name__)
GRADE_FILENAME = "sdo_incident_grade.json"


class ClosureGrader:
    """Freeze one submission attempt independently of terminal receipt collection.

    Persist the claim before network I/O: an ambiguous POST failure or adapter
    restart must never submit the same incident twice. Such failures require
    inspecting conductor evidence, rather than automatically replaying a POST.
    """

    def __init__(
        self,
        directory: Path,
        api_base: str,
        *,
        submit: Callable[[dict[str, Any], str], dict[str, Any]],
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.path = directory / GRADE_FILENAME
        self.api_base = api_base
        self.submit = submit
        self.now = now

    def observe(self, ledger: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_suffix(".lock").open("a", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self._observe(ledger)

    def _observe(self, ledger: dict[str, Any]) -> None:
        closure = ledger.get("closure")
        if not isinstance(closure, dict) or not isinstance(closure.get("result"), dict):
            return
        incident_id = ledger.get("incident_id")
        request = closure.get("request")
        if not incident_id or not isinstance(request, dict) or request.get("incident_id") != incident_id:
            raise ValueError("grading requires an incident-correlated native closure")
        record: dict[str, Any] = {
            "schema_version": "sdo.sregym-incident-grade/v1",
            "incident_id": incident_id,
            "closure": closure,
            "status": "submitting",
            "submission_started_at": self.now().isoformat(),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.path.open("x", encoding="utf-8") as stream:
                json.dump(record, stream, indent=2, sort_keys=True)
        except FileExistsError:
            previous = json.loads(self.path.read_text(encoding="utf-8"))
            if previous.get("incident_id") != incident_id:
                raise ValueError("grading directory already belongs to another incident") from None
            return
        try:
            record["grade"] = self.submit(
                {"schema_version": "sdo.production-receipt/v2", "native_ledger": ledger}, self.api_base
            )
            finished = self.now()
            record.update(status="graded", graded_at=finished.isoformat())
            verified = _timestamp(closure.get("verified_at"))
            if verified is not None:
                record["closure_to_grade_seconds"] = (finished - verified).total_seconds()
        except Exception as exc:
            record.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            logger.exception("benchmark submission failed for closed incident %s", incident_id)
        dispatched = _timestamp(closure.get("dispatched_at"))
        completed = _timestamp(closure.get("responder_completed_at"))
        if dispatched is not None and completed is not None:
            record["responder_runtime_seconds"] = (completed - dispatched).total_seconds()
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self.path)


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value or value.startswith("0001-"):
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ClosureGradingMonitor:
    """Poll native closure while the caller independently collects terminal evidence."""

    def __init__(
        self,
        grader: ClosureGrader,
        *,
        read: Callable[[], dict[str, Any] | None],
        poll_seconds: float = wait_for(WaitKind.POLL_FAST),
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("grading poll interval must be positive")
        self.grader = grader
        self.read = read
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="sdo-closure-grading", daemon=True)
        self.error: Exception | None = None

    def __enter__(self) -> ClosureGradingMonitor:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        self._thread.join()
        if self.error is not None:
            raise RuntimeError("benchmark closure grading monitor failed") from self.error

    def _run(self) -> None:
        while True:
            stopping = self._stop.is_set()
            try:
                evidence = self.read()
            except (OSError, RuntimeError) as exc:
                # Controller/ledger may not exist until lifecycle installation finishes.
                logger.debug("native closure not yet available: %s", exc)
                if stopping:
                    self.error = exc
                    return
            except Exception as exc:
                self.error = exc
                return
            else:
                try:
                    if evidence is not None:
                        self.grader.observe(evidence)
                except Exception as exc:
                    self.error = exc
                    return
                if self.grader.path.exists():
                    return
            if stopping:
                return
            self._stop.wait(self.poll_seconds)
