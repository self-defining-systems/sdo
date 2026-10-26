"""Eval-only grading at durable native closure, never a production input."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

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
