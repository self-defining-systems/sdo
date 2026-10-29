"""Interpret values-free provider wait records at the benchmark boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pathlib import Path

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator

from libs.agent_cli.errors import ProviderRateLimited


class ProviderWaitInterval(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["rate_limit", "concurrency"]
    started_at: AwareDatetime
    finished_at: AwareDatetime

    @model_validator(mode="after")
    def ordered(self) -> ProviderWaitInterval:
        if self.finished_at < self.started_at:
            raise ValueError("provider wait cannot finish before it starts")
        return self


class AccountEvent(BaseModel):
    """Values-free account selection/failover evidence attached to each row."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    schema_version: Literal["sdo.provider-wait/v1"]
    kind: Literal["account"]
    state: Literal["selected", "failover", "exhausted"]
    turn_id: str
    incident_id: str = ""
    phase: str = "responder"
    recorded_at: AwareDatetime
    account_alias: str = Field(default="", pattern=r"^(a[0-9]{2,6})?$")
    reason: Literal[
        "primary",
        "fallback",
        "auth_failed",
        "credential_unavailable",
        "registration_disabled",
        "quota_exhausted",
        "rate_limited",
        "account_exhausted",
    ]


@dataclass(frozen=True)
class ProviderWaitEvidence:
    intervals: tuple[ProviderWaitInterval, ...] = ()
    exhausted: bool = False
    active: bool = False
    account_events: tuple[AccountEvent, ...] = ()
    exhausted_reason: str = "rate_limited"


class RateLimitedRun(ProviderRateLimited):
    """An ungradable benchmark attempt, retaining exported wait evidence."""

    def __init__(self, evidence: ProviderWaitEvidence) -> None:
        self.evidence = evidence
        self.failure_reason = evidence.exhausted_reason
        super().__init__("provider capacity prevented grading", retryable=False)


def capacity_prevented_completion(evidence: ProviderWaitEvidence, error: str) -> bool:
    """A hard outer deadline can interrupt a retry before its own cap expires."""
    deadline = any(
        marker in error.lower()
        for marker in (
            "timeout",
            "timed out",
            "deadline",
            "verified no new incident within",
            "context canceled",
        )
    )
    return evidence.exhausted or evidence.active or (bool(evidence.intervals) and deadline)


def wait_seconds_between(
    intervals: list[ProviderWaitInterval] | tuple[ProviderWaitInterval, ...],
    start: datetime | None,
    end: datetime | None,
    *,
    kind: str | None = None,
) -> float:
    """Union clipped intervals: parallel workers and duplicate exports cannot double count."""
    if start is None or end is None or end <= start:
        return 0.0
    clipped = sorted(
        (max(start, item.started_at), min(end, item.finished_at))
        for item in intervals
        if kind is None or item.kind == kind
    )
    total = 0.0
    cursor = start
    for left, right in clipped:
        left = max(left, cursor)
        if right > left:
            total += (right - left).total_seconds()
            cursor = right
    return total


def read_provider_waits(
    directory: Path,
    *,
    incident_id: str | None = None,
    observed_at: datetime | None = None,
    after: datetime | None = None,
) -> ProviderWaitEvidence:
    """Read only the dedicated exported telemetry, never model transcripts or credentials.

    Started-without-finished waits retain a killed worker's infrastructure cause.
    A captured cumulative export is filtered by incident and responder phase.
    """
    events: dict[tuple[str, str], _WaitEvent] = {}
    account_events: dict[str, AccountEvent] = {}
    exhausted = False
    exhausted_reason = "rate_limited"
    for path in sorted(directory.rglob("provider-waits.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                account = AccountEvent.model_validate_json(line)
            except ValidationError:
                pass
            else:
                if (incident_id is None or account.incident_id == incident_id) and (
                    after is None or account.recorded_at >= after
                ):
                    account_events[account.model_dump_json()] = account
                    if account.state == "exhausted":
                        exhausted = True
                        exhausted_reason = "account_exhausted"
                continue
            try:
                event = _WaitEvent.model_validate_json(line)
            except ValidationError:
                # A killed writer may leave a partial final line; complete
                # earlier starts still retain its infrastructure cause.
                continue
            if incident_id is not None and event.incident_id != incident_id:
                continue
            if event.phase != "responder":
                continue
            if after is not None and (event.finished_at or event.recorded_at) < after:
                continue
            if event.state == "exhausted":
                exhausted = True
            if event.state not in ("started", "finished") or not event.wait_id:
                continue
            key = (event.turn_id, event.wait_id)
            previous = events.get(key)
            if previous is None or event.state == "finished":
                events[key] = event
    active = any(item.state == "started" for item in events.values())
    observed = observed_at or datetime.now(timezone.utc)
    intervals = tuple(
        ProviderWaitInterval(kind=item.kind, started_at=item.started_at, finished_at=item.finished_at or observed)
        for item in events.values()
        if item.started_at is not None
    )
    return ProviderWaitEvidence(
        intervals,
        exhausted=exhausted,
        active=active,
        account_events=tuple(account_events.values()),
        exhausted_reason=exhausted_reason,
    )


class _WaitEvent(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    schema_version: Literal["sdo.provider-wait/v1"]
    turn_id: str
    incident_id: str = ""
    phase: str = "responder"
    kind: Literal["rate_limit", "concurrency"]
    state: Literal["started", "finished", "exhausted"]
    recorded_at: AwareDatetime
    wait_id: str = ""
    started_at: AwareDatetime | None = None
    finished_at: AwareDatetime | None = None
