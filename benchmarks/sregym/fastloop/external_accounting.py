"""Tokens, requests and USD of one phase of an external agent cell, from the request logs the command writes.

The accountant is the one the Claude Code arm uses (``libs.agent_cli.claude_usage.ClaudeLedger``, agentshim's
price table); the input is a JSONL log of final model responses in the shape ``ClaudeLedger.observe`` reads:

    {"type": "assistant", "timestamp": "...", "phase": "incident",
     "message": {"id": "msg_...", "model": "...", "usage": {<raw provider usage>}}}

plus ``{"type": "request_error", ...}`` lines for requests that failed. An optional second log, written by a
recorder that sits outside the agent, is cross-checked against the first: the same message ids, the same token
fields. Anything else is a failed cross-check and the phase cannot be graded.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from agentshim import default_pricing

from benchmarks.sregym.fastloop.records import TokenCounts
from libs.agent_cli.claude_usage import ClaudeLedger, api_tokens, json_records, signature

if TYPE_CHECKING:
    from pathlib import Path

ACCOUNTING_SCHEMA = "sdo.external-accounting/v1"
ACCOUNTING_FILE = "accounting.json"
#: The phase whose requests are online (the incident itself); every other phase is offline.
ONLINE_PHASE = "incident"


@dataclass(frozen=True)
class PhaseAccounting:
    phase: str
    online: bool
    tokens: TokenCounts
    requests: int
    usd: float | None
    usd_source: str
    #: ``request_error`` lines in the command's own log and non-2xx records in the independent log; never priced.
    request_errors: int
    independent_errors: int
    #: Requests priced from the independent log alone (a timed-out phase's cut responses).
    partial_requests: int
    partial: bool
    #: ``passed``, ``skipped`` (no independent log declared) or ``failed``.
    crosscheck: str
    crosscheck_problems: tuple[str, ...] = field(default_factory=tuple)

    def to_json(self) -> dict[str, Any]:
        data = asdict(self)
        data["tokens"] = self.tokens.model_dump(mode="json")
        data["crosscheck_problems"] = list(self.crosscheck_problems)
        return data


def _for_phase(records: list[dict[str, Any]], phase: str) -> list[dict[str, Any]]:
    """The records of *phase*: a log shared by phases tags each record, an untagged record belongs to every phase."""
    return [item for item in records if item.get("phase") in (None, phase)]


def _is_response(record: dict[str, Any]) -> bool:
    message = record.get("message")
    return record.get("type") == "assistant" and isinstance(message, dict) and bool(message.get("usage"))


def _fields(record: dict[str, Any]) -> tuple[int, ...]:
    usage = api_tokens(record["message"]["usage"])
    return (*signature(usage), usage.cache_write_1h_input_tokens)


def _identity(record: dict[str, Any]) -> str:
    return str(record["message"].get("id") or record.get("uuid") or "")


def _largest(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """One record per message id; a response streamed twice keeps the one with the most output."""
    kept: dict[str, dict[str, Any]] = {}
    for record in records:
        identity = _identity(record)
        previous = kept.get(identity)
        if previous is None or _fields(record)[3] >= _fields(previous)[3]:
            kept[identity] = record
    return kept


def _status_ok(record: dict[str, Any]) -> bool:
    status = record.get("http_status")
    return status is None or (isinstance(status, int) and 200 <= status < 300)


def _crosscheck(
    own: dict[str, dict[str, Any]], independent: list[dict[str, Any]], *, timed_out: bool
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    """``(problems, extra)``: *extra* are cut responses only the independent log saw, allowed on a timed-out phase."""
    complete: dict[str, dict[str, Any]] = {}
    incomplete: dict[str, dict[str, Any]] = {}
    for record in independent:
        if not _is_response(record) or not _status_ok(record):
            continue
        (complete if record.get("complete", True) else incomplete)[_identity(record)] = record
    problems: list[str] = []
    extra = {key: value for key, value in incomplete.items() if key not in own} if timed_out else {}
    missing = sorted(set(own) - set(complete))
    unseen = sorted(set(complete) - set(own))
    stray = sorted(set(incomplete) - set(extra))
    if missing:
        problems.append(f"{len(missing)} request(s) only in the command's log: {', '.join(missing[:3])}")
    if unseen:
        problems.append(f"{len(unseen)} complete request(s) only in the independent log: {', '.join(unseen[:3])}")
    if stray:
        problems.append(f"{len(stray)} incomplete independent record(s) not allowed here: {', '.join(stray[:3])}")
    problems.extend(
        f"{key}: token fields differ ({_fields(own[key])} vs independent {_fields(complete[key])})"
        for key in sorted(set(own) & set(complete))
        if _fields(own[key]) != _fields(complete[key])
    )
    return problems, extra


def account_phase(
    phase: str, requests: Path, independent: Path | None, *, timed_out: bool, pricing: Any = None
) -> PhaseAccounting:
    """Bill *phase* from the command's request log, cross-checked against the independent log when one is declared."""
    records = _for_phase(json_records(requests), phase)
    own = _largest([item for item in records if _is_response(item)])
    request_errors = sum(item.get("type") == "request_error" for item in records)
    problems: list[str] = []
    extra: dict[str, dict[str, Any]] = {}
    independent_errors = 0
    crosscheck = "skipped"
    if independent is not None:
        records = _for_phase(json_records(independent), phase)
        independent_errors = sum(item.get("type") == "request_error" or not _status_ok(item) for item in records)
        problems, extra = _crosscheck(own, records, timed_out=timed_out)
        crosscheck = "failed" if problems else "passed"
    ledger = ClaudeLedger()
    for record in (*own.values(), *extra.values()):
        ledger.observe(record)
    bill = ledger.since(ClaudeLedger(), pricing or default_pricing())
    usage = bill.tokens
    tokens = TokenCounts(
        input_tokens=usage.input_tokens,
        cached_input_tokens=usage.cache_read_input_tokens,
        cache_write_input_tokens=usage.cache_write_input_tokens,
        cache_write_1h_input_tokens=usage.cache_write_1h_input_tokens,
        output_tokens=usage.output_tokens,
        reasoning_output_tokens=usage.reasoning_output_tokens,
    )
    return PhaseAccounting(
        phase=phase,
        online=phase == ONLINE_PHASE,
        tokens=tokens,
        requests=bill.requests,
        usd=bill.usd,
        usd_source=bill.source,
        request_errors=request_errors,
        independent_errors=independent_errors,
        partial_requests=len(extra),
        partial=bool(extra),
        crosscheck=crosscheck,
        crosscheck_problems=tuple(problems),
    )
