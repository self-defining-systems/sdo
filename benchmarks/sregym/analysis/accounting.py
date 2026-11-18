"""One accountant for both arms of an evaluation: SDO and vanilla Claude Code are measured the same way.

The same quantities come out for either arm from the same sources and one price table:

* **tokens** split into uncached input, cache read, cache write and output, counted once per billable request
  (unique assistant message IDs in the main session and every sub-agent sidechain, :class:`ClaudeLedger`); never from a
  stream's ``result.usage`` (last turn only, no sub-agents) or ``num_turns`` (conversation turns);
* **requests**, the same unique IDs;
* **USD** priced per request with the agentshim price table (:func:`spend_of`); a request whose model has no price
  makes the total unknown, never zero. The CLI's own ``total_cost_usd`` is kept only as a cross-check column
  (:func:`cross_check`), because it includes requests the transcript lacks (title generation) and is a different basis;
* **online / offline** for SDO: online is the responder's work up to the closing turn plus its investigators; offline
  is the closing and cleanup turns. They share one session file, so the cut is the ``reflection_dispatch`` time of the
  incident's ledger. Lifecycle agents (designer, deployer, judge, onboarding) are one-time per app and kept apart.
  The vanilla arm has no offline part.

``claude --resume`` appends to the same session JSONL under the same session ID (settled on CLI 2.1.325), so a resumed
turn needs no special handling. Spend is a pure function of the files, so it can be recomputed for any cell.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from agentshim import PricingTable, TokenUsage, cost_usd, default_pricing, price_for

from benchmarks.fuzzops import sink
from libs.agent_cli.claude_usage import ClaudeLedger, epoch, json_records

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "AccountingError",
    "CellAccounting",
    "Spend",
    "TranscriptFiles",
    "account_sdo_cell",
    "account_vanilla_cell",
    "cli_totals",
    "cross_check",
    "epoch",
    "ledger_of",
    "normalize_model",
    "require_price",
    "spend_of",
    "stream_ledger",
    "transcript_files",
]

#: The CLI's own cost counts requests the transcript lacks; a difference under this fraction (or the floor) is expected.
CROSS_CHECK_FRACTION = 0.10
CROSS_CHECK_FLOOR_USD = 0.02
_VERSION_SUFFIX = re.compile(r"@.*$")


class AccountingError(ValueError):
    """A model has no price, or a cell lacks what the accountant needs."""


@dataclass(frozen=True)
class Spend:
    """What a set of requests cost: the four token classes, the request count and USD."""

    uncached_input: int = 0
    cache_read: int = 0
    cache_write: int = 0
    output: int = 0
    requests: int = 0
    #: USD from the price table; ``None`` when any request's model has no price.
    usd: float | None = 0.0
    unpriced_requests: int = 0
    #: Requests with no timestamp: in a windowed split they count before the window's end (online).
    untimed_requests: int = 0
    #: True when the source is known to undercount (a stream with no session file, or a sub-agent file missing).
    partial: bool = False

    @property
    def input_tokens(self) -> int:
        """All input tokens: uncached plus cache reads and writes."""
        return self.uncached_input + self.cache_read + self.cache_write

    def __add__(self, other: Spend) -> Spend:
        usd = None if self.usd is None or other.usd is None else self.usd + other.usd
        return Spend(
            self.uncached_input + other.uncached_input,
            self.cache_read + other.cache_read,
            self.cache_write + other.cache_write,
            self.output + other.output,
            self.requests + other.requests,
            usd,
            self.unpriced_requests + other.unpriced_requests,
            self.untimed_requests + other.untimed_requests,
            self.partial or other.partial,
        )

    def token_row(self) -> dict[str, int | float | None]:
        """The eval runner's ``TokenRow`` fields."""
        return {
            "uncached_input": self.uncached_input,
            "cache_read": self.cache_read,
            "cache_write": self.cache_write,
            "output": self.output,
            "requests": self.requests,
            "usd": self.usd,
        }


@dataclass(frozen=True)
class CellAccounting:
    """One cell's spend: online and offline, per-role rows, lifecycle (SDO only) and the CLI cross-check (vanilla)."""

    online: Spend
    offline: Spend = field(default_factory=Spend)
    roles: dict[str, Spend] = field(default_factory=dict)
    lifecycle: Spend = field(default_factory=Spend)
    cross_check: dict[str, Any] | None = None
    notes: tuple[str, ...] = ()

    def detail(self) -> dict[str, Any]:
        """The per-role rows for ``accounting.json``."""
        return {
            "online": self.online.token_row(),
            "offline": self.offline.token_row(),
            "roles": {name: spend.token_row() for name, spend in self.roles.items()},
            "lifecycle": self.lifecycle.token_row(),
            "cross_check": self.cross_check,
            "notes": list(self.notes),
        }


# ---- prices ----------------------------------------------------------------------------------------------------


def normalize_model(model: str) -> str:
    """The price-table name of a model ID: a Vertex versioned alias (``claude-x@20260101``) is its family."""
    return _VERSION_SUFFIX.sub("", model)


def require_price(model: str, pricing: PricingTable | None = None) -> None:
    """Preflight: the exact model ID in use resolves in the price table, or the accountant could not price the cell."""
    if price_for("claude", normalize_model(model), pricing or default_pricing()) is None:
        raise AccountingError(f"no price for model {model!r} in the agentshim price table")


# ---- transcripts -----------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class TranscriptFiles:
    main: tuple[Path, ...] = ()
    sidechains: tuple[Path, ...] = ()


def transcript_files(home: Path) -> TranscriptFiles:
    """The session and sub-agent transcripts below a Claude config home (or a copy's ``claude`` directory).

    A sidechain is ``projects/<project>/<session>/subagents/agent-<id>.jsonl``; every other ``*.jsonl`` under
    ``projects`` is a main session file.
    """
    projects = home / "projects"
    found = sorted(p for p in projects.rglob("*.jsonl") if p.is_file()) if projects.is_dir() else []
    side = tuple(p for p in found if p.parent.name == "subagents")
    return TranscriptFiles(tuple(p for p in found if p not in side), side)


def ledger_of(files: TranscriptFiles) -> ClaudeLedger:
    ledger = ClaudeLedger()
    for path in files.main:
        ledger.merge_file(path)
    for path in files.sidechains:
        ledger.merge_file(path, subagent=True)
    return ledger


def stream_ledger(*paths: Path) -> ClaudeLedger:
    """A ledger from stream-json files' assistant frames: the fallback for a session killed before its file landed.

    The stream reports a message's usage when it starts, so its output count is low: a spend from it is ``partial``.
    """
    ledger = ClaudeLedger()
    for path in paths:
        ledger.last_assistant_time = None
        for record in json_records(path):
            ledger.observe(record, subagent=bool(record.get("parent_tool_use_id")))
    return ledger


def spend_of(
    ledger: ClaudeLedger, pricing: PricingTable | None, *, start: float | None = None, end: float | None = None
) -> Spend:
    """The spend of the requests in ``[start, end)`` (epoch seconds; unbounded when ``None``), priced per request."""
    table = pricing or default_pricing()
    tokens = TokenUsage()
    usd: float | None = 0.0
    requests = unpriced = untimed = 0
    for request in ledger.requests.values():
        stamp = request.timestamp
        if stamp is None:
            if start is not None:
                continue  # an untimed request is placed before any window's start: it is online
            untimed += 1
        elif (start is not None and stamp < start) or (end is not None and stamp >= end):
            continue
        requests += 1
        tokens += request.tokens
        entry = price_for("claude", normalize_model(request.model), table)
        if entry is not None:
            usd = None if usd is None else usd + cost_usd(request.tokens, entry)
        elif request.tokens.input_tokens or request.tokens.output_tokens:
            unpriced += 1
            usd = None
    return Spend(
        tokens.uncached_input_tokens,
        tokens.cache_read_input_tokens,
        tokens.cache_write_input_tokens,
        tokens.output_tokens,
        requests,
        usd,
        unpriced,
        untimed,
    )


# ---- the CLI's own numbers, as a cross-check -------------------------------------------------------------------


def cli_totals(stream: Path) -> float | None:
    """The CLI's own cost of a stream-json file: each session's last cumulative ``total_cost_usd``, summed.

    ``None`` when the stream has no ``result`` frame (a killed run).
    """
    last: dict[str, float] = {}
    for index, record in enumerate(json_records(stream)):
        if record.get("type") == "result" and isinstance(record.get("total_cost_usd"), (int, float)):
            last[str(record.get("session_id") or f"result-{index}")] = float(record["total_cost_usd"])
    return sum(last.values()) if last else None


def cross_check(spend: Spend, cli_usd: float | None) -> dict[str, Any]:
    """Compare the accountant's USD with the CLI's own. A mismatch beyond the tolerance means a missing transcript or
    an unpriced model, not a different answer to prefer."""
    if cli_usd is None or spend.usd is None:
        return {"cli_usd": cli_usd, "accounted_usd": spend.usd, "delta_usd": None, "mismatch": None}
    delta = cli_usd - spend.usd
    tolerance = max(CROSS_CHECK_FLOOR_USD, CROSS_CHECK_FRACTION * cli_usd)
    return {"cli_usd": cli_usd, "accounted_usd": spend.usd, "delta_usd": delta, "mismatch": abs(delta) > tolerance}


# ---- cells -----------------------------------------------------------------------------------------------------


def _ledger_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _closing_cut(directory: Path) -> float | None:
    ledger = _ledger_json(directory / "ledger.json")
    phases = ledger.get("closure_phase_timestamps")
    stamp = (phases.get("reflection_dispatch") if isinstance(phases, dict) else None) or ledger.get(
        "reflection_dispatch_at"
    )
    return epoch(stamp)


def account_sdo_cell(evidence: Path, pricing: PricingTable | None = None) -> CellAccounting:
    """The SDO arm's cell from a copied sink directory (``incidents/<d>``, ``lifecycle/<key>``).

    Each incident is split at its closing turn: its responder requests (and its investigators') are online, the
    closing and cleanup turns offline. Lifecycle agents are summed apart.
    """
    online = offline = lifecycle = Spend()
    roles = {"responder": Spend(), "closing": Spend(), "investigators": Spend()}
    notes: list[str] = []
    for directory in sink.incident_dirs(evidence):
        ledger = ledger_of(transcript_files(directory / "claude"))
        if sink.is_investigator_dir(directory):
            spend = spend_of(ledger, pricing)
            roles["investigators"] += spend
            online += spend
            continue
        cut = _closing_cut(directory)
        before = spend_of(ledger, pricing, end=cut)
        after = spend_of(ledger, pricing, start=cut) if cut is not None else Spend()
        roles["responder"] += before
        roles["closing"] += after
        online += before
        offline += after
        if cut is None and (directory / "ledger.json").is_file():
            notes.append(f"{directory.name[:12]}: no reflection_dispatch time, all of it counted online")
    tasks = evidence / sink.TASKS
    for directory in sorted(p for p in tasks.iterdir() if p.is_dir()) if tasks.is_dir() else []:
        lifecycle += spend_of(ledger_of(transcript_files(directory / "claude")), pricing)
    return CellAccounting(online, offline, roles, lifecycle, None, tuple(notes))


def account_vanilla_cell(directory: Path, pricing: PricingTable | None = None) -> CellAccounting:
    """The vanilla arm's cell from ``<cell>/session-dir`` (the run's own ``CLAUDE_CONFIG_DIR``) and its stream-json.

    Everything is online. When the session directory holds no transcript (a run killed before its file landed) the
    stream's assistant frames are counted instead and the spend is ``partial``. The stream's ``result`` cost is only
    the cross-check.
    """
    streams = sorted(directory.glob("transcript*.jsonl"))
    files = transcript_files(directory / "session-dir")
    notes: list[str] = []
    if files.main:
        spend = spend_of(ledger_of(files), pricing)
    else:
        spend = replace(spend_of(stream_ledger(*streams), pricing), partial=True)
        notes.append("no session transcript under session-dir: counted from the stream's assistant frames (partial)")
    cli = [total for path in streams if (total := cli_totals(path)) is not None]
    check = cross_check(spend, sum(cli) if cli else None)
    return CellAccounting(spend, Spend(), {"vanilla": spend}, Spend(), check, tuple(notes))
