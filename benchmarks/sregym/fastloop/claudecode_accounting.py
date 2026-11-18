"""Tokens, requests and USD of one vanilla Claude Code cell, from its own session files.

One accountant for both arms (audit F6): ``libs.agent_cli.claude_usage.ClaudeLedger`` over every main and
sub-agent session JSONL under the cell's CLI config home, priced with agentshim's table. Requests are unique
assistant message ids, never ``num_turns``. The CLI's own ``total_cost_usd`` (stream-json ``result`` frames)
is only a cross-check column.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from agentshim import default_pricing

from benchmarks.sregym.fastloop.records import TokenCounts
from libs.agent_cli.claude_usage import ClaudeLedger

if TYPE_CHECKING:
    from pathlib import Path

#: The CLI counter and the priced requests may differ by cache TTL and rounding; beyond this the cell is flagged.
COST_TOLERANCE = 0.10


@dataclass(frozen=True)
class CellAccounting:
    tokens: TokenCounts
    requests: int
    subagent_requests: int
    usd: float | None
    usd_source: str
    cli_total_cost_usd: float | None
    cost_mismatch: bool
    session_files: int

    def to_json(self) -> dict[str, Any]:
        data = asdict(self)
        data["tokens"] = self.tokens.model_dump(mode="json")
        return data


def session_files(config_home: Path) -> tuple[list[Path], list[Path]]:
    """``(main, subagent)`` transcripts under ``config_home/projects``."""
    everything = sorted((config_home / "projects").glob("**/*.jsonl"))
    subagents = [path for path in everything if "subagents" in path.parts]
    return [path for path in everything if path not in subagents], subagents


def cli_total_cost_usd(stream: Path) -> float | None:
    """Sum over sessions of the last ``result`` frame's cumulative ``total_cost_usd``."""
    last: dict[str, float] = {}
    try:
        lines = stream.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        try:
            frame = json.loads(line)
        except ValueError:
            continue
        if (
            isinstance(frame, dict)
            and frame.get("type") == "result"
            and isinstance(frame.get("total_cost_usd"), int | float)
        ):
            last[str(frame.get("session_id"))] = float(frame["total_cost_usd"])
    return sum(last.values()) if last else None


def account_cell(config_home: Path, stream: Path) -> CellAccounting:
    ledger = ClaudeLedger()
    main, subagents = session_files(config_home)
    for path in main:
        ledger.merge_file(path)
    for path in subagents:
        ledger.merge_file(path, subagent=True)
    bill = ledger.since(ClaudeLedger(), default_pricing())
    usage = bill.tokens
    tokens = TokenCounts(
        input_tokens=usage.input_tokens,
        cached_input_tokens=usage.cache_read_input_tokens,
        cache_write_input_tokens=usage.cache_write_input_tokens,
        cache_write_1h_input_tokens=usage.cache_write_1h_input_tokens,
        output_tokens=usage.output_tokens,
        reasoning_output_tokens=usage.reasoning_output_tokens,
    )
    cli = cli_total_cost_usd(stream)
    mismatch = bool(
        cli is not None and bill.usd is not None and abs(cli - bill.usd) > COST_TOLERANCE * max(cli, bill.usd)
    )
    return CellAccounting(
        tokens=tokens,
        requests=bill.requests,
        subagent_requests=bill.subagent_requests,
        usd=bill.usd,
        usd_source=bill.source,
        cli_total_cost_usd=cli,
        cost_mismatch=mismatch,
        session_files=len(main) + len(subagents),
    )
