"""Re-price recorded Claude rows without a run-local reconciliation script.

Usage is deduplicated by provider message ID and bounded by worker phase
journals (legacy guard records are also readable). Provider billing is checked
against request counters; unknown evidence remains unknown, never zero.

    uv run python -m benchmarks.sregym.analysis.claude_cost --input rows.json --output priced.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from agentshim import PricingTable, TokenUsage, cost_usd, default_pricing, normalized_usage, price_for

from libs.agent_cli.claude_usage import ClaudeBill, ClaudeLedger, epoch, json_records, token_mapping

_ROLES = ("responder", "reflection")
_EVENT_FILES = ("trial2-turns.jsonl", "agent-resolved.jsonl", "claude-turns.jsonl")


def _unknown() -> dict[str, Any]:
    return {"tokens": None, "usd": None, "usd_complete": False, "tokens_complete": False}


def _summary(bill: ClaudeBill) -> dict[str, Any]:
    tokens: dict[str, Any] = bill.usage()
    tokens.pop("total_cost_usd", None)
    if not bill.ttl_complete:
        tokens["cache_write_1h_input_tokens"] = None
    return {
        "tokens": tokens,
        "raw_total_tokens": bill.tokens.input_tokens + bill.tokens.output_tokens,
        "usd": bill.usd,
        "usd_complete": bill.usd_complete,
        "tokens_complete": bill.usd_complete,
        "cache_write_ttl_complete": bill.ttl_complete,
        "usd_source": bill.source,
        "accounting_source": "deduplicated API requests and cumulative billing deltas",
        "reconciliation_evidence": list(bill.evidence),
    }


def _zero() -> dict[str, Any]:
    return {
        "tokens": token_mapping(TokenUsage(), 0),
        "raw_total_tokens": 0,
        "usd": 0.0,
        "usd_complete": True,
        "tokens_complete": True,
        "no_model_dispatched": True,
    }


def _guard_fallback(
    events: list[dict[str, Any]], role: str, model: str, pricing: PricingTable
) -> dict[str, Any] | None:
    selected = [event for event in events if event.get("role") == role]
    starts = [event for event in selected if event["event"] == "start"]
    done = [
        event
        for event in selected
        if (event["event"] == "complete" or event["event"] == "error" and event.get("usd_complete"))
    ]
    if (
        not starts
        or len(starts) != len(done)
        or any(event["event"] in {"error", "rejected"} and not event.get("usd_complete") for event in selected)
    ):
        return None
    entry = price_for("claude", model, pricing)
    counts = TokenUsage()
    dollars = 0.0
    requests = 0
    ttl_complete = True
    for event in done:
        usage = event.get("usage") or {}
        required = (
            "uncached_input_tokens",
            "cache_read_input_tokens",
            "cache_write_input_tokens",
            "cache_write_1h_input_tokens",
            "output_tokens",
            "total_cost_usd",
        )
        if any(name not in usage for name in required):
            return None
        tokens = normalized_usage(
            input_tokens=int(usage.get("input_tokens") or sum(usage[key] for key in required[:3])),
            output_tokens=int(usage["output_tokens"]),
            cache_read_input_tokens=int(usage["cache_read_input_tokens"]),
            cache_write_input_tokens=int(usage["cache_write_input_tokens"]),
            cache_write_1h_input_tokens=int(usage["cache_write_1h_input_tokens"]),
        )
        captured_fresh = (
            event.get("schema") == "sdo.claude-usage/v1"
            and event.get("usd_complete") is True
            and event.get("resumed") is False
        )
        price_matches = entry is not None and abs(cost_usd(tokens, entry) - usage["total_cost_usd"]) <= 1e-7
        if not captured_fresh and not price_matches:
            return None  # A resumed session cumulative dollar amount is not a turn delta.
        ttl_complete = ttl_complete and event.get("cache_write_ttl_complete", True)
        counts += tokens
        dollars += usage["total_cost_usd"]
        requests += int(usage.get("model_requests", usage.get("llm_calls", 0)))
    bill = ClaudeBill(
        counts,
        requests,
        dollars,
        True,
        ttl_complete=ttl_complete,
        source="captured fresh provider billing or complete CLI result with exact token-price identity",
        evidence=({"billing_start_usd": 0.0, "billing_end_usd": dollars, "provider_transcript_present": False},),
    )
    return _summary(bill)


def _events(directories: list[Path]) -> list[dict[str, Any]]:
    unique = {}
    for directory in directories:
        for name in _EVENT_FILES:
            for event in json_records(directory / name):
                if event.get("event") and event.get("role") in _ROLES:
                    unique[json.dumps(event, sort_keys=True)] = event
    return sorted(unique.values(), key=lambda event: event.get("recorded_at_epoch", 0))


def _episode(directories: list[Path], model: str, pricing: PricingTable) -> dict[str, Any]:
    events = _events(directories)
    sids = {event["session_id"] for event in events if event.get("session_id")}
    ledgers: dict[str, ClaudeLedger] = {}
    for directory in directories:
        for path in sorted(directory.glob("claude/projects/**/*.jsonl")):
            if sids and path.stem not in sids:
                continue
            ledgers.setdefault(path.stem, ClaudeLedger()).merge_file(path)
    role_sessions = {
        role: {event["session_id"] for event in events if event.get("role") == role and event.get("session_id")}
        for role in _ROLES
    }
    responder_end = max(
        (
            event["recorded_at_epoch"]
            for event in events
            if event.get("role") == "responder" and event["event"] in {"complete", "error"}
        ),
        default=None,
    )
    result = {}
    for role in _ROLES:
        intervals = []
        started = None
        for event in events:
            if event.get("role") != role:
                continue
            if event["event"] == "start":
                if started is not None:
                    break  # Ambiguous overlapping starts cannot establish a phase boundary.
                started = event.get("recorded_at_epoch")
            elif event["event"] in {"complete", "error"} and started is not None:
                intervals.append((started, event["recorded_at_epoch"]))
                started = None
        if started is not None and role == "responder":
            following = [
                event["recorded_at_epoch"]
                for event in events
                if event.get("role") == "reflection"
                and event["event"] == "start"
                and event["recorded_at_epoch"] >= started
            ]
            if following:
                intervals.append((started, min(following)))
                started = None
        parts = []
        if intervals and started is None:
            first, last = min(a for a, _ in intervals), max(b for _, b in intervals)
            for sid, ledger in ledgers.items():
                shared = (
                    sid in role_sessions["responder"]
                    and sid in role_sessions["reflection"]
                    or len(ledgers) == 1
                    and not all(role_sessions.values())
                )
                handoff = responder_end if role == "reflection" and shared and responder_end is not None else None
                phase = [
                    request
                    for request in ledger.requests.values()
                    if request.timestamp is not None and any(a <= request.timestamp <= b for a, b in intervals)
                ]
                if phase:
                    before = (
                        ledger.through(handoff) if handoff is not None and handoff >= first else ledger.before(first)
                    )
                    summary = _summary(ledger.through(last).since(before, pricing))
                    if handoff is not None and handoff > first:
                        summary["guard_overlap_seconds"] = handoff - first
                        summary["phase_boundary_source"] = (
                            "shared session: responder termination precedes reflection ownership"
                        )
                    parts.append(summary)
        result[role + "_usage"] = (
            _combine(parts) if parts else (_guard_fallback(events, role, model, pricing) or _unknown())
        )
    # A phase with no dispatch is zero only when closed billing belongs entirely
    # to its peer, with no earlier responder history or unfinished reflection.
    roles = {event["role"] for event in events if event["event"] == "start"}
    result["roles"] = roles
    return result


def _combine(parts: list[dict[str, Any]]) -> dict[str, Any]:
    if not parts:
        return _unknown()
    if len(parts) == 1:
        return parts[0]
    complete = all(part.get("usd_complete") for part in parts)
    counts: dict[str, Any] = {}
    for part in parts:
        for key, value in (part.get("tokens") or {}).items():
            if value is None or (key in counts and counts[key] is None):
                counts[key] = None
            else:
                counts[key] = counts.get(key, 0) + value
    return {
        "tokens": counts,
        "raw_total_tokens": sum(part.get("raw_total_tokens") or 0 for part in parts),
        "usd": sum(part.get("usd") or 0 for part in parts) if complete else None,
        "usd_complete": complete,
        "tokens_complete": complete,
        "cache_write_ttl_complete": all(part.get("cache_write_ttl_complete", True) for part in parts),
        "reconciliation_evidence": [item for part in parts for item in part.get("reconciliation_evidence", [])],
        "accounting_source": "all current-row production episodes, once per session fragment",
    }


def reconcile_claude_row(row: dict[str, Any], pricing: PricingTable | None = None) -> None:
    """Update one analysis row in place; Codex rows are unchanged."""
    if row.get("provider") != "claude" or not row.get("incident_id"):
        return
    pricing = pricing or default_pricing()
    root = Path(row["results"])
    primary = hashlib.sha256(row["incident_id"].encode()).hexdigest()[:32]
    cutoff = epoch(row.get("injection_started_at"))
    fragments: dict[str, list[Path]] = {}
    for directory in root.rglob("sdo_runtime/sessions/*"):
        if not directory.is_dir():
            continue
        events = _events([directory])
        current = cutoff is not None and any(
            event["event"] == "start" and event.get("recorded_at_epoch", 0) >= cutoff for event in events
        )
        if directory.name == primary or current:
            fragments.setdefault(directory.name, []).append(directory)
    episodes = []
    for fragment, directories in fragments.items():
        episode = _episode(directories, row.get("model") or "", pricing)
        roles = episode.pop("roles")
        for role, other in (("responder", "reflection"), ("reflection", "responder")):
            known = episode[other + "_usage"]
            proof = known.get("reconciliation_evidence") or []
            if (
                role not in roles
                and known.get("usd_complete")
                and proof
                and (
                    role == "reflection"
                    and row.get("status") == "settled"
                    or role == "responder"
                    and all(item.get("billing_start_usd") == 0 for item in proof)
                )
            ):
                episode[role + "_usage"] = _zero()
        episode["session_fragment"] = fragment
        episodes.append(episode)
    entry = price_for("claude", row.get("model"), pricing)
    for role in _ROLES:
        previous = row.get(role + "_usage")
        summary = _combine([episode[role + "_usage"] for episode in episodes])
        if previous:
            summary["transport_usage"] = previous.get("transport_usage", previous)
        summary["pricing_table_version"] = pricing.version
        summary["weighted_base_input_tokens"] = (
            summary["usd"] * 1e6 / entry.input_usd if summary.get("usd_complete") and entry else None
        )
        row[role + "_usage"] = summary
    row["cost_episodes"] = episodes
    row["cost_complete"] = all(row[role + "_usage"]["usd_complete"] for role in _ROLES)
    row["cost_pricing_version"] = pricing.version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.input.resolve() == args.output.resolve():
        parser.error("output must differ from the recorded input")
    if any(part in {"results", "live", ".sdo", "application_workspace"} for part in args.output.parts):
        parser.error("output must be outside recorded run directories")
    document = json.loads(args.input.read_text(encoding="utf-8"))
    rows = (
        [pair[key] for pair in document["pairs"] for key in ("first", "repeat") if pair.get(key)]
        if isinstance(document, dict) and "pairs" in document
        else document
    )
    for row in rows:
        reconcile_claude_row(row)
    args.output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
