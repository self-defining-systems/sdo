"""Claude request accounting shared by live capture and offline analysis.

Assistant message IDs identify billable requests. CLI cost-state counters are
session cumulative and may lag a cancelled response; neither result frames nor
export copies are additional requests.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from agentshim import PricingTable, TokenUsage, cost_usd, normalized_usage, price_for

if TYPE_CHECKING:
    from pathlib import Path

_CORE = ("uncached_input_tokens", "cache_read_input_tokens", "cache_write_input_tokens", "output_tokens")
_COUNTERS = ("inputTokens", "cacheReadInputTokens", "cacheCreationInputTokens", "outputTokens")


def epoch(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (AttributeError, TypeError, ValueError):
        return None


def json_records(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    records: list[dict[str, Any]] = []
    for line in lines:
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            records.append(cast("dict[str, Any]", value))
    return records


def api_tokens(usage: dict[str, Any]) -> TokenUsage:
    writes = int(usage.get("cache_creation_input_tokens") or 0)
    reads = int(usage.get("cache_read_input_tokens") or 0)
    breakdown: dict[str, Any] = usage.get("cache_creation") or {}
    details: dict[str, Any] = usage.get("output_tokens_details") or {}
    return normalized_usage(
        input_tokens=int(usage.get("input_tokens") or 0) + reads + writes,
        output_tokens=int(usage.get("output_tokens") or 0),
        cache_read_input_tokens=reads,
        cache_write_input_tokens=writes,
        cache_write_1h_input_tokens=int(breakdown.get("ephemeral_1h_input_tokens") or 0),
        reasoning_output_tokens=int(details.get("thinking_tokens") or 0),
        turns=1,
    )


def token_mapping(tokens: TokenUsage, requests: int) -> dict[str, int | float]:
    return {
        **{
            name: getattr(tokens, name)
            for name in (
                "input_tokens",
                "uncached_input_tokens",
                "output_tokens",
                "cached_input_tokens",
                "cache_read_input_tokens",
                "cache_write_input_tokens",
                "cache_write_1h_input_tokens",
                "reasoning_output_tokens",
            )
        },
        "llm_calls": requests,
        "model_requests": requests,
    }


def signature(tokens: TokenUsage) -> tuple[int, ...]:
    return tuple(getattr(tokens, name) for name in _CORE)


@dataclass(frozen=True)
class ClaudeRequest:
    model: str
    tokens: TokenUsage
    timestamp: float | None


@dataclass(frozen=True)
class BillingState:
    usd: float
    models: dict[str, Any]
    anchor: float | None
    unknown: bool = False

    def signature(self) -> tuple[int, ...]:
        return tuple(sum(int(model.get(key) or 0) for model in self.models.values()) for key in _COUNTERS)


@dataclass(frozen=True)
class ClaudeBill:
    tokens: TokenUsage
    requests: int
    usd: float | None
    usd_complete: bool
    ttl_complete: bool = True
    source: str = "unique API requests"
    evidence: tuple[dict[str, Any], ...] = ()

    def usage(self) -> dict[str, int | float]:
        result = token_mapping(self.tokens, self.requests)
        if self.usd is not None:
            result["total_cost_usd"] = self.usd
        return result


@dataclass
class ClaudeLedger:
    requests: dict[str, ClaudeRequest] = field(default_factory=dict[str, ClaudeRequest])
    states: list[BillingState] = field(default_factory=list[BillingState])
    last_assistant_time: float | None = None
    invalid: bool = False

    def observe(self, record: dict[str, Any], *, model: str | None = None) -> None:
        if record.get("type") == "cost-state":
            if isinstance(record.get("totalCostUSD"), (int, float)):
                state = BillingState(
                    float(record["totalCostUSD"]),
                    record.get("modelUsage") or {},
                    self.last_assistant_time,
                    bool(record.get("hasUnknownModelCost")),
                )
                if state not in self.states:
                    self.states.append(state)
            return
        message: dict[str, Any] = record.get("message") or {}
        if record.get("type") != "assistant" or not message.get("usage"):
            return
        identity = message.get("id") or record.get("uuid")
        if not identity:
            self.invalid = True
            identity = "unidentified:" + hashlib.sha256(json.dumps(message, sort_keys=True).encode()).hexdigest()
        timestamp = epoch(record.get("timestamp"))
        if timestamp is not None:
            self.last_assistant_time = max(self.last_assistant_time or timestamp, timestamp)
        request = ClaudeRequest(message.get("model") or model or "", api_tokens(message["usage"]), timestamp)
        previous = self.requests.get(identity)
        if previous is None or request.tokens.output_tokens >= previous.tokens.output_tokens:
            self.requests[identity] = request

    def merge_file(self, path: Path) -> None:
        # Anchors belong to this file's ordering, not to the previous export.
        self.last_assistant_time = None
        for record in json_records(path):
            self.observe(record)

    def before(self, timestamp: float) -> ClaudeLedger:
        return ClaudeLedger(
            requests={
                key: value
                for key, value in self.requests.items()
                if value.timestamp is not None and value.timestamp < timestamp
            },
            states=[state for state in self.states if state.anchor is not None and state.anchor < timestamp],
            invalid=self.invalid,
        )

    def through(self, timestamp: float) -> ClaudeLedger:
        return ClaudeLedger(
            requests={
                key: value
                for key, value in self.requests.items()
                if value.timestamp is not None and value.timestamp <= timestamp
            },
            states=[state for state in self.states if state.anchor is not None and state.anchor <= timestamp],
            invalid=self.invalid,
        )

    def since(self, before: ClaudeLedger, pricing: PricingTable) -> ClaudeBill:
        selected = [value for key, value in self.requests.items() if key not in before.requests]
        tokens = sum((value.tokens for value in selected), TokenUsage())
        valid = [
            state
            for state in self.states
            if not state.unknown
            and state.models
            and all(price_for("claude", model, pricing) is not None for model in state.models)
        ]

        def matching(ledger: ClaudeLedger) -> float | None:
            counts = sum((value.tokens for value in ledger.requests.values()), TokenUsage())
            if not any(signature(counts)):
                return 0.0
            for state in reversed(valid):
                if state.signature() == signature(counts):
                    return state.usd
            return None

        start, end = matching(before), matching(self)
        source = "provider counter exactly reconciled to unique API requests"
        ttl_complete = True
        dollars: float | None = None
        if start is not None and end is not None:
            dollars = end - start
        else:
            begin_states = [state for state in before.states if state in valid]
            begin = max(begin_states, key=lambda state: state.usd) if begin_states else None
            finish = max(valid, key=lambda state: state.usd) if valid else None
            counters = tuple(
                b - a
                for b, a in zip(
                    finish.signature() if finish else (0, 0, 0, 0),
                    begin.signature() if begin else (0, 0, 0, 0),
                    strict=True,
                )
            )
            if finish is not None:
                start, end = begin.usd if begin else 0.0, finish.usd
            actual = signature(tokens)
            if finish is not None and all(value >= 0 for value in counters) and actual == counters:
                start, end = begin.usd if begin else 0.0, finish.usd
                dollars = end - start
                source = "provider phase delta matches unique API requests"
            elif finish is not None and all(a <= b for a, b in zip(actual, counters, strict=True)):
                start, end = begin.usd if begin else 0.0, finish.usd
                dollars = end - start
                ttl_complete = tokens.cache_write_input_tokens == counters[2]
                tokens = normalized_usage(
                    input_tokens=sum(counters[:3]),
                    output_tokens=counters[3],
                    cache_read_input_tokens=counters[1],
                    cache_write_input_tokens=counters[2],
                    cache_write_1h_input_tokens=tokens.cache_write_1h_input_tokens if ttl_complete else 0,
                    turns=len(selected),
                )
                source = "provider billing includes requests absent from assistant JSONL"
            elif finish is None or all(a >= b for a, b in zip(actual, counters, strict=True)):
                prices = [price_for("claude", value.model, pricing) for value in selected]
                if selected and all(
                    entry is not None or not any(signature(value.tokens))
                    for value, entry in zip(selected, prices, strict=True)
                ):
                    dollars = sum(
                        cost_usd(value.tokens, entry)
                        for value, entry in zip(selected, prices, strict=True)
                        if entry is not None
                    )
                source = "recorded API requests priced; CLI counter lag retained"
            else:
                source = "conflicting request/counter fields"
        complete = dollars is not None and dollars >= 0 and not self.invalid and bool(selected)
        evidence = (
            {
                "billing_start_usd": start,
                "billing_end_usd": end,
                "collected_usd": dollars,
                "client_counter_usd": end - start if start is not None and end is not None else None,
                "counter_matches_requests": "exactly reconciled" in source,
                "source": source,
                "unique_assistant_messages": len(selected),
            },
        )
        return ClaudeBill(
            tokens, len(selected), dollars if complete else None, complete, ttl_complete, source, evidence
        )


def session_ledger(home: Path, session_id: str | None) -> ClaudeLedger:
    ledger = ClaudeLedger()
    if session_id:
        for path in sorted((home / "projects").glob(f"**/{session_id}.jsonl")):
            ledger.merge_file(path)
    return ledger
