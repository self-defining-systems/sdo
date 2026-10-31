"""Deterministic synthetic token usage.

Every model response of a scripted turn gets usage computed from the
directive's seed, the turn kind and the response index, so a run's expected
totals are known exactly and each incident and turn kind is distinguishable
(a double-counted or dropped turn shows up as a mismatch, never as noise).

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

from benchmarks.sregym.agent_script.wire import RequestUsage

#: Distinct offsets per turn kind, so one kind's tokens can never masquerade as another's.
KIND_OFFSETS = {
    "responder": 0,
    "responder-warm": 101,
    "reflection-resume": 211,
    "reflection-fresh": 307,
    "reflection-retry": 401,
}


def request_usage(seed: int, kind: str, index: int) -> RequestUsage:
    """Usage of the ``index``-th model response of a ``kind`` turn under ``seed``."""

    if kind not in KIND_OFFSETS:
        raise ValueError(f"unknown turn kind {kind!r}")
    if seed < 0 or index < 0:
        raise ValueError("seed and index must not be negative")
    offset = KIND_OFFSETS[kind]
    input_tokens = 20_000 + 1_500 * index + 7 * seed + offset
    # The first response of a session reads nothing from cache; later ones reuse the prefix.
    cached = 0 if index == 0 else input_tokens - 1_900 - 13 * (index % 5)
    output_tokens = 300 + 17 * index + (seed % 50) + offset % 37
    reasoning = 120 + index + (seed % 11)
    return RequestUsage(
        input_tokens=input_tokens,
        cached_input_tokens=cached,
        output_tokens=output_tokens,
        reasoning_output_tokens=min(reasoning, output_tokens),
    )
