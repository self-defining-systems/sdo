"""Bind the hand-written repair catalog to the existing scripted CLI directive."""

from __future__ import annotations

from benchmarks.sregym.adapter import SCRIPTED_REPLAY_SLOTS as SLOTS

from .directive import Directive

REPLAY_FAULT = "recorded_repair"


def directive_for(index: int) -> Directive:
    if type(index) is not int or not 0 <= index < len(SLOTS):
        raise ValueError("slot must be an integer from 0 through 9")
    slot = SLOTS[index]
    return Directive(
        scenario=f"rh3-slot-{index}",
        fault=REPLAY_FAULT,
        target=slot.namespace,
        reflection="no_change",
        status_attempts=121,
        extra={"slot": str(index)},
    )
