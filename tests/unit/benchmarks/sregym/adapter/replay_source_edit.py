"""Apply one replay slot's accepted-source edits to a proposal checkout.

Hermetic test helper: it runs exactly the source edit of ``scripted_replay`` for
each restore/credentials step, without a cluster or the ``sdo`` tool.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from benchmarks.sregym.adapter.scripted_replay import _edit_source
from benchmarks.sregym.adapter.scripted_replay_catalog import SCRIPTED_REPLAY_SLOTS


def apply_slot_source_edits(slot: int, root: Path) -> list[str]:
    if type(slot) is not int or not 0 <= slot < len(SCRIPTED_REPLAY_SLOTS):
        raise ValueError("slot must be an integer from 0 through 9")
    edited = []
    for step in SCRIPTED_REPLAY_SLOTS[slot].steps:
        if step.operation not in {"restore", "credentials"}:
            continue
        _edit_source(
            root, {"kind": step.kind, "metadata": {"name": step.name}}, credentials=step.operation == "credentials"
        )
        edited.append(f"{step.kind}/{step.name}")
    return edited


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slot", required=True, type=int)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    print(",".join(apply_slot_source_edits(args.slot, args.root)))


if __name__ == "__main__":
    main()
