"""The fuzzops ``swap`` receipt: what the bench reads after a swap."""

from __future__ import annotations

from pathlib import Path  # noqa: TC003

from benchmarks.devswap.swap import Phases
from benchmarks.fuzzops import hooks


def test_swap_receipt_has_the_fields_the_bench_reads_after_prepare(tmp_path: Path) -> None:
    receipt = hooks.swap_receipt(tmp_path, "abc", "m1", 3.14)
    assert {"pack", "operator_version", "memory"} <= receipt.keys()
    assert receipt["pack"] == str((tmp_path / "pack").resolve())


def test_swap_receipt_carries_the_seconds_of_each_phase(tmp_path: Path) -> None:
    phases = Phases()
    with phases("project"):
        pass
    receipt = hooks.swap_receipt(tmp_path, "abc", "m1", 3.14, phases)
    assert list(receipt["phases"]) == ["project"]
