"""The eval runner's real ``Accountant`` and ``EvidenceGate``: thin adapters over the one accountant and the one gate.

Layout of a collected cell (``FastloopRowRunner.collect``): the fastloop row's results directory is ``<cell>/fastloop/``
(for an SDO row it holds the copied evidence sink as ``evidence/`` and ``incidents.jsonl``), and a vanilla row's own
session home, stream-json and clock are ``<cell>/artifacts/``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarks.fuzzops import cellevidence
from benchmarks.sregym.analysis.accounting import CellAccounting, account_sdo_cell, account_vanilla_cell
from benchmarks.sregym.eval.cell import Accounting, GateVerdict
from benchmarks.sregym.eval.results import TokenRow
from benchmarks.sregym.fastloop import evidence_sink
from benchmarks.sregym.fastloop.records import load_records

if TYPE_CHECKING:
    from pathlib import Path

    from benchmarks.sregym.eval.cell import ArmResult
    from benchmarks.sregym.eval.plan import Cell


def _row(spend: object) -> TokenRow:
    return TokenRow(**spend.token_row())  # type: ignore[attr-defined]


class LedgerAccountant:
    """Tokens, requests and USD from the transcripts themselves (online/offline split at the closing turn)."""

    def account(self, cell: Cell, directory: Path, result: ArmResult | None) -> Accounting:
        if cell.arm == "vanilla":
            accounting: CellAccounting = account_vanilla_cell(directory / "artifacts")
        else:
            evidence = directory / "fastloop" / evidence_sink.COPY_DIRNAME
            if not evidence.is_dir():
                return Accounting(TokenRow(), TokenRow(), {"source": "none", "notes": ["no evidence copy to account"]})
            accounting = account_sdo_cell(evidence)
        detail = {k: v for k, v in accounting.detail().items() if k not in ("online", "offline")}
        return Accounting(_row(accounting.online), _row(accounting.offline), {"source": "ledger", **detail})


class CellEvidenceGate:
    """Refuse to grade a cell whose transcripts are missing or truncated."""

    def check(self, cell: Cell, directory: Path) -> GateVerdict:
        if cell.arm == "vanilla":
            problems = cellevidence.trial_problems(directory / "artifacts")
            return GateVerdict(not problems, tuple(problems))
        results = directory / "fastloop"
        path = results / "incidents.jsonl"
        if not path.is_file():
            return GateVerdict(False, ("no incident record collected",))
        stamps = [r.evidence_incomplete for r in load_records(path) if r.evidence_incomplete]
        if stamps:
            return GateVerdict(False, tuple(stamps))
        if not (results / evidence_sink.RECEIPT_FILENAME).is_file():
            return GateVerdict(False, ("no verified evidence copy (evidence-copy.json)",))
        return GateVerdict(True)


def ledger_accountant() -> LedgerAccountant:
    return LedgerAccountant()


def cell_gate() -> CellEvidenceGate:
    return CellEvidenceGate()
