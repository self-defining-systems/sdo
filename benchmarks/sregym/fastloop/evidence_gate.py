"""The refuse-to-grade gate of a fastloop SDO run: a row whose expected transcripts are missing is not gradable.

A row is graded by the oracle on what the cluster shows, but the comparison it feeds also needs the agents' own
transcripts (cost, request counts, time in model and tools, what the agent did). A cell that lost one must not be
graded as if it had them. The gate reads the run's copy of the evidence sink (:mod:`evidence_sink`) with the same
validator fuzzops cells use (:func:`benchmarks.fuzzops.cellevidence.copy_problems`) and stamps every SDO row it cannot
vouch for with ``evidence_incomplete``, which makes the row ``NOT-GRADED``. It runs once when the run ends, also after
a crash, from the files on disk, so it never depends on the run having finished.

What makes a row not gradable:

* the run's sink was waived, or no verified copy exists;
* a transcript of the row's own incident, or of an investigator working for it, is missing or truncated, or the row
  names an incident the copy has no directory for;
* a lifecycle agent's transcript (designer, deployer, judge) is missing: that is the whole cell's, so it refuses
  every row, as does a problem in an incident directory no row owns.

The baseline arm has no controller sink and is not gated here. A row that never opened an incident has no transcript
of its own to miss.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchmarks.fuzzops import cellevidence, sink
from benchmarks.sregym.fastloop import evidence_sink
from benchmarks.sregym.fastloop.records import IncidentRecord, load_records, write_records

if TYPE_CHECKING:
    from pathlib import Path

INCIDENTS_FILENAME = "incidents.jsonl"
#: Reasons are one line each; a row's reason keeps the first few problems.
_REASON_PROBLEMS = 4


@dataclass(frozen=True)
class RefusedRow:
    """A row the gate refused to grade, and why."""

    index: int
    problem_id: str
    reason: str


def _reason(problems: list[str]) -> str:
    shown = "; ".join(problems[:_REASON_PROBLEMS])
    return shown + (f"; +{len(problems) - _REASON_PROBLEMS} more" if len(problems) > _REASON_PROBLEMS else "")


def row_problems(results_dir: Path, records: list[IncidentRecord]) -> dict[int, list[str]]:
    """The evidence problems of each SDO row of ``results_dir``, by record position (rows with none are absent)."""
    sdo = [i for i, record in enumerate(records) if record.agent == "sdo"]
    if not sdo:
        return {}
    if evidence_sink.waived(results_dir):
        return {i: ["the evidence sink was waived: no transcripts were kept"] for i in sdo}
    copy = results_dir / evidence_sink.COPY_DIRNAME
    if not (results_dir / evidence_sink.RECEIPT_FILENAME).is_file() or not copy.is_dir():
        return {i: ["no verified evidence copy (evidence-copy.json)"] for i in sdo}
    found = cellevidence.copy_problems(copy)
    if "" in found:
        return dict.fromkeys(sdo, found[""])
    # Each incident directory belongs to the incident it works for: an investigator to its parent.
    owner: dict[str, str] = {}
    for key in found:
        if key.startswith(f"{sink.INCIDENTS}/"):
            owner[key] = sink.parent_incident_id(sink.incident_identity(copy / key))
    out: dict[int, list[str]] = {}
    claimed: set[str] = set()
    cellwide = [text for key, texts in found.items() if key.startswith(f"{sink.TASKS}/") for text in texts]
    present = {d.name for d in sink.incident_dirs(copy)}
    for i in sdo:
        incident = records[i].incident_id
        problems = list(cellwide)
        if incident:
            if sink.incident_dir_name(incident) not in present:
                problems.append(f"no transcript directory for incident {incident[:24]}")
            for key, texts in found.items():
                if owner.get(key) == incident:
                    claimed.add(key)
                    problems.extend(texts)
        if problems:
            out[i] = problems
    stray = [
        text
        for key, texts in found.items()
        if key.startswith(f"{sink.INCIDENTS}/")
        and key not in claimed
        and not any(owner.get(key) == records[i].incident_id for i in sdo)
        for text in texts
    ]
    if stray:
        for i in sdo:
            out.setdefault(i, []).extend(stray)
    return out


def apply(results_dir: Path) -> list[RefusedRow]:
    """Stamp the rows of ``results_dir/incidents.jsonl`` the gate cannot vouch for and return them.

    Idempotent: a row's stamp is recomputed from the files on disk each time (a row that now passes is cleared, which
    a later copy can earn). Nothing is rewritten when no stamp changes.
    """
    path = results_dir / INCIDENTS_FILENAME
    if not path.is_file():
        return []
    records = load_records(path)
    problems = row_problems(results_dir, records)
    updated: list[IncidentRecord] = []
    refused: list[RefusedRow] = []
    for i, record in enumerate(records):
        reason = _reason(problems[i]) if i in problems else None
        updated.append(
            record
            if record.evidence_incomplete == reason
            else record.model_copy(update={"evidence_incomplete": reason})
        )
        if reason:
            refused.append(RefusedRow(record.index, record.problem_id, reason))
    if any(new is not old for new, old in zip(updated, records, strict=True)):
        write_records(path, updated)
    return refused


def announce(refused: list[RefusedRow]) -> bool:
    """Print the refused rows to stderr; True when there are none."""
    for row in refused:
        print(f"NOT GRADED: row {row.index} ({row.problem_id}): evidence incomplete: {row.reason}", file=sys.stderr)
    return not refused
