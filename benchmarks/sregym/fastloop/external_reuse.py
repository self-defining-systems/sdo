"""An external-agent row reuses the lane's running application only when it is provably a fresh deploy's state.

SDO rows reuse the app through the sealed admission receipt. An external agent has no SDO admission, so its
rows redeployed every time (~2.5 minutes of an incident). The same rule applies to every external arm: the
previous row on this lane ended with full cleanup verified and a passing recovery oracle, and the live app is
the deploy of this row's planned source. The pre-injection health gate (traffic probe and sustained pod gate,
``rehearse_with_repair``) then runs as for any row and recreates the app once when it fails. Anything unknown
redeploys.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.fastloop.records import load_records

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.fastloop.records import IncidentRecord

INCIDENTS_FILENAME = "incidents.jsonl"


@dataclass(frozen=True)
class ExternalReuseDecision:
    reused: bool
    reasons: tuple[str, ...]
    previous_run_id: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "reused": self.reused,
            "receipt": None,
            "changed": [] if self.reused else list(self.reasons),
            "basis": "external_previous_row_clean" if self.reused else "external_redeploy",
            "previous_run_id": self.previous_run_id,
            "reasons": list(self.reasons),
        }


def _previous_row(run_dir: Path, current_run_id: str) -> tuple[str, IncidentRecord] | None:
    """The newest recorded row of any other run on this lane."""
    files = [
        path
        for path in (run_dir / "results").glob(f"*/{INCIDENTS_FILENAME}")
        if path.parent.name != current_run_id and path.is_file() and path.stat().st_size
    ]
    for path in sorted(files, key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            records = load_records(path)
        except (OSError, ValueError):
            return None
        if records:
            return path.parent.name, max(records, key=lambda item: item.index)
    return None


def _previous_row_problems(record: IncidentRecord) -> list[str]:
    problems: list[str] = []
    if record.agent == "sdo":
        problems.append("previous_row_was_sdo")
    if record.error or record.infra_error:
        problems.append("previous_row_errored")
    if record.recovery_error:
        problems.append("previous_row_recovery_error")
    if not record.full_cleanup_verified:
        problems.append("previous_row_full_cleanup_unverified")
    if record.recovery_oracle is None or not record.recovery_oracle.success:
        problems.append("previous_row_recovery_oracle_not_passed")
    return problems


def external_reuse_decision(
    run_dir: Path, current_run_id: str, source_change: Callable[[], object | None]
) -> ExternalReuseDecision:
    """Reuse only when the previous row left a clean app and the deployed source is this row's planned source."""
    previous = _previous_row(run_dir, current_run_id)
    if previous is None:
        return ExternalReuseDecision(False, ("no_previous_row",))
    run_id, record = previous
    reasons = _previous_row_problems(record)
    try:
        change = source_change()
    except Exception as exc:  # unknown source identity never grants reuse
        reasons.append(f"deployed_source_unknown: {type(exc).__name__}: {exc}")
    else:
        if change is not None:
            reasons.append(f"deployed_source_differs: {change}")
    if reasons:
        return ExternalReuseDecision(False, tuple(reasons), run_id)
    return ExternalReuseDecision(True, ("previous_row_clean_and_source_matches",), run_id)
