"""One row per cell, the cross-cell results table, and the per-cell manifest with checksums."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

MANIFEST_SCHEMA = "sdo.sregym-eval-manifest/v1"
MANIFEST_FILE = "manifest.json"
ROW_SCHEMA = "sdo.sregym-eval-row/v1"
CellStatus = Literal["graded", "not_graded", "error"]


class TokenRow(BaseModel):
    """One side (online or offline) of an arm's tokens; zeros when the arm has none, ``None`` when unavailable."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    uncached_input: int | None = None
    cache_read: int | None = None
    cache_write: int | None = None
    output: int | None = None
    requests: int | None = None
    usd: float | None = None


def cost_note(online: TokenRow, *, resolution_path: str | None, no_page: bool, pending: bool = False) -> str | None:
    """Why a row's online cost is not a positive number; ``None`` when it is one. A cost is never silently empty."""
    if pending:
        return "pending: the sealed row carries the cost once the closing turn has drained"
    if online.usd is not None and online.usd > 0:
        return None
    if online.requests or online.uncached_input or online.cache_read or online.cache_write or online.output:
        return "cost-unavailable: responder usage was observed without priced USD"
    if resolution_path == "deterministic":
        return "playbook-applied: the automatic stage fixed it, no agent turn"
    if no_page:
        return "no-page: SDO opened no incident, so no agent turn"
    if online.usd is None:
        return "cost-unavailable: accounting returned no USD for a paged row"
    return "no-agent-usage: accounting found no responder requests"


class ResultRow(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_: str = ROW_SCHEMA
    stage: str
    app: str
    pass_: str
    problem: str
    arm: str
    snapshot: str | None = None
    snapshot_commit: str | None = None
    #: Identity of the application source the row ran (tree outside SDO memory plus ``.sdo/deployment``),
    #: verified against the live app at row setup. Rows with equal identities ran the same application.
    app_source: str | None = None
    lane: str | None = None
    status: CellStatus
    #: Why a cell is not gradable (missing transcripts, evidence gate) or errored.
    reason: str | None = None
    #: SREGym's one-shot mitigation oracle at the end of the cell.
    success: bool | None = None
    # Event times, epoch seconds.
    t_inject: float | None = None
    t_page: float | None = None
    page_source: str = "none"
    no_page: bool = True
    fallback_page: bool = False
    t_healthy: float | None = None
    t_recovered: float | None = None
    t_closed: float | None = None
    # Derived durations, seconds.
    inject_to_healthy_s: float | None = None
    time_to_mitigate_s: float | None = None
    confirmation_s: float | None = None
    healthy_hold_seconds: float | None = None
    flaps: int | None = None
    unhealthy_seconds: float | None = None
    resolution_path: str | None = None
    applied_playbooks: str | None = None
    binding_sources: str | None = None
    agent_seconds: float | None = None
    overhead_seconds: float | None = None
    online: TokenRow = TokenRow()
    offline: TokenRow = TokenRow()
    #: The reason the online cost is not positive (``cost_note``); ``None`` when the row paid for agent requests.
    cost_note: str | None = None
    harm: str | None = None
    memory_commit_after: str | None = None
    # The row is the online part, claim to its end (outcome committed and graded); everything after it drains on the
    # held lane. ``lane_occupancy_s`` is claim to the lane being free again, so it is ``row_wall_s + drain_s``.
    row_wall_s: float | None = None
    drain_s: float | None = None
    lane_occupancy_s: float | None = None
    #: The closing turn's own wall time and the end-of-row fault recovery, both inside the drain.
    closing_s: float | None = None
    recovery_s: float | None = None
    #: ``pending`` in ``row.json`` (written at the row's end); ``closing.json`` and the table carry the terminal value.
    closing_status: str | None = None
    closing_decision: str | None = None
    manifest_sha256: str | None = None

    def flat(self) -> dict[str, Any]:
        data = self.model_dump()
        data["schema"] = data.pop("schema_")
        data["pass"] = data.pop("pass_")
        for side in ("online", "offline"):
            for key, value in data.pop(side).items():
                data[f"{side}_{key}"] = value
        return data


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_manifest(directory: Path, identity: dict[str, Any]) -> str:
    """Checksum every file already in the cell directory, write ``manifest.json`` last, return its own sha256."""
    files = {
        str(path.relative_to(directory)): sha256_file(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path.name not in (MANIFEST_FILE, f"{MANIFEST_FILE}.tmp")
    }
    document = {"schema": MANIFEST_SCHEMA, **identity, "files": files}
    temporary = directory / f"{MANIFEST_FILE}.tmp"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(directory / MANIFEST_FILE)
    return sha256_file(directory / MANIFEST_FILE)


def verify_manifest(directory: Path) -> list[str]:
    """Problems with a cell directory: an empty list means complete and unmodified."""
    path = directory / MANIFEST_FILE
    if not path.is_file():
        return ["no manifest"]
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"manifest unreadable: {exc}"]
    problems = []
    for relative, expected in document.get("files", {}).items():
        file = directory / relative
        if not file.is_file():
            problems.append(f"missing {relative}")
        elif sha256_file(file) != expected:
            problems.append(f"changed {relative}")
    return problems


class ResultsTable:
    """Append-only JSONL, latest row per cell wins; ``write_csv`` renders the table with fixed columns."""

    def __init__(self, directory: Path) -> None:
        self._jsonl = directory / "results.jsonl"
        self._csv = directory / "results.csv"
        directory.mkdir(parents=True, exist_ok=True)

    def append(self, row: ResultRow) -> None:
        line = json.dumps(row.flat(), sort_keys=True) + "\n"
        with self._jsonl.open("a", encoding="utf-8") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def rows(self) -> list[dict[str, Any]]:
        if not self._jsonl.is_file():
            return []
        latest: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for line in self._jsonl.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                latest[(row["stage"], row["app"], row["pass"], row["problem"])] = row
        return list(latest.values())

    def write_csv(self) -> Path:
        rows = self.rows()
        columns = [{"schema_": "schema", "pass_": "pass"}.get(c, c) for c in ResultRow.model_fields]
        flat_columns: list[str] = []
        for column in columns:
            if column in ("online", "offline"):
                flat_columns.extend(f"{column}_{key}" for key in TokenRow.model_fields)
            else:
                flat_columns.append(column)
        with self._csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=flat_columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        return self._csv
