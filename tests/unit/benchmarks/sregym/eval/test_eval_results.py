"""Manifest with checksums, and the cross-cell results table."""

from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING

from benchmarks.sregym.eval.results import (
    MANIFEST_FILE,
    ResultRow,
    ResultsTable,
    TokenRow,
    verify_manifest,
    write_manifest,
)

if TYPE_CHECKING:
    from pathlib import Path


def row(**overrides: object) -> ResultRow:
    values: dict[str, object] = {
        "stage": "small",
        "app": "hotel",
        "pass_": "pass1",
        "problem": "a",
        "arm": "sdo",
        "status": "graded",
    }
    return ResultRow(**{**values, **overrides})  # type: ignore[arg-type]


def test_manifest_checksums_every_file_and_detects_change_and_loss(tmp_path: Path) -> None:
    (tmp_path / "timings.json").write_text("{}")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "probes.jsonl").write_text("x\n")
    digest = write_manifest(tmp_path, {"cell": "hotel/pass1/a"})
    document = json.loads((tmp_path / MANIFEST_FILE).read_text())
    assert set(document["files"]) == {"timings.json", "sub/probes.jsonl"}
    assert document["cell"] == "hotel/pass1/a"
    assert len(digest) == 64
    assert verify_manifest(tmp_path) == []
    (tmp_path / "timings.json").write_text('{"x": 1}')
    (tmp_path / "sub" / "probes.jsonl").unlink()
    assert sorted(verify_manifest(tmp_path)) == ["changed timings.json", "missing sub/probes.jsonl"]


def test_a_directory_without_manifest_is_incomplete(tmp_path: Path) -> None:
    assert verify_manifest(tmp_path) == ["no manifest"]


def test_latest_row_per_cell_wins(tmp_path: Path) -> None:
    table = ResultsTable(tmp_path)
    table.append(row(status="error", reason="lane lost"))
    table.append(row(status="graded", success=True))
    table.append(row(problem="b"))
    rows = table.rows()
    assert len(rows) == 2
    assert next(r for r in rows if r["problem"] == "a")["status"] == "graded"


def test_csv_has_fixed_columns_with_flattened_tokens(tmp_path: Path) -> None:
    table = ResultsTable(tmp_path)
    table.append(row(online=TokenRow(output=5, usd=0.5), no_page=False, time_to_mitigate_s=42.0))
    path = table.write_csv()
    with path.open() as handle:
        loaded = list(csv.DictReader(handle))
    assert loaded[0]["online_output"] == "5"
    assert loaded[0]["offline_output"] == ""
    assert loaded[0]["time_to_mitigate_s"] == "42.0"
    assert loaded[0]["pass"] == "pass1"
    assert "schema" in loaded[0]
