"""Grading rows name the operator build; on the dev-mount path it hashes the mounted operator source."""

from datetime import datetime, timezone
from pathlib import Path

from benchmarks.sregym.fastloop import dev
from benchmarks.sregym.fastloop.records import IncidentRecord, append_record, load_records


def _worktree(tmp_path: Path, script: str) -> Path:
    root = tmp_path / "source"
    (root / "sdo/operator").mkdir(parents=True)
    (root / ".fastloop").mkdir()
    binary = root / ".fastloop/sdo-operator"
    binary.write_text(script)
    binary.chmod(0o755)
    return root


def test_operator_build_asks_the_binary_to_hash_the_mounted_source(tmp_path: Path):
    root = _worktree(tmp_path, '#!/bin/sh\n[ "$1" = build-id ] && echo "src-$2"\n')
    assert dev.operator_build(root) == f"src-{root}/sdo/operator"


def test_operator_build_is_none_without_a_prepared_binary(tmp_path: Path):
    assert dev.operator_build(tmp_path) is None
    root = _worktree(tmp_path, "#!/bin/sh\nexit 1\n")
    assert dev.operator_build(root) is None


def test_incident_rows_carry_operator_build_through_both_result_files(tmp_path: Path):
    when = datetime(2026, 11, 14, tzinfo=timezone.utc)
    record = IncidentRecord(
        run_id="r",
        index=0,
        agent="sdo",
        problem_id="p",
        model="m",
        operator_build="src-0123456789abcdef",
        injection={"state": "injected", "started_at": when, "finished_at": when},
    )
    path = tmp_path / "incidents.jsonl"
    append_record(path, record)
    assert load_records(path)[0].operator_build == "src-0123456789abcdef"
    assert "src-0123456789abcdef" in path.with_suffix(".csv").read_text()
