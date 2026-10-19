"""Benchmarks read operational memory through ``sdo-operator memory``, not by recomputing paths."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.memory_cli import (
    artifact_ids,
    listing_arguments,
    outcomes_arguments,
    parse_listing,
    parse_outcomes,
    read_listing,
    read_outcomes,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "sdo"
GOOD_MEMORY = REPO_ROOT / "sdo" / "operator" / "memory" / "validate" / "testdata" / "good"


@pytest.fixture(scope="module")
def operator_bin(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if shutil.which("go") is None:
        pytest.skip("the Go toolchain is required to build sdo-operator")
    binary = tmp_path_factory.mktemp("operator") / "sdo-operator"
    subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/sdo-operator"],
        cwd=REPO_ROOT / "sdo" / "operator",
        check=True,
        capture_output=True,
    )
    return binary


@pytest.fixture
def cli(operator_bin: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SDO_OPERATOR_BIN", str(operator_bin))


def test_arguments_name_the_memory_subcommands() -> None:
    assert outcomes_arguments("/repo") == ["memory", "outcomes", "--root", "/repo", "--json"]
    assert listing_arguments(Path("/repo")) == ["memory", "ls", "--root", "/repo", "--json"]


def test_parses_cli_output_into_generated_messages() -> None:
    outcomes = parse_outcomes(
        '{"outcomes": [{"schema_version": 1, "incident_id": "i-1", "classification": "success"}]}'
    )
    assert [(o.incident_id, o.classification) for o in outcomes] == [("i-1", "success")]
    listing = parse_listing(
        '{"artifacts": [{"kind": "detector", "id": "a", "path": "p", "detector_class": "incident"},'
        ' {"kind": "detector", "id": "b", "path": "q", "detector_class": "health"},'
        ' {"kind": "playbook", "id": "c", "path": "r"}]}'
    )
    assert artifact_ids(listing, "detector") == ["a", "b"]
    assert artifact_ids(listing, "detector", detector_class="incident") == ["a"]
    assert artifact_ids(listing, "playbook") == ["c"]


@pytest.mark.usefixtures("cli")
def test_reads_stored_outcomes_through_the_cli() -> None:
    outcomes = read_outcomes(FIXTURES / "outcome_repo")
    assert [(o.incident_id, o.classification) for o in outcomes] == [("incident-1", "success")]


@pytest.mark.usefixtures("cli")
def test_lists_artifacts_through_the_cli() -> None:
    listing = read_listing(GOOD_MEMORY)
    assert artifact_ids(listing, "detector", detector_class="incident") == ["cache"]
    assert artifact_ids(listing, "detector", detector_class="health") == ["health"]
    assert artifact_ids(listing, "playbook") == ["base"]
