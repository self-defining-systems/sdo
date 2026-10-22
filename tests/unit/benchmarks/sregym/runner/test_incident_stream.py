"""The deterministic incident stream: same seed, same incidents, learnable structure."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import tomllib

from benchmarks.sregym.runner.incident_stream import (
    HOTEL_CATALOG,
    NOVEL_COUNT,
    STREAM_LENGTH,
    STREAM_OPENING,
    STREAM_SEED,
    FaultFamily,
    StreamIncident,
    generate_stream,
    render_baseline_toml,
    render_manifest,
    render_sdo_pipeline_toml,
    write_configs,
)
from benchmarks.sregym.runner.incident_stream import main as incident_stream_main

if TYPE_CHECKING:
    from pathlib import Path

PILOT = 8


def _stream() -> list[StreamIncident]:
    return generate_stream(STREAM_SEED, STREAM_LENGTH, opening=STREAM_OPENING)


def test_the_same_seed_gives_the_same_stream_and_another_seed_a_different_one() -> None:
    assert _stream() == _stream()
    assert [i.problem_id for i in generate_stream(STREAM_SEED + 1, STREAM_LENGTH, opening=STREAM_OPENING)] != [
        i.problem_id for i in _stream()
    ]


def test_the_stream_starts_with_the_pinned_opening() -> None:
    assert tuple(i.problem_id for i in _stream()[: len(STREAM_OPENING)]) == STREAM_OPENING


def test_an_opening_outside_the_catalog_is_rejected() -> None:
    with pytest.raises(ValueError, match="opening"):
        generate_stream(STREAM_SEED, STREAM_LENGTH, opening=("not_a_problem",))


def test_every_repeat_and_variant_follows_its_first_occurrence() -> None:
    seen_problems: set[str] = set()
    seen_families: set[str] = set()
    for incident in _stream():
        if incident.kind == "exact":
            assert incident.problem_id in seen_problems
        elif incident.kind == "variant":
            assert incident.family in seen_families
            assert incident.problem_id not in seen_problems
        else:
            assert incident.kind in {"first", "novel"}
            assert incident.family not in seen_families
        seen_problems.add(incident.problem_id)
        seen_families.add(incident.family)


def test_the_stream_mixes_every_incident_type_and_its_pilot_prefix_already_does() -> None:
    kinds = [i.kind for i in _stream()]
    assert {"first", "exact", "variant", "novel"} <= set(kinds)
    assert kinds.count("first") == len(HOTEL_CATALOG.core)
    assert kinds.count("novel") == NOVEL_COUNT
    assert {"first", "exact", "variant"} <= {i.kind for i in _stream()[:PILOT]}
    assert "first" not in kinds[PILOT:]


def test_every_core_fault_recurs_exactly_after_its_first_occurrence() -> None:
    exact = {i.problem_id for i in _stream() if i.kind == "exact"}
    assert {family.problem_id for family in HOTEL_CATALOG.core} <= exact


def test_no_fault_is_injected_twice_in_a_row() -> None:
    ids = [i.problem_id for i in _stream()]
    assert all(a != b for a, b in zip(ids, ids[1:], strict=False))


def test_every_problem_id_is_a_known_hotel_reservation_registration() -> None:
    known = {p for family in HOTEL_CATALOG.core + HOTEL_CATALOG.novel for p in family.all_problem_ids}
    assert {i.problem_id for i in _stream()} <= known


def test_a_catalog_family_needs_a_base_problem() -> None:
    with pytest.raises(ValueError, match="problem_id"):
        FaultFamily(name="x", problem_id="", variants=())


def test_the_stream_needs_enough_variant_supply_for_its_length() -> None:
    with pytest.raises(ValueError, match="length"):
        generate_stream(STREAM_SEED, 3)


def test_the_manifest_records_index_problem_kind_and_family() -> None:
    manifest = json.loads(render_manifest(_stream()))
    assert manifest["seed"] == STREAM_SEED
    assert [row["index"] for row in manifest["incidents"]] == list(range(STREAM_LENGTH))
    assert set(manifest["incidents"][0]) == {"index", "problem_id", "kind", "family"}


def test_the_sdo_pipeline_has_one_chained_stage_per_incident_on_one_persistent_controller() -> None:
    document = tomllib.loads(render_sdo_pipeline_toml(_stream(), name="t"))
    stages = document["stages"]
    assert [s["runner"]["problems"] for s in stages] == [[i.problem_id] for i in _stream()]
    assert stages[0]["chain_application_workspace"] is False
    assert all(s["chain_application_workspace"] for s in stages[1:])
    assert document["defaults"]["agent_config"]["sdo_codex"]["persistent_controller"] is True
    assert document["defaults"]["require_strict_receipt"] is True
    assert document["defaults"]["allow_failed_verdicts"] is True


def test_both_arms_enable_the_source_build_cache_identically() -> None:
    sdo = tomllib.loads(render_sdo_pipeline_toml(_stream(), name="t"))["defaults"]["env"]
    baseline = tomllib.loads(render_baseline_toml(_stream()[:PILOT]))["runner"]["env"]
    for key in ("source_build_cache", "fast_namespace_teardown", "preserve_infrastructure"):
        assert sdo[key] is True
        assert baseline[key] is True


def test_the_baseline_lists_the_identical_problems_in_order() -> None:
    document = tomllib.loads(render_baseline_toml(_stream()[:PILOT]))
    assert document["runner"]["problems"] == [i.problem_id for i in _stream()[:PILOT]]
    assert document["runner"]["agent"] == "codex"


def test_write_configs_emits_parseable_stream_configs_and_a_matching_manifest(tmp_path: Path) -> None:
    out = tmp_path / "generated"
    written = {path.name: path for path in write_configs(out)}

    assert set(written) == {
        "sdo_codex_luna_stream.toml",
        "sdo_codex_luna_stream_pilot.toml",
        "codex_luna_stream_baseline_1_8.toml",
        "codex_luna_stream_baseline_5_8.toml",
        "codex_luna_stream_baseline_9_24.toml",
        "stream_learning_curve_manifest.json",
    }
    stream = _stream()
    assert tomllib.loads(written["sdo_codex_luna_stream.toml"].read_text(encoding="utf-8"))["stages"][0]["runner"][
        "problems"
    ] == [stream[0].problem_id]
    assert tomllib.loads(written["codex_luna_stream_baseline_9_24.toml"].read_text(encoding="utf-8"))["runner"][
        "problems"
    ] == [i.problem_id for i in stream[PILOT:]]
    assert written["stream_learning_curve_manifest.json"].read_text(encoding="utf-8") == render_manifest(stream)


def test_the_generator_cli_requires_an_output_directory_and_writes_there(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        incident_stream_main([])
    assert incident_stream_main(["--out-dir", str(tmp_path / "out")]) == 0
    assert (tmp_path / "out" / "stream_learning_curve_manifest.json").is_file()
