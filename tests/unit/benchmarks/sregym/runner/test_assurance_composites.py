"""The first-party composite catalog agrees with the SREGym fork's composite table.

``benchmarks/sregym/experiments/assurance/composites.toml`` carries the assurance
metadata; ``third_party/sregym/.../composite_specs.json`` defines the problems the
conductor and the fast-loop worker build. Both are read as data: production and
benchmark code never import SREGym for this.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import tomllib

ROOT = Path(__file__).resolve().parents[5]
CATALOG = ROOT / "benchmarks" / "sregym" / "experiments" / "assurance" / "composites.toml"
SPECS = ROOT / "third_party" / "sregym" / "sregym" / "conductor" / "problems" / "composite_specs.json"
PHASE_ONE = {
    "K1": "composite_policy_and_rate_configmap_hotel_reservation",
    "K2": "composite_frontend_selector_and_readiness_hotel_reservation",
}
CHANGES = {"added", "removed", "modified"}


def _catalog() -> list[dict]:
    raw = tomllib.loads(CATALOG.read_text(encoding="utf-8"))
    assert raw["schema"] == "sdo.assurance-composites/v1"
    return raw["composite"]


def _specs() -> dict:
    if not SPECS.is_file():
        pytest.skip("SREGym submodule is not checked out at a commit with composite problems")
    return json.loads(SPECS.read_text(encoding="utf-8"))


def _component_key(component: dict) -> tuple:
    return (
        component["role"],
        component.get("problem"),
        component.get("decoy"),
        json.dumps(component.get("params", {}), sort_keys=True),
    )


def test_every_catalog_composite_is_a_submodule_composite_with_the_same_components_in_order() -> None:
    specs = _specs()
    catalog = _catalog()

    assert {entry["problem_id"] for entry in catalog} <= set(specs)
    for entry in catalog:
        expected = [_component_key(component) for component in specs[entry["problem_id"]]["components"]]
        assert [_component_key(component) for component in entry["components"]] == expected, entry["id"]


def test_phase_one_holds_exactly_k1_and_k2() -> None:
    catalog = _catalog()

    assert {entry["id"]: entry["problem_id"] for entry in catalog if entry["phase"] == 1} == PHASE_ONE
    assert len({entry["id"] for entry in catalog}) == len(catalog)


@pytest.mark.parametrize("entry", _catalog(), ids=lambda entry: entry["id"])
def test_each_composite_documents_its_assurance_metadata(entry: dict) -> None:
    for field in ("difficulty", "correct_mitigation", "partial_fix_trap"):
        assert entry[field].strip(), field
    for component in entry["components"]:
        assert component["role"] in {"fault", "decoy"}
        assert component["target"]
    diff = entry["expected_diff"]
    assert diff, "a composite names its expected healthy-state diff"
    objects = [item["object"] for item in diff]
    assert len(set(objects)) == len(objects)
    for item in diff:
        kind, _, name = item["object"].partition("/")
        assert kind, item["object"]
        assert name, item["object"]
        assert item["change"] in CHANGES
        assert isinstance(item["required"], bool)
    has_decoy = any(component["role"] == "decoy" for component in entry["components"])
    assert any(item.get("decoy", False) for item in diff) is has_decoy
    assert sum(item["required"] and not item.get("decoy", False) for item in diff) >= sum(
        component["role"] == "fault" for component in entry["components"]
    )
