"""Live-run assembly: config tables, the no-store guard and factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.eval.__main__ import load_config
from benchmarks.sregym.eval.live import NoStore, build_store, load_factory

if TYPE_CHECKING:
    from pathlib import Path


def test_load_config_ignores_the_live_tables(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text(
        f"""
stage = "s"
runs_root = "{tmp_path}"
gate = "x:y"

[[apps]]
name = "hotel"
problems = ["a"]
sdo_lanes = ["h1"]
vanilla_lanes = ["v1"]

[row]
code_root = "/x"

[store]
root = "/y"
"""
    )
    assert load_config(path).stage == "s"


def test_without_a_store_a_snapshot_is_a_configuration_error() -> None:
    store, hook = build_store(None)
    assert isinstance(store, NoStore)
    assert hook is None
    with pytest.raises(ValueError, match=r"\[store\]"):
        store.resolve("hotel", "base")


def test_factories_are_module_attr() -> None:
    assert load_factory("json:dumps")({}) == "{}"
    with pytest.raises(ValueError, match="module:attr"):
        load_factory("json")
