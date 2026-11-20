"""The series spec is data: load, validate, and turn one commit into a replay config."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.common.replay.series import Series

if TYPE_CHECKING:
    from pathlib import Path

SPEC = {
    "name": "demo",
    "description": "three synthetic commits",
    "source": "synthetic",
    "upstream": {"repo": "/srv/up.git", "base": "b0", "commits": ["c1", "c2", "c3"]},
    "schedule": [0, 30, 90],
    "faults": {"2": "some-fault"},
}


def test_config_chains_each_commit_onto_the_previous() -> None:
    series = Series.from_dict(SPEC)
    first, second = series.config(1, "remote", "sdo-source"), series.config(2, "remote", "sdo-source")
    assert (first.base, first.commits) == ("b0", ("c1",))
    assert (second.base, second.commits) == ("c1", ("c2",))
    assert series.offset(3) == 90
    assert series.faults == {2: "some-fault"}


def test_load_reads_a_file_and_names_it_on_error(tmp_path: Path) -> None:
    good = tmp_path / "s.json"
    good.write_text(json.dumps(SPEC))
    assert Series.load(good).name == "demo"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({**SPEC, "colour": "red"}))
    with pytest.raises(ValueError, match="bad.json.*colour"):
        Series.load(bad)


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ({"faults": {"4": "x"}}, "outside the series"),
        ({"schedule": [0, 30]}, "one offset"),
        ({"source": ""}, "source"),
        ({"on_conflict": "retry"}, "on_conflict"),
    ],
)
def test_invalid_series_is_refused_at_load(change: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Series.from_dict({**SPEC, **change})
