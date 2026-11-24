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


def _events(series: Series) -> list[tuple[str, object]]:
    return [("commit", e.commit) if e.kind == "commit" else ("fault", e.fault) for e in series.events]


def test_with_no_events_each_position_pushes_one_commit() -> None:
    series = Series.from_dict({k: v for k, v in SPEC.items() if k != "faults"})
    assert _events(series) == [("commit", 1), ("commit", 2), ("commit", 3)]


def test_a_fault_listed_after_a_commit_is_its_own_event_right_after_it() -> None:
    series = Series.from_dict(SPEC)
    assert _events(series) == [("commit", 1), ("commit", 2), ("fault", "some-fault"), ("commit", 3)]
    assert [series.commits_before(p) for p in range(1, 5)] == [1, 2, 2, 3]
    assert series.fault_ids == ("some-fault",)


def test_events_put_faults_anywhere_and_several_after_one_commit() -> None:
    events = [{"fault": "f0"}, {"commit": 1}, {"fault": "f1"}, {"fault": "f2"}, {"commit": 2}, {"commit": 3}]
    series = Series.from_dict({**{k: v for k, v in SPEC.items() if k != "faults"}, "events": events})
    assert _events(series) == [
        ("fault", "f0"),
        ("commit", 1),
        ("fault", "f1"),
        ("fault", "f2"),
        ("commit", 2),
        ("commit", 3),
    ]
    assert series.event(4).fault == "f2"
    assert series.commits_before(1) == 0
    assert series.commits_before(4) == 1
    assert series.offset(3) == 90  # the schedule is per commit, not per position
    with pytest.raises(ValueError, match="position 7"):
        series.event(7)


@pytest.mark.parametrize(
    ("events", "match"),
    [
        ([{"commit": 1}, {"commit": 3}], "every commit exactly once"),
        ([{"commit": 1}, {"commit": 2}, {"commit": 2}, {"commit": 3}], "every commit exactly once"),
        ([{"commit": 2}, {"commit": 1}, {"commit": 3}], "in order"),
        ([{"commit": 1}, {"commit": 2}, {"commit": 3}, {"commit": 4}], "outside the series"),
        ([{"commit": 1, "fault": "x"}, {"commit": 2}, {"commit": 3}], "exactly one of commit or fault"),
        ([{"commit": 1}, {"fault": ""}, {"commit": 2}, {"commit": 3}], "fault id"),
        ([{"commit": 1}, {"pause": 3}, {"commit": 2}, {"commit": 3}], "exactly one of commit or fault"),
    ],
)
def test_invalid_events_are_refused_at_load(events: list[dict[str, object]], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Series.from_dict({**{k: v for k, v in SPEC.items() if k != "faults"}, "events": events})


def test_events_and_faults_together_are_refused() -> None:
    with pytest.raises(ValueError, match="events or faults"):
        Series.from_dict({**SPEC, "events": [{"commit": 1}, {"commit": 2}, {"commit": 3}]})
