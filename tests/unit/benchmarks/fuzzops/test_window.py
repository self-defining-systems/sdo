"""Which incidents belong to a fuzzops run: detected in the window, or open at the inject on the faulted component."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from benchmarks.fuzzops.observe import Watcher
from benchmarks.fuzzops.table import outcomes_in
from benchmarks.fuzzops.window import concerns, fault_targets

START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
INJECT = START + timedelta(seconds=50)
END = START + timedelta(seconds=720)


def _at(seconds: int) -> str:
    return (START + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _outcome(detected: int, completed: int | None, **refs: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {"incident_id": f"i{detected}", "timestamps": {"detected_at": _at(detected)}}
    if completed is not None:
        doc["timestamps"]["completed_at"] = _at(completed)
    doc["findings"] = refs.get("findings", [])
    doc["confirmed_root_causes"] = refs.get("causes", [])
    doc["repair_actions"] = refs.get("actions", [])
    return doc


def _finding(name: str, kind: str = "Service", related: tuple[str, ...] = ()) -> dict[str, Any]:
    return {
        "primary_resource": {"kind": kind, "name": name},
        "related_resources": [{"kind": "Service", "name": n} for n in related],
    }


TARGETS = fault_targets("unreachable@dependency:sts/db:5432", "sts/db")


@pytest.mark.parametrize(
    ("fault", "component", "names"),
    [
        ("unreachable@dependency:sts/db:5432", "", {"db"}),
        ("image@delivery:deploy/web/main", "deploy/web", {"web"}),
        ("cfg@config:deploy/web/c/object:configmap/web-cfg", "", {"web", "web-cfg"}),
        ("loss@link:web->store:8333", "deploy/web", {"web", "store"}),
        ("noop", "", set()),
    ],
)
def test_fault_targets_read_resource_names_from_the_instance(fault: str, component: str, names: set[str]) -> None:
    assert fault_targets(fault, component) == names


def test_incident_opened_before_the_inject_and_open_at_it_belongs_by_resource_ref() -> None:
    open_at_inject = _outcome(-17, 300, findings=[_finding("db")])
    got = outcomes_in([open_at_inject], START + timedelta(seconds=60), END, INJECT, TARGETS)
    assert got == [open_at_inject]


def test_open_incident_belongs_through_causes_or_repair_actions() -> None:
    by_cause = _outcome(-17, 300, causes=[{"resources": [{"kind": "StatefulSet", "name": "db"}]}])
    by_action = _outcome(-18, None, actions=[{"started_at": _at(80), "resources": [{"kind": "Pod", "name": "db-0"}]}])
    assert outcomes_in([by_cause, by_action], START + timedelta(seconds=60), END, INJECT, TARGETS) == [
        by_action,
        by_cause,
    ]


@pytest.mark.parametrize(
    "doc",
    [
        _outcome(-17, 300, findings=[_finding("cache")]),  # another component
        _outcome(-17, 30, findings=[_finding("db")]),  # completed before the inject
        _outcome(-17, 300, actions=[{"started_at": _at(10), "resources": [{"kind": "Service", "name": "db"}]}]),
    ],
)
def test_open_incident_not_on_the_faulted_component_or_over_before_the_inject_is_not_the_runs(
    doc: dict[str, Any],
) -> None:
    assert outcomes_in([doc], START + timedelta(seconds=60), END, INJECT, TARGETS) == []


def test_free_text_does_not_attribute_an_incident() -> None:
    doc = _outcome(-17, 300, findings=[{**_finding("cache"), "summary": "sts/db is unreachable"}])
    assert outcomes_in([doc], START + timedelta(seconds=60), END, INJECT, TARGETS) == []


def test_incident_detected_in_the_window_still_belongs_without_targets() -> None:
    inside = _outcome(100, 200)
    assert outcomes_in([inside, _outcome(-300, 10)], START, END) == [inside]


def test_concerns_ignores_documents_without_refs() -> None:
    assert not concerns({}, TARGETS)
    assert not concerns(_outcome(0, None, findings=[_finding("db")]), frozenset())


def test_watcher_adopts_an_open_incident_older_than_its_lookback_that_names_the_faulted_component() -> None:
    state = {
        "incident_open": True,
        "incident_detected_at": _at(-200),
        "incident_request": {"incident_id": "old", "findings": [_finding("db", "Service")]},
    }
    other = {**state, "incident_request": {"incident_id": "old", "findings": [_finding("cache")]}}
    assert Watcher(started=START, targets=TARGETS).step(state)[0]
    assert Watcher(started=START, targets=TARGETS).step(other) == ([], None)
    assert Watcher(started=START).step(state) == ([], None)
