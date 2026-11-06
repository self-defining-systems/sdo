"""The toy operability assertions: report green, and hold routing, over synthetic JSON."""

from __future__ import annotations

import pytest

from benchmarks.sregym.fastloop import toy_operability as op
from tests.unit.benchmarks.sregym.fastloop.operability_fixtures import COMMIT, configmap, green_read

ACCEPTED = {COMMIT}


def test_green_report_has_no_violations():
    assert op.report_violations(green_read(items={"O3": "note", "O4": "skipped"}), ACCEPTED) == []


@pytest.mark.parametrize(
    ("read", "needle"),
    [
        (None, "absent"),
        (green_read({"O4": "fail"}), "O4 is fail"),
        (green_read({"O2": "pending"}), "O2 is pending"),
        (green_read(drop=("O5",)), "O5 is missing"),
        (green_read(stage="deployment"), "stage is 'deployment'"),
        (green_read(commit="zzz"), "not an accepted commit"),
    ],
)
def test_report_violations(read, needle):
    assert any(needle in v for v in op.report_violations(read, ACCEPTED))


def test_summary_and_report_must_agree():
    cm = configmap()
    cm["data"]["report.json"] = cm["data"]["report.json"].replace(COMMIT, "other")
    assert any("disagree" in v for v in op.report_violations(op.parse_operability(cm), {COMMIT, "other"}))


def test_item_without_checks_is_not_green():
    cm = configmap()
    cm["data"]["report.json"] = cm["data"]["report.json"].replace(
        '"checks": [{"id": "O3.x", "status": "pass"}]', '"checks": []'
    )
    assert any("O3 has no checks" in v for v in op.report_violations(op.parse_operability(cm), ACCEPTED))


@pytest.mark.parametrize("missing", ["summary.json", "report.json"])
def test_missing_key_is_an_error_not_an_absent_report(missing):
    cm = configmap()
    del cm["data"][missing]
    with pytest.raises(ValueError, match=missing):
        op.parse_operability(cm)


def test_hold_routing_prefix():
    r = op.parse_hold_routing("cause=rollout owner=responder items=O1 rollout: workload toy not ready")
    assert r == op.HoldRouting("rollout", "responder", ("O1",), "rollout")
    assert op.parse_hold_routing("cause=environment owner=human items=- ref_rewritten: x").items == ()
    assert op.parse_hold_routing("rollout: plain") is None


def test_routing_expectation():
    ok = "cause=source_fault owner=responder items=O4 health: floor fired"
    assert op.routing_violations(ok, op.BROKEN_EXPECTATION) == []
    bad = op.routing_violations("cause=patch owner=deployer items=O1 merge_conflict: x", op.BROKEN_EXPECTATION)
    assert len(bad) == 2
    assert op.routing_violations("health: x", op.BROKEN_EXPECTATION)


@pytest.mark.parametrize(
    "reason",
    [
        "cause=patch owner=deployer items=O1 merge_conflict (Dockerfile)",
        "cause=coverage owner=designer items=O3 shape_change: component added",
        "cause=scope owner=human items=O6 memory_touched: .sdo/goal/goal.md",
        "cause=environment owner=human items=- ref_rewritten: not an ancestor",
    ],
)
def test_every_documented_hold_route_matches_the_table(reason):
    assert op.table_violations(reason) == []


def test_table_rejects_a_route_outside_it():
    assert op.table_violations("cause=patch owner=human items=O1 merge_conflict: x")
    assert op.hold_kind("cause=render owner=deployer items=O1 render (a, b)") == "render"
