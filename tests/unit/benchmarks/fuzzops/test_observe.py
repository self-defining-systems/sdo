"""The exec:sdo watcher: SDO's steps become fuzzops actions and its diagnosis a fuzzops report."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from benchmarks.fuzzops.observe import Watcher, classify, component, report_from_result

START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _at(seconds: int) -> str:
    return (START + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _finding() -> dict[str, Any]:
    return {
        "detector_id": "workload-ready",
        "summary": "Deployment web has 0/1 ready pods: ImagePullBackOff",
        "primary_resource": {"kind": "Deployment", "name": "web"},
    }


def _result() -> dict[str, Any]:
    return {
        "incident_id": "inc-1",
        "status": "mitigated",
        "confirmed_root_causes": [
            {
                "summary": "web was rolled out with an image tag that cannot be pulled",
                "resources": [{"kind": "Deployment", "name": "web"}],
            }
        ],
        "repair_actions": [
            {
                "action_id": "a1",
                "kind": "patch",
                "target": "Deployment/web",
                "summary": "restore the source image",
                "success": True,
            }
        ],
    }


@pytest.mark.parametrize(
    ("text", "mechanism"),
    [
        ("pods are in ImagePullBackOff", "image-wrong"),
        ("container was OOMKilled under its memory limit", "resource-exhausted"),
        ("Service targetPort 8081 does not match the container port", "port-mismatch"),
        ("memcached holds garbage cache entries for rates", "data-corrupt"),
        ("nothing recognisable", ""),
    ],
)
def test_classify_maps_root_cause_text_to_fuzzops_mechanisms(text: str, mechanism: str) -> None:
    assert classify(text)[0] == mechanism


def test_component_uses_short_kind_names() -> None:
    assert component({"kind": "Deployment", "name": "web"}) == "deploy/web"
    assert component({"kind": "Service", "name": "web"}) == "svc/web"
    assert component({"kind": "Widget", "name": "w"}) == "widget/w"
    assert component({"kind": "Deployment"}) == ""


def test_report_from_result_keeps_the_raw_root_cause() -> None:
    report = report_from_result(_result())
    assert report == {
        "component": "deploy/web",
        "location": "delivery",
        "fault_type": "unavailable",
        "mechanism": "image-wrong",
        "summary": "SDO responder: web was rolled out with an image tag that cannot be pulled",
    }
    assert report_from_result({"confirmed_root_causes": []}) is None


def test_watcher_reports_each_step_once_and_closes() -> None:
    w = Watcher(started=START)
    base = {
        "incident_open": True,
        "incident_detected_at": _at(5),
        "incident_request": {"incident_id": "inc-1", "findings": [_finding()]},
    }
    lines, report = w.step(base)
    assert lines == [
        "sdo: incident opened at 12:00:05 (workload-ready: Deployment web has 0/1 ready pods: ImagePullBackOff)"
    ]
    assert report is not None
    assert report["component"] == "deploy/web"
    assert report["summary"].startswith("SDO detector")
    assert w.step(base) == ([], None)

    dispatched = {**base, "incident_dispatched_at": _at(10)}
    assert w.step(dispatched)[0] == ["sdo: responder dispatched"]

    done = {**dispatched, "incident_result": _result(), "responder_done": True}
    lines, report = w.step(done)
    assert lines == [
        "sdo repair (ok): patch Deployment/web: restore the source image",
        "sdo diagnosis: SDO responder: web was rolled out with an image tag that cannot be pulled",
        "sdo: responder done (mitigated)",
    ]
    assert report is not None
    assert report["summary"].startswith("SDO responder")
    assert w.step(done) == ([], None)

    closed = {"incident_open": False, "closure_receipt": {"incident_id": "inc-1"}}
    assert w.step(closed)[0] == ["sdo: incident inc-1 closed"]
    assert w.closed


def test_watcher_ignores_incidents_detected_before_it_started() -> None:
    w = Watcher(started=START)
    old = {
        "incident_open": True,
        "incident_detected_at": _at(-30),
        "incident_request": {"incident_id": "old", "findings": [_finding()]},
        "incident_result": _result(),
        "closure_receipt": {"incident_id": "old"},
    }
    assert w.step(old) == ([], None)
    assert not w.closed


def test_watcher_reads_lower_camel_protojson() -> None:
    w = Watcher(started=START)
    lines, _ = w.step(
        {
            "incidentOpen": True,
            "incidentDetectedAt": _at(1),
            "incidentRequest": {"incidentId": "inc-2", "findings": [{"detectorId": "d", "summary": "s"}]},
        }
    )
    assert lines == ["sdo: incident opened at 12:00:01 (d: s)"]


def test_typed_cause_class_and_reach_reach_the_report() -> None:
    result = {
        "confirmed_root_causes": [
            {
                "summary": "link to the cache is slow",
                "cause_class": "capacity-exhausted",
                "reach": "in-scope",
                "resources": [{"kind": "Deployment", "name": "cache"}],
            },
            {
                "summary": "upstream network is down",
                "cause_class": "network-loss",
                "reach": "out-of-reach",
                "resources": [{"kind": "Service", "name": "upstream"}],
            },
        ]
    }
    report = report_from_result(result)
    assert report is not None
    assert report["mechanism"] == "resource-exhausted"
    assert report["sdo_cause_class"] == "capacity-exhausted"
    assert report["reach"] == "in-scope"
    assert [c["mechanism"] for c in report["causes"]] == ["resource-exhausted", "network-loss"]


def test_watcher_report_carries_lifecycle_stamps_from_sdo_records() -> None:
    w = Watcher(started=START)
    base = {
        "incident_open": True,
        "incident_detected_at": _at(5),
        "incident_request": {"incident_id": "inc-1", "findings": [_finding()]},
    }
    _, report = w.step(base)
    assert report is not None
    assert report["detected_at"] == _at(5)
    assert "localized_at" in report
    result = _result()
    result["repair_actions"][0].update({"started_at": _at(20), "completed_at": _at(25)})
    result["confirmed_root_causes"][0]["reach"] = "out-of-reach"
    _, report = w.step({**base, "incident_result": result, "responder_done": True})
    assert report is not None
    assert report["mitigation_started_at"] == _at(20)
    assert report["mitigated_at"] == _at(25)
    assert "diagnosed_at" in report
    assert "escalated_at" in report
    assert report["summary"].startswith("SDO responder")
    assert w.step({**base, "incident_result": result, "responder_done": True})[1] is None


def test_timeline_events_follow_stamps_once_and_carry_the_cause() -> None:
    from datetime import UTC, datetime

    from benchmarks.fuzzops.observe import Watcher, timeline_events

    w = Watcher(started=datetime.now(UTC))
    w.stamps = {"detected_at": "t1"}
    emitted: set[str] = set()
    assert timeline_events(w, emitted) == [{"event": "detected", "at": "t1"}]
    assert timeline_events(w, emitted) == []
    w.cause = {"component": "sts/x", "mechanism": "scaled-down", "summary": "s", "reach": "out-of-reach"}
    w.stamps.update({"diagnosed_at": "t2", "escalated_at": "t3"})
    events = timeline_events(w, emitted)
    assert [e["event"] for e in events] == ["diagnosed", "escalated"]
    assert events[0]["component"] == "sts/x"
    assert events[1]["reach"] == "out-of-reach"
