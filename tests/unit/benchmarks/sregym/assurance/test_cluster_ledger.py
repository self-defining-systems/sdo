from __future__ import annotations

import pytest

from benchmarks.sregym.assurance.cluster_ledger import DriftAssessmentUnavailable, DriftMismatch, drift_empty

DIGEST = "ab" * 32


def fingerprint(namespace: str, kind: str, name: str) -> str:
    return f"drift.unexplained/unexplained/{namespace}/{kind}/{name}/{DIGEST}"


def state(*rows: dict, iteration: int = 1) -> dict:
    return {"evaluation_iteration": iteration, "history": list(rows)}


def drift_row(status: str, *fingerprints: str, **extra: str) -> dict:
    return {"detector_id": "drift.unexplained", "status": status, "fingerprints": list(fingerprints), **extra}


def test_firing_drift_is_named_per_object_and_sorted() -> None:
    row = drift_row(
        "firing",
        fingerprint("shop", "Service", "redirect"),
        fingerprint("shop", "ConfigMap", "settings"),
        fingerprint("other", "ConfigMap", "elsewhere"),
    )

    assert drift_empty(state(row), ["shop"]) == [
        DriftMismatch("shop", "ConfigMap", "settings", fingerprint("shop", "ConfigMap", "settings")),
        DriftMismatch("shop", "Service", "redirect", fingerprint("shop", "Service", "redirect")),
    ]


def test_a_clear_drift_row_is_empty() -> None:
    assert drift_empty(state(drift_row("clear")), ["shop"]) == []


@pytest.mark.parametrize(
    "unevaluated",
    [
        {},
        {"evaluation_iteration": 0, "history": [drift_row("clear")]},
        {"evaluation_iteration": 3, "history": []},
        # The history is a 100-entry log over all detectors; a trimmed drift row reads as absent, not as clear.
        {"evaluation_iteration": 3, "history": [{"detector_id": "workload-health", "status": "clear"}]},
    ],
)
def test_an_assessment_the_controller_has_not_published_is_not_read_as_empty(unevaluated: dict) -> None:
    with pytest.raises(DriftAssessmentUnavailable, match="not published|evaluat"):
        drift_empty(unevaluated, ["shop"])


def test_a_firing_row_without_fingerprints_is_not_read_as_empty() -> None:
    with pytest.raises(DriftAssessmentUnavailable, match="firing"):
        drift_empty(state(drift_row("firing")), ["shop"])


def test_drift_outside_the_restored_namespaces_is_not_a_mismatch() -> None:
    assert drift_empty(state(drift_row("firing", fingerprint("other", "ConfigMap", "x"))), ["shop"]) == []


@pytest.mark.parametrize("status", ["pending", "error"])
def test_an_unfinished_assessment_is_not_read_as_empty(status: str) -> None:
    with pytest.raises(DriftAssessmentUnavailable, match=status):
        drift_empty(state(drift_row(status, reason="window incomplete")), ["shop"])


def test_an_unreadable_fingerprint_is_not_dropped() -> None:
    with pytest.raises(DriftAssessmentUnavailable, match="unreadable"):
        drift_empty(state(drift_row("firing", "drift.unexplained/unexplained/shop")), ["shop"])


def test_a_scope_is_required() -> None:
    with pytest.raises(ValueError, match="namespace"):
        drift_empty(state(drift_row("clear")), [])
