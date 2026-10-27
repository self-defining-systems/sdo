from __future__ import annotations

import pytest

from benchmarks.sregym.assurance.cluster_ledger import DriftAssessmentUnavailable, DriftMismatch, drift_empty

DIGEST = "ab" * 32


def fingerprint(namespace: str, kind: str, name: str) -> str:
    return f"drift.unexplained/unexplained/{namespace}/{kind}/{name}/{DIGEST}"


def state(*rows: dict) -> dict:
    return {"history": list(rows)}


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


def test_clear_or_unevaluated_drift_is_empty() -> None:
    assert drift_empty(state(drift_row("clear")), ["shop"]) == []
    assert drift_empty(state({"detector_id": "workload-health", "status": "clear"}), ["shop"]) == []
    assert drift_empty({}, ["shop"]) == []


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
