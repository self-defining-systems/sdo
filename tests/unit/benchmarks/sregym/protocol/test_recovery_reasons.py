"""The recovery diagnostic boundary admits no credential-bearing text."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from benchmarks.sregym.protocol.recovery import RecoveryFailureReason, recovery_failure_reasons


@pytest.mark.parametrize("value", [True, -1, "PRIVATE_CANARY", {}, []])
def test_untrusted_recovery_metadata_is_projected_to_fixed_codes(value: object) -> None:
    reasons = recovery_failure_reasons(
        {
            "errors": [
                {
                    "incident": value,
                    "component_index": value,
                    "status": value,
                    "step": "PRIVATE_CANARY",
                    "code": "PRIVATE_CANARY",
                    "error": "PRIVATE_CANARY",
                }
            ]
        },
        "recover",
    )
    assert [reason.model_dump() for reason in reasons] == [
        {
            "incident_index": None,
            "component_index": None,
            "step": "recover",
            "code": "unknown",
        }
    ]


@pytest.mark.parametrize("field", ["incident_index", "component_index", "step", "code", "error"])
def test_typed_recovery_reason_cannot_store_arbitrary_text(field: str) -> None:
    raw = {"step": "recover", "code": "unknown", field: "PRIVATE_CANARY"}
    with pytest.raises(ValidationError):
        RecoveryFailureReason.model_validate(raw)


@pytest.mark.parametrize("errors", [None, "PRIVATE_CANARY", {}, [None, "PRIVATE_CANARY"]])
def test_missing_structured_worker_error_has_an_explicit_unknown_reason(errors: object) -> None:
    (reason,) = recovery_failure_reasons({"errors": errors}, "refresh")
    assert reason.model_dump() == {
        "incident_index": None,
        "component_index": None,
        "step": "refresh",
        "code": "unknown",
    }
