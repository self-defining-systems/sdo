"""MI1 wire contracts: identity, bounded configurable rounds and safe terminal evidence."""

from __future__ import annotations

import json

import pytest

from sdo import contracts


def selection_payload() -> dict[str, object]:
    return {
        "version": 1,
        "incident_id": "synthetic-b3",
        "session_id": "provider-session",
        "dispatch_epoch": "7",
        "request_id": "retry-stable-request",
        "menu_id": "menu-revision",
        "option_id": "routing-then-restart",
        "program_revision": "program-digest",
        "dependency_revision": "dependencies-digest",
        "source_commit": "accepted-source",
        "scope_digest": "named-scope",
        "policy_digest": "menu-policy",
        "bindings": [{"name": "service", "object": {"kind": "Service", "namespace": "app", "name": "backend"}}],
        "round_limit": 2,
    }


def test_selection_protojson_preserves_semantic_identity() -> None:
    message = contracts.parse_json(json.dumps(selection_payload()), contracts.MitigationSelection())
    contracts.validate(message)
    assert message.dispatch_epoch == 7
    assert contracts.parse_json(contracts.to_canonical_json(message), type(message)()) == message


@pytest.mark.parametrize(
    "field",
    [
        "incident_id",
        "session_id",
        "request_id",
        "menu_id",
        "option_id",
        "program_revision",
        "dependency_revision",
        "source_commit",
        "scope_digest",
        "policy_digest",
        "dispatch_epoch",
        "version",
    ],
)
def test_selection_rejects_missing_fence(field: str) -> None:
    payload = selection_payload()
    del payload[field]
    message = contracts.parse_json(json.dumps(payload), contracts.MitigationSelection())
    with pytest.raises(ValueError, match="invalid MitigationSelection"):
        contracts.validate(message)


@pytest.mark.parametrize("limit", [0, 101])
def test_selection_rejects_out_of_slice_round_limits(limit: int) -> None:
    payload = selection_payload() | {"round_limit": limit}
    message = contracts.parse_json(json.dumps(payload), contracts.MitigationSelection())
    with pytest.raises(ValueError, match="round_limit"):
        contracts.validate(message)


def test_selection_rejects_duplicate_binding_names() -> None:
    payload = selection_payload()
    payload["bindings"] = [payload["bindings"][0], payload["bindings"][0]]
    message = contracts.parse_json(json.dumps(payload), contracts.MitigationSelection())
    with pytest.raises(ValueError, match="binding names must be unique"):
        contracts.validate(message)


def test_terminal_reports_have_no_preimages_or_unbounded_opaque_output() -> None:
    fields = contracts.MitigationReport.DESCRIPTOR.fields_by_name
    assert {"attempt_id", "acceptance_epoch", "status", "failed_phase", "rounds", "remaining_batches"} <= set(fields)
    action_fields = contracts.MitigationActionReceipt.DESCRIPTOR.fields_by_name
    assert {"action_id", "state", "compensation_state", "resource", "uid", "resource_version"} <= set(action_fields)
    for message in (
        contracts.MitigationReport,
        contracts.MitigationRoundReport,
        contracts.MitigationActionReceipt,
        contracts.MitigationBarrier,
    ):
        assert not {"pre_images", "object", "patch_json", "stdout", "stderr", "logs", "probe_body"} & set(
            message.DESCRIPTOR.fields_by_name
        )


@pytest.mark.parametrize(
    "status",
    [
        "STATUS_SUCCESS",
        "STATUS_REFUSED",
        "STATUS_EXECUTION_ERROR",
        "STATUS_UNRESOLVED",
        "STATUS_DIFFERENT_CAUSE",
        "STATUS_SUPERSEDED",
        "STATUS_INTERRUPTED",
        "STATUS_TIMEOUT",
    ],
)
def test_all_terminal_statuses_round_trip(status: str) -> None:
    message = contracts.parse_json(
        json.dumps(
            {
                "version": 1,
                "incident_id": "synthetic-b3",
                "session_id": "provider-session",
                "request_id": "retry-stable-request",
                "attempt_id": "attempt-unique",
                "acceptance_epoch": "7",
                "status": status,
            }
        ),
        contracts.MitigationReport(),
    )
    contracts.validate(message)
    assert contracts.parse_json(contracts.to_canonical_json(message), type(message)()) == message


@pytest.mark.parametrize("limit", [3, 5, 7])
def test_selection_wire_allows_controller_configured_round_cap(limit: int) -> None:
    message = contracts.parse_json(
        json.dumps(selection_payload() | {"round_limit": limit}), contracts.MitigationSelection()
    )
    contracts.validate(message)
