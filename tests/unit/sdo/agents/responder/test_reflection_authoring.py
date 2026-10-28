"""Authoring tools hide protocol translation while preserving captured bytes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from sdo.agents.responder.reflection_authoring import (
    ResourceBinding,
    SessionObservations,
    authoring_reference,
    prepare_corpus,
    prepare_session_corpus,
)
from sdo.agents.responder.tool_output import MAX_TOOL_REPLY_BYTES


def test_session_fixture_does_not_require_controller_open_or_fixed_captures(tmp_path: Path) -> None:
    observed = '{"namespace":"app","services":[{"metadata":{"name":"api","namespace":"app"}}]}'
    prepare_session_corpus(
        tmp_path,
        "first",
        "routing",
        ["routing"],
        {"service": ResourceBinding(kind="Service", namespace="app", name="api")},
        SessionObservations(snapshot_json=observed),
    )
    case = tmp_path / ".sdo/corpus/routing"
    assert (case / "open.json").read_text() == observed
    assert (case / "fixed.json").read_text() == observed
    assert json.loads((case / "labels.json").read_text())["provenance"] == {"incident_id": "first"}
    assert (case / "recordings.json").read_text() == "[]"


def _full_reference(reply: str) -> str:
    (line,) = [line for line in reply.splitlines() if line.startswith("Full text: ")]
    return Path(line.removeprefix("Full text: ")).read_text()


def test_program_reference_uses_shipped_sdk_and_worked_example(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    reference = _full_reference(authoring_reference("programs"))
    assert "# Program notes" in reference
    assert "func" in reference
    assert "program.ReadView" in reference
    assert "ProbeTCP" in reference
    sdk_reference = _full_reference(authoring_reference("sdk"))
    assert "# Recorded Kubernetes reads" in sdk_reference
    assert "RESTMapper" in sdk_reference
    assert "type ReadView interface" in sdk_reference
    assert "type DetectionContext interface" in sdk_reference
    assert "DiffAgainstSource(ref ObjectRef) (SourceDiff, error)" in sdk_reference


@pytest.mark.parametrize("topic", ["programs", "sdk", "rules"])
def test_reference_reply_is_an_index_with_a_greppable_full_text(tmp_path: Path, monkeypatch, topic) -> None:
    # Lane 52: a 76 KB reply was truncated, so reflection re-called the tool 7x to search it.
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    reply = authoring_reference(topic)
    assert len(reply.encode()) <= MAX_TOOL_REPLY_BYTES
    assert "Sections:" in reply
    assert _full_reference(reply).startswith("## ")
    assert authoring_reference(topic) == reply


def test_reference_section_returns_only_the_requested_declaration(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    reply = authoring_reference("sdk", section="type ReadView interface")
    assert "type ReadView interface" in reply
    assert "type DetectionContext interface" not in reply
    assert len(reply.encode()) <= MAX_TOOL_REPLY_BYTES
    missing = authoring_reference("sdk", section="no such section anywhere")
    assert "No section matches" in missing
    assert "Full text: " in missing


@pytest.mark.parametrize("phase", ["detect", "pre-apply"])
def test_corpus_tool_supplies_provenance_and_preserves_capture_bytes(tmp_path: Path, phase: str) -> None:
    captures = tmp_path / "captures"
    captures.mkdir()
    opened, fixed, recordings = b'{"pods":null}\n', b'{"pods":[]}\n', b"[]\n"
    (captures / f"open@{phase}.json").write_bytes(opened)
    (captures / f"open@{phase}-recordings.json").write_bytes(recordings)
    (captures / "fixed.json").write_bytes(fixed)
    root = tmp_path / "checkout"
    root.mkdir()
    result = prepare_corpus(root, captures, "incident-1", "case-1", phase, ["routing"], {}, {})
    case = root / ".sdo/corpus/case-1"
    assert (case / "open.json").read_bytes() == opened
    assert (case / "fixed.json").read_bytes() == fixed
    assert (case / "healthy.json").read_bytes() == fixed
    assert (case / "recordings.json").read_bytes() == recordings
    labels = json.loads((case / "labels.json").read_text())
    assert labels["provenance"] == {
        "incident_id": "incident-1",
        "open_at": phase,
        "open_sha256": hashlib.sha256(opened).hexdigest(),
        "fixed_sha256": hashlib.sha256(fixed).hexdigest(),
    }
    assert result["case_id"] == "case-1"
    with pytest.raises(FileExistsError):
        prepare_corpus(root, captures, "incident-1", "case-1", phase, ["routing"], {}, {})


def test_corpus_tool_cannot_escape_checkout(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="logical case ID"):
        prepare_corpus(tmp_path, tmp_path, "incident", "../escape", "detect", ["routing"], {}, {})


def _committed_outcome(root: Path, incident: str) -> None:
    from google.protobuf import json_format
    from google.protobuf.timestamp_pb2 import Timestamp

    from sdo.contracts import OutcomeRecord

    outcome = OutcomeRecord(incident_id=incident, classification="success")
    started, completed = Timestamp(), Timestamp()
    started.FromJsonString("2026-10-28T16:37:12.653402895Z")
    completed.FromJsonString("2026-10-28T16:37:12.740672356Z")
    action = outcome.repair_actions.add(action_id="hand-1", kind="sdo-act", success=True, script_digest="abc")
    action.started_at.CopyFrom(started)
    action.completed_at.CopyFrom(completed)
    action.resources.add(
        api_version="admissionregistration.k8s.io/v1", kind="MutatingWebhookConfiguration", name="hook"
    )
    outcome.repair_actions.add(action_id="failed", kind="sdo-act", success=False)
    path = root / ".sdo/outcomes" / (hashlib.sha256(incident.encode()).hexdigest() + ".json")
    path.parent.mkdir(parents=True)
    path.write_text(json_format.MessageToJson(outcome, preserving_proto_field_name=True))


def test_corpus_tool_links_named_hand_repairs_from_the_committed_outcome(tmp_path: Path) -> None:
    """One call writes the exact RepairReceipt entry the memory check links (memory.CaptureRepairReceipts shape)."""
    _committed_outcome(tmp_path, "incident")
    prepare_session_corpus(
        tmp_path,
        "incident",
        "webhook",
        ["webhook-ca-sync"],
        {
            "configuration": ResourceBinding(
                api_version="admissionregistration.k8s.io/v1", kind="MutatingWebhookConfiguration", name="hook"
            )
        },
        SessionObservations(snapshot_json='{"namespace":"app"}'),
        repair_receipt_ids=["hand-1"],
    )
    (entry,) = json.loads((tmp_path / ".sdo/corpus/webhook/recordings.json").read_text())
    assert entry == {
        "method": "RepairReceipt",
        "arguments": [],
        "result": {
            "script_digest": "abc",
            "id": "hand-1",
            "incident_id": "incident",
            "success": True,
            "started_at": "2026-10-28T16:37:12.653402895Z",
            "completed_at": "2026-10-28T16:37:12.740672356Z",
            "resources": [
                {
                    "api_version": "admissionregistration.k8s.io/v1",
                    "kind": "MutatingWebhookConfiguration",
                    "name": "hook",
                }
            ],
        },
    }


def test_corpus_tool_names_the_known_receipts_when_one_is_missing(tmp_path: Path) -> None:
    _committed_outcome(tmp_path, "incident")
    with pytest.raises(ValueError, match="hand-1"):
        prepare_session_corpus(
            tmp_path,
            "incident",
            "webhook",
            ["webhook-ca-sync"],
            {},
            SessionObservations(snapshot_json="{}"),
            repair_receipt_ids=["failed"],
        )
    assert not (tmp_path / ".sdo/corpus/webhook").exists(), "a refused call leaves no partial case"
