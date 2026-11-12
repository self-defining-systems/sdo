"""Synthetic closure and outcome fixtures shared by the closing-turn and fastloop tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from sdo.contracts import IncidentClosure, OperationalMemorySnapshot
from sdo.contracts.proto import (
    ConfirmedRootCause,
    DetectorEvaluation,
    Finding,
    ObjectRef,
    OutcomeRecord,
    OutcomeTimestamps,
    RepairActionReceipt,
)

_T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
_EVIDENCE = "FailedMount: configmap mongo-geo-script not found " * 40


def _stamp(at: datetime) -> Timestamp:
    value = Timestamp()
    value.FromDatetime(at)
    return value


def _evaluations(detector_id: str, statuses: list[str], *, fingerprint: str = "fp") -> list[DetectorEvaluation]:
    return [
        DetectorEvaluation(
            detector_id=detector_id,
            evaluated_at=_stamp(_T0 + timedelta(seconds=index)),
            status=status,
            fingerprints=[fingerprint] if status == "firing" else [],
        )
        for index, status in enumerate(statuses)
    ]


def _outcome(incident_id: str, *, evaluations: int = 20) -> OutcomeRecord:
    ref = ObjectRef(api_version="v1", kind="ConfigMap", namespace="app", name="mongo-geo-script")
    return OutcomeRecord(
        schema_version=1,
        incident_id=incident_id,
        source_commit="source",
        deployed_commit="deployed",
        classification="success",
        responder_backend="codex",
        responder_model="gpt-test",
        detector_history=_evaluations("health-objective", ["firing"] * evaluations)
        + _evaluations("service-endpoints", ["firing"] * evaluations),
        findings=[
            Finding(
                detector_id="health-objective",
                rule_id="service-without-ready-endpoints",
                status="active",
                severity="critical",
                summary="Service mongodb-geo has no ready endpoints",
                evidence=_EVIDENCE,
                primary_resource=ref,
                fingerprint="health-objective/no-endpoints",
            )
        ],
        applied_playbooks=["missing-configmap"],
        confirmed_root_causes=[
            ConfirmedRootCause(summary=f"{incident_id}: required ConfigMap was removed", resources=[ref]),
        ],
        repair_actions=[
            RepairActionReceipt(
                action_id="restore",
                kind="apply",
                target="ConfigMap/mongo-geo-script",
                summary="restore the ConfigMap from tracked source",
                details="kubectl apply -f kubernetes/geo/mongo-geo-script-configmap.yaml " * 20,
                started_at=_stamp(_T0),
                completed_at=_stamp(_T0),
                success=True,
                reversible=True,
            )
        ],
        timestamps=OutcomeTimestamps(detected_at=_stamp(_T0), dispatched_at=_stamp(_T0), completed_at=_stamp(_T0)),
    )


FIXTURE = Path(__file__).resolve().parents[4] / "fixtures" / "sdo" / "contracts" / "go" / "closure_repaired.json"


def _closure() -> IncidentClosure:
    closure = json_format.Parse(FIXTURE.read_text(encoding="utf-8"), IncidentClosure())
    closure.request.memory_snapshot.CopyFrom(
        OperationalMemorySnapshot(
            accepted_commit=closure.request.repository_base_commit,
            goal_markdown="Health objective",
            architecture_markdown="Architecture",
        )
    )
    closure.result.responder_session_id = "responder-session-0001"
    return closure
