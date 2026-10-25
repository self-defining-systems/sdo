"""Controller-only facts for learning in the responder's existing session.

The transcript already contains dispatch evidence, observations and reported
repairs. Only independent closure decisions and unseen receipts belong in the
resumed turn. This projection never grants authority to an unverified cause.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypedDict

from google.protobuf import json_format

if TYPE_CHECKING:
    from collections.abc import Sequence

    from google.protobuf.message import Message

    from sdo.contracts import IncidentClosure, OutcomeRecord, ReflectionCauseWork, ReflectionWorkItem


class ReflectionDelta(TypedDict):
    outcome_commit: str
    verdict: str
    confirmed_fixed: list[dict[str, Any]]
    unseen_receipts: dict[str, list[dict[str, Any]]]
    cause_work: list[dict[str, Any]]
    unassigned_work: list[dict[str, Any]]


def _unseen(current: Sequence[Message], seen: Sequence[Message]) -> list[dict[str, Any]]:
    known = {item.SerializeToString(deterministic=True) for item in seen}
    return [
        json_format.MessageToDict(item, preserving_proto_field_name=True)
        for item in current
        if item.SerializeToString(deterministic=True) not in known
    ]


def reflection_delta_values(
    outcome: OutcomeRecord,
    closure: IncidentClosure,
    *,
    outcome_commit: str,
    cause_work: Sequence[ReflectionCauseWork] = (),
    unassigned_work: Sequence[ReflectionWorkItem] = (),
) -> ReflectionDelta:
    """Keep verified cause indices and new execution/verification receipts intact.

    Older successful outcomes predate explicit eligibility. Failed historical
    outcomes provide no independent proof and cannot promote reported causes.
    A changed receipt is new information even if its action/application ID is
    unchanged, for example when Go adds final verification after the response.
    """
    if outcome.HasField("reflection_eligibility"):
        verified = {item.cause_index for item in outcome.reflection_eligibility.causes}
    elif outcome.classification == "success":
        verified = set(range(len(outcome.confirmed_root_causes)))
    else:
        verified = set()
    fixed = [
        {
            "cause_index": index,
            "summary": cause.summary,
            "resources": [json_format.MessageToDict(ref, preserving_proto_field_name=True) for ref in cause.resources],
        }
        for index, cause in enumerate(outcome.confirmed_root_causes)
        if index in verified
    ]
    result = closure.result
    return ReflectionDelta(
        outcome_commit=outcome_commit,
        verdict=outcome.classification,
        confirmed_fixed=fixed,
        unseen_receipts={
            "repair_actions": _unseen(outcome.repair_actions, result.repair_actions),
            "plan_applications": _unseen(outcome.plan_applications, result.plan_applications),
            "program_tool_calls": _unseen(outcome.program_tool_calls, result.program_tool_calls),
        },
        cause_work=[
            json_format.MessageToDict(item, preserving_proto_field_name=True)
            for item in (cause_work or outcome.reflection_eligibility.causes)
        ],
        unassigned_work=[
            json_format.MessageToDict(item, preserving_proto_field_name=True)
            for item in (unassigned_work or outcome.reflection_eligibility.unassigned)
        ],
    )
