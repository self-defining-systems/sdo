"""Controller-only facts for learning in the responder's existing session.

The transcript already contains dispatch evidence, observations and reported
repairs. Only independent closure decisions and unseen receipts belong in the
resumed turn. This projection never grants authority to an unverified cause.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from string import Template
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, TypedDict

from google.protobuf import json_format

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from sdo.contracts import IncidentClosure, OutcomeRecord, ReflectionCauseWork, ReflectionWorkItem


class ReflectionDelta(TypedDict):
    outcome_commit: str
    verdict: str
    confirmed_fixed: list[dict[str, Any]]
    unseen_receipts: dict[str, list[dict[str, Any]]]
    cause_work: list[dict[str, Any]]
    unassigned_work: list[dict[str, Any]]


def reflection_delta_values(
    outcome: OutcomeRecord,
    closure: IncidentClosure,
    *,
    outcome_commit: str,
    cause_work: Sequence[ReflectionCauseWork] = (),
    unassigned_work: Sequence[ReflectionWorkItem] = (),
) -> ReflectionDelta:
    """Keep verified cause indices and final execution/verification receipts intact.

    Older successful outcomes predate explicit eligibility. Failed historical
    outcomes provide no independent proof and cannot promote reported causes.
    Native receipts and final application proof are merged after the model
    returns, so presence in closure.result does not establish model visibility.
    Tool calls already observed during the response are omitted; controller
    calls after the model-return timestamp remain new facts.
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
    known_calls = {item.SerializeToString(deterministic=True) for item in result.program_tool_calls}
    returned_at = result.timing.completed_at.ToNanoseconds() if result.timing.HasField("completed_at") else None
    new_calls = [
        json_format.MessageToDict(item, preserving_proto_field_name=True)
        for item in outcome.program_tool_calls
        if item.SerializeToString(deterministic=True) not in known_calls
        or (returned_at is not None and item.at.ToNanoseconds() > returned_at)
    ]
    return ReflectionDelta(
        outcome_commit=outcome_commit,
        verdict=outcome.classification,
        confirmed_fixed=fixed,
        unseen_receipts={
            "repair_actions": [
                json_format.MessageToDict(item, preserving_proto_field_name=True) for item in outcome.repair_actions
            ],
            "plan_applications": [
                json_format.MessageToDict(item, preserving_proto_field_name=True) for item in outcome.plan_applications
            ],
            "program_tool_calls": new_calls,
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


@contextmanager
def reflection_delta_data(values: ReflectionDelta) -> Iterator[str]:
    """Keep inline facts below 4k characters without discarding overflow evidence.

    The private file lives only for the model turn. It is outside the authored
    checkout and contains the complete new facts so a session can search the
    relevant receipts instead of consuming a truncated prefix.
    """
    prompts = Path(__file__).with_name("prompts")
    substitutions = {
        key: value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        for key, value in values.items()
    }
    rendered = Template((prompts / "reflection_delta.md").read_text(encoding="utf-8")).substitute(substitutions)
    if len(rendered) <= 4000:
        yield rendered
        return
    with NamedTemporaryFile(mode="w", encoding="utf-8", prefix="sdo-reflection-", suffix=".json") as evidence:
        json.dump(values, evidence, ensure_ascii=False)
        evidence.flush()
        yield Template((prompts / "reflection_delta_overflow.md").read_text(encoding="utf-8")).substitute(
            verdict=values["verdict"], outcome_commit=values["outcome_commit"], details_file=evidence.name
        )
