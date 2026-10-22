"""Short confirm-or-refine reflection request.

The Go controller chooses this depth from typed outcome facts when accepted
programs already installed in operational memory diagnosed, repaired and
verified the incident. The reflection still runs in the responder's session,
so it can compare what happened with those programs; it just does not receive
the authoring protocol, the whole memory snapshot or the outcome documents.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdo.contracts import IncidentClosure, OutcomeRecord

_FIELD_CHARS = 200
_MAX_ITEMS = 8


def _clip(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _FIELD_CHARS else f"{text[:_FIELD_CHARS]}..."


def refine_learning_request(*, outcome: OutcomeRecord, closure: IncidentClosure, outcome_commit: str) -> str:
    snapshot = closure.request.memory_snapshot
    programs = sorted({program for item in outcome.plan_applications for program in item.program_ids})
    causes = [f"- {_clip(cause.summary)}" for cause in outcome.confirmed_root_causes[:_MAX_ITEMS]]
    actions = [
        f"- {action.kind} {_clip(action.target)}: {_clip(action.summary)}"
        for action in outcome.repair_actions[:_MAX_ITEMS]
    ]
    installed = ", ".join(sorted(item.id for item in snapshot.playbooks)) or "none"
    return (
        "This is a confirm-or-refine reflection, not a full one. The controller verified this incident and "
        f"committed its outcome (commit {outcome_commit}). Accepted programs already in operational memory "
        f"diagnosed it, their plans were applied, and the controller verified NoMatch afterwards. Programs: "
        f"{', '.join(programs)}.\n"
        "Confirmed root causes:\n" + ("\n".join(causes) or "- none recorded") + "\n"
        "Repair actions:\n" + ("\n".join(actions) or "- none recorded") + "\n"
        f"Installed logical playbook and program IDs: {installed}.\n\n"
        "Compare what you saw in this session with those programs. If they handled every confirmed fault "
        "correctly and safely, return learning_decision=no_change with a no_change_reason naming the program IDs, "
        "and leave the checkout unchanged: author no new program, playbook, detector or corpus case, and do not "
        "run check_memory. Edit only when this session contradicts memory, for example a program needed manual "
        "correction, matched the wrong resource, or planned an unsafe or incomplete repair. Then make the "
        "smallest change to the existing program: run `sdo-operator memory ls`, read the installed "
        "sdo-playbook-authoring and sdo-memory skills, edit the final files in `.sdo/` of this checkout, call the "
        "sdo_reflection check_memory tool until it exits zero, and return learning_decision=updated with "
        "playbook_ids listing every changed logical playbook ID. Use logical IDs only, never physical memory "
        "paths. Source edits are not part of reflection; outcomes, goal and architecture are protected. "
        "Return decisions and logical IDs only.\n"
    )
