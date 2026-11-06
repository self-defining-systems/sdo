"""Toy assertions over SDO's operability report, read as JSON only.

The contract is the ``sdo-operability`` ConfigMap in the control namespace (``summary.json`` and ``report.json``) and
the ``cause=<c> owner=<o> items=<O1,O3|->`` prefix of an upstream hold's reason. Nothing here imports SDO code and
nothing recomputes SDO's decision: the checks read the stored statuses and the owners SDO routed, and compare them
with the harness's own expectation for each scripted scenario.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

SUMMARY_KEY = "summary.json"
REPORT_KEY = "report.json"
OPERABILITY_CONFIGMAP = "sdo-operability"
#: Items every stage-``complete`` report must carry (the stage that gates the install).
COMPLETE_ITEMS = ("O1", "O2", "O3", "O4", "O5", "O6")
#: Statuses that do not stop acceptance; ``fail`` and ``pending`` do.
GREEN_STATUSES = frozenset({"pass", "note", "skipped"})
_ROUTING = re.compile(r"cause=(?P<cause>\w+) owner=(?P<owner>\w+) items=(?P<items>[A-Za-z0-9,]+|-) ")


@dataclass(frozen=True)
class ItemState:
    id: str
    status: str
    first_failed_check: str
    cause: str
    owner: str


@dataclass(frozen=True)
class OperabilityRead:
    """The summary envelope plus the check statuses of the report, as exposed in the control namespace."""

    accepted_commit: str
    stage: str
    digest: str
    items: tuple[ItemState, ...]
    report_commit: str
    report_stage: str
    report_checks: Mapping[str, tuple[str, ...]]  # item id -> check statuses


def parse_operability(configmap: Mapping[str, Any]) -> OperabilityRead:
    """Parse ``kubectl get configmap sdo-operability -o json``; a malformed document is an error, not "no report"."""
    data = configmap.get("data") or {}
    for key in (SUMMARY_KEY, REPORT_KEY):
        if key not in data:
            raise ValueError(f"{OPERABILITY_CONFIGMAP} lacks {key}")
    summary, report = json.loads(data[SUMMARY_KEY]), json.loads(data[REPORT_KEY])
    for key in ("accepted_commit", "stage", "digest", "items"):
        if not summary.get(key):
            raise ValueError(f"{SUMMARY_KEY} lacks {key}")
    items = tuple(
        ItemState(
            id=i["id"],
            status=i["status"],
            first_failed_check=i.get("first_failed_check", ""),
            cause=i.get("cause", ""),
            owner=i.get("owner", ""),
        )
        for i in summary["items"]
    )
    subject = report.get("subject") or {}
    checks = {item["id"]: tuple(c["status"] for c in item.get("checks") or []) for item in report.get("items") or []}
    return OperabilityRead(
        summary["accepted_commit"],
        summary["stage"],
        summary["digest"],
        items,
        subject.get("accepted_commit", ""),
        report.get("stage", ""),
        checks,
    )


def report_violations(read: OperabilityRead | None, accepted_shas: Collection[str]) -> list[str]:
    """Why the report is not green: absent, wrong stage, an item missing or failing, or bound to no accepted commit."""
    if read is None:
        return [f"{OPERABILITY_CONFIGMAP} ConfigMap is absent: no operability report was exposed"]
    out: list[str] = []
    if read.stage != "complete":
        out.append(f"operability report stage is {read.stage!r}, expected 'complete'")
    if (read.report_stage, read.report_commit) != (read.stage, read.accepted_commit):
        out.append("operability summary and report.json disagree on stage or accepted commit")
    by_id = {i.id: i for i in read.items}
    for item_id in COMPLETE_ITEMS:
        item = by_id.get(item_id)
        if item is None:
            out.append(f"operability item {item_id} is missing")
        elif item.status not in GREEN_STATUSES:
            detail = f" ({item.first_failed_check}: cause {item.cause}, owner {item.owner})" if item.cause else ""
            out.append(f"operability item {item_id} is {item.status}{detail}")
        elif not read.report_checks.get(item_id):
            out.append(f"operability item {item_id} has no checks in report.json")
    if read.accepted_commit not in accepted_shas:
        out.append(f"operability report names {read.accepted_commit[:12]}, which is not an accepted commit")
    return out


@dataclass(frozen=True)
class HoldRouting:
    cause: str
    owner: str
    items: tuple[str, ...]
    kind: str  # the typed hold reason after the routing prefix (``rollout``, ``health``, ...)


def parse_hold_routing(reason: str) -> HoldRouting | None:
    """The routing prefix of a hold reason, or None when the reason carries none."""
    match = _ROUTING.match(reason)
    if match is None:
        return None
    kind, sep, _ = reason[match.end() :].partition(":")
    items = () if match["items"] == "-" else tuple(match["items"].split(","))
    return HoldRouting(match["cause"], match["owner"], items, kind.strip() if sep else "")


@dataclass(frozen=True)
class Expectation:
    """What the harness expects SDO to route for a scripted scenario: the owners that may act, and agent turns."""

    scenario: str
    causes: frozenset[str]
    owner: str
    items: tuple[str, ...]
    max_agent_tasks: int


#: A broken commit's failed rollout or firing floor detector is the responder's, in advance mode.
BROKEN_EXPECTATION = Expectation("broken", frozenset({"rollout", "source_fault"}), "responder", (), max_agent_tasks=0)


def routing_violations(reason: str, expectation: Expectation) -> list[str]:
    """Compare a hold's routing prefix with the scenario's expected cause and owner."""
    routing = parse_hold_routing(reason)
    if routing is None:
        return [f"hold reason {reason!r} carries no cause/owner/items routing"]
    out: list[str] = []
    if routing.cause not in expectation.causes:
        out.append(f"hold cause {routing.cause!r} is not one of {sorted(expectation.causes)}")
    if routing.owner != expectation.owner:
        out.append(f"hold owner is {routing.owner!r}, expected {expectation.owner!r}")
    if expectation.items and routing.items != expectation.items:
        out.append(f"hold items are {list(routing.items)}, expected {list(expectation.items)}")
    return out
