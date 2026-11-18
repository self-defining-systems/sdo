"""Phase attribution per cell: where the time from inject to close went, and where the agent's span went.

Marks, in seconds after inject (``None`` when the records lack them):

- ``harm_onset``: first failing user second (grader record);
- ``page``: the incident was detected (SDO cells only);
- ``start``: the agent was dispatched (SDO), or its first trajectory event (vanilla cells, flagged);
- ``first_action``: the agent's first tool call;
- ``first_change``: the first change the agent made. SDO cells use the outcome record's first agent change; vanilla
  cells use the first cluster-writing command in the trajectory (flagged, a heuristic). It is the first *recorded*
  change: ``users_recovered`` before it is flagged, since the change then cannot be what recovered users;
- ``first_change_traj``: the first cluster-writing command in any agent's trajectory, subagents included (the recorded
  change time can lag the command that made it);
- ``playbook_applied``: a learned playbook finished applying (SDO cells);
- ``users_recovered``: the last failing user second (grader record);
- ``closed``: the incident was verified closed (SDO cells only).

Within the agent's span (start to close for SDO cells, whole trajectory for vanilla) the main thread and the
subagents are each split into model time, tool time by category, and waiting (sleeps and rollout polls, idle,
and waiting on subagents), with the same categories as ``agent_wall_time``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.analysis import agent_wall_time as wall
from benchmarks.sregym.analysis.cells import Cell, delta, load_cell

if TYPE_CHECKING:
    from pathlib import Path

MARKS = (
    "harm_onset",
    "page",
    "start",
    "playbook_applied",
    "first_action",
    "first_change",
    "first_change_traj",
    "users_recovered",
    "closed",
)
WAITING = {wall.SUBAGENT_WAIT, wall.IDLE, "waits (sleep/rollout/poll)"}
CHANGE_CATEGORIES = {"kubectl/helm writes+exec"}


@dataclass
class Phases:
    cell: str
    mode: str  # "sdo" or "vanilla"
    marks: dict[str, float | None]
    segments: list[tuple[str, str, float]] = field(default_factory=list)  # from mark, to mark, seconds
    main: dict[str, float] = field(default_factory=dict)  # model / waiting / tools by category, seconds
    subagents: dict[str, float] = field(default_factory=dict)
    main_wall_s: float | None = None
    subagents_wall_s: float | None = None
    flags: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _fold(buckets: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {"model": 0.0, "waiting": 0.0}
    for name, secs in buckets.items():
        if name == wall.MODEL:
            out["model"] += secs
        elif name in WAITING:
            out["waiting"] += secs
        else:
            out[f"tool: {name}"] = out.get(f"tool: {name}", 0.0) + secs
    return {k: v for k, v in out.items() if v >= 0.05}


def _calls(agent: wall.Agent) -> tuple[float | None, float | None]:
    """First tool call and first cluster-writing call of one agent."""
    first_call = first_change = None
    jobs: dict[str, str] = {}
    for ts, role, blocks, _ in agent.events:
        if role != "assistant":
            continue
        for block in blocks:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                first_call = ts if first_call is None else first_call
                text = wall._command_text(str(block.get("name")), block.get("input"))
                category, _ = wall.categorize(text, wall.DEFAULT_RULES, jobs)
                if first_change is None and category in CHANGE_CATEGORIES:
                    first_change = ts
    return first_call, first_change


def _first_marks(agents: dict[str, wall.Agent], main_key: str) -> tuple[float | None, float | None, float | None]:
    """First event and first tool call of the main agent; first cluster-writing call of any agent."""
    first_event = agents[main_key].events[0][0] if agents[main_key].events else None
    first_call, _ = _calls(agents[main_key])
    changes = [c for _, c in (_calls(a) for a in agents.values()) if c is not None]
    return first_event, first_call, min(changes, default=None)


def attribute(cell: Cell) -> Phases:
    incident = cell.first_incident
    mode = "sdo" if incident is not None else "vanilla"
    flags: list[str] = []
    files, outside_sdo = cell.trajectory_files()
    agents = wall.load_agents(files) if files else {}
    if incident is not None and agents:  # the responder's span ends at close; the closing turn is learning, not repair
        end = incident.verified_at
        if end is not None:
            for a in agents.values():
                a.events = [e for e in a.events if e[0] <= end]
            agents = {k: a for k, a in agents.items() if a.events}
    main_key = ""
    first_event = first_call = traj_change = None
    if agents:
        main_key = wall.MAIN if wall.MAIN in agents else min(agents, key=lambda k: agents[k].span[0])
        first_event, first_call, traj_change = _first_marks(agents, main_key)
    else:
        flags.append("no agent trajectory found")

    start = incident.dispatched_at if incident else first_event
    first_change = incident.first_agent_change_at if incident else traj_change
    if incident is None:
        flags.append("no SDO timeline: start and first change derived from the trajectory (heuristic)")
    elif outside_sdo and files:
        flags.append("trajectory found outside SDO evidence")
    if incident is not None and incident.used_responder and first_change is None:
        flags.append("no agent change recorded")
    recovered = cell.users_recovered
    change = min((t for t in (first_change, traj_change) if t is not None), default=None)
    if change is not None and recovered is not None and recovered < change:
        flags.append("users recovered before the first change")
    closed = incident.verified_at if incident else None
    if closed is not None and recovered is not None and closed < recovered:
        flags.append("incident closed before users stopped failing")
    absolute = {
        "harm_onset": cell.harm_onset,
        "page": incident.detected_at if incident else None,
        "start": start,
        "first_action": first_call,
        "first_change": first_change,
        "first_change_traj": traj_change if incident else None,
        "playbook_applied": incident.applied_completed_at if incident else None,
        "users_recovered": recovered,
        "closed": incident.verified_at if incident else None,
    }
    marks = {k: delta(v, cell.inject_at) for k, v in absolute.items()}
    known = [(k, marks[k]) for k in MARKS if marks[k] is not None]
    known.sort(key=lambda kv: kv[1] or 0.0)
    segments = [
        (a, b, tb - ta) for (a, ta), (b, tb) in zip(known, known[1:], strict=False) if ta is not None and tb is not None
    ]

    result = Phases(cell.name, mode, marks, segments, flags=flags)
    if agents:
        report = wall.analyze_agents(agents)
        result.main = _fold(dict(report["main"].buckets))
        result.main_wall_s = report["main"].wall
        subs: dict[str, float] = {}
        for sub in report["subagents"]:
            for k, v in _fold(dict(sub.buckets)).items():
                subs[k] = subs.get(k, 0.0) + v
        result.subagents = subs
        result.subagents_wall_s = sum(s.wall for s in report["subagents"])
    return result


def rows(paths: list[Path]) -> list[Phases]:
    return [attribute(load_cell(p)) for p in paths]


def _s(v: float | None) -> str:
    return "-" if v is None else f"{v:.0f}"


def render(items: list[Phases]) -> str:
    lines = [
        "# Phase attribution",
        "",
        "Marks are seconds after inject. first_change is the first recorded agent change (see flags).",
        "",
        "| cell | mode | " + " | ".join(MARKS) + " | flags |",
        "|---|---|" + "---|" * len(MARKS) + "---|",
    ]
    lines.extend(
        f"| {p.cell} | {p.mode} | " + " | ".join(_s(p.marks[m]) for m in MARKS) + f" | {'; '.join(p.flags)} |"
        for p in items
    )
    lines += ["", "## Where the agent's span went (seconds)", ""]
    for p in items:
        if not p.main and not p.subagents:
            continue
        lines.append(f"### {p.cell}")
        lines.append("")
        lines.append(
            f"main thread {_s(p.main_wall_s)} s: "
            + ", ".join(f"{k} {v:.0f}" for k, v in sorted(p.main.items(), key=lambda kv: -kv[1]))
        )
        if p.subagents:
            lines.append(
                f"subagents (summed) {_s(p.subagents_wall_s)} s: "
                + ", ".join(f"{k} {v:.0f}" for k, v in sorted(p.subagents.items(), key=lambda kv: -kv[1]))
            )
        lines.append("")
    return "\n".join(lines) + "\n"
