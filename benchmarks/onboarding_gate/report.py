"""Per-session comparison of rejection classes between a session's recorded kit and a candidate kit."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from benchmarks.onboarding_gate.classify import journeys_rejections

if TYPE_CHECKING:
    from benchmarks.onboarding_gate.check import Judgement
    from benchmarks.onboarding_gate.replay import Replayed
    from benchmarks.onboarding_gate.transcript import ToolCall

# journeys items that need the live application; an offline replay cannot judge them.
LIVE_ITEMS = frozenset({"passes", "latency-bound", "concurrent", "altered-answers", "healthy-run", "scheduled"})


@dataclass
class SessionComparison:
    session: str
    snapshots: int
    fidelity: str
    recorded: Counter[str] = field(default_factory=Counter)  # class -> snapshots with it, recorded kit
    candidate: Counter[str] = field(default_factory=Counter)
    live_seen: Counter[str] = field(default_factory=Counter)  # live-only red items the agent saw
    errors: list[str] = field(default_factory=list)
    note: str = ""
    examples: dict[str, str] = field(default_factory=dict)

    @property
    def gone(self) -> list[str]:
        return sorted(set(self.recorded) - set(self.candidate))

    @property
    def still(self) -> list[str]:
        return sorted(set(self.recorded) & set(self.candidate))

    @property
    def new(self) -> list[str]:
        return sorted(set(self.candidate) - set(self.recorded))

    def to_json(self) -> dict[str, object]:
        return {
            "session": self.session,
            "snapshots": self.snapshots,
            "fidelity": self.fidelity,
            "recorded": dict(self.recorded),
            "candidate": dict(self.candidate),
            "gone": self.gone,
            "still": self.still,
            "new": self.new,
            "live_seen": dict(self.live_seen),
            "errors": self.errors,
            "note": self.note,
            "examples": self.examples,
        }


def compare(
    session: Replayed,
    calls: list[ToolCall],
    recorded: dict[str, Judgement],
    candidate: dict[str, Judgement],
) -> SessionComparison:
    """Count, per class, the validator snapshots each kit rejects for that class."""
    trees = list(dict.fromkeys(snapshot.tree for snapshot in session.snapshots))
    fidelity = f"{session.matched_commits}/{session.agent_commits}" if session.agent_commits else "n/a"
    result = SessionComparison(session.session, len(trees), fidelity)
    memory_only = any(not j.plan_supported for t, j in recorded.items() if t in trees)
    if memory_only:
        result.note = "recorded kit has no journeys --plan: compared on memory-check classes only"
    for tree in trees:
        for counter, judged, label in (
            (result.recorded, recorded, "recorded"),
            (result.candidate, candidate, "candidate"),
        ):
            judgement = judged.get(tree)
            if judgement is None and label == "candidate":
                continue  # not selected: its recorded kit accepted it and it is not the final state
            if judgement is None or judgement.error:
                result.errors.append(f"{label} {tree[:8]}: {judgement.error if judgement else 'not judged'}")
                continue
            rejections = {r for r in judgement.rejections if not memory_only or r.origin == "memory"}
            for kind in {rejection.kind for rejection in rejections}:
                counter[kind] += 1
            for rejection in rejections:
                result.examples.setdefault(f"{label}:{rejection.kind}", str(rejection))
    for call in calls:
        if "onboarding journeys" in call.command:
            for rejection in journeys_rejections(call.result):
                if rejection.kind in LIVE_ITEMS:
                    result.live_seen[rejection.kind] += 1
    return result


def text(comparisons: list[SessionComparison], recorded_label: str, candidate_label: str) -> str:
    lines = [
        f"rejection classes per session: recorded kit ({recorded_label}) -> candidate ({candidate_label})",
        "counts are validator snapshots rejected for the class",
    ]
    totals_recorded: Counter[str] = Counter()
    totals_candidate: Counter[str] = Counter()
    for item in comparisons:
        totals_recorded.update(item.recorded)
        totals_candidate.update(item.candidate)
        lines.append(f"\n{item.session}: {item.snapshots} distinct snapshots, replay fidelity {item.fidelity}")
        if item.note:
            lines.append(f"  note: {item.note}")
        for label, kinds in (("gone", item.gone), ("still", item.still), ("new", item.new)):
            if kinds:
                lines.append(f"  {label:5} " + ", ".join(f"{k} {item.recorded[k]}->{item.candidate[k]}" for k in kinds))
        if not (item.gone or item.still or item.new):
            lines.append("  no offline rejection under either kit")
        if item.live_seen:
            lines.append(
                "  live-only red the agent saw (not replayable): "
                + ", ".join(f"{k} {n}" for k, n in sorted(item.live_seen.items()))
            )
        lines.extend(f"  error {error.splitlines()[0][:200]}" for error in item.errors[:3])
    lines.append("\ntotal snapshots rejected per class (recorded -> candidate):")
    lines.extend(
        f"  {kind:32} {totals_recorded[kind]:3} -> {totals_candidate[kind]}"
        for kind in sorted(set(totals_recorded) | set(totals_candidate))
    )
    return "\n".join(lines) + "\n"
