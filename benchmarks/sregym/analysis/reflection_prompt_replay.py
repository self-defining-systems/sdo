"""Offline estimate of reflection-prompt savings from recorded Codex rollouts.

Reads the reflection request each recorded rollout actually sent (the second
task of a Codex session: the resumed reflection), re-parses its ``Current
outcome`` and ``Outcome history`` JSON into outcome records, renders both
sections again with the bounded views in
``sdo.agents.responder.reflection_outcomes``, and reports the character
and estimated-token difference. No model is called.

A prompt token is re-read by every later request of the session, so the
weighted saving is ``tokens * (1 + cache_read_weight * (requests - 1))``.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path

from sdo.agents.responder import current_outcome_view, history_view
from sdo.contracts import OutcomeRecord

#: o200k measured 3.93-4.08 characters per token on recorded reflection prompts.
CHARS_PER_TOKEN = 4.0
CACHE_READ_WEIGHT = 0.1

_CURRENT = "Current outcome:\n"
_HISTORY = "Outcome history:\n"


@dataclass(frozen=True)
class PromptReplay:
    """Old and new size of the outcome sections of one recorded reflection request."""

    rollout: str
    incident_id: str
    prior_outcomes: int
    requests: int
    old_chars: int
    new_chars: int

    @property
    def saved_chars(self) -> int:
        return self.old_chars - self.new_chars

    @property
    def saved_tokens(self) -> int:
        return math.ceil(self.saved_chars / CHARS_PER_TOKEN)

    @property
    def saved_weighted_tokens(self) -> float:
        return self.saved_tokens * (1 + CACHE_READ_WEIGHT * max(self.requests - 1, 0))


def estimate_tokens(text: str) -> int:
    return math.ceil(len(text) / CHARS_PER_TOKEN)


def _reflection_request(rollout: Path) -> tuple[str, int] | None:
    """The reflection request text and its model-request count, or None without a reflection task."""

    tasks: list[dict[str, object]] = []
    for line in rollout.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        payload = record.get("payload") or {}
        if record.get("type") == "event_msg" and payload.get("type") == "task_started":
            tasks.append({"user": None, "requests": 0})
        elif not tasks:
            continue
        elif (
            record.get("type") == "response_item" and payload.get("type") == "message" and payload.get("role") == "user"
        ):
            text = "".join(str(part.get("text", "")) for part in payload.get("content", []))
            if tasks[-1]["user"] is None and not text.startswith("<environment"):
                tasks[-1]["user"] = text
        elif record.get("type") == "event_msg" and payload.get("type") == "token_count" and payload.get("info"):
            tasks[-1]["requests"] = int(tasks[-1]["requests"]) + 1  # type: ignore[call-overload]
    if len(tasks) < 2 or not isinstance(tasks[1]["user"], str):
        return None
    return tasks[1]["user"], int(tasks[1]["requests"])  # type: ignore[call-overload]


def replay_rollout(rollout: Path) -> PromptReplay | None:
    request = _reflection_request(rollout)
    if request is None:
        return None
    text, requests = request
    current_at = text.find(_CURRENT)
    history_at = text.find(_HISTORY)
    if current_at < 0 or history_at < current_at:
        return None
    current = OutcomeRecord.model_validate_json(text[current_at + len(_CURRENT) : history_at])
    history = [OutcomeRecord.model_validate(item) for item in json.loads(text[history_at + len(_HISTORY) :])]
    old = text[current_at:]
    new = (
        f"Current outcome:\n{json.dumps(current_outcome_view(current), indent=2)}\n\n"
        f"Outcome history:\n{json.dumps(history_view(history, current_incident_id=current.incident_id), indent=2)}\n"
    )
    return PromptReplay(
        rollout=rollout.name,
        incident_id=current.incident_id,
        prior_outcomes=len([record for record in history if record.incident_id != current.incident_id]),
        requests=requests,
        old_chars=len(old),
        new_chars=len(new),
    )


def replay_directories(roots: list[Path]) -> list[PromptReplay]:
    """Replay each distinct rollout under ``roots`` (a session file repeats across cumulative stage folders)."""

    seen: set[str] = set()
    replays: list[PromptReplay] = []
    for root in roots:
        for rollout in sorted(root.rglob("rollout-*.jsonl")):
            if rollout.name in seen:
                continue
            seen.add(rollout.name)
            replay = replay_rollout(rollout)
            if replay is not None:
                replays.append(replay)
    return replays


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("roots", nargs="+", type=Path, help="pipeline log directories containing Codex rollouts")
    args = parser.parse_args(argv)
    replays = replay_directories(args.roots)
    print("incident_id prior_outcomes requests old_chars new_chars saved_tokens saved_weighted_tokens")
    for replay in replays:
        print(
            f"{replay.incident_id} {replay.prior_outcomes} {replay.requests} {replay.old_chars} {replay.new_chars} "
            f"{replay.saved_tokens} {replay.saved_weighted_tokens:.0f}"
        )
    if replays:
        print(
            f"total: {len(replays)} reflections, saved {sum(r.saved_tokens for r in replays)} prompt tokens, "
            f"{sum(r.saved_weighted_tokens for r in replays):.0f} weighted tokens "
            f"({sum(r.saved_weighted_tokens for r in replays) / len(replays):.0f} per reflection)"
        )


if __name__ == "__main__":
    main()
