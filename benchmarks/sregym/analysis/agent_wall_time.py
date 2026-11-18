"""Where an agent session's wall time went: model, tools by category, and waiting on subagents.

Reads Claude Code trajectories in both forms SDO records:

- the CLI's ``stream-json`` output (subagent events carry ``agent_id`` / ``parent_tool_use_id``);
- Claude Code session JSONL (``isSidechain`` / ``agentId`` lines, or one file per subagent).

Every assistant and user event has a ``timestamp``. Per agent, each gap between consecutive events
is attributed to exactly one bucket, so the buckets sum to the agent's wall time:

- gap ending in a tool result: that tool's category (the result is the first thing that proves the
  tool finished, so with parallel tool calls the gap goes to the first result seen);
- gap ending in an assistant event: model time (inference);
- for the main agent, a gap that starts after its turn ended (assistant event with no pending tool
  call) and overlaps a running subagent: waiting on subagents (the overlap; the rest is idle).

Categories are ordered regex rules matched against ``"<Tool>: <command>"`` (the first match wins);
``--categories rules.json`` replaces the defaults with a JSON list of
``{"name": ..., "pattern": ..., "waits": false}``. A rule with ``"waits": true`` marks a polling
command: if it mentions an output file that an earlier command wrote (``> f``, ``--report f``,
``tee f``), it takes that command's category, so ``until grep ^overall run.out`` counts toward the
check that writes ``run.out``.

Usage::

    uv run python -m benchmarks.sregym.analysis.agent_wall_time <transcript.jsonl|dir> [...] \\
        [--categories rules.json] [--top 5] [--json]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

MAIN = "main"
MODEL = "model (inference)"
SUBAGENT_WAIT = "waiting on subagents"
IDLE = "idle (turn ended)"
_INVOCATION = r"(?m)(?:^|[;&|(]\s*|\bnohup\s+|\btimeout\s+\d+\s+)sdo-operator\s+onboarding\s+check\b(?![^;&|\n]*--help)"


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    waits: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("rule name must not be empty")


def _rule(name: str, pattern: str, waits: bool = False) -> Rule:
    return Rule(name, re.compile(pattern), waits)


DEFAULT_RULES: tuple[Rule, ...] = (
    _rule("file reads", r"^(Read|Grep|Glob):"),
    _rule("subagent launch", r"^(Agent|Task):"),
    _rule("file edits/git", r"^(Edit|Write|NotebookEdit):"),
    _rule("check --quick", _INVOCATION + r"[^;&|\n]*--quick"),
    _rule("check full", _INVOCATION),
    _rule(
        "builds/tests/image pushes",
        r"(docker|podman|buildah)\s+(build|push)|\bkind load\b|\bcrane push\b"
        r"|\bgo\s+(build|test)\b|\bmake\s+(image|build)",
    ),
    _rule(
        "waits (sleep/rollout/poll)",
        r"^(Monitor|ScheduleWakeup):|\bsleep\s+\d|rollout status|kubectl[^\n|;]*\bwait\b|\buntil\b|\bwhile\b",
        waits=True,
    ),
    _rule(
        "kubectl/helm writes+exec",
        r"\bhelm\s+(upgrade|install|uninstall)\b"
        r"|kubectl\s+[^\n|;]*\b(apply|patch|delete|scale|rollout restart|replace|create|annotate|label|exec)\b",
    ),
    _rule("kubectl reads/curl", r"\bkubectl\b|\bhelm\s+(get|list|template)\b|\bcurl\b"),
    _rule("file edits/git", r"\bgit\b|\bsed\s+-i\b|\bcat\s*>"),
)
_OUTPUT_FILE = re.compile(r"(?:>>?|--report|\btee)\s*(?:-a\s+)?(/?[\w./-]+\.(?:out|txt|log))")


def load_rules(path: Path) -> tuple[Rule, ...]:
    raw = json.loads(path.read_text())
    if not isinstance(raw, list):
        raise ValueError(f"{path}: categories must be a JSON list")
    return tuple(Rule(str(r["name"]), re.compile(str(r["pattern"])), bool(r.get("waits", False))) for r in raw)


@dataclass
class Call:
    tool: str
    text: str
    start: float
    category: str = "other"


@dataclass
class Agent:
    key: str
    events: list[tuple[float, str, list[Any], str | None]] = field(default_factory=list)  # ts, role, content, uuid

    @property
    def span(self) -> tuple[float, float]:
        return self.events[0][0], self.events[-1][0]


@dataclass
class Breakdown:
    key: str
    wall: float
    buckets: Counter[str]
    counts: Counter[str]
    calls: list[tuple[float, str, str]]  # duration, category, text
    launches: Counter[str]  # calls that matched a rule themselves, not polls of an earlier job's output


def _ts(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _records(path: Path) -> list[tuple[dict[str, Any], str]]:
    files = sorted(path.rglob("*.jsonl")) if path.is_dir() else [path]
    out: list[tuple[dict[str, Any], str]] = []
    for file in files:
        for line in file.read_text().splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if isinstance(rec, dict):
                out.append((rec, file.stem.removeprefix("agent-") if "subagents" in file.parts else ""))
    return out


def load_agents(paths: list[Path]) -> dict[str, Agent]:
    """Group timestamped assistant/user events by agent, merging the two trajectory forms."""
    agents: dict[str, Agent] = {}
    seen: set[str] = set()
    for path in paths:
        for rec, file_agent in _records(path):
            message = rec.get("message")
            if (
                rec.get("type") not in ("assistant", "user")
                or not isinstance(message, dict)
                or not rec.get("timestamp")
            ):
                continue
            uuid = rec.get("uuid")
            if isinstance(uuid, str):
                if uuid in seen:
                    continue
                seen.add(uuid)
            key = rec.get("agent_id") or rec.get("agentId") or file_agent
            if not key:
                key = f"sidechain-{rec.get('parent_tool_use_id')}" if rec.get("isSidechain") else MAIN
            content = message.get("content")
            blocks: list[Any] = [{"type": "text", "text": content}] if isinstance(content, str) else list(content or [])
            agents.setdefault(str(key), Agent(str(key))).events.append(
                (_ts(rec["timestamp"]), str(rec["type"]), blocks, uuid)
            )
    for agent in agents.values():
        agent.events.sort(key=lambda e: e[0])
    return agents


def _command_text(name: str, tool_input: Any) -> str:
    if isinstance(tool_input, dict):
        body = (
            tool_input.get("command")
            or tool_input.get("file_path")
            or tool_input.get("prompt")
            or json.dumps(tool_input)
        )
    else:
        body = str(tool_input)
    return f"{name}: {body}"


def categorize(text: str, rules: tuple[Rule, ...], jobs: dict[str, str]) -> tuple[str, bool]:
    """Category of one tool call, and whether the rule matched the call itself (not a polled job)."""
    for rule in rules:
        if rule.pattern.search(text):
            if rule.waits:
                for known, category in jobs.items():
                    if known in text:
                        return category, False
            return rule.name, True
    return "other tool", True


def _sub_union(agents: dict[str, Agent], main: str) -> list[tuple[float, float]]:
    spans = sorted(a.span for k, a in agents.items() if k != main)
    merged: list[list[float]] = []
    for lo, hi in spans:
        if merged and lo <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return [(lo, hi) for lo, hi in merged]


def _overlap(lo: float, hi: float, spans: list[tuple[float, float]]) -> float:
    return sum(max(0.0, min(hi, b) - max(lo, a)) for a, b in spans)


def analyze_agent(
    agent: Agent, rules: tuple[Rule, ...], subagent_spans: list[tuple[float, float]] | None = None
) -> Breakdown:
    pending: dict[str, Call] = {}
    jobs: dict[str, str] = {}
    launches: Counter[str] = Counter()
    buckets: Counter[str] = Counter()
    counts: Counter[str] = Counter()
    calls: list[tuple[float, str, str]] = []
    prev: float | None = None
    turn_ended = False
    for ts, role, blocks, _ in agent.events:
        gap = 0.0 if prev is None else ts - prev
        if role == "assistant":
            if turn_ended and subagent_spans is not None:
                wait = _overlap(prev if prev is not None else ts, ts, subagent_spans)
                buckets[SUBAGENT_WAIT] += wait
                buckets[IDLE] += gap - wait
            else:
                buckets[MODEL] += gap
            for block in blocks:
                if isinstance(block, dict) and block.get("type") == "tool_use":
                    text = _command_text(str(block.get("name")), block.get("input"))
                    call = Call(str(block.get("name")), text, ts)
                    call.category, direct = categorize(text, rules, jobs)
                    if direct:
                        launches[call.category] += 1
                    for path in _OUTPUT_FILE.findall(text):
                        if call.category.startswith("check"):
                            jobs[path] = call.category
                    pending[str(block.get("id"))] = call
            turn_ended = not any(isinstance(b, dict) and b.get("type") == "tool_use" for b in blocks) and not pending
        else:
            results = [b for b in blocks if isinstance(b, dict) and b.get("type") == "tool_result"]
            done = [pending.pop(str(b.get("tool_use_id"))) for b in results if str(b.get("tool_use_id")) in pending]
            if done:
                buckets[done[0].category] += gap
                for call in done:
                    counts[call.category] += 1
                    calls.append((ts - call.start, call.category, call.text))
                turn_ended = False
            else:
                buckets[MODEL] += gap
        prev = ts
    lo, hi = agent.span
    counts[MODEL] = sum(1 for e in agent.events if e[1] == "assistant")
    return Breakdown(agent.key, hi - lo, buckets, counts, calls, launches)


def analyze(paths: list[Path], rules: tuple[Rule, ...] = DEFAULT_RULES) -> dict[str, Any]:
    return analyze_agents(load_agents(paths), rules)


def analyze_agents(agents: dict[str, Agent], rules: tuple[Rule, ...] = DEFAULT_RULES) -> dict[str, Any]:
    if not agents:
        raise ValueError("no timestamped assistant/user events found")
    main = MAIN if MAIN in agents else min(agents, key=lambda k: agents[k].span[0])
    spans = _sub_union(agents, main)
    result = {k: analyze_agent(a, rules, spans if k == main else None) for k, a in agents.items()}
    return {"main": result[main], "subagents": [v for k, v in result.items() if k != main]}


def _table(title: str, wall: float, buckets: Counter[str], counts: Counter[str]) -> list[str]:
    lines = [f"== {title}: {wall / 60:.1f} min", f"{'category':32}{'min':>8}{'%wall':>7}{'n':>6}"]
    for name, secs in sorted(buckets.items(), key=lambda kv: -kv[1]):
        if secs >= 0.05:
            lines.append(f"{name:32}{secs / 60:8.1f}{100 * secs / wall if wall else 0:7.1f}{counts.get(name, 0):6}")
    return lines


def render(report: dict[str, Any], top: int = 5) -> str:
    main: Breakdown = report["main"]
    subs: list[Breakdown] = report["subagents"]
    lines = _table(f"main thread ({main.key})", main.wall, main.buckets, main.counts)
    if subs:
        total: Counter[str] = Counter()
        counts: Counter[str] = Counter()
        for sub in subs:
            total.update(sub.buckets)
            counts.update(sub.counts)
        lines += ["", *_table(f"subagents, summed over {len(subs)}", sum(s.wall for s in subs), total, counts)]
        lines += ["", "per subagent: " + "; ".join(f"{s.key} {s.wall / 60:.1f} min" for s in subs)]
    every = [c for b in [main, *subs] for c in b.calls]
    lines += ["", f"top {top} longest tool calls:"]
    for dur, cat, text in sorted(every, reverse=True)[:top]:
        lines.append(f"  {dur / 60:6.1f} min [{cat}] {' '.join(text.split())[:150]}")
    checks = [(b, c) for b in [main, *subs] for c in b.buckets if c.startswith("check")]
    if checks:
        runs = sum(b.launches[c] for b, c in checks)
        mins = sum(b.buckets[c] for b, c in checks) / 60
        lines.append(
            f"\ncheck runs: {runs} (a call may chain a check with other commands); "
            f"{mins:.1f} min in them, polls of their output included"
        )
    return "\n".join(lines)


def main_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("paths", nargs="+", type=Path, help="stream-json / session JSONL files or directories")
    parser.add_argument("--categories", type=Path, help="JSON list of {name, pattern, waits} replacing the defaults")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = analyze(args.paths, load_rules(args.categories) if args.categories else DEFAULT_RULES)
    if args.json:
        flat = lambda b: {"agent": b.key, "wall_s": b.wall, "buckets_s": dict(b.buckets), "counts": dict(b.counts)}  # noqa: E731
        print(json.dumps({"main": flat(report["main"]), "subagents": [flat(s) for s in report["subagents"]]}, indent=1))
    else:
        print(render(report, args.top))
    return 0


if __name__ == "__main__":
    raise SystemExit(main_cli())
