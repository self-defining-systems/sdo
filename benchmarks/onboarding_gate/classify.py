"""Rejection classes in ``memory check`` and ``onboarding journeys`` output.

A rejection is one blocking finding: a ``red`` line of ``onboarding journeys`` or an error line of
``memory check``. Its class is the validator rule (``map.coverage``) or the journeys item
(``budget``), with compile errors split by kind (``compile:import``). Classes are what a change
to the SDK, skill or CLI removes; identical findings on repeated calls count once per snapshot.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# "red ITEM SUBJECT: detail" (budget, compiles, older kits) or "red SUBJECT: ITEM: detail" (scenario findings)
# or "red ITEM: detail" (memory-check in newer kits).
RED_LINE = re.compile(r"^red (?P<first>[\w.-]+)(?: (?P<second>[^\s:]+))?: (?P<detail>.*)$")
ITEMS = frozenset(
    {
        "compiles",
        "memory-check",
        "budget",
        "scheduled",
        "passes",
        "cleanup",
        "concurrent",
        "latency-bound",
        "altered-answers",
        "healthy-run",
        "application-map",
    }
)
RULE = re.compile(r"(?:^|: )(?P<rule>[a-z][a-z0-9_-]*(?:\.[a-z0-9_-]+)+): ")
COMPILE_KINDS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("import", re.compile(r"may not import|not allowed|is not in std|cannot find package|no required module")),
    ("syntax", re.compile(r"syntax error|expected '|non-declaration statement")),
    ("undefined", re.compile(r"undefined: |has no field or method|unknown field")),
    ("type", re.compile(r"cannot use |mismatched types|not enough arguments|too many arguments|cannot convert")),
    ("unused", re.compile(r"declared and not used|imported and not used")),
    ("format", re.compile(r"gofmt|not formatted")),
)
# Lines of memory check output that are progress or notes, never rejections.
NOT_A_REJECTION = re.compile(r"^(green |coverage_gap |program |note |\d{4}/\d\d/\d\d |memory check|journeys|planned )")


@dataclass(frozen=True, order=True)
class Rejection:
    """One blocking finding, reduced to a class and a short subject."""

    kind: str
    subject: str
    detail: str
    # "memory" for what memory check (or the probe build) found, "journeys" for the journeys-only items
    origin: str = "journeys"

    def __str__(self) -> str:
        return f"{self.kind} {self.subject}: {self.detail[:160]}"


def compile_kind(detail: str) -> str:
    for name, pattern in COMPILE_KINDS:
        if pattern.search(detail):
            return f"compile:{name}"
    return "compile:other"


def _rule_kind(detail: str) -> str | None:
    match = RULE.search(detail)
    if match is None:
        return None
    rule = match.group("rule")
    if rule.startswith(("go.build", "go.compile", "go.vet")) or "compile" in rule:
        return compile_kind(detail)
    if rule == "go.format":
        return "compile:format"
    if rule in ("detector.test", "go.test"):
        return f"{rule}:{test_failure_kind(detail[match.end() :])}"
    return rule


def test_failure_kind(message: str) -> str:
    """A short, identifier-free name for the first failing check in a generated test's output."""
    found = re.search(r"scenario \S+?:? (?P<text>[^;(]+)", message)
    text = found.group("text") if found else message
    text = re.sub(r"\bstep \S+", "step", text)
    text = re.sub(r'"[^"]*"|`[^`]*`', "Q", text)
    text = re.sub(r"\b(GET|HEAD|POST|PUT|PATCH|DELETE)\b", "METHOD", text)
    text = re.sub(r"\d+", "N", text)
    words = re.findall(r"[A-Za-z][\w-]*|Q|N", text)
    return " ".join(words[:9]) or "other"


def _subject(detail: str) -> str:
    path = re.match(r"^(\.sdo/\S+?)(?::\d+)?: ", detail)
    return path.group(1) if path else "all"


def journeys_rejections(output: str) -> set[Rejection]:
    """Red lines of an ``onboarding journeys`` report (text form)."""
    found = set()
    for line in output.splitlines():
        match = RED_LINE.match(line.strip())
        if match is None:
            continue
        first, second, detail = match.group("first"), match.group("second"), match.group("detail")
        if first in ITEMS:
            item, subject = first, second or "all"
        else:
            head, _, rest = detail.partition(": ")
            item, subject, detail = (head, first, rest) if head in ITEMS else ("other", first, detail)
        if item == "compiles":
            found.add(Rejection(compile_kind(detail), "all", _first_line(detail), "memory"))
        elif item == "memory-check":
            kind = _rule_kind(detail) or "memory-check"
            found.add(Rejection(kind, _subject(detail), _first_line(detail), "memory"))
        else:
            found.add(Rejection(item, subject, _first_line(detail)))
    return found


def memory_check_rejections(output: str) -> set[Rejection]:
    """Error lines of a text ``memory check`` report."""
    found = set()
    for line in output.splitlines():
        line = line.strip()
        if not line or NOT_A_REJECTION.match(line):
            continue
        kind = _rule_kind(line)
        if kind is not None:
            found.add(Rejection(kind, _subject(line), _first_line(line), "memory"))
    return found


def _first_line(text: str) -> str:
    return re.sub(r"/tmp/[\w./-]*?/(traffic|detectors|generated)/", r"\1/", text.strip().splitlines()[0])


def classes(rejections: set[Rejection]) -> set[str]:
    return {rejection.kind for rejection in rejections}
