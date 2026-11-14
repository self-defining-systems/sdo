"""The harness-paged arm: the benchmark pages SDO's responder the way the vanilla baseline B1 is paged.

At the inject plus ``check_after`` the pager runs a short live user check (``fuzzops check --trace``); at the inject
plus ``after`` it opens one SDO incident through the external-incident inbox (the ``sdo-external-incident``
ConfigMap) whose message lists the failing and passing user operations of that check. The text is B1's page
(failing operations with a sample error and counts, the operations that still work, how to try one), without B1's
agent instructions, which SDO's own prompt replaces. SDO's detectors only observe in this arm
(``mode: external`` with ``response.external_clues: true``): the harness opens the incident and the responder's
completion closes it.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

INBOX = "sdo-external-incident"
DEFAULT_ENTRY = "http://gitlab-webservice-default:8181"
#: ``ExternalIncident.message`` max_len: the controller's inbox refuses a longer alert.
MAX_MESSAGE = 4096
_FROM_A_POD = " (run from a pod in the namespace, e.g. kubectl exec deploy/gitlab-toolbox -- ...)"


@dataclass(frozen=True)
class PageConfig:
    """``settings.harness_page`` of the operator block: when the page is sent, relative to the pager's start."""

    after: float = 30.0
    check_after: float = 20.0
    check_duration: float = 10.0
    rate: int = 6
    #: Further live checks when one saw no failing operation: a page states what users see failing, so a clean
    #: check is repeated (delaying the page) before the page says the failures were not reproduced.
    rechecks: int = 2

    def __post_init__(self) -> None:
        for name in ("after", "check_after", "check_duration"):
            if getattr(self, name) < 0:
                raise ValueError(f"harness_page.{name} must not be negative")
        if self.check_after + self.check_duration > self.after + 15:
            raise ValueError("harness_page: the live check must end about when the page is sent")
        if self.rate < 1:
            raise ValueError("harness_page.rate must be at least 1")
        if self.rechecks < 0:
            raise ValueError("harness_page.rechecks must not be negative")

    @classmethod
    def from_settings(cls, raw: Any) -> PageConfig | None:
        """``None`` for an absent or false value; ``true`` is the defaults (B1's schedule)."""
        if raw is None or raw is False:
            return None
        if raw is True:
            return cls()
        if not isinstance(raw, Mapping):
            raise TypeError("harness_page must be true or an object")
        unknown = set(raw) - {"after", "check_after", "check_duration", "rate", "rechecks"}
        if unknown:
            raise ValueError(f"harness_page has unknown keys {sorted(unknown)}")
        return cls(
            after=float(raw.get("after", 30.0)),
            check_after=float(raw.get("check_after", 20.0)),
            check_duration=float(raw.get("check_duration", 10.0)),
            rate=int(raw.get("rate", 6)),
            rechecks=int(raw.get("rechecks", 2)),
        )


def journeys(pack: Path) -> dict[str, Any]:
    return yaml.safe_load((pack / "journeys.yaml").read_text())


def parse_trace(stdout: str, stderr: str, names: Sequence[str]) -> dict[str, dict[str, Any]]:
    """Per failing journey: failed count, total requests and one sample error, from ``check --trace`` output."""
    res: dict[str, dict[str, Any]] = {}
    for line in stderr.splitlines():
        for name in sorted(names, key=len, reverse=True):
            if re.search(r"(?<![\w-])" + re.escape(name) + r"(?![\w-])", line):
                entry = res.setdefault(name, {"failed": 0, "sample": ""})
                entry["failed"] += 1
                entry["sample"] = entry["sample"] or re.sub(r"\s+", " ", line.split(name, 1)[1]).strip(" :-")[:120]
                break
    match = re.search(r"traffic: requests=(\d+)", stdout)
    for entry in res.values():
        entry["total"] = int(match[1]) if match else 0
        entry["sample"] = entry["sample"].replace("(failed) ", "")
    return res


def recipe(
    pack_journeys: Mapping[str, Any], failing: Sequence[str], entry: str = DEFAULT_ENTRY, budget: int | None = None
) -> str:
    """One line: how to try each failing operation against the entry Service (from the pack's journeys). With a
    ``budget`` (characters), operations past it are counted instead of spelled out."""
    by_name = {j["name"]: j for j in pack_journeys["journeys"]}
    header = ((pack_journeys.get("login") or {}).get("header")) or {}
    flags = "".join(f" -H '{k}: {v}'" for k, v in header.items())
    parts = []
    for name in failing:
        request = (by_name.get(name) or {}).get("request") or {}
        method = request.get("method", "GET")
        path = re.sub(r"\{\{[^}]*\}\}", "1", request.get("path", ""))
        parts.append(
            f"{name}: curl -s -w ' [%{{http_code}}]' -X {method}{flags} '{entry}{path}'"
            + (" (a write; JSON body)" if method != "GET" else "")
        )
    shown = len(parts)
    while budget is not None and shown and len(_recipe_line(parts, shown)) > budget:
        shown -= 1
    return _recipe_line(parts, shown)


def _recipe_line(parts: Sequence[str], shown: int) -> str:
    rest = len(parts) - shown
    more = f"; (+{rest} more operations: the same form against each one's path)" if rest else ""
    return "; ".join(parts[:shown]) + more + _FROM_A_POD


def page_message(
    namespace: str,
    onset: str,
    failing: Mapping[str, Mapping[str, Any]],
    names: Sequence[str],
    pack_journeys: Mapping[str, Any],
    entry: str = DEFAULT_ENTRY,
    checks: int = 1,
) -> str:
    """B1's page: the failing and passing user operations of the live check and how to try them. When no check
    reproduced a failure the page never says an operation works: users still see failures somewhere."""
    if failing:
        bullets = "\n".join(
            f"  - {n}: {r['sample'] or 'request failed'} "
            f"({r['failed']} failed of {r['total']} requests in the last 10 s)"
            for n, r in failing.items()
        )
        ok = ", ".join(n for n in names if n not in failing)
        works = f"These still work: {ok}.\n"
    else:
        bullets = (
            f"  - (not reproduced: {checks} short live checks of every operation returned no error; users still "
            "report failures, so any operation may be affected, including writes whose data is lost or wrong)"
        )
        works = ""
    head = (
        f"Page for the application in Kubernetes namespace {namespace}.\n"
        f"Since {onset}, users are seeing failures:\n{bullets}\n"
        f"{works}"
        f"To check an operation yourself: "
    )
    # B1's page has no bound; the contract does. The failing and passing operations are kept whole and only
    # how-to-check lines that do not fit are counted, so the alert is never refused.
    return (head + recipe(pack_journeys, list(failing), entry, budget=MAX_MESSAGE - len(head)))[:MAX_MESSAGE]


def inbox_manifest(control_namespace: str, incident_id: str, namespace: str, message: str) -> dict[str, Any]:
    alert = {"id": incident_id, "namespace": namespace, "message": message}
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": INBOX, "namespace": control_namespace},
        "data": {"incident.json": json.dumps(alert)},
    }


def _stamp(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


@dataclass
class Pager:
    """Runs the live check and submits the one external incident. I/O edges are injectable for tests."""

    config: PageConfig
    namespace: str
    control_namespace: str
    pack: Path
    incident_id: str
    out: Path
    live_check: Callable[[], tuple[str, str]]
    submit: Callable[[dict[str, Any]], None]
    entry: str = DEFAULT_ENTRY
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], None] = time.sleep

    def run(self, t0: float) -> str:
        """Check at ``t0 + check_after``, page at ``t0 + after``; returns the message that was submitted."""
        pack_journeys = journeys(self.pack)
        names = [j["name"] for j in pack_journeys["journeys"]]
        self.sleep(max(0.0, t0 + self.config.check_after - self.clock()))
        stdout, stderr = self.live_check()
        (self.out / "page-live-check.txt").write_text(stdout + "\n--- stderr\n" + stderr)
        failing = parse_trace(stdout, stderr, names)
        checks = 1
        while not failing and checks <= self.config.rechecks:
            stdout, stderr = self.live_check()
            checks += 1
            (self.out / f"page-live-check-{checks}.txt").write_text(stdout + "\n--- stderr\n" + stderr)
            failing = parse_trace(stdout, stderr, names)
        self.sleep(max(0.0, t0 + self.config.after - self.clock()))
        message = page_message(self.namespace, _stamp(t0), failing, names, pack_journeys, self.entry, checks)
        sent = self.clock()
        self.submit(inbox_manifest(self.control_namespace, self.incident_id, self.namespace, message))
        (self.out / "page.json").write_text(
            json.dumps(
                {
                    "incident_id": self.incident_id,
                    "t0": t0,
                    "at": _stamp(sent),
                    "offset_s": round(sent - t0, 1),
                    "message": message,
                },
                indent=2,
            )
        )
        return message


def fuzzops_check(
    binary: Path, pack: Path, namespace: str, config: PageConfig, run: Callable[..., Any] = subprocess.run
) -> tuple[str, str]:
    """The live check B1 uses: a short ``fuzzops check --trace`` of the user operations."""
    proc = run(
        [
            str(binary),
            "check",
            "--pack",
            str(pack),
            "--namespace",
            namespace,
            "--duration",
            f"{config.check_duration:g}s",
            "--rate",
            str(config.rate),
            "--trace",
            "--lines",
        ],
        capture_output=True,
        text=True,
        cwd=binary.resolve().parent.parent,
        timeout=120,
        check=False,
    )
    return proc.stdout, proc.stderr


def kubectl_apply(manifest: Mapping[str, Any], kubeconfig: str = "", context: str = "") -> None:
    cmd = [shutil.which("kubectl") or "kubectl"]
    if kubeconfig:
        cmd += ["--kubeconfig", kubeconfig]
    if context:
        cmd += ["--context", context]
    cmd += ["apply", "-f", "-"]
    subprocess.run(cmd, input=json.dumps(manifest), text=True, check=True, capture_output=True, timeout=60)
