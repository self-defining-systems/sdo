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
DEFAULT_FALLBACK_SECONDS = 120.0
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


#: ``verify:`` section lines of ``check --trace``: a write whose read-back failed, with the read-back path and result.
_VERIFY_LINE = re.compile(r"^\s+(missing|wrong|unreachable): (\S+) (\S+): (.*)$")


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
    for line in stdout.splitlines():
        found = _VERIFY_LINE.match(line)
        if found and found[2] in res:
            res[found[2]].setdefault("records", []).append(
                {"kind": found[1], "path": found[3], "detail": found[4].strip()}
            )
    match = re.search(r"traffic: requests=(\d+)", stdout)
    for entry in res.values():
        entry["total"] = int(match[1]) if match else 0
        entry["sample"] = entry["sample"].replace("(failed) ", "")
    return res


def _bind(template: str, actual: str) -> dict[str, str] | None:
    """Template variables (``{{p}}``) bound by matching ``template`` against a real path, else ``None``."""
    pattern = ""
    for piece in re.split(r"(\{\{\s*\w+\s*\}\})", template):
        var = re.fullmatch(r"\{\{\s*(\w+)\s*\}\}", piece)
        pattern += f"(?P<{var[1]}>[^/?]+)" if var else re.escape(piece)
    found = re.fullmatch(pattern, actual)
    return found.groupdict() if found else None


def _fill(template: str, bound: Mapping[str, str]) -> str | None:
    """``template`` with its variables replaced, or ``None`` when one is not bound."""
    missing = False

    def sub(m: re.Match[str]) -> str:
        nonlocal missing
        if m[1] in bound:
            return bound[m[1]]
        missing = True
        return m[0]

    out = re.sub(r"\{\{\s*(\w+)\s*\}\}", sub, template)
    return None if missing else out


def _curl(method: str, flags: str, entry: str, path: str, body: str | None = None) -> str:
    data = f" --data '{body}'" if body is not None and method != "GET" else ""
    return f"curl -s -w ' [%{{http_code}}]' -X {method}{flags}{data} '{entry}{path}'"


def _sample_steps(journey: Mapping[str, Any], record: Mapping[str, str], flags: str, entry: str) -> str:
    """The failing sample's own requests: the write the read-back depends on (when its path is recoverable from
    the read-back path), then the read-back that failed, with the real path and the response the user got."""
    request = journey.get("request") or {}
    readback = ((journey.get("readback") or {}).get("request")) or {}
    bound = _bind(readback.get("path", ""), record["path"]) if readback else None
    steps = []
    write_path = _fill(request.get("path", ""), bound) if bound is not None else None
    if write_path is not None and request.get("method", "GET") != "GET":
        body = _fill(str(request["body"]), bound or {}) if request.get("body") is not None else None
        steps.append(f"1. {_curl(request['method'], flags, entry, write_path, body)} (the write succeeded)")
    method = readback.get("method", "GET")
    steps.append(f"{len(steps) + 1}. {_curl(method, flags, entry, record['path'])} -> user got {record['detail']}")
    return " ".join(steps)


def _sample_part(name: str, journey: Mapping[str, Any], result: Mapping[str, Any], flags: str, entry: str) -> str:
    """A failing operation whose check recorded real read-back failures: replay those, not a template request."""
    records = list(result["records"])
    readback = ((journey.get("readback") or {}).get("request")) or {}
    binds = [_bind(readback.get("path", ""), r["path"]) or {} for r in records]
    shared = {k: v for k, v in binds[0].items() if len(records) > 1 and all(b.get(k) == v for b in binds)}
    involved = " (all with " + ", ".join(f"{k}={v}" for k, v in shared.items()) + ")" if shared else ""
    plural = "s" if len(records) > 1 else ""
    head = (
        f"{name}: the users' test does the write, then reads it back; {len(records)} read-back{plural} failed in "
        f"the last check{involved}. Replay the failing sample itself (these records, not another one): "
        f"{_sample_steps(journey, records[0], flags, entry)}"
    )
    if len(records) > 1:
        head += f"; also failed: {records[1]['path']} -> {records[1]['detail']}"
    return head


def recipe(
    pack_journeys: Mapping[str, Any],
    failing: Sequence[str],
    entry: str = DEFAULT_ENTRY,
    budget: int | None = None,
    results: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    """One line: how to try each failing operation against the entry Service (from the pack's journeys). An
    operation whose check recorded failing read-backs is shown as the failing sample's own requests (real
    records and the response); the others as the journey's request with a placeholder record. With a ``budget``
    (characters), operations past it are counted instead of spelled out."""
    by_name = {j["name"]: j for j in pack_journeys["journeys"]}
    header = ((pack_journeys.get("login") or {}).get("header")) or {}
    flags = "".join(f" -H '{k}: {v}'" for k, v in header.items())
    parts = []
    for name in failing:
        result = (results or {}).get(name) or {}
        if result.get("records"):
            parts.append(_sample_part(name, by_name.get(name) or {}, result, flags, entry))
            continue
        request = (by_name.get(name) or {}).get("request") or {}
        method = request.get("method", "GET")
        path = re.sub(r"\{\{[^}]*\}\}", "1", request.get("path", ""))
        parts.append(
            f"{name}: {_curl(method, flags, entry, path)} (a generic request, record 1 is a placeholder: the "
            "failing requests' records were not captured" + ("; a write, JSON body)" if method != "GET" else ")")
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
    clean_checks: Sequence[str] = (),
) -> str:
    """B1's page: the failing and passing user operations of the live check and how to try them.

    The page always states that users have been failing since onset. A live check that saw no error is an extra
    fact with its time, never "nothing is failing"; "These still work" is listed only from a check that reproduced
    the failure, since after clean checks alone it would name the failing operations as working."""
    clean = ""
    if clean_checks:
        clean = f"live check{'s' if len(clean_checks) > 1 else ''} at {', '.join(clean_checks)} saw no error status"
    if failing:
        bullets = "\n".join(
            f"  - {n}: {r['sample'] or 'request failed'} "
            f"({r['failed']} failed of {r['total']} requests in the last 10 s)"
            for n, r in failing.items()
        )
        if clean:
            bullets += f"\n  (an earlier {clean}; a later check reproduced the failures above)"
        ok = ", ".join(n for n in names if n not in failing)
        works = f"These still work: {ok}.\n"
    else:
        bullets = (
            f"  - users have been failing since {onset}; the {clean or 'live check saw no error status'}, and a "
            "check sees error statuses, not wrong or lost data, so which operations fail is not yet known"
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
    return (head + recipe(pack_journeys, list(failing), entry, budget=MAX_MESSAGE - len(head), results=failing))[
        :MAX_MESSAGE
    ]


def fallback_message(namespace: str) -> str:
    return f"Users are reporting problems with the application in namespace {namespace}; investigate and fix it."


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
        clean: list[str] = []
        failing: dict[str, dict[str, Any]] = {}
        for attempt in range(1 + self.config.rechecks):
            at = _stamp(self.clock())
            stdout, stderr = self.live_check()
            name = "page-live-check.txt" if attempt == 0 else f"page-live-check-{attempt + 1}.txt"
            (self.out / name).write_text(stdout + "\n--- stderr\n" + stderr)
            failing = parse_trace(stdout, stderr, names)
            if failing:
                break
            clean.append(at)
        self.sleep(max(0.0, t0 + self.config.after - self.clock()))
        message = page_message(self.namespace, _stamp(t0), failing, names, pack_journeys, self.entry, clean)
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
