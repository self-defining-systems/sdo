"""``python3 -m sdo.agents.responder.status_cli``: the responder's verify-before-submit check.

The controller closes an incident only after its health detectors clear, and
the synthetic-traffic detectors clear only when the health judge's scenarios
meet their SLOs again. This command runs the judge's verify-burst workload
through the same isolated prober, now, so the responder can confirm the
repair against the acceptance test the controller will apply instead of
guessing from rollout state. The controller passes the prober's address as
``SDO_PROBER_URL``; without it the command reports ``unavailable`` and the
responder falls back to its own verification.
"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from sdo.contracts import Finding, FindingStatus, IncidentRequest, IncidentView, StateChange

if TYPE_CHECKING:
    from collections.abc import Sequence

PROBER_URL_ENVIRONMENT = "SDO_PROBER_URL"
REQUEST_PATH_ENVIRONMENT = "SDO_REQUEST_PATH"
DEFAULT_REQUEST_PATH = Path("/sdo/request/incident-request.json")
#: Rule-ID prefix of the synthetic-traffic health detector's findings.
SCENARIO_RULE_PREFIX = "scenario-slo."
#: Where the controller publishes its state, as NAMESPACE/NAME of a ConfigMap.
CONTROLLER_STATE_ENVIRONMENT = "SDO_CONTROLLER_STATE"
CONTROLLER_STATE_KEY = "runtime-state.json"
CONTROLLER_STATE_TIMEOUT_SECONDS = 15.0
BURST_PATH = "/v1/bursts"
#: A verify burst is capped at 60 seconds by the workload schema.
BURST_TIMEOUT_SECONDS = 90.0

EXIT_HEALTHY = 0
EXIT_UNHEALTHY = 1
EXIT_UNAVAILABLE = 3

_MAX_FAILURES_SHOWN = 2


class IncidentStatusState(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNAVAILABLE = "unavailable"


class VerifyScenarioVerdict(BaseModel):
    """One scenario's judgement after a verify burst, as the prober reports it."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    scenario: str
    healthy: bool
    qualified: bool = True
    evaluated: bool = False
    samples: int = 0
    error_rate: float = Field(default=0.0, alias="errorRate")
    latency_ms: int = Field(default=0, alias="latencyMs")
    violations: list[str] = Field(default_factory=list)
    status_counts: dict[str, int] = Field(default_factory=dict, alias="statusCounts")
    recent_failures: list[str] = Field(default_factory=list, alias="recentFailures")

    @property
    def blocking(self) -> bool:
        return self.qualified and not self.healthy


class VerifyBurstResult(BaseModel):
    """A verify burst's outcome; the prober's raw observation window is dropped."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    workload: str
    healthy: bool
    started_at: str = Field(default="", alias="startedAt")
    duration_ns: int = Field(default=0, alias="durationNs")
    verdicts: list[VerifyScenarioVerdict] = Field(default_factory=list)


class ControllerViewError(RuntimeError):
    """The controller's state ConfigMap could not be read."""


@dataclass(frozen=True)
class IncidentStatusReport:
    state: IncidentStatusState
    burst: VerifyBurstResult | None
    detail: str
    incident_scenarios: tuple[str, ...] = field(default=())
    #: The controller's live view of this incident, when it could be read.
    view: IncidentView | None = None
    #: Why no view was used, or where it came from.
    controller_detail: str = ""
    #: Active non-traffic findings of the detectors closure waits on, not yet confirmed clear.
    blocking_findings: tuple[Finding, ...] = field(default=())
    #: Non-traffic findings the controller confirmed clear; they hold only closure's own clear persistence.
    clearing_findings: tuple[Finding, ...] = field(default=())
    #: Configuration changes the controller sees now that the request's diff lacks.
    new_state_changes: tuple[StateChange, ...] = field(default=())

    def __post_init__(self) -> None:
        if not isinstance(self.state, IncidentStatusState):
            raise TypeError("state must be a IncidentStatusState")
        if self.state != IncidentStatusState.UNAVAILABLE and self.burst is None:
            raise ValueError("a healthy or unhealthy status needs the verify burst that produced it")
        if (self.blocking_findings or self.clearing_findings) and self.view is None:
            raise ValueError("blocking and clearing findings come from the controller's view")

    @property
    def exit_code(self) -> int:
        return {
            IncidentStatusState.HEALTHY: EXIT_HEALTHY,
            IncidentStatusState.UNHEALTHY: EXIT_UNHEALTHY,
            IncidentStatusState.UNAVAILABLE: EXIT_UNAVAILABLE,
        }[self.state]

    def to_json(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "detail": self.detail,
            "incident_scenarios": list(self.incident_scenarios),
            "burst": None if self.burst is None else self.burst.model_dump(mode="json"),
            "controller": {
                "detail": self.controller_detail,
                "observed_at": None if self.view is None else self.view.observed_at.isoformat(),
                "blocking_detectors": sorted({finding.detector_id for finding in self.blocking_findings}),
                "blocking_findings": [finding.model_dump(mode="json") for finding in self.blocking_findings],
                "clearing_detectors": sorted({finding.detector_id for finding in self.clearing_findings}),
                "clearing_findings": [finding.model_dump(mode="json") for finding in self.clearing_findings],
                "new_state_changes": [change.model_dump(mode="json") for change in self.new_state_changes],
            },
        }

    def render(self) -> str:
        if self.burst is None:
            return (
                f"incident status: UNAVAILABLE ({self.detail}). No synthetic-traffic verification is available; "
                "rely on your own verification of the health objective."
            )
        seconds = self.burst.duration_ns / 1e9
        lines = [
            f"incident status: {self.state.value.upper()} "
            f"(verify burst {self.burst.workload!r}, {len(self.burst.verdicts)} scenario(s), {seconds:.1f}s)"
        ]
        incident = set(self.incident_scenarios)
        ordered = sorted(self.burst.verdicts, key=lambda verdict: (verdict.scenario not in incident, verdict.scenario))
        for verdict in ordered:
            label = verdict.scenario + (" (triggered this incident)" if verdict.scenario in incident else "")
            if verdict.healthy:
                lines.append(
                    f"  ok    {label}: {verdict.samples} samples, error rate {verdict.error_rate:.2f}, "
                    f"latency {verdict.latency_ms}ms"
                )
                continue
            tag = "FAIL " if verdict.qualified else "skip "
            text = f"  {tag} {label}: {verdict.samples} samples, error rate {verdict.error_rate:.2f}"
            if verdict.violations:
                text += f"; {'; '.join(verdict.violations)}"
            if verdict.status_counts:
                counts = ", ".join(f"{status} x{count}" for status, count in sorted(verdict.status_counts.items()))
                text += f"; statuses: {counts}"
            if not verdict.qualified:
                text += "; never passed since the controller started, so it does not block closure"
            lines.append(text)
            lines.extend(f"          {failure}" for failure in verdict.recent_failures[:_MAX_FAILURES_SHOWN])
        for finding in self.blocking_findings:
            resource = finding.primary_resource
            lines.append(
                f"  FAIL  controller detector {finding.detector_id}: {finding.summary} "
                f"({resource.kind}/{resource.name}): {finding.evidence}"
            )
        for finding in self.clearing_findings:
            resource = finding.primary_resource
            lines.append(
                f"  ok    controller detector {finding.detector_id}: confirmed clear on fresh evaluations "
                f"({resource.kind}/{resource.name}); closure confirms it on its own schedule"
            )
        if self.new_state_changes:
            lines.append("Changed since the incident request was taken (not in its state diff):")
            lines.extend(f"  {change.kind}/{change.name} {change.change}" for change in self.new_state_changes)
        if self.state == IncidentStatusState.UNHEALTHY:
            if any(verdict.blocking for verdict in self.burst.verdicts):
                lines.append(
                    "The controller will not close this incident while these scenarios fail. Keep repairing, then run "
                    "`python3 -m sdo.agents.responder.status_cli` again before submitting mitigation "
                    "or returning the result."
                )
            if self.blocking_findings and self.view is not None:
                lines.append(
                    "The controller will not close this incident while its health detectors still fire, even when "
                    f"traffic is healthy. This is its evaluation at {self.view.observed_at.isoformat()}. Once the "
                    "cause is repaired it re-checks these detectors every second and confirms them clear within a "
                    "few seconds; run `python3 -m sdo.agents.responder.status_cli` again before submitting "
                    "mitigation or "
                    "returning the result."
                )
        return "\n".join(lines)


Opener = Callable[..., Any]


def incident_status(
    *,
    prober_url: str | None,
    request_path: Path | None,
    workload: str | None = None,
    scenarios: Sequence[str] = (),
    timeout_seconds: float = BURST_TIMEOUT_SECONDS,
    opener: Opener = urllib.request.urlopen,
    controller_state: str | None = None,
    controller_reader: Callable[[str], str] | None = None,
) -> IncidentStatusReport:
    """Run a verify burst now and judge it as the closure gate would.

    The burst covers the synthetic-traffic scenarios. Closure also waits on
    every other health detector, so when the controller's state ConfigMap is
    reachable (``controller_state`` is ``NAMESPACE/NAME``), their active
    findings for this incident make the status unhealthy too, and changes the
    controller saw after the request was taken are reported.
    """

    incident = _incident_scenarios(request_path)
    if not prober_url:
        return IncidentStatusReport(
            state=IncidentStatusState.UNAVAILABLE,
            burst=None,
            detail=f"{PROBER_URL_ENVIRONMENT} is not set",
            incident_scenarios=incident,
        )
    body: dict[str, Any] = {}
    if workload:
        body["workload"] = workload
    if scenarios:
        body["scenarios"] = list(scenarios)
    request = urllib.request.Request(
        prober_url.rstrip("/") + BURST_PATH,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with opener(request, timeout=timeout_seconds) as response:
            burst = VerifyBurstResult.model_validate_json(response.read())
    except urllib.error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace").strip() or str(exc)
        return IncidentStatusReport(
            state=IncidentStatusState.UNAVAILABLE,
            burst=None,
            detail=f"prober rejected the burst: {message}",
            incident_scenarios=incident,
        )
    except (OSError, ValidationError, ValueError) as exc:
        return IncidentStatusReport(
            state=IncidentStatusState.UNAVAILABLE,
            burst=None,
            detail=f"prober unreachable: {exc}",
            incident_scenarios=incident,
        )
    view, controller_detail = _controller_view(
        controller_state, controller_reader or read_controller_state, request_path
    )
    # Traffic findings are the controller's view of the same scenarios the burst just measured afresh.
    blocking_findings = () if view is None else _non_traffic(view.blocking_findings)
    clearing_findings = () if view is None else _non_traffic(view.clearing_findings)
    new_changes = () if view is None else _new_state_changes(_load_request(request_path), view)
    scenarios_fail = any(verdict.blocking for verdict in burst.verdicts) or not burst.verdicts
    state = IncidentStatusState.UNHEALTHY if scenarios_fail or blocking_findings else IncidentStatusState.HEALTHY
    if state == IncidentStatusState.HEALTHY:
        detail = "every qualified scenario meets its SLO"
    elif scenarios_fail:
        detail = "scenarios violate their SLO"
    else:
        detail = "the controller's health detectors still fire"
    return IncidentStatusReport(
        state=state,
        burst=burst,
        detail=detail,
        incident_scenarios=incident,
        view=view,
        controller_detail=controller_detail,
        blocking_findings=blocking_findings,
        clearing_findings=clearing_findings,
        new_state_changes=new_changes,
    )


def _non_traffic(findings: Sequence[Finding]) -> tuple[Finding, ...]:
    return tuple(finding for finding in findings if not finding.rule_id.startswith(SCENARIO_RULE_PREFIX))


def read_controller_state(location: str) -> str:
    """The controller's ``runtime-state.json`` from its ConfigMap at ``NAMESPACE/NAME``, read with kubectl."""

    namespace, _, name = location.partition("/")
    if not namespace or not name or "/" in name:
        raise ControllerViewError(f"{CONTROLLER_STATE_ENVIRONMENT} must be NAMESPACE/NAME, got {location!r}")
    try:
        completed = subprocess.run(
            ["kubectl", "get", "configmap", name, "-n", namespace, "-o", "json"],
            capture_output=True,
            text=True,
            timeout=CONTROLLER_STATE_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ControllerViewError(f"kubectl get configmap {location}: {exc}") from exc
    if completed.returncode != 0:
        raise ControllerViewError(completed.stderr.strip() or f"kubectl exited {completed.returncode}")
    try:
        payload = json.loads(completed.stdout)["data"][CONTROLLER_STATE_KEY]
    except (ValueError, KeyError, TypeError) as exc:
        raise ControllerViewError(f"ConfigMap {location} has no {CONTROLLER_STATE_KEY}") from exc
    if not isinstance(payload, str):
        raise ControllerViewError(f"ConfigMap {location} has no {CONTROLLER_STATE_KEY}")
    return payload


def _controller_view(
    location: str | None, read: Callable[[str], str], request_path: Path | None
) -> tuple[IncidentView | None, str]:
    """The controller's view of the incident being answered, or why none applies."""

    if not location:
        return None, f"{CONTROLLER_STATE_ENVIRONMENT} is not set"
    try:
        state = json.loads(read(location))
        raw = state.get("incident_view") if isinstance(state, dict) else None
        if raw is None:
            return None, f"the controller reports no open incident ({location})"
        view = IncidentView.model_validate(raw)
    except ControllerViewError as exc:
        return None, f"controller state unreadable: {exc}"
    except (ValueError, ValidationError) as exc:
        return None, f"controller state invalid: {exc}"
    request = _load_request(request_path)
    if request is not None and request.incident_id != view.incident_id:
        return None, f"the controller's open incident is {view.incident_id}, not {request.incident_id}"
    return view, f"controller view of {view.incident_id} at {view.observed_at.isoformat()}"


def _new_state_changes(request: IncidentRequest | None, view: IncidentView) -> tuple[StateChange, ...]:
    if view.state_changes is None:
        return ()
    known = set()
    if request is not None and request.state_changes is not None:
        known = {(change.kind, change.name, change.change) for change in request.state_changes.changes}
    return tuple(
        change for change in view.state_changes.changes if (change.kind, change.name, change.change) not in known
    )


def _load_request(request_path: Path | None) -> IncidentRequest | None:
    if request_path is None or not request_path.is_file():
        return None
    try:
        return IncidentRequest.model_validate_json(request_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _incident_scenarios(request_path: Path | None) -> tuple[str, ...]:
    request = _load_request(request_path)
    if request is None:
        return ()
    return tuple(
        sorted(
            {
                finding.rule_id.removeprefix(SCENARIO_RULE_PREFIX)
                for finding in request.findings
                if finding.status == FindingStatus.ACTIVE and finding.rule_id.startswith(SCENARIO_RULE_PREFIX)
            }
        )
    )


def live_incident_status(*, workload: str | None = None, scenarios: Sequence[str] = ()) -> IncidentStatusReport:
    """The incident status for the responder's environment (prober URL and mounted request)."""

    return incident_status(
        prober_url=os.environ.get(PROBER_URL_ENVIRONMENT, "").strip() or None,
        request_path=Path(os.environ.get(REQUEST_PATH_ENVIRONMENT, str(DEFAULT_REQUEST_PATH))),
        workload=workload,
        scenarios=scenarios,
        controller_state=os.environ.get(CONTROLLER_STATE_ENVIRONMENT, "").strip() or None,
    )


def run_incident_status_cli(*, workload: str | None, scenarios: Sequence[str], as_json: bool) -> int:
    """Entry point for ``python3 -m sdo.agents.responder.status_cli``."""

    status = live_incident_status(workload=workload, scenarios=scenarios)
    print(json.dumps(status.to_json(), indent=2) if as_json else status.render())
    return status.exit_code
