"""The fallback page: when SDO has not opened its own incident ``fallback_page_seconds`` after injection, the harness
pages it like an on-call human, through the production external-incident inbox.

The page text is the vanilla arm's incident text: the application, namespace and description block of SREGym's
claudecode instruction (``clients/claudecode/driver.py:build_instruction``) without its submission protocol. It carries
no fault name and no SDO-specific hint. One decision per cell: it pages once, or never when SDO opened its own incident.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import (
    apply_external_incident,
    control_namespace_for,
    read_runtime_state,
    state_incident_ids,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.eval.cell import Cell, Lane
    from benchmarks.sregym.eval.plan import EvalConfig


def incident_notice(application: str, namespace: str, description: str) -> str:
    """The incident text both arms receive: the verbatim block ``build_instruction`` renders for the app."""
    return f"Application: {application}\nNamespace: {namespace}\n\n{description}"


def lane_notice(lane: Lane) -> str:
    """The notice for a lane, from the application facts its backend recorded at claim."""
    return incident_notice(lane.info["application"], lane.info["namespace"], lane.info.get("description", ""))


def has_own_incident(state: dict[str, Any], known: frozenset[str]) -> bool:
    """Whether SDO opened an incident since injection: one is open, or the state names an incident ID not ``known``
    at injection (open, awaiting acknowledgement, or already Recovered, closed and acknowledged)."""
    return bool(state.get("incident_open")) or bool(state_incident_ids(state) - known)


class PageCluster(Protocol):
    def own_incident(self, lane: Lane, known: frozenset[str]) -> bool:
        """Whether SDO opened an incident of its own on the lane's cluster since injection."""
        ...

    def send(self, lane: Lane, incident_id: str, message: str) -> None:
        """Write the external incident to the lane's inbox."""
        ...


class KubectlPageCluster:
    """The lane's controller state and external-incident inbox, over kubectl and the lane kubeconfig."""

    def own_incident(self, lane: Lane, known: frozenset[str]) -> bool:
        control = control_namespace_for(lane.info["namespace"])
        return has_own_incident(read_runtime_state(control, kubeconfig=_kubeconfig(lane)), known)

    def send(self, lane: Lane, incident_id: str, message: str) -> None:
        namespace = lane.info["namespace"]
        apply_external_incident(
            control_namespace_for(namespace), namespace, incident_id, message, kubeconfig=_kubeconfig(lane)
        )


def _kubeconfig(lane: Lane) -> Path:
    return Path(lane.info["kubeconfig"])


def _page_id() -> str:
    return f"page-{uuid.uuid4().hex}"


@dataclass
class FallbackPageHook:
    """Pages SDO once, at ``injected_at + fallback_page_seconds``, only if it has no incident of its own by then.

    ``page`` is polled while the arm waits; before the deadline it costs nothing. The first call at or after the
    deadline decides the cell: an own incident (open, or already closed) means no page, otherwise the page is
    written and its time (the write's acknowledgement) is returned. Later calls return ``None``.
    """

    fallback_page_seconds: float
    cluster: PageCluster
    now: Callable[[], float]
    notice: Callable[[Lane], str] = lane_notice
    new_id: Callable[[], str] = _page_id
    paged: dict[str, float] = field(default_factory=dict)
    #: When the hook found the cell's own incident instead of paging.
    own: dict[str, float] = field(default_factory=dict)
    decided: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if self.fallback_page_seconds <= 0:
            raise ValueError("fallback_page_seconds must be positive")

    def page(
        self, lane: Lane, cell: Cell, injected_at: float, known_incident_ids: frozenset[str] = frozenset()
    ) -> float | None:
        if cell.arm != "sdo" or cell.id in self.decided:
            return None
        if self.now() < injected_at + self.fallback_page_seconds:
            return None
        if self.cluster.own_incident(lane, known_incident_ids):
            self.decided.add(cell.id)
            self.own[cell.id] = self.now()
            return None
        self.cluster.send(lane, self.new_id(), self.notice(lane))
        self.decided.add(cell.id)
        at = self.now()
        self.paged[cell.id] = at
        return at

    def own_incident_at(self, cell: Cell) -> float | None:
        return self.own.get(cell.id)

    def record(self, cell: Cell) -> dict[str, Any] | None:
        return None


def first_unhealthy_at(probes: Path, since: float) -> float | None:
    """The time of the runner's first unhealthy prober sample at or after ``since`` (``health.Prober`` writes one JSON
    line per sample). A gap line (the host could not ask) is not a sample, and a partly written last line is skipped."""
    try:
        text = probes.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    for line in text.splitlines():
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(sample, dict) and sample.get("healthy") is False and float(sample["at"]) >= since:
            return float(sample["at"])
    return None


@dataclass
class ImpactPageHook:
    """Pages SDO once, at the first unhealthy sample of the runner's own prober after injection (not SDO's detectors),
    or at ``injected_at + deadline_seconds`` when the prober saw nothing by then. ``injected_at`` is the injection
    start the row recorded before calling the injector (``fastloop.injection_marker``), so a slow injector return
    delays neither. For the detector-ablation arm, whose
    controller runs with ``detection.pages_only``: SDO's detectors never open an incident, so this page always does.

    The text is the same generic notice as the fallback page (``lane_notice``): no detector, alert or diagnosis content.
    ``record`` is what the row writes to ``page.json``: ``source`` (``impact`` or ``deadline``), ``impact_at``
    (``None`` for a deadline page), ``paged_at`` (the write's acknowledgement), ``text``, and whether SDO had an
    incident of its own before the page (``sdo_incident_before_page``; always false under ``pages_only``).
    """

    deadline_seconds: float
    cluster: PageCluster
    now: Callable[[], float]
    #: The cell's prober output (``<cell directory>/probes.jsonl``).
    probes: Callable[[Cell], Path]
    notice: Callable[[Lane], str] = lane_notice
    new_id: Callable[[], str] = _page_id
    records: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")

    def page(
        self, lane: Lane, cell: Cell, injected_at: float, known_incident_ids: frozenset[str] = frozenset()
    ) -> float | None:
        if cell.arm != "sdo" or cell.id in self.records:
            return None
        impact = first_unhealthy_at(self.probes(cell), injected_at)
        if impact is None and self.now() < injected_at + self.deadline_seconds:
            return None
        own = self.cluster.own_incident(lane, known_incident_ids)
        text = self.notice(lane)
        self.cluster.send(lane, self.new_id(), text)
        at = self.now()
        self.records[cell.id] = {
            "source": "deadline" if impact is None else "impact",
            "injected_at": injected_at,
            "impact_at": impact,
            "paged_at": at,
            "text": text,
            "sdo_incident_before_page": own,
        }
        return at

    def own_incident_at(self, cell: Cell) -> float | None:
        return None

    def record(self, cell: Cell) -> dict[str, Any] | None:
        return self.records.get(cell.id)


@dataclass
class ScheduledPageHook:
    """Pages SDO once, at ``injected_at + offset_seconds``, whatever SDO or the prober saw.

    For the arm that hides detector findings from the agent (``detection.pages_only`` and
    ``detection.hide_findings_from_agent``): ``offset_seconds`` is the offset of SDO's own page from the injection start
    in the paired cell ``paired_cell``, so the agent is told at the same time as in that cell and only what it is told
    differs. The text is the generic notice (``lane_notice``). ``record`` is what the row writes to ``page.json``:
    ``source`` ``scheduled``, ``offset_s``, ``paired_cell``, ``scheduled_at``, ``paged_at`` (the write's
    acknowledgement), ``text`` and ``sdo_incident_before_page``.
    """

    offset_seconds: float
    paired_cell: str
    cluster: PageCluster
    now: Callable[[], float]
    notice: Callable[[Lane], str] = lane_notice
    new_id: Callable[[], str] = _page_id
    records: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.offset_seconds < 0:
            raise ValueError("offset_seconds must not be negative")
        if not self.paired_cell:
            raise ValueError("a scheduled page names its paired cell")

    def page(
        self, lane: Lane, cell: Cell, injected_at: float, known_incident_ids: frozenset[str] = frozenset()
    ) -> float | None:
        if cell.arm != "sdo" or cell.id in self.records:
            return None
        scheduled = injected_at + self.offset_seconds
        if self.now() < scheduled:
            return None
        own = self.cluster.own_incident(lane, known_incident_ids)
        text = self.notice(lane)
        self.cluster.send(lane, self.new_id(), text)
        at = self.now()
        self.records[cell.id] = {
            "source": "scheduled",
            "injected_at": injected_at,
            "offset_s": self.offset_seconds,
            "paired_cell": self.paired_cell,
            "scheduled_at": scheduled,
            "paged_at": at,
            "text": text,
            "sdo_incident_before_page": own,
        }
        return at

    def own_incident_at(self, cell: Cell) -> float | None:
        return None

    def record(self, cell: Cell) -> dict[str, Any] | None:
        return self.records.get(cell.id)


def page_hook(
    config: EvalConfig, cluster: PageCluster, now: Callable[[], float]
) -> FallbackPageHook | ImpactPageHook | ScheduledPageHook:
    """The SDO arm's page hook the config names (``EvalConfig.page``); ``fallback_page_seconds`` is the deadline of
    the fallback and impact pages."""
    if config.page == "scheduled":
        assert config.page_offset_seconds is not None
        return ScheduledPageHook(config.page_offset_seconds, config.page_paired_cell, cluster, now)
    if config.page == "impact":
        return ImpactPageHook(
            config.fallback_page_seconds, cluster, now, probes=lambda cell: cell.directory(config) / "probes.jsonl"
        )
    return FallbackPageHook(config.fallback_page_seconds, cluster, now)
