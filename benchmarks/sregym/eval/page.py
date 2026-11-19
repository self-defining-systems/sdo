"""The fallback page: when SDO has not opened its own incident ``fallback_page_seconds`` after injection, the harness
pages it like an on-call human, through the production external-incident inbox.

The page text is the vanilla arm's incident text: the application, namespace and description block of SREGym's
claudecode instruction (``clients/claudecode/driver.py:build_instruction``) without its submission protocol. It carries
no fault name and no SDO-specific hint. One decision per cell: it pages once, or never when SDO opened its own incident.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import apply_external_incident, control_namespace_for, read_runtime_state

if TYPE_CHECKING:
    from collections.abc import Callable

    from benchmarks.sregym.eval.cell import Cell, Lane

#: Controller state keys that mean SDO has its own incident (open, requested, or closed but not yet acknowledged).
OWN_INCIDENT_KEYS = ("incident_open", "incident_request", "pending_closure")


def incident_notice(application: str, namespace: str, description: str) -> str:
    """The incident text both arms receive: the verbatim block ``build_instruction`` renders for the app."""
    return f"Application: {application}\nNamespace: {namespace}\n\n{description}"


def lane_notice(lane: Lane) -> str:
    """The notice for a lane, from the application facts its backend recorded at claim."""
    return incident_notice(lane.info["application"], lane.info["namespace"], lane.info.get("description", ""))


def has_own_incident(state: dict[str, Any]) -> bool:
    """Whether the controller's runtime state shows an incident SDO opened itself."""
    return any(bool(state.get(key)) for key in OWN_INCIDENT_KEYS)


class PageCluster(Protocol):
    def own_incident(self, lane: Lane) -> bool:
        """Whether SDO has opened its own incident on the lane's cluster."""
        ...

    def send(self, lane: Lane, incident_id: str, message: str) -> None:
        """Write the external incident to the lane's inbox."""
        ...


class KubectlPageCluster:
    """The lane's controller state and external-incident inbox, over kubectl and the lane kubeconfig."""

    def own_incident(self, lane: Lane) -> bool:
        control = control_namespace_for(lane.info["namespace"])
        return has_own_incident(read_runtime_state(control, kubeconfig=_kubeconfig(lane)))

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
    decided: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if self.fallback_page_seconds <= 0:
            raise ValueError("fallback_page_seconds must be positive")

    def page(self, lane: Lane, cell: Cell, injected_at: float) -> float | None:
        if cell.arm != "sdo" or cell.id in self.decided:
            return None
        if self.now() < injected_at + self.fallback_page_seconds:
            return None
        if self.cluster.own_incident(lane):
            self.decided.add(cell.id)
            return None
        self.cluster.send(lane, self.new_id(), self.notice(lane))
        self.decided.add(cell.id)
        at = self.now()
        self.paged[cell.id] = at
        return at
