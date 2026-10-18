"""Responder-side cause-admissibility gate.

SDO's responder proposes confirmed root causes for an incident. The
controller's deterministic ``verify_diagnosis`` later checks each proposed
cause against controller-owned facts (contradiction, explained-detector
flips, and repair attribution). That verifier is fenced: it is the
authoritative check and is deliberately left untouched here.

This module is strictly *upstream* of the verifier. It improves the quality
of the evidence the responder asserts, so a weakly-evidenced cause is not
emitted as a confirmed root cause in the first place. It can only *remove*
causes the responder proposed; it never relaxes, duplicates, or substitutes
for the verifier, and it can never cause a confirmation the verifier would
not otherwise make.

Admissibility is decided only from the production contracts (the responder's
own :class:`~sdo.contracts.IncidentResult` and the controller's
:class:`~sdo.contracts.IncidentRequest`). It inspects evidence *kinds* and
whether an explained detector actually fired for this incident; it never
reads benchmark verdicts and encodes no fault-specific knowledge.

The rule is intentionally conservative, so it cannot drop a genuine cause
(including every component of a real composite): a cause is withheld *only*
when its entire evidentiary basis is unverifiable narrative. Concretely, a
cause is admissible when either

- it cites at least one piece of evidence of a *corroborating* kind
  (``detector-finding``, ``synthetic-traffic``, or ``state-change``) that an
  independent check could anchor to an incident signal, or
- it names at least one ``explained_detector`` that actually fired for this
  incident.

A cause that rests only on ``live-observation`` items -- which the contract
documents as "accepted as live but unverified" -- and explains no detector
that fired is weakly evidenced and is withheld.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sdo.contracts import DetectorEvaluationStatus, FindingStatus

if TYPE_CHECKING:
    from sdo.contracts import ConfirmedRootCause, IncidentRequest, IncidentResult

#: Environment variable selecting the gate mode on the responder Job.
CAUSE_ADMISSIBILITY_ENV = "SDO_CAUSE_ADMISSIBILITY"

#: Supported gate modes. ``off`` (default) returns the responder's result
#: byte-for-byte unchanged, so enabling the gate is opt-in and a run without it
#: is reproducible; ``on`` withholds inadmissible causes. The default stays off
#: until a gate-on-vs-off validation arm earns a flip.
CAUSE_ADMISSIBILITY_MODES: tuple[str, ...] = ("on", "off")

#: Evidence kinds an independent check can anchor to an incident signal: a
#: detector finding, a failing synthetic scenario, or a configuration change
#: since the healthy baseline. ``live-observation`` is excluded because the
#: contract accepts it as live but unverifiable.
CORROBORATING_EVIDENCE_KINDS: frozenset[str] = frozenset({"detector-finding", "synthetic-traffic", "state-change"})


class CauseAdmissibilityError(ValueError):
    """An invalid cause-admissibility configuration."""


@dataclass(frozen=True)
class CauseAdmissibilityDecision:
    """Why one proposed root cause was admitted or withheld."""

    summary: str
    admitted: bool
    reason: str

    def __post_init__(self) -> None:
        if not self.summary:
            raise ValueError("decision requires the cause summary")
        if not self.reason:
            raise ValueError("decision requires a reason")


@dataclass(frozen=True)
class AdmissibilityReview:
    """The outcome of reviewing every proposed confirmed root cause."""

    admitted: tuple[ConfirmedRootCause, ...] = ()
    decisions: tuple[CauseAdmissibilityDecision, ...] = ()

    @property
    def withheld(self) -> tuple[CauseAdmissibilityDecision, ...]:
        return tuple(decision for decision in self.decisions if not decision.admitted)


@dataclass(frozen=True)
class CauseAdmissibilityPolicy:
    """Configuration of the responder-side cause-admissibility gate."""

    mode: str = "off"

    def __post_init__(self) -> None:
        if self.mode not in CAUSE_ADMISSIBILITY_MODES:
            raise CauseAdmissibilityError(
                f"cause-admissibility mode must be one of {', '.join(CAUSE_ADMISSIBILITY_MODES)}: {self.mode!r}"
            )

    @property
    def enabled(self) -> bool:
        return self.mode == "on"

    @classmethod
    def from_environment(cls, mode: str | None = None) -> CauseAdmissibilityPolicy:
        """Build a policy from ``mode`` or, when ``None``, the environment (default ``off``)."""

        selected = (os.getenv(CAUSE_ADMISSIBILITY_ENV, "") if mode is None else mode).strip().lower() or "off"
        return cls(mode=selected)


def firing_detector_ids(request: IncidentRequest) -> frozenset[str]:
    """Detectors that fired for this incident, from the request's own findings and history."""

    ids: set[str] = set()
    for finding in request.findings:
        if finding.status == FindingStatus.ACTIVE:
            ids.add(finding.detector_id)
    for evaluation in request.detector_history:
        if evaluation.status == DetectorEvaluationStatus.FIRING:
            ids.add(evaluation.detector_id)
    return frozenset(ids)


def is_admissible(cause: ConfirmedRootCause, *, firing_detectors: frozenset[str]) -> tuple[bool, str]:
    """Decide whether one proposed root cause is admissible, with a human-readable reason."""

    corroborating = sorted(
        {evidence.kind for evidence in cause.evidence if evidence.kind in CORROBORATING_EVIDENCE_KINDS}
    )
    if corroborating:
        return True, f"cites corroborating-kind evidence ({', '.join(corroborating)})"
    grounded = sorted(detector for detector in cause.explained_detectors if detector in firing_detectors)
    if grounded:
        return True, f"explains detector(s) that fired this incident ({', '.join(grounded)})"
    return (
        False,
        "rests only on unverifiable live observations and explains no detector that fired this incident",
    )


def review_confirmed_causes(result: IncidentResult, request: IncidentRequest) -> AdmissibilityReview:
    """Review every proposed confirmed root cause without mutating the result."""

    firing = firing_detector_ids(request)
    admitted: list[ConfirmedRootCause] = []
    decisions: list[CauseAdmissibilityDecision] = []
    for cause in result.confirmed_root_causes:
        ok, reason = is_admissible(cause, firing_detectors=firing)
        decisions.append(CauseAdmissibilityDecision(summary=cause.summary, admitted=ok, reason=reason))
        if ok:
            admitted.append(cause)
    return AdmissibilityReview(admitted=tuple(admitted), decisions=tuple(decisions))


def apply_cause_admissibility(
    result: IncidentResult,
    request: IncidentRequest,
    *,
    policy: CauseAdmissibilityPolicy | None = None,
) -> IncidentResult:
    """Withhold weakly-evidenced confirmed root causes from ``result``.

    With the gate disabled (``policy.mode == "off"``) the result is returned
    unchanged. Otherwise inadmissible causes are dropped from
    ``confirmed_root_causes`` and each withheld cause is logged to stderr for
    provenance. Static context, repairs, and every other field are untouched.
    """

    policy = policy or CauseAdmissibilityPolicy.from_environment()
    if not policy.enabled or not result.confirmed_root_causes:
        return result
    review = review_confirmed_causes(result, request)
    if not review.withheld:
        return result
    for decision in review.withheld:
        print(
            f"cause-admissibility: withheld root cause {decision.summary!r}: {decision.reason}",
            file=sys.stderr,
        )
    return result.model_copy(update={"confirmed_root_causes": list(review.admitted)})
