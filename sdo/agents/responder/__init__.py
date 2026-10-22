"""Backend-neutral responder session lifecycle.

Exports resolve on first access so that entry points such as the `sdo` tool CLI
do not import the Codex, Kubernetes and protobuf stacks at process start.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

_EXPORTS = {
    "AGENT_SKILLS": "credentials",
    "AdmissibilityReview": "cause_admissibility",
    "CAUSE_ADMISSIBILITY_ENV": "cause_admissibility",
    "CAUSE_ADMISSIBILITY_MODES": "cause_admissibility",
    "CORROBORATING_EVIDENCE_KINDS": "cause_admissibility",
    "CauseAdmissibilityDecision": "cause_admissibility",
    "CauseAdmissibilityError": "cause_admissibility",
    "CauseAdmissibilityPolicy": "cause_admissibility",
    "ClaudeSessionBackend": "reflection",
    "CodexSessionBackend": "reflection",
    "INCIDENT_REASONING_EFFORT": "reflection",
    "IncidentStatusReport": "incident_status",
    "IncidentStatusState": "incident_status",
    "KubernetesResultPublisher": "job",
    "REFLECTION_GUIDANCE_MODES": "reflection",
    "ReflectionTurn": "reflection",
    "SessionReflector": "reflection",
    "VerifyBurstResult": "incident_status",
    "VerifyScenarioVerdict": "incident_status",
    "apply_cause_admissibility": "cause_admissibility",
    "current_outcome_view": "reflection_outcomes",
    "execute_incident": "codex",
    "history_view": "reflection_outcomes",
    "incident_status": "incident_status",
    "live_incident_status": "incident_status",
    "prepare_claude_home": "credentials",
    "prepare_codex_home": "credentials",
    "reflection_output_schema": "reflection",
    "review_confirmed_causes": "cause_admissibility",
    "run_incident_status_cli": "incident_status",
    "run_reflection_task": "reflection_worker",
    "run_worker_task": "worker",
    "write_application_kubeconfig": "job",
}

if TYPE_CHECKING:
    from sdo.agents.responder.cause_admissibility import (
        CAUSE_ADMISSIBILITY_ENV as CAUSE_ADMISSIBILITY_ENV,
    )
    from sdo.agents.responder.cause_admissibility import (
        CAUSE_ADMISSIBILITY_MODES as CAUSE_ADMISSIBILITY_MODES,
    )
    from sdo.agents.responder.cause_admissibility import (
        CORROBORATING_EVIDENCE_KINDS as CORROBORATING_EVIDENCE_KINDS,
    )
    from sdo.agents.responder.cause_admissibility import (
        AdmissibilityReview as AdmissibilityReview,
    )
    from sdo.agents.responder.cause_admissibility import (
        CauseAdmissibilityDecision as CauseAdmissibilityDecision,
    )
    from sdo.agents.responder.cause_admissibility import (
        CauseAdmissibilityError as CauseAdmissibilityError,
    )
    from sdo.agents.responder.cause_admissibility import (
        CauseAdmissibilityPolicy as CauseAdmissibilityPolicy,
    )
    from sdo.agents.responder.cause_admissibility import (
        apply_cause_admissibility as apply_cause_admissibility,
    )
    from sdo.agents.responder.cause_admissibility import (
        review_confirmed_causes as review_confirmed_causes,
    )
    from sdo.agents.responder.codex import (
        execute_incident as execute_incident,
    )
    from sdo.agents.responder.credentials import (
        AGENT_SKILLS as AGENT_SKILLS,
    )
    from sdo.agents.responder.credentials import (
        prepare_claude_home as prepare_claude_home,
    )
    from sdo.agents.responder.credentials import (
        prepare_codex_home as prepare_codex_home,
    )
    from sdo.agents.responder.incident_status import (
        IncidentStatusReport as IncidentStatusReport,
    )
    from sdo.agents.responder.incident_status import (
        IncidentStatusState as IncidentStatusState,
    )
    from sdo.agents.responder.incident_status import (
        VerifyBurstResult as VerifyBurstResult,
    )
    from sdo.agents.responder.incident_status import (
        VerifyScenarioVerdict as VerifyScenarioVerdict,
    )
    from sdo.agents.responder.incident_status import (
        incident_status as incident_status,
    )
    from sdo.agents.responder.incident_status import (
        live_incident_status as live_incident_status,
    )
    from sdo.agents.responder.incident_status import (
        run_incident_status_cli as run_incident_status_cli,
    )
    from sdo.agents.responder.job import (
        KubernetesResultPublisher as KubernetesResultPublisher,
    )
    from sdo.agents.responder.job import (
        write_application_kubeconfig as write_application_kubeconfig,
    )
    from sdo.agents.responder.reflection import (
        INCIDENT_REASONING_EFFORT as INCIDENT_REASONING_EFFORT,
    )
    from sdo.agents.responder.reflection import (
        REFLECTION_GUIDANCE_MODES as REFLECTION_GUIDANCE_MODES,
    )
    from sdo.agents.responder.reflection import (
        ClaudeSessionBackend as ClaudeSessionBackend,
    )
    from sdo.agents.responder.reflection import (
        CodexSessionBackend as CodexSessionBackend,
    )
    from sdo.agents.responder.reflection import (
        ReflectionTurn as ReflectionTurn,
    )
    from sdo.agents.responder.reflection import (
        SessionReflector as SessionReflector,
    )
    from sdo.agents.responder.reflection import (
        reflection_output_schema as reflection_output_schema,
    )
    from sdo.agents.responder.reflection_outcomes import (
        current_outcome_view as current_outcome_view,
    )
    from sdo.agents.responder.reflection_outcomes import (
        history_view as history_view,
    )
    from sdo.agents.responder.reflection_worker import (
        run_reflection_task as run_reflection_task,
    )
    from sdo.agents.responder.worker import (
        run_worker_task as run_worker_task,
    )

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(f"{__name__}.{module}"), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*globals(), *_EXPORTS])
