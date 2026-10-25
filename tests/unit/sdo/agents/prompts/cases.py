"""Handwritten toy inputs, never experiment or model outputs."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdo.agents.deployer import worker
from sdo.agents.lifecycle.agents import (
    ActiveTopologyResourceDTO,
    CodexLifecycleBackend,
    DeployerAssessment,
    DeployerDraft,
    HealthJudgeDraft,
    TopologyResourceDTO,
)
from sdo.agents.responder import reflection
from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.reflection import CodexSessionBackend, ReflectionTurn, SessionReflector
from sdo.agents.responder.reflection_brief import incident_brief
from sdo.agents.responder.session_recovery import SessionRecovery
from sdo.agents.responder.session_turn import branch_correction, branch_instruction
from sdo.contracts import (
    DetectorEvaluation,
    Finding,
    IncidentClosure,
    IncidentRequest,
    IncidentResult,
    MemoryDetector,
    MemoryDiagnostic,
    MemoryPlaybook,
    MemoryScript,
    MemoryWatch,
    MitigationReport,
    MitigationSession,
    ObjectRef,
    OperationalMemorySnapshot,
    OutcomeRecord,
    PriorOutcomeEvidence,
    ReflectionWorkItem,
    SourceDeploymentRequest,
    StateChange,
    StateChanges,
    SurfacedPlaybook,
)


def incident(
    count: int = 2,
    *,
    warm: bool = False,
    drift: bool = False,
    oversized: bool = False,
    pending: bool = False,
    memory: bool = True,
) -> IncidentRequest:
    request = IncidentRequest(
        schema_version="sdo.dev/v1alpha1",
        application="toy",
        namespace="toy",
        incident_id="toy-incident",
        repository_worktree="/application",
        repository_base_commit="a" * 40,
        source_commit="b" * 40,
        deployed_commit="c" * 40,
        repair_policy="recorded",
        max_response_loops=3,
        memory_snapshot=OperationalMemorySnapshot(
            accepted_commit="a" * 40,
            goal_markdown="Keep the toy application available.\n" if memory else "",
            architecture_markdown="# Toy architecture\nAPI depends on store.\n" if memory else "",
        ),
    )
    if oversized:
        request.memory_snapshot.goal_markdown = "目" * 4001
        request.memory_snapshot.architecture_markdown = "界" * 8001
    for index in range(count):
        name = f"component-{index}"
        request.findings.append(
            Finding(
                detector_id=f"detector-{index}",
                rule_id="unavailable",
                fingerprint=f"fingerprint-{index}",
                status="active",
                severity="critical",
                summary=f"{name} is unavailable",
                evidence=f"Observed {name} has no ready endpoints.",
                playbook_ids=[f"repair-{index}"],
                primary_resource=ObjectRef(api_version="v1", kind="Service", namespace="toy", name=name),
            )
        )
        request.detector_history.append(
            DetectorEvaluation(
                detector_id=f"detector-{index}",
                status="firing",
                fingerprints=[f"fingerprint-{index}"],
            )
        )
    if memory:
        request.memory_snapshot.playbook_index_markdown = "# Playbooks\nrepair-0\n"
        request.memory_snapshot.playbooks.append(
            MemoryPlaybook(
                id="repair-0",
                markdown="手" * 9000 if oversized else "# Repair\nRestore the accepted target port.\n",
                scripts=[MemoryScript(name="diagnose.go", content="検" * 3000 if oversized else "package diagnose\n")],
            )
        )
        request.surfaced_playbooks.append(SurfacedPlaybook(playbook_id="repair-0"))
        request.memory_snapshot.detectors.append(
            MemoryDetector(
                id="detector-0",
                detector_class="incident",
                owner="responder",
                originating_incident="prior-toy",
                possible_playbook_ids=["repair-0"],
                description="Toy service port mismatch",
                predicate_excerpt="return service.Port != intended.Port",
                watches=[MemoryWatch(api_version="v1", kind="Service")],
            )
        )
    if warm:
        request.relevant_outcomes.append(
            PriorOutcomeEvidence(
                incident_id="prior-toy",
                match_reason="exact-fingerprint",
                applied_playbook_ids=["repair-0"],
            )
        )
    if pending:
        request.findings[0].detector_id = "health-objective"
    if drift:
        request.drift_events.add()
        request.state_changes.CopyFrom(
            StateChanges(
                changes=[StateChange(kind="Service", name="component-0", change="modified")],
                unobserved_kinds=["Secret"],
                omitted=2,
            )
        )
    return request


class ReflectionCapture:
    def __init__(self) -> None:
        self.prompt = ""

    def resume(self, *, session_id: str, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
        self.prompt = prompt
        return ReflectionTurn(summary="toy capture", learning_decision="no_change", no_change_reason="synthetic")

    def fresh(self, *, worktree: Path, prompt: str, idempotency_key: str) -> ReflectionTurn:
        return self.resume(session_id="", worktree=worktree, prompt=prompt, idempotency_key=idempotency_key)


def rendered_cases() -> dict[str, str]:
    prompts: dict[str, str] = {}
    with pytest.MonkeyPatch.context() as patch:
        for name in ("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "SDO_LATE_FINDINGS", "SDO_API_HISTORY_DIR"):
            patch.delenv(name, raising=False)
        for name, request in {
            "cold_two": incident(memory=False),
            "cold_five": incident(5, memory=False),
            "surfaced": incident(),
            "warm": incident(warm=True),
            "warm_pending": incident(warm=True, pending=True),
            "drift": incident(drift=True),
            "oversized": incident(5, oversized=True),
            "warm_oversized": incident(warm=True, oversized=True),
        }.items():
            prompts[f"responder_{name}"] = _responder_prompt(request)
        patch.setenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "Use the toy incident transport.")
        patch.setenv("SDO_LATE_FINDINGS", "pull")
        prompts["responder_optional"] = _responder_prompt(incident())

        for mode in ("resume", "fresh", "retry"):
            for count in (2, 5):
                capture = ReflectionCapture()
                request = incident(count, warm=True, drift=True)
                closure = IncidentClosure(
                    request=request,
                    result=IncidentResult(
                        incident_id=request.incident_id,
                        status="completed",
                        responder_session_id="toy-session",
                    ),
                )
                outcome = OutcomeRecord(
                    schema_version=1,
                    incident_id=request.incident_id,
                    classification="success",
                    findings=request.findings,
                )
                SessionReflector(capture, guidance="generalize-spec").resume(
                    session_id="toy-session",
                    incident_id=request.incident_id,
                    worktree=Path("/application"),
                    outcome=outcome,
                    history=[OutcomeRecord(schema_version=1, incident_id="prior-toy", classification="success")],
                    outcome_commit="d" * 40,
                    closure=closure,
                    session_mode="fresh" if mode == "fresh" else "resume",
                    diagnostics=[
                        MemoryDiagnostic(rule="toy-rule", path="toy.go", line=3, message="fix toy", hint="check type")
                    ]
                    if mode == "retry"
                    else (),
                )
                prompts[f"reflection_{mode}_{count}"] = capture.prompt
        capture = ReflectionCapture()
        SessionReflector(capture).resume(
            session_id="toy-session",
            incident_id="toy-incident",
            worktree=Path("/application"),
            outcome=OutcomeRecord(schema_version=1, incident_id="toy-incident", classification="failed"),
            history=[],
            outcome_commit="d" * 40,
            closure=IncidentClosure(request=incident(5)),
            unassigned_work=[
                ReflectionWorkItem(
                    program_id="repair-0",
                    disposition="question",
                    question="Which live observation establishes the repair?",
                    reason="synthetic work item",
                )
            ],
        )
        prompts["reflection_cause_work"] = capture.prompt

        class CapturedPrompt(Exception):
            pass

        for api_history in (False, True):
            if api_history:
                patch.setenv("SDO_API_HISTORY_DIR", "/synthetic-history")
            else:
                patch.delenv("SDO_API_HISTORY_DIR", raising=False)

            def capture_transport(
                _provider: str, prompt: str, *, _api_history: bool = api_history, **_kwargs: object
            ) -> None:
                prompts[f"reflection_transport_{_api_history}"] = prompt
                raise CapturedPrompt

            patch.setattr(reflection, "run_structured_turn", capture_transport)
            try:
                CodexSessionBackend().fresh(
                    worktree=Path("/application"),
                    prompt=capture.prompt,
                    idempotency_key="reflection:toy-incident:outcome",
                )
            except CapturedPrompt:
                pass

        for count in (2, 5):
            closure = IncidentClosure(request=incident(count, oversized=True))
            prompts[f"reflection_brief_{count}"] = incident_brief(
                closure, worktree=Path("/application"), detector_detail=True, event_dependence=True
            )
        for pending in (False, True):
            session = MitigationSession(incident_id="toy-incident", session_id="logical-toy")
            if pending:
                session.pending_reports.append(
                    MitigationReport(
                        version=1,
                        incident_id="toy-incident",
                        session_id="logical-toy",
                        request_id="request",
                        attempt_id="attempt",
                        acceptance_epoch=1,
                        status=MitigationReport.STATUS_INTERRUPTED,
                    )
                )
            prompts[f"recovery_{pending}"] = SessionRecovery(
                session, "codex", Path("/application"), lambda _: None
            ).recovered_prompt()
        for branch in ("incident_result", "reflection"):
            prompts[f"branch_instruction_{branch}"] = branch_instruction(branch)
            prompts[f"branch_correction_{branch}"] = branch_correction(branch)

        def source_turn(_provider: str, prompt: str, **_kwargs: object) -> SimpleNamespace:
            prompts["deployer_source"] = prompt
            return SimpleNamespace(
                session_id="toy-source",
                output_json=json.dumps(
                    {
                        "proposed": False,
                        "summary": "synthetic capture",
                        "image_builds": [],
                        "manifest_paths": [],
                        "expected_workload_ids": [],
                    }
                ),
            )

        patch.setattr(worker, "run_structured_turn", source_turn)
        worker.propose_source(
            SourceDeploymentRequest(
                application="toy",
                namespace="toy",
                health_objective="Keep the toy application available.",
                registry_host="registry.local:5000",
            ),
            Path("/application"),
        )
        backend = CodexLifecycleBackend()
        assessment = DeployerAssessment(
            source_commit="a" * 40,
            topology_fingerprint="b" * 64,
            architecture_summary_markdown="Toy API depends on the store.",
            session_id="toy-architecture",
            resources=[
                TopologyResourceDTO(
                    kind="Service", name="api", namespace="default", source="deploy/api.yaml", dependencies=[]
                )
            ],
        )

        def execute(_repository: Path, prompt: str, output_type: type, **_kwargs: object) -> tuple:
            prompts[f"lifecycle_{output_type.__name__}"] = prompt
            if output_type is DeployerDraft:
                return DeployerDraft(**assessment.model_dump(exclude={"resources", "session_id"})), "toy-architecture"
            return HealthJudgeDraft(
                round=1,
                objective_digest="c" * 64,
                source_commit="a" * 40,
                covered_resources=[],
                failure_patterns=["toy"],
                detector_ids=["health-objective"],
            ), "toy-judge"

        patch.setattr(backend, "_execute", execute)
        backend.run_deployer(repository=Path("/application"), application="toy", correction_feedback=None)
        backend.run_health_judge(
            repository=Path("/application"),
            application="toy",
            health_objective="Keep toy healthy.",
            deployer=assessment,
            round_index=1,
            previous=None,
            correction_feedback=None,
            active_resources=[ActiveTopologyResourceDTO(kind="Service", name="api")],
        )
    return prompts
