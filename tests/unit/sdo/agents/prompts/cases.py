"""Handwritten toy inputs, never experiment or model outputs."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from benchmarks.sregym.adapter.runtime import _responder_instructions
from sdo.agents.lifecycle import agents as lifecycle_agents
from sdo.agents.lifecycle.agents import (
    CodexLifecycleBackend,
    HealthJudgeDraft,
    TopologyResourceDTO,
)
from sdo.agents.responder import codex, reflection
from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.memory_check import ValidationResult
from sdo.agents.responder.reflection import cleanup_prompt, closing_prompt
from sdo.agents.responder.session_recovery import SessionRecovery
from sdo.agents.responder.session_turn import branch_correction, branch_instruction
from sdo.agents.source_deployment import worker
from sdo.contracts import (
    DetectorEvaluation,
    Finding,
    HealthJudgeRequest,
    IncidentRequest,
    MemoryDetector,
    MemoryDiagnostic,
    MemoryPlaybook,
    MemoryScript,
    MemoryWatch,
    MitigationReport,
    MitigationSession,
    ObjectRef,
    OperationalMemorySnapshot,
    PriorOutcomeEvidence,
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
    real_scale: bool = False,
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
        detected_at=Timestamp(seconds=1_700_000_000),
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
        if real_scale:
            # Live drift fingerprints run to about 150 characters, evidence to several hundred.
            request.findings[-1].fingerprint = f"drift/apps/v1/Deployment/toy/{name}/" + "f" * 110
            request.findings[-1].evidence = f"Observed {name} has no ready endpoints; " * 12
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


def responder_prompt(request: IncidentRequest) -> str:
    return _responder_prompt(request)


def _toy_ref(kind: str, name: str, api_version: str = "v1") -> dict[str, str]:
    return {"api_version": api_version, "kind": kind, "namespace": "toy", "name": name}


SEED_HEALTH_SIZES = (("hotel", 866, 793), ("social", 101, 788), ("astronomy", 972, 793))


def rendered_cases(*, scale: int = 1) -> dict[str, str]:
    """Golden prompts; ``scale`` multiplies findings and lengthens them to live sizes."""

    def scaled(count: int = 2, **options: bool) -> IncidentRequest:
        return incident(count * scale, real_scale=scale > 1, **options)

    prompts: dict[str, str] = {}
    with pytest.MonkeyPatch.context() as patch:
        # Infrastructure configuration is an explicit fixture input, independent
        # of the checkout that runs the production builders.
        patch.setattr(codex, "SKILLS_ROOT", Path("/opt/sdo/skills"))
        patch.setattr(reflection, "SKILLS_ROOT", Path("/opt/sdo/skills"))
        patch.setattr(lifecycle_agents, "SKILLS_ROOT", Path("/opt/sdo/skills"))
        # Prompt size caps count temporary-path characters; the golden root is
        # a fixed input rather than whatever TMPDIR the runner happens to use.
        patch.setattr(tempfile, "tempdir", "/tmp")
        for name in ("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "SDO_API_HISTORY_DIR"):
            patch.delenv(name, raising=False)
        prompts["responder_cold_two"] = responder_prompt(scaled(memory=False))
        patch.setenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "Use the toy incident transport.")
        with_changes = scaled()
        with_changes.recent_changes.extend(
            [
                "CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)",
                "api  on every failing path",
                "  -1m Deployment/api changed vs accepted source (spec.template) by kubectl",
                "live: sdo detect (this view); sdo changes and sdo timeline list every row",
            ]
        )
        prompts["responder_optional"] = responder_prompt(with_changes)
        patch.setenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", _responder_instructions())
        prompts["responder_transport"] = responder_prompt(scaled())
        patch.setenv("SDO_RESPONDER_EXTRA_INSTRUCTIONS", "Use the toy incident transport.")

        changes = ["1. 03:04:05 api-exec Pod/db-1", "2. 03:04:06 api-delete Pod/api-1"]
        prompts["closing_learning"] = closing_prompt(incident_id="toy-incident", changes=changes)
        prompts["closing_cleanup"] = cleanup_prompt(leftovers=["app/main.go", "app/debug.txt"], turn=1)
        prompts["reflection_memory_check_correction"] = reflection.memory_check_correction(
            ValidationResult(1, False, "playbook.component-literal: names a component", ""),
            "reflection:toy-incident:outcome",
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
            prompts["source_deployment"] = prompt
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
        backend = CodexLifecycleBackend(reasoning_effort="medium", timeout_seconds=900)
        architecture = "Toy API depends on the store."
        resources = [
            TopologyResourceDTO(
                kind="Service", name="api", namespace="default", source="deploy/api.yaml", dependencies=[]
            )
        ]

        def execute(_repository: Path, prompt: str, output_type: type, **_kwargs: object) -> tuple:
            prompts[f"lifecycle_{output_type.__name__}"] = prompt
            return HealthJudgeDraft(failure_patterns=["toy"], detector_ids=["toy-endpoints"]), "toy-judge"

        patch.setattr(backend, "_execute", execute)
        # SEEDBUDGET1: measured fresh seed goals are 866/101/972 characters;
        # worker rejection diagnostics are 793/788. Astronomy did not dispatch,
        # so use the observed maximum there. Construct synthetic text only.
        if scale > 1:
            from sdo.agents.health_judge.worker import _judge_feedback

            for app, goal_chars, feedback_chars in SEED_HEALTH_SIZES:
                goal = "Synthetic required health objective. ".ljust(goal_chars, "g")
                message = "Synthetic worker rejection diagnostic. ".ljust(feedback_chars, "f")
                feedback = _judge_feedback(
                    HealthJudgeRequest(
                        previous_diagnostics=[MemoryDiagnostic(rule="worker.job_failed", message=message)]
                    )
                )
                backend.run_health_judge(
                    repository=Path("/application"),
                    application=app,
                    health_objective=goal,
                    architecture=architecture,
                    resources=resources,
                    correction_feedback=feedback,
                )
                prompt = prompts.pop("lifecycle_HealthJudgeDraft")
                assert goal in prompt
                assert message in prompt
                prompts[f"lifecycle_seed_{app}"] = prompt
        backend.run_health_judge(
            repository=Path("/application"),
            application="toy",
            health_objective="Keep toy healthy.",
            architecture=architecture,
            resources=resources,
            correction_feedback=None,
        )
    from sdo.agents.prompt_templates import render_prompt

    prompts["incident_result_correction"] = render_prompt(
        "responder",
        "result_correction",
        part="session.instructions",
        result_type="IncidentResultValidationError",
        diagnostic="status: invalid value",
    )
    return prompts
