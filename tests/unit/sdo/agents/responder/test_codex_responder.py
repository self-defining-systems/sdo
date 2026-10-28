from __future__ import annotations

import json
from pathlib import Path

import pytest
from agentshim.providers.codex import CodexSandboxConfig, parse_sandbox
from agentshim.testing import FakeRun
from google.protobuf import json_format

from sdo.agents.responder.codex import (
    ResponderExecutionError,
    _incident_result_schema,
    execute_incident,
)
from sdo.agents.responder.codex import (
    _responder_prompt as _build_responder_prompt,
)
from sdo.agents.responder.credentials import SKILLS_ROOT
from sdo.agents.responder.incident_context import prepared_incident_data
from sdo.contracts import (
    IncidentRequest,
    IncidentResult,
    MemoryDetector,
    MemoryPlaybook,
    MemoryScript,
    MemoryWatch,
    PriorOutcomeEvidence,
)
from tests.context_helpers import incident_document, incident_facts_path
from tests.proto_messages import evolve, from_dict
from tests.structured_turns import ScriptedAgent, failure


def _fixture(name: str) -> str:
    root = Path(__file__).resolve().parents[5]
    source = (root / "tests" / "fixtures" / "sdo" / "contracts" / name).read_text(encoding="utf-8")
    if name not in {"incident_request.json", "incident_request_state_changes.json"}:
        return source
    request = json.loads(source)
    request["memory_snapshot"] = {
        "accepted_commit": request["repository_base_commit"],
        "goal_markdown": "Stay healthy.",
        "architecture_markdown": "# Architecture\nSource-grounded system.",
    }
    return json.dumps(request)


def _responder_prompt(request: IncidentRequest) -> str:
    with prepared_incident_data(request) as data:
        return _build_responder_prompt(request, incident_data=data)


def _workflow() -> str:
    return (
        (SKILLS_ROOT / "sdo-incident-response/SKILL.md").read_text(encoding="utf-8")
        + "\n"
        + (SKILLS_ROOT / "sdo-incident-response/references/live-repairs.md").read_text()
        + "\n"
        + (SKILLS_ROOT / "sdo-incident-response/references/coverage.md").read_text()
    )


def _request(name: str = "incident_request.json") -> IncidentRequest:
    return json_format.Parse(_fixture(name), IncidentRequest())


def _result() -> IncidentResult:
    return json_format.Parse(_fixture("incident_result.json"), IncidentResult())


def _reply(result: IncidentResult) -> dict[str, object]:
    return {
        "incident_result": json_format.MessageToDict(result, preserving_proto_field_name=True),
        "reflection": None,
    }


def test_codex_responder_captures_resumable_session_id() -> None:
    request = _request()
    result = evolve(_result(), responder_session_id=None)
    message = {"type": "agent_message", "id": "msg", "text": json.dumps(_reply(result))}
    agent = ScriptedAgent(
        lambda _request: FakeRun(
            stdout=[
                '{"type":"thread.started","thread_id":"session-from-codex"}\n',
                json.dumps({"type": "item.completed", "item": message}) + "\n",
                '{"type":"turn.completed","usage":{"input_tokens":321,"cached_input_tokens":123,'
                '"cache_write_input_tokens":0,"output_tokens":45,"reasoning_output_tokens":12}}\n',
            ]
        )
    )

    completed = execute_incident(request, model="gpt-5.5", executor=agent.executor)

    assert completed.responder_session_id == "session-from-codex"
    assert completed.usage.llm_calls == 1
    assert completed.usage.input_tokens == 321
    assert completed.usage.cached_input_tokens == 123
    assert completed.usage.cache_read_input_tokens == 123
    assert completed.usage.uncached_input_tokens == 198
    assert completed.usage.output_tokens == 45
    assert completed.usage.reasoning_output_tokens == 12
    (argv,) = agent.argvs
    assert argv[argv.index("--model") + 1] == "gpt-5.5"
    assert parse_sandbox(argv) == CodexSandboxConfig(mode="danger-full-access")
    assert agent.requests[0].cwd == str(Path(request.repository_worktree).resolve())


def test_claude_responder_captures_resumable_session_id(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))
    result = evolve(_result(), responder_session_id=None)
    event = {
        "type": "result",
        "session_id": "session-from-claude",
        "is_error": False,
        "structured_output": _reply(result),
        "num_turns": 4,
        "total_cost_usd": 0.25,
        "usage": {
            "input_tokens": 500,
            "cache_creation_input_tokens": 100,
            "cache_read_input_tokens": 200,
            "output_tokens": 75,
        },
    }
    init = {"type": "system", "subtype": "init", "session_id": "session-from-claude"}
    agent = ScriptedAgent(lambda _request: FakeRun(stdout=[json.dumps(init) + "\n", json.dumps(event) + "\n"]))

    completed = execute_incident(request, provider="claude", model="haiku", executor=agent.executor)

    assert completed.responder_session_id == "session-from-claude"
    assert completed.usage.llm_calls == 4
    assert completed.usage.input_tokens == 800
    assert completed.usage.cached_input_tokens == 200
    assert completed.usage.cache_read_input_tokens == 200
    assert completed.usage.cache_write_input_tokens == 100
    assert completed.usage.uncached_input_tokens == 500
    assert completed.usage.model_requests == 4
    assert completed.usage.output_tokens == 75
    assert completed.usage.total_cost_usd == 0.25
    (argv,) = agent.argvs
    assert argv[argv.index("--model") + 1] == "haiku"
    assert "--settings" not in argv


def test_responder_cannot_reflect_before_controller_verification(monkeypatch) -> None:
    request = _request()

    prompt = _responder_prompt(request)

    assert "Operational memory is read-only" in prompt
    assert "controller rechecks" in _workflow()
    assert "After recovery, reflect into `.sdo`" not in prompt

    assert "SREGym" not in prompt
    assert "benchmarks.sregym" not in prompt

    monkeypatch.setenv(
        "SDO_RESPONDER_EXTRA_INSTRUCTIONS",
        "Use the configured external incident transport, but never treat its response as health evidence.",
    )
    extended_prompt = _responder_prompt(request)
    assert "Use the configured external incident transport" in extended_prompt
    assert "never treat its response as health evidence" in extended_prompt


def test_codex_failure_reports_structured_events_and_stderr() -> None:
    request = _request()

    agent = ScriptedAgent(
        lambda _request: failure(stdout='{"type":"error","message":"schema rejected"}\n', stderr="PATH warning\n")
    )

    with pytest.raises(ResponderExecutionError) as captured:
        execute_incident(request, executor=agent.executor)

    assert "schema rejected" in str(captured.value)
    assert "PATH warning" in str(captured.value)


def test_codex_output_schema_is_strict_and_requires_defaulted_fields() -> None:
    schema = _incident_result_schema()

    assert schema["required"] == list(schema["properties"])
    assert schema["additionalProperties"] is False
    applied = schema["properties"]["applied_playbooks"]["items"]
    assert applied["required"] == ["playbook_id"]
    assert "parameter_bindings" not in applied["properties"]
    assert "responder_session_id" not in schema["properties"]
    assert "usage" not in schema["properties"]
    assert "repair_actions" not in schema["properties"]
    cause = schema["properties"]["confirmed_root_causes"]["items"]
    assert cause["required"] == list(cause["properties"])
    assert cause["additionalProperties"] is False
    evidence = cause["properties"]["evidence"]["items"]
    assert evidence["required"] == list(evidence["properties"])
    assert "static-artifact" not in evidence["properties"]["kind"]["enum"]
    assert set(evidence["properties"]["kind"]["enum"]) == {
        "detector-finding",
        "synthetic-traffic",
        "state-change",
        "live-observation",
    }


def test_recorded_actions_policy_is_explicit_in_responder_prompt() -> None:
    prompt = _responder_prompt(evolve(_request(), repair_policy="recorded-actions"))
    assert "recorded-actions" in prompt
    assert "Use recorded actions for live mutations" in _workflow()
    assert "never hand-write receipts" in _workflow()


def test_prompt_tells_the_responder_to_cancel_an_already_healed_finding() -> None:
    prompt = _responder_prompt(_request())
    assert "sdo-incident-response/SKILL.md" in prompt
    assert "return cancelled rather than inventing one" in _workflow()
    assert "never claim done without this self-check" in _workflow()
    assert "rather than inventing one" in _workflow()


def _exact_match_outcome(match_reason: str = "exact-fingerprint") -> PriorOutcomeEvidence:
    return PriorOutcomeEvidence(
        incident_id="inc-prior",
        match_reason=match_reason,
        root_cause_summaries=["geo required the absent geo-config ConfigMap"],
        repair_action_summaries=["restored geo-config from source"],
        applied_playbook_ids=["missing-configmap"],
        source_commit="1111111111111111111111111111111111111111",
        exact_source_match=True,
    )


def _warm_request(worktree: Path, *, match_reason: str = "exact-fingerprint") -> IncidentRequest:
    request = _request()
    snapshot = evolve(
        request.memory_snapshot,
        detectors=[
            MemoryDetector(
                id="missing-configmap",
                detector_class="incident",
                owner="responder",
                watches=[MemoryWatch(api_version="apps/v1", kind="Deployment")],
                firing=1,
                possible_playbook_ids=["missing-configmap"],
                originating_incident="incident-seed",
            )
        ],
        playbooks=[
            MemoryPlaybook(
                id="missing-configmap",
                markdown="Restore `<MISSING_CONFIG_MAP>` from source.",
                scripts=[MemoryScript(name="verify.sh", content='kubectl -n "$1" get configmap geo-config')],
            )
        ],
    )
    return evolve(
        request,
        repository_worktree=str(worktree),
        relevant_outcomes=[_exact_match_outcome(match_reason)],
        memory_snapshot=snapshot,
    )


INCIDENT_PLAYBOOK = "missing-configmap"


def _not_yet_fired_request(worktree: Path, *, prior_incident: str = "incident-seed") -> IncidentRequest:
    """Only the health detector fired; the incident detector learned from the exact prior has not."""

    request = _warm_request(worktree)
    health = evolve(
        request.findings[0],
        detector_id="health-objective",
        rule_id="required-configmap-missing",
        playbook_ids=["health-objective"],
        fingerprint="health-objective/required-configmap-missing/hotel-reservation/geo-config",
    )
    prior = evolve(request.relevant_outcomes[0], incident_id=prior_incident, applied_playbook_ids=["health-objective"])
    return evolve(request, findings=[health], surfaced_playbooks=[], relevant_outcomes=[prior])


def test_matched_memory_uses_the_workflow_without_repeating_playbook_bodies(tmp_path: Path) -> None:
    prompt = _responder_prompt(_warm_request(tmp_path))
    document = incident_document(prompt, full=True)
    assert document["available_playbook_ids"] == [INCIDENT_PLAYBOOK]
    assert "Restore `<MISSING_CONFIG_MAP>` from source" not in prompt
    assert "verify.sh" not in prompt
    assert "learned mitigations first, repair the rest" in prompt
    assert "check rollout, current status, late findings" in _workflow()
    assert "preconditions and accepted-source values" in _workflow()


def test_historical_detector_match_does_not_invent_current_coverage(tmp_path: Path) -> None:
    request = _not_yet_fired_request(tmp_path)
    prompt = _responder_prompt(request)
    document = incident_document(prompt, full=True)
    assert document["findings"][0]["detector_id"] == "health-objective"
    assert "Warm path" not in prompt
    assert "preconditions and accepted-source values" in _workflow()


def test_warm_and_cold_paths_share_live_precondition_checks(tmp_path: Path) -> None:
    for request in (_warm_request(tmp_path), _not_yet_fired_request(tmp_path), _request()):
        prompt = _responder_prompt(request)
        assert str(SKILLS_ROOT / "sdo-incident-response/SKILL.md") in prompt
        assert "accepted-source values" in _workflow()


def test_nonexact_history_remains_advice_without_a_warm_instruction_branch(tmp_path: Path) -> None:
    request = _warm_request(tmp_path, match_reason="detector-rule-resource-kind")
    prompt = _responder_prompt(request)
    document = incident_document(prompt, full=True)
    assert document["relevant_outcomes"][0]["match_reason"] == "detector-rule-resource-kind"
    assert "Warm path" not in prompt
    assert "Operational memory is fallible advice" in _workflow()


def test_cold_prompt_offers_logical_ids_without_inlining_playbook_content() -> None:
    request = _request()
    request.memory_snapshot.playbooks.add(id="missing-configmap", markdown="Large reusable repair content")
    prompt = _responder_prompt(request)
    document = incident_document(prompt, full=True)
    assert document["available_playbook_ids"] == ["missing-configmap"]
    assert "Large reusable repair content" not in prompt
    assert ".sdo/" not in prompt


def test_every_path_requires_detector_self_check_then_independent_closure(tmp_path: Path) -> None:
    for request in (_request(), _warm_request(tmp_path)):
        prompt = _responder_prompt(request)
        assert "self-check before claiming recovery" in prompt
    workflow = _workflow()
    assert "SDO's independent verification is the source of truth" in workflow
    assert "wait on it instead of sleeping" in workflow
    assert "continue until it verifies" in workflow
    assert "sdo detect" not in workflow
    assert "sdo verify" not in workflow
    assert "never claim done without this self-check" in workflow
    assert "controller rechecks" in workflow


def test_workflow_prefers_finishing_blocking_operations_to_model_polling() -> None:
    assert "finish in one call instead of polling across model turns" in _workflow()


def test_workflow_requires_helper_objects_to_carry_the_cleanup_label() -> None:
    workflow = _workflow()
    assert "sdo.dev/responder-helper=true" in workflow
    assert "never application workloads" in workflow
    assert "Clean up temporary investigation resources" in workflow


def test_prompt_requires_live_evidence_for_every_root_cause() -> None:
    prompt = _responder_prompt(_request())
    assert "sdo-incident-response/SKILL.md" in prompt
    assert "Cite live cause observations, explained_detectors" in _workflow()
    assert "static_context" in _workflow()


def test_prompt_keeps_typed_healthy_baseline_changes_once() -> None:
    request = _request("incident_request_state_changes.json")
    prompt = _responder_prompt(request)
    document = incident_document(prompt, full=True)
    assert document["state_changes"] == json_format.MessageToDict(
        request.state_changes, preserving_proto_field_name=True
    )
    assert prompt.count("current_service_name=frontend,io.kompose.service=frontend") == 1


def test_prompt_distinguishes_an_empty_baseline_diff_from_an_absent_one() -> None:
    request = _request("incident_request_state_changes.json")
    request = evolve(
        request, state_changes=evolve(request.state_changes, changes=[], omitted=None, unobserved_kinds=[])
    )
    document = incident_document(_responder_prompt(request), full=True)
    assert document["state_changes"] == json_format.MessageToDict(
        request.state_changes, preserving_proto_field_name=True
    )
    assert "state_changes" not in incident_document(_responder_prompt(_request()), full=True)


def test_warm_path_requires_the_incident_playbook_to_exist(tmp_path: Path) -> None:
    request = _warm_request(tmp_path)
    request = evolve(request, memory_snapshot=evolve(request.memory_snapshot, playbooks=[]))
    prompt = _responder_prompt(request)

    assert "Warm path" not in prompt


def test_prompt_never_inlines_large_playbook_bodies(tmp_path: Path) -> None:
    request = _warm_request(tmp_path)
    request.memory_snapshot.playbooks[0].markdown = "x" * 50000 + "TAIL-MARKER"
    prompt = _responder_prompt(request)
    assert "TAIL-MARKER" not in prompt
    assert len(prompt) < 6000
    assert INCIDENT_PLAYBOOK in prompt


def test_prompt_compacts_detector_history_to_latest_evidence(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))
    prompt = _responder_prompt(request)
    document = incident_document(prompt, full=True)
    assert "detector_history" not in document
    latest = next(
        item for item in document["latest_detector_evaluations"] if item["detector_id"] == "missing-configmap"
    )
    assert latest["evaluated_at"] == "2026-07-09T18:00:30Z"
    assert latest["status"] == "firing"
    assert "hotel-reservation/geo/geo-config" in prompt


def test_prompt_keeps_accepted_health_intent_and_bounds_large_intent(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))
    request.memory_snapshot.goal_markdown = "The application serves successful requests."
    document = incident_document(_responder_prompt(request), full=True)
    assert document["intent"]["goal"] == request.memory_snapshot.goal_markdown
    request.memory_snapshot.goal_markdown = "y" * 20000
    with prepared_incident_data(request) as data:
        assert len(data) <= 12000
        assert "y" * 5000 not in data
        assert "Full:" in data


def test_prompt_keeps_accepted_architecture_and_bounds_large_intent(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))
    request.memory_snapshot.architecture_markdown = "The entrypoint uses the configuration service."
    document = incident_document(_responder_prompt(request), full=True)
    assert document["intent"]["architecture"] == request.memory_snapshot.architecture_markdown
    request.memory_snapshot.architecture_markdown = "y" * 20000
    with prepared_incident_data(request) as data:
        assert len(data) <= 12000
        assert "y" * 5000 not in data
        assert "Full:" in data


def test_program_prompt_guides_codex_and_claude_sessions() -> None:
    raw = json.loads(_fixture("incident_request.json"))
    raw["max_response_loops"] = 5
    raw["initial_program_runs"] = [
        {"program_id": "targetport", "verdict": {"result": "RESULT_MATCH"}, "duration": "0.02s"}
    ]
    request = from_dict(IncidentRequest, raw)
    prompt = _responder_prompt(request)
    assert "learned mitigations first, repair the rest" in prompt
    assert "Commit source repairs" in prompt
    assert "self-check" in prompt
    assert "targetport" in prompt


def test_responder_prompt_no_longer_asks_for_scratch_scripts() -> None:
    prompt = _responder_prompt(from_dict(IncidentRequest, json.loads(_fixture("incident_request.json"))))
    assert "parameterized script" not in prompt
    assert "scratch directory" not in prompt


def _program_sweep_request() -> IncidentRequest:
    raw = json.loads(_fixture("incident_request.json"))
    raw["max_response_loops"] = 5
    raw["initial_program_runs"] = [
        {
            "program_id": "unrelated-program",
            "bindings": [
                {
                    "name": "service",
                    "object": {
                        "api_version": "v1",
                        "kind": "Service",
                        "namespace": "app",
                        "name": f"quiet-{index:02d}",
                    },
                }
            ],
            "verdict": {
                "result": "RESULT_NO_MATCH",
                "evidence": [
                    {
                        "resource": {
                            "api_version": "v1",
                            "kind": "Service",
                            "namespace": "app",
                            "name": f"quiet-{index:02d}",
                        },
                        "fact": "targetPort",
                        "observed": "8080",
                        "expected": "8080",
                    }
                ],
            },
            "duration": "0.02s",
            "binding_source": "sweep",
            "program_revision": "a" * 40,
        }
        for index in range(19)
    ] + [
        {
            "program_id": "targetport",
            "bindings": [
                {
                    "name": "service",
                    "object": {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"},
                }
            ],
            "verdict": {
                "result": "RESULT_MATCH",
                "evidence": [
                    {
                        "resource": {"api_version": "v1", "kind": "Service", "namespace": "app", "name": "api"},
                        "fact": "targetPort",
                        "observed": "9999",
                        "expected": "8080",
                    }
                ],
            },
            "duration": "0.02s",
        }
    ]
    return from_dict(IncidentRequest, raw)


def test_prompt_omits_no_match_sweep_but_preserves_match_evidence() -> None:
    request = _program_sweep_request()
    original = request.SerializeToString()

    prompt = _responder_prompt(request)
    view = incident_document(prompt, full=True)

    assert len(view["initial_program_runs"]) == 1
    assert view["initial_program_runs"][0]["program_id"] == "targetport"
    assert view["initial_program_runs"][0]["verdict"]["evidence"][0]["observed"] == "9999"
    assert view["omitted_no_match_program_runs"] == 19
    assert "unrelated-program" not in prompt
    assert "quiet-" not in prompt
    assert len(prompt) < 14_000
    assert request.SerializeToString() == original


@pytest.mark.parametrize("result", ["RESULT_UNKNOWN", "RESULT_NEEDS_JUDGEMENT"])
def test_prompt_keeps_inconclusive_program_evidence_and_questions(result: str) -> None:
    request = _program_sweep_request()
    run = request.initial_program_runs[-1]
    run.verdict.result = run.verdict.DESCRIPTOR.fields_by_name["result"].enum_type.values_by_name[result].number
    if result == "RESULT_NEEDS_JUDGEMENT":
        run.verdict.question = "Is the source port correct for this service?"
    run.error = "live source unavailable"

    prompt = _responder_prompt(request)

    assert result in prompt
    assert "live source unavailable" in prompt
    assert "9999" in prompt
    if run.verdict.question:
        assert run.verdict.question in prompt


@pytest.mark.parametrize("can_mitigate", [True, False, None])
def test_prompt_marks_known_program_capabilities(can_mitigate: bool | None) -> None:
    request = _program_sweep_request()
    if can_mitigate is not None:
        json_format.ParseDict({"can_mitigate": can_mitigate}, request.initial_program_runs[-1])

    prompt = _responder_prompt(request)
    view = incident_document(prompt, full=True)["initial_program_runs"][0]

    if can_mitigate is False:
        assert view["capability"] == "diagnosis-only"
    elif can_mitigate is True:
        assert view["capability"] == "diagnosis-and-mitigation"
    else:
        assert view["capability"] == "unknown"
    assert "Diagnosis-only matches supply evidence" in _workflow()
    assert "Counts inform; never permission gates" in _workflow()
    assert "Discover capabilities and argument shapes" in _workflow()
    assert "--bind" not in prompt.split("Incident request:", 1)[0]
    assert "sdo plan --help" in incident_document(prompt, full=True)["incident_tools"]["commands"]["plan"]


def _scripted_reply(result: IncidentResult, session: str) -> FakeRun:
    message = {"type": "agent_message", "id": "msg", "text": json.dumps(_reply(result))}
    return FakeRun(
        stdout=[
            json.dumps({"type": "thread.started", "thread_id": session}) + "\n",
            json.dumps({"type": "item.completed", "item": message}) + "\n",
            '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,'
            '"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":1}}\n',
        ]
    )


def test_workflow_separates_static_context_from_live_cause_evidence() -> None:
    assert "Static files belong in static_context" in _workflow()
    assert "live cause evidence" in _workflow()


def test_memory_matches_do_not_override_accepted_source(tmp_path: Path) -> None:
    for request in (_warm_request(tmp_path), _not_yet_fired_request(tmp_path), _request()):
        prompt = _responder_prompt(request)
        assert "sdo-incident-response/SKILL.md" in prompt
        assert "Accepted source and developer changes express intent." in _workflow()
        assert "Operational memory is fallible advice." in _workflow()
        assert "Recovery requires evidence from the intended application behavior." in _workflow()
        assert "repair exactly" not in prompt
        assert "do not explore the repository or cluster" not in prompt
        assert "Prefer a matching program's plan" not in prompt


def test_controller_continuation_requires_existing_provider_session(monkeypatch) -> None:
    monkeypatch.setenv("SDO_RESPONSE_CONTINUATION", '{"healthy":false,"attempt":1}')
    monkeypatch.setattr("sdo.agents.responder.codex.open_recovery", lambda *_: None)
    with pytest.raises(ResponderExecutionError, match="existing provider session"):
        execute_incident(_request(), provider="codex")


def test_controller_verification_resumes_same_session_with_remaining_budget(monkeypatch) -> None:
    from types import SimpleNamespace

    from libs.agent_cli.structured import StructuredTurn

    feedback = '{"healthy":false,"attempt":1,"summary":"still unavailable","still_broken":[],"already_done":[]}'
    monkeypatch.setenv("SDO_RESPONSE_CONTINUATION", feedback)
    recovery = SimpleNamespace(
        resume_session_id=lambda: "same-provider-session",
        recovered_prompt=lambda: "",
        remaining_seconds=lambda: 43.0,
        checkpoint=lambda _: None,
        acknowledge_reports=lambda: None,
    )
    monkeypatch.setattr("sdo.agents.responder.codex.open_recovery", lambda *_: recovery)
    captured = {}

    def run_turn(provider, prompt, **options):
        captured.update(provider=provider, prompt=prompt, **options)
        return StructuredTurn(output_json=json.dumps(_reply(_result())), session_id="same-provider-session", usage=None)

    monkeypatch.setattr("sdo.agents.responder.codex.run_structured_turn", run_turn)
    monkeypatch.setattr("sdo.agents.responder.codex.turn_usage", lambda _: {"llm_calls": 1})
    monkeypatch.setattr("sdo.agents.responder.codex.recorded_actions.merge_into", lambda _: None)
    execute_incident(_request(), provider="codex")
    assert captured["resume_session_id"] == "same-provider-session"
    assert captured["timeout_seconds"] == 43.0
    assert "still unavailable" in captured["prompt"]
    assert "still_broken: (0)" in captured["prompt"]
    evidence = Path(captured["prompt"].split("Full JSON: ", 1)[1].splitlines()[0])
    assert json.loads(evidence.read_text()) == json.loads(feedback)


@pytest.mark.parametrize("fails", [False, True])
def test_continuation_keeps_response_scoped_facts_alive_until_turn_exit(monkeypatch, fails: bool) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace

    from libs.agent_cli.structured import StructuredTurn, StructuredTurnError
    from sdo.agents.responder.continuation import continuation_prompt

    feedback = '{"healthy":false,"attempt":1,"summary":"still unavailable","still_broken":[],"already_done":[]}'
    monkeypatch.setenv("SDO_RESPONSE_CONTINUATION", feedback)
    active = []

    @contextmanager
    def facts(request):
        active.append(request.incident_id)
        try:
            yield "original response facts"
        finally:
            active.clear()

    recovery = SimpleNamespace(
        resume_session_id=lambda: "same-provider-session",
        recovered_prompt=lambda: "",
        remaining_seconds=lambda: 43.0,
        checkpoint=lambda _: None,
        acknowledge_reports=lambda: None,
    )
    monkeypatch.setattr("sdo.agents.responder.codex.prepared_incident_data", facts)
    monkeypatch.setattr("sdo.agents.responder.codex.open_recovery", lambda *_: recovery)

    def run_turn(provider, prompt, **options):
        assert active == [_request().incident_id]
        assert prompt == continuation_prompt(feedback)
        if fails:
            raise StructuredTurnError("synthetic continuation failure")
        return StructuredTurn(output_json=json.dumps(_reply(_result())), session_id="same-provider-session", usage=None)

    monkeypatch.setattr("sdo.agents.responder.codex.run_structured_turn", run_turn)
    monkeypatch.setattr("sdo.agents.responder.codex.turn_usage", lambda _: {"llm_calls": 1})
    monkeypatch.setattr("sdo.agents.responder.codex.recorded_actions.merge_into", lambda _: None)
    if fails:
        with pytest.raises(ResponderExecutionError, match="synthetic continuation failure"):
            execute_incident(_request(), provider="codex")
    else:
        execute_incident(_request(), provider="codex")
    assert active == []


def test_app_drift_defaults_to_verified_accepted_source_restoration() -> None:
    request = _request()
    request.drift_events.add()
    prompt = _responder_prompt(request)
    assert "drift_events" in prompt
    assert "Restore namespace drift to accepted source" in _workflow()
    assert "unless live evidence establishes an intended difference" in _workflow()
    assert "uncertainty alone doesn't block it" in _workflow()
    assert "Explain benign drift with live evidence" in _workflow()


@pytest.mark.parametrize("fails", [False, True])
def test_real_turn_can_read_complete_overflow_facts_and_cleans_them_up(fails: bool) -> None:
    request = _request()
    request.findings[-1].evidence = "start " + "large live evidence 界" * 1000 + " end"
    files: list[Path] = []

    def respond(command) -> FakeRun:
        prompt = command.stdin or ""
        data = prompt
        assert len(data) < 12000
        details = incident_facts_path(data)
        files.append(details)
        assert (
            json.loads(details.read_text(encoding="utf-8"))["findings"][-1]["evidence"] == request.findings[-1].evidence
        )
        if fails:
            return failure(stderr="synthetic model failure", returncode=1)
        return _scripted_reply(_result(), "session-overflow")

    agent = ScriptedAgent(respond)
    if fails:
        with pytest.raises(ResponderExecutionError, match="synthetic model failure"):
            execute_incident(request, executor=agent.executor)
    else:
        execute_incident(request, executor=agent.executor)
    assert files
    assert all(not path.exists() for path in files)


def test_responder_checkpoint_binds_worker_task_and_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from agentshim import normalized_usage
    from agentshim.testing import scripted_turn

    directory = tmp_path / "usage"
    monkeypatch.setenv("SDO_RESPONDER_USAGE_DIR", str(directory))
    request = evolve(_request(), repository_worktree=str(tmp_path))
    task_id = request.incident_id + "-continue-1"
    monkeypatch.setenv("SDO_RESPONDER_TASK_ID", task_id)
    agent = ScriptedAgent(
        lambda _: scripted_turn(
            "codex",
            session_id="synthetic-usage-session",
            structured_output=_reply(_result()),
            usage=normalized_usage(input_tokens=120, output_tokens=10, turns=1),
        )
    )
    result = execute_incident(request, provider="codex", executor=agent.executor)
    documents = [json.loads(path.read_text()) for path in directory.glob("*.json")]
    assert len(documents) == 1
    assert documents[0]["incident_id"] == request.incident_id
    assert documents[0]["task_id"] == task_id
    assert documents[0]["phase"] == "responder"
    assert documents[0]["complete"] is True
    assert documents[0]["usage"]["input_tokens"] == result.usage.input_tokens == 120


@pytest.mark.parametrize("provider", ["codex", "claude"])
def test_worker_ignores_malformed_controller_fields(provider: str, tmp_path: Path) -> None:
    from tests.structured_turns import reply

    payload = json_format.MessageToDict(_result(), preserving_proto_field_name=True)
    payload["drift_reprompt"] = {"findings_before": 0, "findings_after": 0}
    payload["unresolvedFindings"] = [{"invalid": True}]
    agent = ScriptedAgent(lambda _: reply(provider, payload, session_id="worker-session", branch="incident_result"))
    result = execute_incident(
        evolve(_request(), repository_worktree=str(tmp_path)), provider=provider, executor=agent.executor
    )
    assert not result.HasField("drift_reprompt")
    assert not result.unresolved_findings
    assert len(agent.requests) == 1


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("corrected", [True, False])
def test_worker_schema_violation_gets_one_same_session_correction(
    provider: str,
    corrected: bool,
    tmp_path: Path,
) -> None:
    from tests.structured_turns import reply

    valid = json_format.MessageToDict(_result(), preserving_proto_field_name=True)
    invalid = {**valid, "status": "not-a-status"}
    outputs = iter([invalid, valid if corrected else invalid])
    agent = ScriptedAgent(
        lambda _: reply(provider, next(outputs), session_id="worker-session", branch="incident_result")
    )
    request = evolve(_request(), repository_worktree=str(tmp_path))
    if corrected:
        result = execute_incident(request, provider=provider, executor=agent.executor)
        assert result.status == "completed"
        assert result.usage.llm_calls == 2
    else:
        with pytest.raises(ResponderExecutionError, match="status"):
            execute_incident(request, provider=provider, executor=agent.executor)
    assert len(agent.requests) == 2
    assert "contract validation (IncidentResultValidationError)" in agent.prompts[1]
    assert "worker-session" in agent.argvs[1]
