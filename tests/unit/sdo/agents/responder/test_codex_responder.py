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
    _responder_prompt,
    execute_incident,
)
from sdo.contracts import (
    ConfirmedRootCause,
    IncidentRequest,
    IncidentResult,
    MemoryDetector,
    MemoryPlaybook,
    MemoryScript,
    MemoryWatch,
    PriorOutcomeEvidence,
    RootCauseEvidence,
)
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
    assert "separate reflection task may later edit operational memory in its own checkout" in prompt
    assert "controller verifies recovery" in prompt
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
    action = schema["properties"]["repair_actions"]["items"]
    assert action["required"] == list(action["properties"])
    cause = schema["properties"]["confirmed_root_causes"]["items"]
    assert cause["required"] == list(cause["properties"])
    assert cause["additionalProperties"] is False
    assert cause["properties"]["evidence"]["minItems"] == 1
    assert cause["properties"]["explained_detectors"]["minItems"] == 1
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
    request = evolve(_request(), repair_policy="recorded-actions")

    prompt = _responder_prompt(request)

    assert "recorded-actions" in prompt
    assert "sdo act" in prompt


def test_prompt_tells_the_responder_to_cancel_an_already_healed_finding() -> None:
    request = _request()

    prompt = _responder_prompt(request)

    assert "self-heal" in prompt
    assert "cancelled" in prompt
    assert "no repair actions and no applied playbooks" in prompt
    assert "Never return `completed` while the health objective is still failing" in prompt


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


def test_exact_match_on_incident_detector_inlines_playbook_and_fast_procedure(tmp_path: Path) -> None:
    prompt = _responder_prompt(_warm_request(tmp_path))

    assert "Warm path" in prompt
    assert "Restore `<MISSING_CONFIG_MAP>` from source" in prompt
    assert "verify.sh" in prompt
    assert 'kubectl -n "$1" get configmap geo-config' in prompt
    assert "already establishes the playbook's preconditions" in prompt
    assert "one combined sanity check" in prompt
    assert "replaces the playbook's own diagnosis steps" in prompt
    assert "has not fired" not in prompt
    assert "full investigation only if" in prompt
    assert "Confirm a surfaced playbook against live state before applying it" not in prompt
    # The learned playbook owns the repair; SDO adds no fault-specific repair tip of its own.
    assert "mount backoff" not in prompt
    assert "stuck pods" not in prompt


def test_registered_but_not_fired_incident_detector_gets_precondition_sanity_check(tmp_path: Path) -> None:
    prompt = _responder_prompt(_not_yet_fired_request(tmp_path))

    assert "Warm path" in prompt
    assert f"`{INCIDENT_PLAYBOOK}`: owned by the registered incident detector `missing-configmap`" in prompt
    assert "has not fired for this incident, so no incident-detector evidence exists yet" in prompt
    assert "Deployment (apps/v1)" in prompt
    assert "namespace `hotel-reservation`" in prompt
    assert "Run `verify.sh` inside that check" in prompt
    assert "one combined sanity check" in prompt
    assert "contradicts the playbook's preconditions" in prompt
    assert "already establishes the playbook's preconditions" not in prompt
    assert "Confirm a surfaced playbook against live state before applying it" not in prompt


def test_cold_prompt_is_unchanged_without_exact_incident_detector_match(tmp_path: Path) -> None:
    registered = tmp_path / "registered"
    no_prior = evolve(_request(), repository_worktree=str(registered))
    cold_requests = [
        no_prior,
        _warm_request(registered, match_reason="detector-rule-resource-kind"),
        evolve(
            _warm_request(registered),
            memory_snapshot=evolve(_warm_request(registered).memory_snapshot, detectors=[]),
        ),
        _not_yet_fired_request(registered, prior_incident="unrelated-incident"),
    ]

    for request in cold_requests:
        prompt = _responder_prompt(request)
        assert "Warm path" not in prompt
        assert "Treat them as hypotheses" in prompt
        assert "Confirm a surfaced playbook against live state before applying it" in prompt


def test_cold_prompt_inlines_surfaced_logical_playbook_without_memory_file_read() -> None:
    request = _request()
    snapshot = evolve(
        request.memory_snapshot,
        playbooks=[
            MemoryPlaybook(
                id="missing-configmap",
                markdown="Restore the source-backed configuration safely.",
                scripts=[MemoryScript(name="verify.sh", content="kubectl get configmap")],
            )
        ],
    )
    request = evolve(request, memory_snapshot=snapshot)

    prompt = _responder_prompt(request)

    assert "Warm path" not in prompt
    assert "Restore the source-backed configuration safely." in prompt
    assert "verify.sh" in prompt
    assert ".sdo/" not in prompt


def test_every_path_verifies_with_incident_status_before_submitting(tmp_path: Path) -> None:
    cold = _request()

    for prompt in (_responder_prompt(cold), _responder_prompt(_warm_request(tmp_path))):
        assert "`python3 -m sdo.agents.responder.status_cli`" in prompt
        assert "same synthetic traffic the controller requires before it closes this incident" in prompt
        assert "other health detectors still fire" in prompt
        assert "changed after this request was taken" in prompt
        assert "Do not submit mitigation through any channel, and do not return a completed result" in prompt
        assert "reports unavailable" in prompt
        assert "`sdo-incident-status`" in prompt
    warm = _responder_prompt(_warm_request(tmp_path))
    assert "Run the playbook's verification and `python3 -m sdo.agents.responder.status_cli`" in warm


def test_every_path_prefers_one_blocking_wait_over_manual_poll_turns(tmp_path: Path) -> None:
    """Each check-in on a backgrounded command is a separate model turn that resends the whole context.

    Audited rollouts show the
    responder issuing a short-yield exec check, getting nothing new, and returning later in a fresh
    turn to check again; each such turn cost about as much context as any other turn in the session.
    The prompt should steer the responder toward a single command that blocks until done instead.
    """
    cold = _request()

    for prompt in (_responder_prompt(cold), _responder_prompt(_warm_request(tmp_path))):
        assert "one blocking command" in prompt
        assert "checking in on a backgrounded" in prompt
        assert "kubectl rollout status" in prompt
        assert "--timeout" in prompt


def test_prompt_requires_helper_objects_to_carry_the_cleanup_label() -> None:
    prompt = _responder_prompt(_request())

    assert "sdo.dev/responder-helper=true" in prompt
    assert "deletes them when you finish" in prompt


def test_prompt_requires_live_evidence_for_every_root_cause() -> None:
    prompt = _responder_prompt(_request())

    assert "Every confirmed root cause needs live evidence" in prompt
    assert "static_context" in prompt
    assert "explained_detectors" in prompt
    assert "must clear after your fix" in prompt


def test_prompt_states_the_cause_admissibility_bar_only_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("SDO_CAUSE_ADMISSIBILITY", "on")

    prompt = _responder_prompt(_request())

    assert "Admissibility of a confirmed root cause" in prompt
    assert "benign drift" in prompt
    assert "record it in static_context, never in confirmed_root_causes" in prompt
    assert "weakly evidenced; do not assert it" in prompt
    # Problem-agnostic: no injected-fault vocabulary leaks into the bar.
    assert "SREGym" not in prompt
    assert "LOG_LEVEL" not in prompt


def test_default_off_keeps_the_prompt_free_of_the_admissibility_bar(monkeypatch) -> None:
    # Default (environment unset) and explicit off both omit the bar, so the
    # prompt is byte-identical to the pre-gate responder prompt.
    monkeypatch.delenv("SDO_CAUSE_ADMISSIBILITY", raising=False)
    request = _request()
    assert "Admissibility of a confirmed root cause" not in _responder_prompt(request)

    monkeypatch.setenv("SDO_CAUSE_ADMISSIBILITY", "off")
    assert "Admissibility of a confirmed root cause" not in _responder_prompt(request)


def test_execute_incident_withholds_a_weakly_evidenced_cause(monkeypatch) -> None:
    monkeypatch.setenv("SDO_CAUSE_ADMISSIBILITY", "on")
    request = _request()
    result = _result()
    weak = evolve(
        result.confirmed_root_causes[0],
        summary="benign image drift",
        evidence=[
            RootCauseEvidence(
                kind="live-observation",
                source="kubectl get deploy",
                observation="image tag differs from a prior build",
            )
        ],
        explained_detectors=["phantom-detector"],
    )
    assert isinstance(weak, ConfirmedRootCause)
    payload = evolve(result, confirmed_root_causes=[*result.confirmed_root_causes, weak])
    message = {"type": "agent_message", "id": "msg", "text": json.dumps(_reply(payload))}
    agent = ScriptedAgent(
        lambda _request: FakeRun(
            stdout=[
                '{"type":"thread.started","thread_id":"session-1"}\n',
                json.dumps({"type": "item.completed", "item": message}) + "\n",
                '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,'
                '"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":1}}\n',
            ]
        )
    )

    completed = execute_incident(request, executor=agent.executor)

    summaries = [cause.summary for cause in completed.confirmed_root_causes]
    assert "benign image drift" not in summaries
    assert summaries == ["The geo Deployment referenced an absent required ConfigMap"]


def test_execute_incident_default_off_keeps_every_proposed_cause(monkeypatch) -> None:
    # With the gate at its default (off), execute_incident does not touch the
    # agent's confirmed_root_causes: behavior is byte-identical to pre-gate.
    monkeypatch.delenv("SDO_CAUSE_ADMISSIBILITY", raising=False)
    request = _request()
    result = _result()
    weak = evolve(
        result.confirmed_root_causes[0],
        summary="benign image drift",
        evidence=[
            RootCauseEvidence(
                kind="live-observation",
                source="kubectl get deploy",
                observation="image tag differs from a prior build",
            )
        ],
        explained_detectors=["phantom-detector"],
    )
    payload = evolve(result, confirmed_root_causes=[*result.confirmed_root_causes, weak])
    message = {"type": "agent_message", "id": "msg", "text": json.dumps(_reply(payload))}
    agent = ScriptedAgent(
        lambda _request: FakeRun(
            stdout=[
                '{"type":"thread.started","thread_id":"session-1"}\n',
                json.dumps({"type": "item.completed", "item": message}) + "\n",
                '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,'
                '"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":1}}\n',
            ]
        )
    )

    completed = execute_incident(request, executor=agent.executor)

    summaries = [cause.summary for cause in completed.confirmed_root_causes]
    assert "benign image drift" in summaries


def test_prompt_lists_changes_since_the_healthy_baseline_once() -> None:
    request = _request("incident_request_state_changes.json")

    prompt = _responder_prompt(request)

    assert "Changes since the last healthy state (baseline 2026-09-27T10:00:00Z" in prompt
    assert (
        "- Service/frontend modified: selector: io.kompose.service=frontend -> "
        "current_service_name=frontend,io.kompose.service=frontend" in prompt
    )
    assert "- NetworkPolicy/deny-all added: policyTypes: Ingress" in prompt
    assert "- ConfigMap/geo-config removed" in prompt
    assert "2 more changes omitted" in prompt
    assert "not observed: Secret" in prompt
    assert "existed unchanged while the application was healthy" in prompt
    # The inference is neutral: no steer about names or contents that look suspicious.
    assert "suspicious" not in prompt
    assert "did not cause a new incident on their own" in prompt
    assert prompt.count("current_service_name=frontend,io.kompose.service=frontend") == 1


def test_prompt_says_when_nothing_changed_since_the_healthy_baseline() -> None:
    request = _request("incident_request_state_changes.json")
    unchanged = evolve(
        request,
        state_changes=evolve(request.state_changes, changes=[], omitted=None, unobserved_kinds=[]),
    )

    prompt = _responder_prompt(unchanged)

    assert "No Service, workload, NetworkPolicy, ConfigMap, Secret, or RBAC object changed" in prompt
    assert "Changes since the last healthy state" not in _responder_prompt(_request())


def test_warm_path_requires_the_incident_playbook_to_exist(tmp_path: Path) -> None:
    request = _warm_request(tmp_path)
    request = evolve(request, memory_snapshot=evolve(request.memory_snapshot, playbooks=[]))
    prompt = _responder_prompt(request)

    assert "Warm path" not in prompt


def test_warm_path_caps_inlined_playbook_text(tmp_path: Path) -> None:
    request = _warm_request(tmp_path)
    playbook = evolve(
        request.memory_snapshot.playbooks[0],
        markdown="Restore `<MISSING_CONFIG_MAP>` from source.\n" + "x" * 50_000 + "TAIL-MARKER\n",
    )
    snapshot = evolve(request.memory_snapshot, playbooks=[playbook])
    prompt = _responder_prompt(evolve(request, memory_snapshot=snapshot))

    assert "Warm path" in prompt
    assert "TAIL-MARKER" not in prompt
    assert "truncated" in prompt
    assert len(prompt) < 30_000


def test_prompt_compacts_detector_history_to_latest_evidence(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))

    prompt = _responder_prompt(request)

    assert '"detector_history"' not in prompt
    assert "2026-07-09T18:00:00Z" not in prompt
    assert "missing-configmap: firing at 2026-07-09T18:00:30Z" in prompt
    assert "hotel-reservation/geo/geo-config" in prompt


def test_prompt_inlines_a_small_health_objective(tmp_path: Path) -> None:
    request = evolve(_request(), repository_worktree=str(tmp_path))
    request = evolve(
        request,
        memory_snapshot=evolve(request.memory_snapshot, goal_markdown="The application serves successful requests."),
    )

    prompt = _responder_prompt(request)

    assert "The application serves successful requests." in prompt
    assert "trusted memory snapshot" in prompt

    large = evolve(request.memory_snapshot, goal_markdown="y" * 20_000)
    assert "y" * 5_000 not in _responder_prompt(evolve(request, memory_snapshot=large))


def test_prompt_inlines_a_small_architecture_summary(tmp_path: Path) -> None:
    """The accepted architecture summary is supplied by the logical memory snapshot."""
    request = evolve(_request(), repository_worktree=str(tmp_path))
    request = evolve(
        request,
        memory_snapshot=evolve(
            request.memory_snapshot, architecture_markdown="The entrypoint uses the configuration service."
        ),
    )

    prompt = _responder_prompt(request)

    assert "The entrypoint uses the configuration service." in prompt
    assert "trusted memory snapshot" in prompt

    large = evolve(request.memory_snapshot, architecture_markdown="y" * 20_000)
    assert "y" * 5_000 not in _responder_prompt(evolve(request, memory_snapshot=large))


def test_program_prompt_guides_codex_and_claude_sessions() -> None:
    raw = json.loads(_fixture("incident_request.json"))
    raw["max_response_loops"] = 5
    raw["initial_program_runs"] = [
        {"program_id": "targetport", "verdict": {"result": "RESULT_MATCH"}, "duration": "0.02s"}
    ]
    request = from_dict(IncidentRequest, raw)
    prompt = _responder_prompt(request)
    assert "ranked program verdicts" in prompt
    assert "sdo plan then sdo apply" in prompt
    assert "Always finish with sdo verify" in prompt
    assert "return the structured incident result immediately" in prompt
    assert '"program_id": "targetport"' in prompt


def test_program_prompt_includes_failed_fast_path_handoff() -> None:
    raw = json.loads(_fixture("incident_request.json"))
    raw["fast_path_summary"] = "trusted targetport attempted; verification failed; rollback true"
    prompt = _responder_prompt(from_dict(IncidentRequest, raw))
    assert raw["fast_path_summary"] in prompt


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
    view = json.loads(prompt.split("Incident request:\n", 1)[1])

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
    view = json.loads(prompt.split("Incident request:\n", 1)[1])["initial_program_runs"][0]

    if can_mitigate is False:
        assert view["capability"] == "diagnosis-only"
    elif can_mitigate is True:
        assert view["capability"] == "diagnosis-and-mitigation"
    else:
        assert view["capability"] == "unknown"
    assert "For a Match with can_mitigate=true" in prompt
    assert "sdo source Deployment/app/api" in prompt
    assert "sdo plan targetport --bind service=Service/app/api" in prompt
    assert "Do not print the tool-description catalog" in prompt


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


def _with_state_change_source(source: str) -> IncidentResult:
    result = _result()
    cause = result.confirmed_root_causes[0]
    evidence = [RootCauseEvidence(kind="state-change", source=source, observation="targetPort changed")]
    return evolve(result, confirmed_root_causes=[evolve(cause, evidence=evidence)])


def test_prompt_requires_one_object_per_state_change_entry() -> None:
    prompt = _responder_prompt(_request())

    assert "exactly one `Kind/name` or `Kind/namespace/name`" in prompt
    assert "one state-change entry per object" in prompt


def test_compound_state_change_source_gets_one_targeted_retry() -> None:
    bad = _with_state_change_source("Service/geo; Service/recommendation")
    good = _with_state_change_source("Service/geo")
    replies = iter([_scripted_reply(bad, "session-1"), _scripted_reply(good, "session-1")])
    agent = ScriptedAgent(lambda _request: next(replies))

    completed = execute_incident(_request(), executor=agent.executor)

    assert completed.confirmed_root_causes[0].evidence[0].source == "Service/geo"
    assert len(agent.requests) == 2
    retry = agent.prompts[1]
    assert "Service/geo; Service/recommendation" in retry
    assert "own evidence entry" in retry
    assert "session-1" in " ".join(agent.argvs[1])


def test_compound_state_change_source_still_malformed_after_retry_fails() -> None:
    bad = _with_state_change_source("Service/geo, Service/recommendation")
    agent = ScriptedAgent(lambda _request: _scripted_reply(bad, "session-1"))

    with pytest.raises(ResponderExecutionError, match="exactly one object reference"):
        execute_incident(_request(), executor=agent.executor)
    assert len(agent.requests) == 2
