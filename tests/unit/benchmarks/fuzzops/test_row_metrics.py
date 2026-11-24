"""Measurement contracts for lanes row reports, using synthetic evidence."""

import hashlib
import json
from pathlib import Path

import pytest

from benchmarks.fuzzops.row_metrics import main, measure


def at(second: int) -> str:
    return f"2026-01-01T00:00:{second:02d}Z"


def put(run: Path, name: str, value: object) -> None:
    path = run / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def grade(run: Path, mutations: list[dict[str, str]] | None = None) -> None:
    put(
        run,
        "record.json",
        {
            "fault": "synthetic",
            "valid": True,
            "window": {"BadSeconds": 14},
            "last_bad_second": 29,
            "recovered_in_window": True,
            "lifecycle": {"inject_at": at(0), "observed": {"mutations": mutations or []}},
        },
    )
    put(run, "result.json", {"status": "done"})


def transcript(run: Path, name: str, calls: list[tuple[int, int, str, bool, str]]) -> None:
    events = []
    for i, (start, end, command, error, output) in enumerate(calls):
        events.extend(
            [
                {
                    "type": "assistant",
                    "timestamp": at(start),
                    "message": {
                        "content": [
                            {"type": "tool_use", "id": str(i), "name": "Bash", "input": {"command": command}},
                        ]
                    },
                },
                {
                    "type": "user",
                    "timestamp": at(end),
                    "message": {
                        "content": [
                            {"type": "tool_result", "tool_use_id": str(i), "is_error": error, "content": output},
                        ]
                    },
                },
            ]
        )
    path = run / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(map(json.dumps, events)), encoding="utf-8")


def test_vanilla_wall_clock_and_database_write(tmp_path: Path) -> None:
    grade(tmp_path, [{"at": at(28), "target": "deployment/unrelated", "op": "changed"}])
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
    put(
        tmp_path,
        "operator/usage.json",
        {
            "input_tokens": 2,
            "cache_read_input_tokens": 10,
            "cache_creation_input_tokens": 4,
            "output_tokens": 5,
            "total_cost_usd": 0.01,
        },
    )
    transcript(
        tmp_path,
        "operator/session-dir/project/parent.jsonl",
        [
            (5, 6, "echo 'kubectl delete pod example'", False, ""),
            (8, 9, "kubectl exec database -- psql -c 'SELECT 1 FOR UPDATE'", False, "1"),
            (
                19,
                21,
                "kubectl exec database -- sh -c 'psql -c \"UPDATE widgets SET enabled = true\"'",
                False,
                "UPDATE 4",
            ),
            (23, 24, "kubectl patch deployment/widgets -p '{}'", True, "Forbidden"),
            (25, 26, "kubectl apply -f manifest.yaml", False, "deployment.apps/widgets unchanged"),
            (27, 28, "kubectl rollout status deployment/widgets", False, ""),
            (32, 33, "kubectl delete pod debug", False, 'pod "debug" deleted'),
        ],
    )
    row = measure(tmp_path)
    # Both arms share the headline endpoint, the last fixing write; the session end is kept apart.
    assert (row.arm, row.inject_to_page_s, row.page_source, row.ttf_s) == ("vanilla", 3, "n.a.", 16)
    assert (row.ttf_source, row.agent_wall_s) == ("claude_tool_start", 37)
    assert row.anchor_to_last_write_s == 16
    assert row.last_write_source == "claude_tool_start"
    assert row.mutation_crosscheck == "data_write_not_observed"
    assert (row.responder_input_tokens, row.responder_output_tokens, row.responder_cost_usd) == (16, 5, 0.01)
    assert row.incident_count is None


def test_sdo_keeps_last_plan_and_separate_phase_usage(tmp_path: Path) -> None:
    grade(tmp_path, [{"at": at(24), "target": "deployment/widgets", "op": "changed"}])
    put(
        tmp_path,
        "operator/collect.json",
        {
            "sdo_page_at": at(8),
            "fallback_page_at": at(7),
            "page_source": "fallback",
        },
    )
    put(
        tmp_path,
        "operator/evidence-manifest.json",
        {
            "timelines": [
                {
                    "incident": "incident-a",
                    "opened_at": at(8),
                    "playbook_applied_at": at(11),
                    "health_cleared_at": at(33),
                    "recovered_at": at(39),
                    "resolution_path": "deterministic",
                    "closing_turn_dispatch_at": at(40),
                    "closing_turn_cost_usd": 0.05,
                }
            ],
            "open_incidents": [],
        },
    )
    put(
        tmp_path,
        "operator/evidence/incidents/hash/ledger.json",
        {
            "incident_id": "incident-a",
            "outcome_document": {
                "plan_applications": [
                    {"applied": True, "completed_at": at(11)},
                    {
                        "applied": True,
                        "completed_at": at(23),
                        "object_effects": [
                            {"ref": {"kind": "Deployment", "name": "widgets"}, "confirmed": True},
                        ],
                    },
                    {"applied": True, "completed_at": at(25), "dry_run": True},
                    {"applied": False, "completed_at": at(26)},
                ]
            },
            "reflection_results": [
                {
                    "usage": {
                        "input_tokens": "10",
                        "cache_read_input_tokens": "9",
                        "output_tokens": "2",
                        "total_cost_usd": 0.05,
                    }
                }
            ],
        },
    )
    row = measure(tmp_path)
    assert (row.arm, row.inject_to_page_s, row.page_source, row.ttf_s) == ("sdo", 7, "fallback", 15)
    assert row.ttf_source == "plan_application_completed_at"
    assert row.mutation_crosscheck == "target_observed"
    assert row.fixed_by_playbook_alone is True
    assert row.incident_count == 1
    assert (row.responder_input_tokens, row.closing_turn_input_tokens, row.closing_turn_cost_usd) == (0, 10, 0.05)


def test_sdo_fallback_is_labeled_and_invalid_row_is_preserved(tmp_path: Path, capsys) -> None:
    grade(tmp_path)
    put(tmp_path, "result.json", {"status": "invalid", "reason": "lane lost"})
    put(
        tmp_path,
        "operator/evidence-manifest.json",
        {
            "timelines": [
                {
                    "opened_at": at(8),
                    "dispatched_at": at(10),
                    "first_agent_change_at": at(17),
                    "health_cleared_at": at(33),
                    "responder_usage": {"input_tokens": "18", "output_tokens": 2},
                }
            ]
        },
    )
    row = measure(tmp_path)
    assert (row.valid, row.invalid_reason, row.fixed_in_window) == (False, "lane lost", True)
    assert (row.ttf_s, row.ttf_source, row.anchor_to_last_write_s) == (9, "fallback_first_agent_change_at", None)
    assert main([str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["window_bad_user_s"] == 14
    assert main(["--format", "markdown", str(tmp_path)]) == 0
    assert "lane lost" in capsys.readouterr().out


def test_vanilla_verification_timestamps_the_completed_write(tmp_path: Path) -> None:
    grade(tmp_path)
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
    transcript(
        tmp_path,
        "operator/transcript.jsonl",
        [
            (15, 32, "kubectl exec app -- rails runner 'Widget.first.update!(enabled: true)'", False, "true"),
            (
                34,
                35,
                "kubectl exec database -- psql -c 'SELECT id, updated_at FROM widgets; SELECT now()'",
                False,
                "1 | 2026-01-01 00:00:31.500\n2026-01-01 00:00:35+00",
            ),
        ],
    )
    row = measure(tmp_path)
    assert (row.ttf_s, row.agent_wall_s) == (28.5, 37)
    assert (row.anchor_to_last_write_s, row.last_write_source) == (28.5, "transcript_db_updated_at")


def test_sdo_correlates_codex_receipt_and_counts_open_incident_usage(tmp_path: Path) -> None:
    grade(tmp_path, [{"at": at(23), "target": "sts/database", "op": "changed"}])
    path = "operator/evidence/incidents/closed/rollout-test.jsonl"
    put(
        tmp_path,
        "operator/evidence-manifest.json",
        {
            "timelines": [
                {
                    "incident": "closed-a",
                    "opened_at": at(7),
                    "dispatched_at": at(8),
                    "health_cleared_at": at(27),
                    "closing_turn_dispatch_at": at(30),
                    "responder_usage": {"input_tokens": 20, "output_tokens": 2, "total_cost_usd": 0.1},
                }
            ],
            "open_incidents": ["open"],
            "agents": [{"role": "responder", "incident": "closed-a", "path": path}],
        },
    )
    put(
        tmp_path,
        "operator/evidence/incidents/closed/ledger.json",
        {
            "incident_id": "closed-a",
            "outcome_document": {"agent_changes": [{"at": at(22)}]},
        },
    )
    open_log = tmp_path / "operator/evidence/incidents/open/claude-turns.jsonl"
    open_log.parent.mkdir(parents=True)
    open_log.write_text(
        json.dumps(
            {
                "role": "responder",
                "event": "complete",
                "usage": {
                    "input_tokens": 3,
                    "output_tokens": 1,
                    "total_cost_usd": 0.2,
                },
            }
        )
    )
    log = tmp_path / path
    events = []
    for i, second, output in [
        (1, 20, "statefulset.apps/database patched"),
        (2, 31, "statefulset.apps/database patched"),
    ]:
        events.extend(
            [
                {
                    "type": "response_item",
                    "timestamp": at(second),
                    "payload": {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": str(i),
                        "arguments": json.dumps({"cmd": "kubectl patch statefulset/database -p '{}'"}),
                    },
                },
                {
                    "type": "response_item",
                    "timestamp": at(second + 3),
                    "payload": {
                        "type": "function_call_output",
                        "call_id": str(i),
                        "output": output,
                    },
                },
            ]
        )
    log.write_text("\n".join(map(json.dumps, events)))
    row = measure(tmp_path)
    assert (row.ttf_s, row.ttf_source, row.mutation_crosscheck) == (
        15,
        "transcript_and_agent_change_at",
        "target_observed",
    )
    assert (row.responder_input_tokens, row.responder_output_tokens, row.incident_count) == (23, 3, 2)
    assert row.closing_turn_input_tokens is None


def test_vanilla_without_a_fixing_write_has_no_headline(tmp_path: Path) -> None:
    grade(tmp_path)
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
    row = measure(tmp_path)
    assert (row.ttf_s, row.ttf_source, row.agent_wall_s) == (None, "unavailable", 37)


def test_first_failure_episode_ends_at_a_30_second_clean_gap(tmp_path: Path) -> None:
    # users.bad is per second from users.start; the inject is 2 s after the start. Bad seconds at +5..+9 and
    # +20 belong to one episode (gap under 30 s); the single blips at +60 and +95 do not.
    bad = [0] * 120
    for second in [*range(7, 12), 22, 62, 97]:
        bad[second] = 1
    put(
        tmp_path,
        "record.json",
        {
            "fault": "synthetic",
            "valid": True,
            "window": {"BadSeconds": 8},
            "last_bad_second": 95,
            "recovered_in_window": True,
            "lifecycle": {"inject_at": at(2)},
            "users": {"start": at(0), "bad": bad},
        },
    )
    row = measure(tmp_path)
    assert (row.inject_to_last_bad_s, row.first_episode_end_s) == (95, 20)


def test_first_failure_episode_is_absent_without_per_second_users(tmp_path: Path) -> None:
    grade(tmp_path)
    assert measure(tmp_path).first_episode_end_s is None


def test_vanilla_cache_writes_count_as_fixing_writes(tmp_path: Path) -> None:
    # Cache state is app state: expiring a Rails cache or deleting a Redis key is a fixing write, reads are not.
    grade(tmp_path)
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
    transcript(
        tmp_path,
        "operator/transcript.jsonl",
        [
            (5, 6, "kubectl exec redis-0 -- sh -c 'redis-cli smembers cache:branch_names:x'", False, "master"),
            (9, 10, "kubectl exec redis-0 -- sh -c 'redis-cli DEL cache:branch_names:x'", False, "(integer) 1"),
            (
                14,
                15,
                "kubectl exec deploy/toolbox -- gitlab-rails runner "
                "'Project.find_each{|p| p.repository.expire_branches_cache}'",
                False,
                "",
            ),
            (17, 18, "kubectl exec deploy/toolbox -- gitlab-rails runner 'p Project.count'", False, "3"),
        ],
    )
    row = measure(tmp_path)
    assert (row.ttf_s, row.last_write_source) == (11, "claude_tool_start")


def test_vanilla_sql_ddl_counts_as_a_fixing_write(tmp_path: Path) -> None:
    # Schema changes are app state: dropping a trigger or function, or altering a constraint, is a fixing write.
    grade(tmp_path)
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
    transcript(
        tmp_path,
        "operator/transcript.jsonl",
        [
            (6, 7, "kubectl exec db-0 -- psql -Atc 'select tgname from pg_trigger'", False, "trg"),
            (
                12,
                13,
                "kubectl exec db-0 -- psql -Atc 'DROP TRIGGER trg ON public.issues; DROP FUNCTION public.f()'",
                False,
                "DROP TRIGGER\nDROP FUNCTION",
            ),
        ],
    )
    row = measure(tmp_path)
    assert (row.ttf_s, row.last_write_source) == (9, "claude_tool_start")


def test_sdo_open_or_reset_incident_still_yields_its_fixing_write(tmp_path: Path) -> None:
    # A fallback-paged incident that was reset after the window has no sealed timeline: the responder's writes in its
    # folder still give the time to fix, from the page, bounded by the graded recovery.
    grade(tmp_path)
    put(tmp_path, "operator/evidence-manifest.json", {"timelines": [], "open_incidents": ["open"]})
    put(tmp_path, "operator/collect.json", {"page_source": "fallback", "fallback_page_at": at(5), "sdo_page_at": at(5)})
    put(tmp_path, "operator/evidence/incidents/open/incident.json", {"incident_id": "app-1"})
    transcript(
        tmp_path,
        "operator/evidence/incidents/open/claude/projects/w/session.jsonl",
        [
            (9, 10, "kubectl exec db-0 -- psql -Atc 'select tgname from pg_trigger'", False, "trg"),
            (14, 15, "kubectl exec db-0 -- psql -Atc 'DROP TRIGGER trg ON public.issues'", False, "DROP TRIGGER"),
            (35, 36, "kubectl exec db-0 -- psql -Atc 'UPDATE issues SET title = title'", False, "UPDATE 3"),
        ],
    )
    row = measure(tmp_path)
    # The 35 s write starts after the last graded bad second (29): it cannot explain the recovery.
    assert (row.ttf_s, row.ttf_source) == (9, "claude_tool_start")


def test_terminating_a_blocking_session_is_a_fixing_write(tmp_path: Path) -> None:
    # pg_terminate_backend ends someone's session (it kills the lock holder): a write even inside a SELECT.
    grade(tmp_path)
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
    transcript(
        tmp_path,
        "operator/transcript.jsonl",
        [
            (6, 7, "kubectl exec db-0 -- psql -Atc 'select pid from pg_stat_activity'", False, "41"),
            (
                11,
                12,
                "kubectl exec db-0 -- psql -Atc 'select pg_terminate_backend(pid) from pg_stat_activity where pid=41'",
                False,
                "t",
            ),
        ],
    )
    row = measure(tmp_path)
    assert (row.ttf_s, row.last_write_source) == (8, "claude_tool_start")


def test_a_fix_within_the_client_timeout_of_the_last_bad_second_counts(tmp_path: Path) -> None:
    # A hanging request is charged to the second it started: the kill that ends the hang can land a few seconds after
    # the last bad second. A write well after it cannot explain the recovery.
    grade(tmp_path)  # last_bad_second 29, inject at 0
    put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})

    def row(second: int):
        put(tmp_path, "operator/done.json", {"started_at": at(3), "ended_at": at(40)})
        transcript(
            tmp_path,
            "operator/transcript.jsonl",
            [(second, second + 1, "kubectl delete pod web-1", False, 'pod "web-1" deleted')],
        )
        return measure(tmp_path)

    assert row(32).ttf_s == 29  # within the slack
    assert row(36).ttf_s is None  # too late


ARMS = ("vanilla", "sealed", "open")


def arm_row(run: Path, arm: str, calls: list[tuple[int, int, str, bool, str]]):
    """The same transcript from +5 s as a plain-agent row or an SDO row (sealed or still open incident)."""
    grade(run)
    if arm == "vanilla":
        put(run, "operator/done.json", {"started_at": at(5), "ended_at": at(50)})
        transcript(run, "operator/transcript.jsonl", calls)
        return measure(run)
    folder = hashlib.sha256(b"app-1").hexdigest()[:32] if arm == "sealed" else "open"
    sealed = {"incident": "app-1", "opened_at": at(5), "health_cleared_at": at(50), "resolution_path": "responder"}
    put(
        run,
        "operator/evidence-manifest.json",
        {
            "timelines": [sealed] if arm == "sealed" else [],
            "open_incidents": [] if arm == "sealed" else ["open"],
        },
    )
    put(run, "operator/collect.json", {"page_source": "own", "sdo_page_at": at(5)})
    put(run, f"operator/evidence/incidents/{folder}/incident.json", {"incident_id": "app-1"})
    transcript(run, f"operator/evidence/incidents/{folder}/claude/projects/w/session.jsonl", calls)
    return measure(run)


@pytest.mark.parametrize("arm", ARMS)
@pytest.mark.parametrize(
    ("command", "output"),
    [
        ("kubectl delete pod web-1", 'pod "web-1" deleted'),
        ("kubectl exec db-0 -- psql -Atc 'DROP TRIGGER trg ON public.issues'", "DROP TRIGGER"),
        ("kubectl exec db-0 -- psql -Atc 'select pg_terminate_backend(41)'", "t"),
        ("kubectl exec redis-0 -- redis-cli DEL cache:x", "(integer) 1"),
    ],
)
def test_every_write_kind_counts_the_same_for_both_arms(tmp_path: Path, arm: str, command: str, output: str) -> None:
    assert arm_row(tmp_path, arm, [(12, 13, command, False, output)]).ttf_s == 7


@pytest.mark.parametrize("arm", ARMS)
@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        ([12, 40], 7),  # a write after the graded recovery (last bad second 29) explains nothing
        ([12, 32], 7),  # nor does one in the slack while an earlier one exists
        ([32], 27),  # the slack covers a hanging request's client timeout
        ([36], None),  # past the slack
    ],
)
def test_the_fixing_write_rule_is_the_same_for_both_arms(
    tmp_path: Path, arm: str, seconds: list[int], expected: int | None
) -> None:
    calls = [(s, s + 1, "kubectl delete pod web-1", False, 'pod "web-1" deleted') for s in seconds]
    assert arm_row(tmp_path, arm, calls).ttf_s == expected
