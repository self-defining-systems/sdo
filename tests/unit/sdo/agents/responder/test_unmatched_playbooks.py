"""The responder's first message lists the learned playbooks that were evaluated and did not match."""

from __future__ import annotations

from google.protobuf import json_format, timestamp_pb2

from sdo.agents.responder.codex import _responder_prompt
from sdo.agents.responder.incident_opening import symptom, unmatched_playbooks_block
from sdo.contracts import IncidentRequest, MemoryPlaybook, ProgramRun, ReadStep, Verdict
from tests.unit.sdo.agents.prompts.cases import incident

README = (
    "---\nschema_version: 1\nowner: responder\nfault_class: narrow-column\n"
    "originating_incident: inc-7\ndescription: widen a varchar column the app outgrew\n---\n\n# Narrow column\n"
)


def _run(
    program_id: str,
    result: int = Verdict.RESULT_NO_MATCH,
    reads: tuple[tuple[str, str, str], ...] = (),
    source: str = "finding",
) -> ProgramRun:
    run = ProgramRun(program_id=program_id, binding_source=source)
    run.verdict.result = result
    run.duration.FromMilliseconds(5)
    run.read_trace.extend(ReadStep(op=op, target=target, outcome=outcome) for op, target, outcome in reads)
    return run


def _request(*runs: ProgramRun, readmes: dict[str, str] | None = None) -> IncidentRequest:
    request = incident(1)
    request.detected_at.CopyFrom(timestamp_pb2.Timestamp(seconds=1_700_000_000))
    request.initial_program_runs.extend(runs)
    for program_id, markdown in (readmes or {}).items():
        request.memory_snapshot.playbooks.append(MemoryPlaybook(id=program_id, markdown=markdown))
    return request


def test_a_no_match_playbook_is_listed_with_its_origin_last_read_and_source_path() -> None:
    request = _request(
        _run("narrow-check", reads=(("logs", "app/web/web", "10 bytes"), ("log_matches", "app/web/web", "0 lines"))),
        readmes={"narrow-check": README},
    )

    block = unmatched_playbooks_block(request)

    assert "did not match at opening; rerun `sdo diagnose` for the current answer" in block
    assert "narrow-check: NoMatch" in block
    assert "inc-7" in block
    assert "widen a varchar column" in block
    assert "last read: log_matches app/web/web: 0 lines" in block
    assert ".sdo/playbooks/narrow-check/" in block


def test_an_unknown_playbook_shows_its_error_read() -> None:
    request = _request(
        _run("slow", result=Verdict.RESULT_UNKNOWN, reads=(("observe", "app/rails/rails", "error: timeout"),))
    )

    block = unmatched_playbooks_block(request)

    assert "slow: Unknown" in block
    assert "observe app/rails/rails: error: timeout" in block


def test_a_playbook_without_reads_says_so_and_without_a_readme_omits_the_origin() -> None:
    block = unmatched_playbooks_block(_request(_run("bare")))

    assert "bare: NoMatch" in block
    assert "no external reads" in block
    assert "learned from" not in block


def test_nothing_is_listed_when_there_are_no_unmatched_playbooks() -> None:
    assert unmatched_playbooks_block(_request()) == ""
    matched = _run("good", result=Verdict.RESULT_MATCH)
    assert unmatched_playbooks_block(_request(matched)) == ""


def test_a_playbook_that_matched_for_any_binding_is_not_listed() -> None:
    request = _request(_run("mixed"), _run("mixed", result=Verdict.RESULT_MATCH))

    assert unmatched_playbooks_block(request) == ""


def test_only_the_closest_few_are_shown_and_the_rest_are_counted() -> None:
    far = [_run(f"far-{i}", source="sweep") for i in range(6)]
    near = _run("near", source="credited", reads=(("log_matches", "a/b/c", "0 lines"),))
    block = unmatched_playbooks_block(_request(*far, near))

    assert block.count("\n- ") == 3
    assert "- near: NoMatch" in block
    assert "+4 more" in block
    assert "sdo diagnose" in block


def test_the_block_is_bounded_whatever_the_playbooks_say() -> None:
    long = "x" * 5000
    runs = [_run(f"p{i}", reads=(("observe", long, long),), source="credited") for i in range(40)]
    readmes = {f"p{i}": README.replace("widen a varchar column the app outgrew", long) for i in range(40)}

    assert len(unmatched_playbooks_block(_request(*runs, readmes=readmes))) <= 1200


def test_the_first_message_carries_the_block_and_nothing_when_there_are_none() -> None:
    with_run = _request(_run("narrow-check", reads=(("log_matches", "a/b/c", "0 lines"),)))
    assert "narrow-check: NoMatch" in str(_responder_prompt(with_run))
    assert "did not match" not in str(_responder_prompt(_request()))


def test_the_block_fits_the_first_message_beside_a_worst_case_symptom_without_spilling() -> None:
    request = _request(
        *[_run(f"p{i}", reads=(("observe", "t" * 500, "o" * 500),), source="credited") for i in range(10)],
        readmes={f"p{i}": README for i in range(10)},
    )
    request.ClearField("findings")
    for i in range(60):
        finding = request.findings.add(
            detector_id="traffic-probe", rule_id=f"scenario-slo.{'n' * 80}{i}", fingerprint=f"f{i}", evidence="e"
        )
        finding.summary = "s"
        json_format.ParseDict(
            {"failure_status": str(500 + i % 9), "failure_observed": "e" * 500, "errors": 100, "samples": 100},
            finding.metadata,
        )
    request.opening_keys[:] = [f"{f.detector_id}\x00{f.fingerprint}" for f in request.findings]
    request.passing_tests.extend("p" * 80 + str(i) for i in range(80))

    prompt = str(_responder_prompt(request))

    assert unmatched_playbooks_block(request) in prompt
    assert len(symptom(request)) > 4000
    assert symptom(request) in prompt


def test_the_footer_and_every_source_path_survive_the_bound() -> None:
    long = "x" * 5000
    runs = [_run(f"p{i}", reads=(("observe", long, long),), source="credited") for i in range(6)]
    readmes = {f"p{i}": README.replace("widen a varchar column the app outgrew", long) for i in range(6)}

    block = unmatched_playbooks_block(_request(*runs, readmes=readmes))

    assert len(block) <= 1200
    assert block.count("; source: .sdo/playbooks/p") == 3
    assert block.endswith("sdo diagnose NAME re-runs one)")


def test_front_matter_variants_still_give_the_origin() -> None:
    crlf = README.replace("\n", "\r\n")
    assert "inc-7" in unmatched_playbooks_block(_request(_run("a"), readmes={"a": crlf}))
    only_class = "---\nfault_class: narrow-column\noriginating_incident: inc-7\n---\n"
    assert "narrow-column" in unmatched_playbooks_block(_request(_run("a"), readmes={"a": only_class}))
    folded = "---\ndescription: >\n  folded text\nfault_class: narrow-column\n---\n"
    block = unmatched_playbooks_block(_request(_run("a"), readmes={"a": folded}))
    assert "narrow-column" in block
    assert "; >;" not in block
    nested = "---\nmeta:\n  description: nested\n---\n"
    assert "nested" not in unmatched_playbooks_block(_request(_run("a"), readmes={"a": nested}))


def test_a_long_playbook_id_is_never_clipped() -> None:
    name = "a" + "b" * 90
    block = unmatched_playbooks_block(_request(_run(name)))

    assert f"- {name}: NoMatch" in block
    assert f".sdo/playbooks/{name}/" in block


def test_ranking_prefers_the_credited_binding_then_the_longer_trace() -> None:
    swept = _run("swept", source="sweep", reads=(("read", "t", "0 rows"), ("read", "t", "0 rows")))
    short = _run("short", source="finding")
    long = _run("long", source="finding", reads=(("read", "t", "0 rows"),))
    credited = _run("credited", source="credited")
    block = unmatched_playbooks_block(_request(swept, short, long, credited))

    order = [line.split(":")[0].removeprefix("- ") for line in block.splitlines() if line.startswith("- ")]
    assert order == ["credited", "long", "short"]
