"""The harness-paged arm: one external incident at the configured offset, with B1's page content."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import yaml

from benchmarks.fuzzops.page import MAX_MESSAGE, PageConfig, Pager, inbox_manifest, page_message, parse_trace

if TYPE_CHECKING:
    from pathlib import Path

JOURNEYS = {
    "login": {"header": {"PRIVATE-TOKEN": "t0ken"}},
    "journeys": [
        {"name": "api-user", "request": {"method": "GET", "path": "/api/v4/user"}},
        {"name": "issue-create", "request": {"method": "POST", "path": "/api/v4/projects/{{id}}/issues"}},
        {"name": "sign-in-page", "request": {"method": "GET", "path": "/users/sign_in"}},
    ],
}
STDOUT = "traffic: requests=60 errors=7\n"
STDERR = "(failed) issue-create 500 internal server error\n(failed) issue-create 500 internal server error\n"


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.events: list[tuple[str, float]] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _pager(tmp_path: Path, clock: Clock, config: PageConfig, submitted: list[dict]) -> Pager:
    pack = tmp_path / "pack"
    pack.mkdir()
    (pack / "journeys.yaml").write_text(yaml.safe_dump(JOURNEYS))
    out = tmp_path / "out"
    out.mkdir()

    def live_check() -> tuple[str, str]:
        clock.events.append(("check", clock.now))
        return STDOUT, STDERR

    def submit(manifest: dict) -> None:
        clock.events.append(("page", clock.now))
        submitted.append(manifest)

    return Pager(config, "app", "app-sdo", pack, "page-1", out, live_check, submit, clock=clock.time, sleep=clock.sleep)


def test_the_arm_submits_one_external_incident_at_the_configured_offset_with_the_live_check(tmp_path: Path) -> None:
    clock, submitted = Clock(), []
    pager = _pager(tmp_path, clock, PageConfig(after=30, check_after=20), submitted)
    message = pager.run(1000.0)
    assert clock.events == [("check", 1020.0), ("page", 1030.0)]
    assert len(submitted) == 1
    manifest = submitted[0]
    assert manifest["metadata"] == {"name": "sdo-external-incident", "namespace": "app-sdo"}
    alert = json.loads(manifest["data"]["incident.json"])
    assert alert == {"id": "page-1", "namespace": "app", "message": message}
    # B1's page content: the failing operation with its sample and counts, and what still works.
    assert "issue-create: 500 internal server error (2 failed of 60 requests in the last 10 s)" in message
    assert "These still work: api-user, sign-in-page." in message
    assert "curl -s -w ' [%{http_code}]' -X POST -H 'PRIVATE-TOKEN: t0ken'" in message
    record = json.loads((tmp_path / "out" / "page.json").read_text())
    assert record["offset_s"] == 30.0
    assert record["message"] == message


def test_a_clean_check_still_pages_with_the_users_report(tmp_path: Path) -> None:
    assert parse_trace("traffic: requests=5\n", "", ["a"]) == {}
    clock, submitted = Clock(), []
    pager = _pager(tmp_path, clock, PageConfig(), submitted)
    checks = []
    pager.live_check = lambda: checks.append(clock.now) or ("traffic: requests=5\n", "")
    message = pager.run(1000.0)
    # A clean check is repeated before paging, and the page never claims an operation works or that none fails.
    assert len(checks) == 1 + PageConfig().rechecks
    assert "users still report failures" in message
    assert "not reproduced: 3 short live checks" in message
    assert "These still work" not in message
    assert "no failing operation" not in message


def test_a_recheck_that_reproduces_the_failure_names_it(tmp_path: Path) -> None:
    clock, submitted = Clock(), []
    pager = _pager(tmp_path, clock, PageConfig(), submitted)
    results = iter([("traffic: requests=5\n", ""), (STDOUT, STDERR)])
    pager.live_check = lambda: next(results)
    message = pager.run(1000.0)
    assert "issue-create: 500 internal server error" in message
    assert "These still work: api-user, sign-in-page." in message


def test_page_config_defaults_to_b1_schedule_and_validates() -> None:
    assert PageConfig.from_settings(None) is None
    assert PageConfig.from_settings(False) is None
    assert PageConfig.from_settings(True) == PageConfig(after=30, check_after=20, check_duration=10)
    config = PageConfig.from_settings({"after": 45})
    assert config is not None
    assert config.after == 45
    with pytest.raises(ValueError, match="unknown"):
        PageConfig.from_settings({"when": 1})
    with pytest.raises(ValueError, match="negative"):
        PageConfig(after=-1)


def test_inbox_manifest_is_the_external_incident_inbox() -> None:
    manifest = inbox_manifest("c", "i", "n", "m")
    assert manifest["kind"] == "ConfigMap"
    assert manifest["metadata"]["name"] == "sdo-external-incident"


def test_a_page_with_many_failing_operations_stays_within_the_external_incident_contract() -> None:
    """The controller's inbox refuses an alert over 4096 characters, so the page fits: every failing operation
    keeps its bullet and the passing list stays; only how-to-check lines past the bound are counted."""
    names = [f"operation-{i:02d}-with-a-long-name" for i in range(30)]
    journeys = {
        "login": {"header": {"PRIVATE-TOKEN": "t0ken"}},
        "journeys": [
            {"name": n, "request": {"method": "POST", "path": f"/api/v4/projects/{{{{id}}}}/{n}/long/path"}}
            for n in names
        ],
    }
    failing = {n: {"failed": 3, "total": 47, "sample": '403 {"message":"403 Forbidden"}'} for n in names[:24]}
    message = page_message("app", "2026-01-01T00:00:00Z", failing, names, journeys)
    assert len(message) <= MAX_MESSAGE == 4096
    assert all(f"  - {n}: 403" in message for n in failing)
    assert "These still work: " + ", ".join(names[24:]) + "." in message
    assert "more operations: the same form" in message
    assert message.endswith("(run from a pod in the namespace, e.g. kubectl exec deploy/gitlab-toolbox -- ...)")
