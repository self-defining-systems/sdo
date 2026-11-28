"""The scheduled page: one generic page at a fixed offset from injection, for a ``detection.pages_only`` arm."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from benchmarks.fuzzops import hooks, observe, page, row_metrics

START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
GENERIC = "Users are reporting problems with the application in namespace app; investigate and fix it."
PAGES_ONLY = {"detection": {"pages_only": True}}


def _at(seconds: float) -> str:
    return (START + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _operator_file(tmp: Path, **settings: Any) -> Path:
    path = tmp / "operator.json"
    path.write_text(
        json.dumps({"hooks": "sdo", "revision": "0123456789abcdef", "baseline": str(tmp), "settings": settings})
    )
    return path


@pytest.mark.parametrize("own_incident_at", [None, 8.0])
def test_watch_sends_the_generic_page_at_the_offset_whatever_sdo_saw(
    own_incident_at: float | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The watcher starts two seconds after injection; the offset falls between two regular polls. The page goes out
    # at the offset even when an incident is already open (recorded, never under pages_only on a valid row).
    t = 2.0
    sent: list[tuple[float, dict[str, Any]]] = []

    def sleep(seconds: float) -> None:
        nonlocal t
        t += seconds
        assert t < 60, "watcher never saw incident closure"

    def state(*_args: str) -> dict[str, Any]:
        opened = own_incident_at if own_incident_at is not None else 16.0 if sent else None
        if opened is None or t < opened:
            return {}
        request = {"incidentId": "app-incident"}
        request.update({"page": GENERIC} if own_incident_at is None else {"openingKeys": ["users/failing"]})
        if t >= 30:
            return {
                "pendingClosure": {"request": request, "detectedAt": _at(opened)},
                "lastAcknowledgedIncidentId": "app-incident",
            }
        return {"incidentOpen": True, "incidentDetectedAt": _at(opened), "incidentRequest": request}

    traffic = tmp_path / "traffic.jsonl"
    traffic.write_text(json.dumps({"inject_at": _at(0), "first_bad": None}) + "\n")
    monkeypatch.setenv("FUZZOPS_TRAFFIC", str(traffic))
    monkeypatch.setenv("FUZZOPS_OUT", str(tmp_path))
    monkeypatch.setattr(observe.time, "time", lambda: START.timestamp() + t)
    monkeypatch.setattr(observe.time, "monotonic", lambda: t)
    monkeypatch.setattr(observe.time, "sleep", sleep)
    monkeypatch.setattr(observe, "read_state", state)
    monkeypatch.setattr(observe, "kubectl_apply", lambda manifest, *_: sent.append((t, manifest)))
    argv = ["--control-namespace", "app-sdo", "--page-namespace", "app", "--linger", "0"]
    argv += ["--scheduled-page-after", "15.263", "--paired-cell", "q3fz-p1-gfeat-r1"]
    assert observe.main(argv) == 0
    assert "scheduled page sent" in capsys.readouterr().err
    [(at, manifest)] = sent
    assert at == pytest.approx(15.263)
    alert = json.loads(manifest["data"]["incident.json"])
    record = json.loads((tmp_path / "sdo-page.json").read_text())
    assert alert == {"id": record["fallback_id"], "namespace": "app", "message": GENERIC}
    assert record["page_kind"] == "scheduled"
    assert record["scheduled_offset_s"] == pytest.approx(15.263)
    assert record["paired_cell"] == "q3fz-p1-gfeat-r1"
    assert record["paged_offset_s"] == pytest.approx(15.263, abs=0.001)
    assert record["fallback_page_at"] == _at(15.263)
    if own_incident_at is None:
        assert record["sdo_incident_before_page"] is False
        assert record["page_source"] == "scheduled"
        assert record["sdo_page_at"] == _at(16)
    else:
        assert record["sdo_incident_before_page"] is True
        assert record["page_source"] == "own"


def test_a_scheduled_page_excludes_the_other_pages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    traffic = tmp_path / "traffic.jsonl"
    traffic.write_text("")
    monkeypatch.setenv("FUZZOPS_TRAFFIC", str(traffic))
    monkeypatch.setenv("FUZZOPS_OUT", str(tmp_path))
    base = ["--control-namespace", "app-sdo", "--page-namespace", "app", "--scheduled-page-after", "15"]
    with pytest.raises(ValueError, match="scheduled page excludes"):
        observe.main([*base, "--fallback-page-after", "120"])


def test_watch_hook_passes_the_scheduled_page_instead_of_the_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr(hooks.observe, "main", lambda argv: seen.append(argv) or 0)
    env = {"FUZZOPS_NAMESPACE": "app", "BENCH_FAULT": "f@deploy/web", "BENCH_LANE_DIR": str(tmp_path)}
    scheduled = {"offset_s": 15.263, "paired_cell": "q3fz-p1-gfeat-r1"}
    env["BENCH_OPERATOR"] = str(_operator_file(tmp_path, image_tag="fz-t", config=PAGES_ONLY, scheduled_page=scheduled))
    assert hooks.watch(env) == 0
    argv = seen[0]
    assert "--fallback-page-after" not in argv
    assert "--page" not in argv
    assert float(argv[argv.index("--scheduled-page-after") + 1]) == 15.263
    assert argv[argv.index("--paired-cell") + 1] == "q3fz-p1-gfeat-r1"
    assert argv[argv.index("--page-namespace") + 1] == "app"
    env["BENCH_FAULT"] = "none"
    hooks.watch(env)
    assert "--scheduled-page-after" not in seen[1]
    # Without the setting the hook is unchanged: the fallback page at 120 s.
    env["BENCH_FAULT"] = "f@deploy/web"
    env["BENCH_OPERATOR"] = str(_operator_file(tmp_path, image_tag="fz-t"))
    hooks.watch(env)
    assert "--scheduled-page-after" not in seen[2]
    assert float(seen[2][seen[2].index("--fallback-page-after") + 1]) == 120


@pytest.mark.parametrize(
    ("settings", "error"),
    [
        ({"scheduled_page": {"offset_s": 15, "paired_cell": "row"}}, "pages_only"),
        (
            {"config": {"detection": {"pages_only": False}}, "scheduled_page": {"offset_s": 15, "paired_cell": "row"}},
            "pages_only",
        ),
        (
            {"config": PAGES_ONLY, "scheduled_page": {"offset_s": 15, "paired_cell": "row"}, "harness_page": True},
            "two different arms",
        ),
        ({"config": PAGES_ONLY, "scheduled_page": {"offset_s": 0, "paired_cell": "row"}}, "positive"),
        ({"config": PAGES_ONLY, "scheduled_page": {"offset_s": 15, "paired_cell": " "}}, "paired row"),
        ({"config": PAGES_ONLY, "scheduled_page": {"offset_s": 15}}, "exactly"),
    ],
)
def test_operator_block_refuses_an_unpaired_or_racing_scheduled_page(
    settings: dict[str, Any], error: str, tmp_path: Path
) -> None:
    with pytest.raises(hooks.HookError, match=error):
        hooks.OperatorBlock.load(_operator_file(tmp_path, **settings))


def test_operator_block_without_the_setting_has_no_scheduled_page(tmp_path: Path) -> None:
    block = hooks.OperatorBlock.load(_operator_file(tmp_path))
    assert block.scheduled_page is None
    assert block.effective_config() == {}
    loaded = hooks.OperatorBlock.load(
        _operator_file(tmp_path, config=PAGES_ONLY, scheduled_page={"offset_s": 15.263, "paired_cell": "row"})
    )
    assert loaded.scheduled_page == page.ScheduledPage(15.263, "row")
    assert loaded.effective_config() == PAGES_ONLY


def test_row_metrics_time_a_scheduled_row_from_the_harness_page() -> None:
    row = row_metrics.Row(
        run="r",
        arm="sdo",
        fault="f",
        valid=True,
        invalid_reason=None,
        fixed_in_window=True,
        window_bad_user_s=1.0,
        inject_to_last_bad_s=1.0,
    )
    collect = {"page_source": "scheduled", "sdo_page_at": _at(16.1), "fallback_page_at": _at(15.3)}
    row_metrics._sdo(row, Path("/nonexistent"), {}, collect, START, [])
    assert row.page_source == "scheduled"
    assert row.inject_to_page_s == pytest.approx(15.3)
