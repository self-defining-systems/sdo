"""The plain-agent arm's re-page rule, on a fake clock and a fake FuzzOps traffic stream."""

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from benchmarks.fuzzops import repage, row_metrics, vanilla

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
#: FuzzOps settles a second's requests this long after it.
LAG = 4
SINGLE = "synthetic@deploy/a"
COMPOSITE = "random-faults/app-p1/s1/k3[synthetic@deploy/a&synthetic@deploy/b&synthetic@deploy/c]"


def stamp(second: float) -> str:
    return (T0 + timedelta(seconds=second)).isoformat().replace("+00:00", "Z")


class Window:
    """FuzzOps' window: one traffic sample a second until ``end``; users are hurt in the seconds of ``bad``."""

    def __init__(self, tmp_path: Path, bad: list[range], end: int = 400, old_pin: bool = False):
        self.bad = sorted(second for span in bad for second in span)
        self.end, self.old_pin = end, old_pin
        self.now, self.published = 0.0, 0
        self.traffic = tmp_path / "traffic.jsonl"
        self.traffic.touch()
        self.sessions: list[tuple[float, float]] = []
        self.durations: list[int] = []

    def clock(self) -> float:
        return T0.timestamp() + self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        with self.traffic.open("a") as stream:
            while self.published + 1 <= min(self.now, self.end):
                self.published += 1
                through = self.published - LAG
                seen = [second for second in self.bad if second < through]
                sample = {
                    "inject_at": stamp(0),
                    "observed_at": stamp(self.published),
                    "through": stamp(through),
                    "requests": self.published,
                    "first_bad": {"Start": stamp(seen[0])} if seen else None,
                }
                if not self.old_pin:
                    sample["last_bad"] = {"Start": stamp(seen[-1])} if seen else None
                # A pin without last_bad stops at the first bad bucket.
                if not (self.old_pin and len(seen) > 1):
                    stream.write(json.dumps(sample) + "\n")

    def session(self, env, target: Path, cutoff: float) -> dict:
        started = self.now
        self.sessions.append((started, cutoff - T0.timestamp()))
        self.sleep(self.durations[len(self.sessions) - 1] if self.durations else 40)
        number = len(self.sessions)
        return {
            "started_at": stamp(started),
            "ended_at": stamp(self.now),
            "seconds": self.now - started,
            "session_id": f"s{number}",
            "capped": False,
            "exit": 0,
            "result_event": True,
            "is_error": False,
            "usage": {"input_tokens": 10 * number, "output_tokens": number, "cache_creation": {"ephemeral": 1}},
            "total_cost_usd": 0.5,
            "summary": f"cause {number}",
        }


def run(
    tmp_path: Path, window: Window, fault: str = COMPOSITE, extra: dict[str, str] | None = None
) -> tuple[list[dict], dict, dict]:
    target = tmp_path / "vanilla" / "run"  # where lanes' collect looks for this run
    target.mkdir(parents=True)
    env = {
        "BENCH_LANE_DIR": str(tmp_path),
        "BENCH_FAULT": fault,
        "FUZZOPS_TRAFFIC": str(window.traffic),
        "FUZZOPS_DEADLINE": stamp(1000),
        "FUZZOPS_REPORT": str(tmp_path / "report.json"),
        **(extra or {}),
    }
    window.sleep(10)  # the first onset has settled: the watch hook launched the supervisor
    vanilla.respond(env, target, T0.timestamp(), session=window.session, clock=window.clock, sleep=window.sleep)
    pages = [json.loads(line) for line in (target / vanilla.PAGES).read_text().splitlines()]
    done, usage = (json.loads((target / name).read_text()) for name in ("done.json", "usage.json"))
    assert not (tmp_path / "vanilla" / vanilla.STANDBY).exists()
    return pages, done, usage


def test_users_harmed_after_a_quiet_period_page_a_new_session(tmp_path):
    window = Window(tmp_path, [range(5, 30), range(100, 130)])
    pages, done, usage = run(tmp_path, window)
    assert [(p["page"], p["session_id"], p["onset"][:19], p["users_recovered"]) for p in pages] == [
        (1, "s1", "2026-01-01T00:00:05", True),
        (2, "s2", "2026-01-01T00:01:40", True),
    ]
    # Paged as soon as the new bad second settled, with its own 25 minutes; the first keeps the inject's.
    (_, first_cutoff), (second_start, second_cutoff) = window.sessions
    assert first_cutoff == vanilla.AGENT_SECONDS
    assert 100 < second_start <= 100 + LAG + 2
    assert second_cutoff == second_start + vanilla.AGENT_SECONDS
    # Evidence and cost cover every session.
    assert usage == {"input_tokens": 30, "output_tokens": 3, "cache_creation": {"ephemeral": 2}, "total_cost_usd": 1.0}
    assert (done["pages"], done["agent_seconds"], done["result_event"], done["exit"]) == (2, 80, True, 0)
    assert (done["started_at"], done["repage"]["stopped"]) == (stamp(10), repage.WINDOW_OVER)
    assert json.loads((tmp_path / "report.json").read_text()) == {"summary": "cause 1 | cause 2"}


def test_users_who_never_recovered_page_nobody_again(tmp_path):
    pages, done, _ = run(tmp_path, Window(tmp_path, [range(5, 200), range(205, 400)]))
    assert [p["users_recovered"] for p in pages] == [False]
    assert done["repage"]["stopped"] == repage.WINDOW_OVER


@pytest.mark.parametrize(
    ("bad", "durations", "paged"),
    [
        # Bad again within the quiet period: the same episode, as SDO's incident that was never Recovered.
        ([range(5, 60), range(70, 90)], [40, 40], 1),
        # Exactly the quiet period is enough.
        ([range(5, 60), range(60 + int(repage.QUIET_SECONDS), 90)], [40, 40], 2),
        # A later episode that began and ended while the agent worked pages nobody.
        ([range(5, 30), range(60, 70)], [100], 1),
        # One that began while the agent worked and still hurts when it returns pages at once.
        ([range(5, 30), range(60, 120)], [90, 40], 2),
        # Harm after the window ended is never seen.
        ([range(5, 30), range(410, 420)], [40], 1),
    ],
)
def test_one_page_per_new_episode(tmp_path, bad, durations, paged):
    window = Window(tmp_path, bad)
    window.durations = durations
    pages, _, _ = run(tmp_path, window)
    assert len(pages) == paged


def test_a_window_that_ended_during_the_session_pages_nobody(tmp_path):
    window = Window(tmp_path, [range(5, 30), range(80, 200)], end=100)
    window.durations = [300]
    pages, done, _ = run(tmp_path, window)
    assert (len(pages), done["repage"]["stopped"]) == (1, repage.WINDOW_OVER)


def test_pages_are_bounded_by_the_problems_faults(tmp_path):
    pages, done, _ = run(tmp_path, Window(tmp_path, [range(5, 30), range(80, 100), range(200, 220)]), SINGLE)
    assert (len(pages), done["repage"]["stopped"], done["repage"]["page_limit"]) == (2, repage.PAGE_LIMIT, 2)


@pytest.mark.parametrize(
    ("fault", "limit"),
    [
        (SINGLE, 2),
        (COMPOSITE, 4),
        ("name[a@x&decoy-note@y&b@z]", 3),
        ("", 2),
    ],
)
def test_page_limit(fault, limit):
    assert repage.page_limit(fault) == limit


@pytest.mark.parametrize(("fault", "flagged"), [(SINGLE, False), (COMPOSITE, True)])
def test_a_pin_that_cannot_show_a_new_onset_is_recorded(tmp_path, monkeypatch, fault, flagged):
    pages, done, _ = run(tmp_path, Window(tmp_path, [range(5, 30), range(100, 130)], old_pin=True), fault)
    assert (len(pages), done["repage"]["stopped"], pages[0]["users_recovered"]) == (1, repage.UNSUPPORTED, None)
    monkeypatch.setattr(vanilla, "cell_problems", lambda dest, arm: [])
    assert bool(vanilla.problems_of(tmp_path, done)) is flagged


def test_a_session_without_a_result_leaves_totals_unknown(tmp_path):
    window = Window(tmp_path, [range(5, 30), range(100, 130)])
    whole = window.session

    def second_capped(env, target, cutoff):
        record = whole(env, target, cutoff)
        if record["session_id"] == "s2":
            record |= {"capped": True, "exit": None, "result_event": False, "usage": {}, "total_cost_usd": None}
        return record

    window.session = second_capped
    pages, done, usage = run(tmp_path, window)
    assert usage == {"total_cost_usd": None}
    assert pages[0]["total_cost_usd"] == 0.5
    assert (done["capped"], done["exit"], done["result_event"]) == (True, None, False)


def test_a_watch_is_idle_to_lanes_and_fuzzops_until_collect_ends_it(tmp_path, capsys):
    window = Window(tmp_path, [range(5, 30)])
    standby = tmp_path / "fuzzops-standby"
    env = {"BENCH_LANE_DIR": str(tmp_path), "BENCH_RUN": "run"}
    sleep, session = window.sleep, window.session
    watched = []

    def watching(seconds):
        sleep(seconds)
        if window.now > 60:
            assert standby.exists()
            assert vanilla.main(["idle"], env) == 0
            watched.append(capsys.readouterr().out)
            vanilla.release_standby(env)

    def working(session_env, target, cutoff):
        assert not standby.exists()
        assert not (tmp_path / "vanilla" / vanilla.STANDBY).exists()
        # The session's own time passes without the watch's checks.
        window.sleep = sleep
        record = session(session_env, target, cutoff)
        window.sleep = watching
        return record

    window.session, window.sleep = working, watching
    _, done, _ = run(tmp_path, window, extra={"FUZZOPS_STANDBY": str(standby)})
    assert watched == ["idle\n"]
    assert done["repage"]["stopped"] == repage.COLLECTED
    assert not standby.exists()


def test_row_metrics_count_only_the_agents_own_time(tmp_path):
    op = tmp_path / "operator"
    op.mkdir()
    (op / "done.json").write_text(
        json.dumps({"started_at": stamp(10), "ended_at": stamp(300), "pages": 2, "agent_seconds": 80})
    )
    (op / "usage.json").write_text('{"input_tokens": 30, "output_tokens": 3, "total_cost_usd": 1.0}')
    row = row_metrics.measure(tmp_path)
    assert (row.arm, row.agent_wall_s, row.responder_cost_usd) == ("vanilla", 80, 1.0)


@pytest.mark.parametrize(
    "text", ["2026-01-01T00:00:05Z", "2026-01-01T00:00:05.1234567Z", "2026-01-01T00:00:05.5+00:00"]
)
def test_go_timestamps_parse(text):
    assert repage.parse_time(text).replace(microsecond=0) == T0 + timedelta(seconds=5)


def test_the_quiet_period_is_sdos_stability_window():
    root = Path(__file__).resolve().parents[4]
    config = (root / "sdo/operator/contracts/settings/config.go").read_text()
    found = re.search(r"StabilityWindow: Duration\((\d+) \* time\.Second\)", config)
    assert found
    assert float(found[1]) == repage.QUIET_SECONDS
    assert "stability_window" not in (root / "benchmarks/fuzzops/sdo-bench.yaml").read_text()
