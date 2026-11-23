"""The learning-plan generator: p1 seeds p2 through a memory edge, only p1 (and the warm-up) learns, pools are
claimable by the rows, and deadlines follow a measured run."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.lanes import learning_plan

if TYPE_CHECKING:
    from pathlib import Path

APP = next(p.name for p in sorted(learning_plan.APPS_DIR.iterdir()) if (p / "app.json").is_file())
REAL = ("--tag", "synthetic-tag", "--auth-file", "/synthetic/adc.json", "--vertex-project", "synthetic-project")
POOL_FIELDS = {"name", "app", "namespace", "hold", "lanes", "sequences", "fuzzops", "faults", "operator"}


def args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "--name",
        "synthetic-plan",
        "--pin",
        "0" * 40,
        "--host",
        "test-host",
        "--inputs",
        str(tmp_path / "inputs"),
        "--out",
        str(tmp_path / "out"),
        "--app",
        f"a={APP}:{tmp_path / 'baseline-a'}",
        "--problem",
        "a=problem_one",
        "--problem",
        "a=problem_two",
        *extra,
    ]


def load(tmp_path: Path, name: str) -> dict:
    return json.loads((tmp_path / "out" / name).read_text())


def test_real_plan_seeds_p2_from_p1_and_only_p1_learns(tmp_path: Path) -> None:
    learning_plan.main(args(tmp_path, *REAL))

    plan = load(tmp_path, "plan.yaml")
    assert [arm["name"] for arm in plan["arms"]] == ["p1", "cold", "p2"]
    assert plan["edges"] == [{"from": "p1", "to": "p2", "artifact": "memory"}]
    assert [item["id"] for item in plan["items"]] == ["a-1", "a-2"]
    assert plan["groups"]["a"]["spec"]["operator"]["baseline"] == str(tmp_path / "baseline-a")
    p1, cold, p2 = (load(tmp_path, f"specs/{arm}.json") for arm in ("p1", "cold", "p2"))
    assert "learning" not in p1["operator"]["settings"]["config"]
    for spec in (cold, p2):
        assert spec["operator"]["settings"]["config"]["learning"] == {"closing_turn": False}
    assert p2["operator"]["seed"] == "${artifact.memory}"
    assert "seed" not in p1["operator"]
    assert "seed" not in cold["operator"]
    assert p1["operator"]["settings"]["controller_image"] == "sdo-controller:synthetic-tag"
    assert "ANTHROPIC_VERTEX_PROJECT_ID=synthetic-project" in p1["operator"]["settings"]["config"]["agent"]["extra_env"]


def test_relative_inputs_are_written_absolute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    argv = args(tmp_path, *REAL)
    argv[argv.index("--inputs") + 1] = "inputs"
    argv[argv.index("--app") + 1] = f"a={APP}:baseline-a"

    learning_plan.main(argv)

    assert load(tmp_path, "specs/p1.json")["faults"]["instances"] == str(tmp_path / "inputs" / "instances.json")
    assert load(tmp_path, "plan.yaml")["groups"]["a"]["spec"]["operator"]["baseline"] == str(tmp_path / "baseline-a")


def test_warmup_arm_and_pool_are_the_p1_spec_so_rows_can_claim_the_pool(tmp_path: Path) -> None:
    learning_plan.main(args(tmp_path, *REAL, "--warmup", "--pools"))

    plan = load(tmp_path, "plan.yaml")
    assert [arm["name"] for arm in plan["arms"]] == ["w", "p1", "cold", "p2"]
    assert {"from": "w", "to": "p1"} in plan["edges"]
    assert {"from": "w", "to": "cold"} in plan["edges"]
    p1 = load(tmp_path, "specs/p1.json")
    assert load(tmp_path, "specs/w.json") == p1
    pool = load(tmp_path, "pool-a.json")
    assert {k: v for k, v in pool.items() if k not in POOL_FIELDS} == {
        k: v for k, v in p1.items() if k not in POOL_FIELDS
    }
    assert {k: v for k, v in pool["operator"].items() if k not in ("baseline", "revision")} == p1["operator"]
    assert pool["name"] == "synthetic-plan-pool-a"
    assert pool["hold"] is True
    assert pool["lanes"] == 4
    assert plan["groups"]["a"]["spec"]["claim"] == "synthetic-plan-pool-a"
    assert plan["groups"]["a"]["spec"]["claim_only"] is False


def test_stub_plan_has_one_scripted_arm_and_no_edges(tmp_path: Path) -> None:
    learning_plan.main(args(tmp_path, "--stub"))

    plan = load(tmp_path, "plan.yaml")
    assert [arm["name"] for arm in plan["arms"]] == ["stub"]
    assert "edges" not in plan
    assert "provider" not in load(tmp_path, "specs/stub.json")["operator"]["settings"]


def test_deadlines_follow_a_measured_run(tmp_path: Path) -> None:
    report = tmp_path / "final-speed.json"
    report.write_text(json.dumps([{"wall": 600, "stages": {"gate": 240}}, {"wall": 720, "stages": {"gate": 300}}]))

    learning_plan.main(args(tmp_path, *REAL, "--timing-from", str(report)))

    spec = load(tmp_path, "specs/p1.json")
    assert (spec["ready_timeout"], spec["cap"]) == ("12m", "17m")


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ((), "needs --tag"),
        (("--stub", "--problem", "b=x"), "no --app: b"),
        (("--stub", "--warmup"), "--stub takes no"),
        (("--stub", "--tag", "t"), "--stub takes no"),
        ((*REAL, "--timing-from", "x.json", "--cap", "9m"), "--timing-from sets"),
    ],
)
def test_refuses_incomplete_or_contradictory_plans(tmp_path: Path, extra: tuple[str, ...], message: str) -> None:
    with pytest.raises(SystemExit, match=message):
        learning_plan.main(args(tmp_path, *extra))


def test_refuses_an_unknown_app() -> None:
    with pytest.raises(ValueError, match="unknown SREGym lanes app"):
        learning_plan.parse_app("a=not_an_app:/base")
