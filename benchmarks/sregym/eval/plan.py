"""The evaluation plan: which cells run, in what recorded order, and what each one waits for.

A cell is (app, problem, arm, pass). Every cell is independent given a memory snapshot, so the plan is a small
dependency graph: pass 1 and vanilla cells wait for nothing; a pass-2 cell waits for its app's merge; a pass-2
own-delta cell waits only for the same problem's pass-1 cell.
"""

from __future__ import annotations

import json
import random
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from benchmarks.sregym.eval.health import DEFAULT_HOLD_SECONDS, MAX_INTERVAL_SECONDS, MIN_INTERVAL_SECONDS

PlanPass = Literal["pass1", "pass2", "pass2-own", "vanilla"]
Arm = Literal["sdo", "vanilla"]
ALL_PASSES: tuple[PlanPass, ...] = ("vanilla", "pass1", "pass2", "pass2-own")
PLAN_SCHEMA = "sdo.sregym-eval-plan/v1"
BASE = "base"
MERGED = "merged"
OWN_PREFIX = "own:"


def own_snapshot(problem: str) -> str:
    return f"{OWN_PREFIX}{problem}"


class AppConfig(BaseModel):
    """One application: its problems and the lanes that serve it (assignment is configuration, not code)."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    problems: tuple[str, ...] = Field(min_length=1)
    #: Lanes that run SDO cells for this app. Any lane runs any cell: the memory snapshot is installed at claim.
    sdo_lanes: tuple[str, ...] = ()
    #: Lanes for vanilla cells: independent, so a separate small warm pool.
    vanilla_lanes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def unique(self) -> Self:
        if len(set(self.problems)) != len(self.problems):
            raise ValueError(f"app {self.name}: repeated problem")
        if set(self.sdo_lanes) & set(self.vanilla_lanes):
            raise ValueError(f"app {self.name}: a lane cannot serve both SDO and vanilla cells")
        if len(set(self.sdo_lanes)) != len(self.sdo_lanes) or len(set(self.vanilla_lanes)) != len(self.vanilla_lanes):
            raise ValueError(f"app {self.name}: repeated lane")
        return self


class EvalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    stage: str = Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    runs_root: Path
    apps: tuple[AppConfig, ...] = Field(min_length=1)
    #: Fixes the recorded random order of each app's problems.
    seed: int = 0
    passes: tuple[PlanPass, ...] = ALL_PASSES
    hold_seconds: float = Field(default=DEFAULT_HOLD_SECONDS, gt=0)
    probe_interval_seconds: float = Field(default=3.0, ge=MIN_INTERVAL_SECONDS, le=MAX_INTERVAL_SECONDS)
    #: The fallback page would go this long after injection; the hook is not built, so it is recorded only.
    fallback_page_seconds: float = Field(default=120.0, gt=0)
    cell_cap_seconds: float = Field(default=1800.0, gt=0)
    #: After the agent ends, keep probing at most this long for the hold to be satisfied.
    settle_cap_seconds: float = Field(default=300.0, gt=0)
    after_playbook: Literal["agent", "escalate", "wait"] = "agent"
    #: Start a cell only while the 1-minute load average is below this, never by a fixed concurrency.
    max_load: float = Field(default=40.0, gt=0)
    #: Minimum gap between cell starts, because the load average lags a burst of starts.
    admit_spacing_seconds: float = Field(default=5.0, ge=0)

    @model_validator(mode="after")
    def lanes_for_passes(self) -> Self:
        names = [app.name for app in self.apps]
        if len(set(names)) != len(names):
            raise ValueError("repeated app")
        for app in self.apps:
            if any(p in self.passes for p in ("pass1", "pass2", "pass2-own")) and not app.sdo_lanes:
                raise ValueError(f"app {app.name}: SDO passes selected but no sdo_lanes")
            if "vanilla" in self.passes and not app.vanilla_lanes:
                raise ValueError(f"app {app.name}: vanilla pass selected but no vanilla_lanes")
        if ("pass2" in self.passes or "pass2-own" in self.passes) and "pass1" not in self.passes:
            raise ValueError("pass 2 needs pass 1 in the plan")
        return self


class Cell(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    app: str
    problem: str
    arm: Arm
    pass_: PlanPass
    #: Memory snapshot the cell runs on (``base``, ``merged``, ``own:<problem>``); ``None`` for vanilla.
    snapshot: str | None
    #: Position in the app's recorded random order.
    order: int = Field(ge=0)

    @property
    def id(self) -> str:
        return f"{self.app}/{self.pass_}/{self.problem}"

    @property
    def pool(self) -> tuple[str, Arm]:
        return (self.app, self.arm)

    @property
    def needs(self) -> tuple[str, ...]:
        if self.pass_ == "pass2":
            return (merge_id(self.app),)
        if self.pass_ == "pass2-own":
            return (f"{self.app}/pass1/{self.problem}",)
        return ()

    def directory(self, config: EvalConfig) -> Path:
        return config.runs_root / config.stage / self.app / self.pass_ / self.problem


def merge_id(app: str) -> str:
    return f"{app}/merge"


class Plan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_: str = Field(default=PLAN_SCHEMA, alias="schema")
    config: EvalConfig
    #: Each app's problems in the recorded random order.
    order: dict[str, tuple[str, ...]]
    cells: tuple[Cell, ...]

    def merge_steps(self) -> tuple[str, ...]:
        if "pass2" not in self.config.passes:
            return ()
        return tuple(app.name for app in self.config.apps)

    def pass1_problems(self, app: str) -> tuple[str, ...]:
        return tuple(c.problem for c in self.cells if c.app == app and c.pass_ == "pass1")

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.model_dump(mode="json", by_alias=True), indent=2) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> Plan:
        return cls.model_validate_json(path.read_text(encoding="utf-8"))


def build_plan(config: EvalConfig) -> Plan:
    order: dict[str, tuple[str, ...]] = {}
    cells: list[Cell] = []
    for app in config.apps:
        shuffled = list(app.problems)
        random.Random(f"{config.seed}:{app.name}").shuffle(shuffled)
        order[app.name] = tuple(shuffled)
        for pass_ in ALL_PASSES:
            if pass_ not in config.passes:
                continue
            for index, problem in enumerate(shuffled):
                snapshot = {
                    "vanilla": None,
                    "pass1": BASE,
                    "pass2": MERGED,
                    "pass2-own": own_snapshot(problem),
                }[pass_]
                cells.append(
                    Cell(
                        app=app.name,
                        problem=problem,
                        arm="vanilla" if pass_ == "vanilla" else "sdo",
                        pass_=pass_,
                        snapshot=snapshot,
                        order=index,
                    )
                )
    return Plan(config=config, order=order, cells=tuple(cells))
