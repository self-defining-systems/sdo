"""First-try and recurrence rounds of SDO under fuzzops, one lane at a time.

A lane is one SDO install (``benchmarks.fuzzops.install``) operating one app copy. Its memory persists for the
whole lane: for each fault template of the plan the lane runs

* ``first``: the template's first instance, which this SDO has never seen;
* ``repeat``: the same instance again, on the memory the first round left;
* ``variant``: another instance of the template (another component or other parameters), when the plan has one.

Each round is ``fuzzops run --skip-reset --operator exec:sdo-<round>``, so fuzzops grades every round on its own
against the noop runs of that instance. Before each run the lane waits until SDO is idle: no open incident, no
pending closure and no queued reflection, so a round never overlaps the previous incident and the repeat runs on the
memory reflection accepted. After each run it copies SDO's accepted outcome records and playbooks.

``python -m benchmarks.fuzzops.campaign plan``  picks the rounds from a fuzzops instances file;
``python -m benchmarks.fuzzops.campaign lane``  runs them on one installed lane.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.fuzzops.observe import STATE_CONFIGMAP, STATE_KEY

if TYPE_CHECKING:
    from collections.abc import Callable

ROUNDS = ("first", "repeat", "variant")
IDLE_TIMEOUT_S = 40 * 60.0
REFLECTION_WAIT_S = 15 * 60.0
MEMORY_PATHS = (".sdo/outcomes", ".sdo/playbooks")


class CampaignError(RuntimeError):
    """A lane cannot go on (SDO never became idle, the plan is malformed)."""


class SoftIdle(Exception):
    """SDO has no open incident or closure, but a reflection stayed queued past the lane's patience."""

    def __init__(self, why: str, waited: float) -> None:
        super().__init__(why)
        self.why, self.waited = why, waited


@dataclass(frozen=True)
class PlanItem:
    """One fault template: the instance run first and repeated, and another instance of it for the variant."""

    template: str
    first: str
    variant: str = ""

    def __post_init__(self) -> None:
        if not self.template or not self.first:
            raise ValueError("a plan item needs a template and a first instance")
        if self.variant == self.first:
            raise ValueError(f"{self.template}: the variant must be another instance than the first")

    def rounds(self) -> list[tuple[str, str]]:
        """(round, instance ID) in run order."""
        out = [("first", self.first), ("repeat", self.first)]
        if self.variant:
            out.append(("variant", self.variant))
        return out


@dataclass(frozen=True)
class Plan:
    app: str
    instances_file: str
    items: tuple[PlanItem, ...]

    def __post_init__(self) -> None:
        if not self.app:
            raise ValueError("a plan needs an app")
        names = [i.template for i in self.items]
        if len(set(names)) != len(names):
            raise ValueError("a plan lists each template once")

    @classmethod
    def load(cls, path: Path) -> Plan:
        raw = json.loads(path.read_text())
        return cls(raw["app"], raw["instances_file"], tuple(PlanItem(**i) for i in raw["items"]))

    def dump(self) -> str:
        return (
            json.dumps(
                {"app": self.app, "instances_file": self.instances_file, "items": [asdict(i) for i in self.items]},
                indent=2,
            )
            + "\n"
        )

    def item(self, template: str) -> PlanItem:
        for i in self.items:
            if i.template == template:
                return i
        raise KeyError(template)


def instance_ids(instances: Any) -> list[tuple[str, str]]:
    """(template, instance ID) for every instance in a fuzzops instances file, in file order."""
    rows = instances if isinstance(instances, list) else instances.get("instances", [])
    out = []
    for inst in rows:
        template = inst.get("template", "")
        target = (inst.get("target") or {}).get("id", "")
        iid = inst.get("id") or f"{template}@{target}"
        params = inst.get("params") or {}
        if not inst.get("id") and params:
            iid += "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))
        out.append((template, iid))
    return out


def choose(
    pairs: list[tuple[str, str]], templates: list[str] | None, visible: set[str] | None = None
) -> list[PlanItem]:
    """Per template (in ``templates`` order, else file order) the first instance and a second one as the variant.

    ``visible`` (instance IDs users could see in a screen) drops masked instances first.
    """
    by: dict[str, list[str]] = {}
    for template, iid in pairs:
        if visible is not None and iid not in visible:
            continue
        if iid not in by.setdefault(template, []):
            by[template].append(iid)
    order = templates or list(by)
    return [PlanItem(t, by[t][0], by[t][1] if len(by[t]) > 1 else "") for t in order if by.get(t)]


def slug(text: str, limit: int = 70) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")[:limit]


# ---- SDO state -----------------------------------------------------------------------------------------------


def idle_reason(state: dict[str, Any] | None) -> str:
    """Why SDO is not idle ("" when it is): an open incident, a closure in progress, or reflection still queued."""
    if state is None:
        return "no controller state yet"
    if state.get("incident_open") or state.get("incidentOpen"):
        return "incident open"
    if state.get("pending_closure") or state.get("pendingClosure"):
        return "closure pending"
    if state.get("queued_reflection_incident_ids") or state.get("queuedReflectionIncidentIds"):
        return "reflection queued"
    if state.get("queued_reflections_unknown") or state.get("queuedReflectionsUnknown"):
        return "reflection queue unknown"
    return ""


@dataclass
class Kube:
    """kubectl against one lane's cluster."""

    context: str
    kubeconfig: str = ""

    def cmd(self, *args: str) -> list[str]:
        out = ["kubectl"]
        if self.kubeconfig:
            out += ["--kubeconfig", self.kubeconfig]
        return [*out, "--context", self.context, *args]

    def state(self, control_namespace: str) -> dict[str, Any] | None:
        proc = subprocess.run(
            self.cmd("-n", control_namespace, "get", "configmap", STATE_CONFIGMAP, "-o", "json"),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if proc.returncode != 0:
            return None
        raw = (json.loads(proc.stdout).get("data") or {}).get(STATE_KEY)
        return json.loads(raw) if raw else None

    def controller_pod(self, control_namespace: str) -> str:
        proc = subprocess.run(
            self.cmd("-n", control_namespace, "get", "pods", "-o", "json"),
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
        names = [
            p["metadata"]["name"]
            for p in json.loads(proc.stdout)["items"]
            if "controller" in p["metadata"]["name"] and (p.get("status") or {}).get("phase") == "Running"
        ]
        if not names:
            raise CampaignError(f"no running controller pod in {control_namespace}")
        return sorted(names)[-1]

    def copy_memory(self, control_namespace: str, dest: Path) -> bool:
        """Copy the accepted outcome records and playbooks into ``dest`` (replacing an older copy)."""
        pod = self.controller_pod(control_namespace)
        tar = "cd /workspace/application && tar cf - " + " ".join(MEMORY_PATHS) + " 2>/dev/null; true"
        proc = subprocess.run(
            self.cmd("-n", control_namespace, "exec", pod, "-c", "controller", "--", "sh", "-c", tar),
            capture_output=True,
            timeout=300,
            check=False,
        )
        if proc.returncode != 0 or not proc.stdout:
            return False
        tmp = dest.with_name(dest.name + ".tmp")
        subprocess.run(["rm", "-rf", str(tmp)], check=True)
        tmp.mkdir(parents=True)
        subprocess.run(["tar", "xf", "-", "-C", str(tmp)], input=proc.stdout, check=True)
        subprocess.run(["rm", "-rf", str(dest)], check=True)
        tmp.rename(dest)
        return True


def wait_idle(
    read: Callable[[], dict[str, Any] | None],
    timeout: float = IDLE_TIMEOUT_S,
    poll: float = 10.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
    reflection_wait: float = REFLECTION_WAIT_S,
) -> float:
    """Wait until SDO is idle twice in a row (one poll apart); returns the seconds waited.

    A reflection still queued after ``reflection_wait`` (with no incident or closure open) no longer holds the next
    round: the round runs on whatever memory is active, and the lane records that it did (``soft_idle``).
    """
    start, last, calm = clock(), "", 0
    while True:
        try:
            why = idle_reason(read())
        except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as exc:
            why = f"state read failed: {exc}"
        if why.startswith("reflection") and clock() - start > reflection_wait:
            log(f"proceeding although {why} after {reflection_wait:.0f}s")
            raise SoftIdle(why, clock() - start)
        calm = calm + 1 if not why else 0
        if calm >= 2:
            return clock() - start
        if why != last:
            log(f"waiting for SDO: {why or 'idle, confirming'}")
            last = why
        if clock() - start > timeout:
            raise CampaignError(f"SDO not idle after {timeout:.0f}s: {why}")
        sleep(poll)


# ---- the lane ------------------------------------------------------------------------------------------------


@dataclass
class LaneConfig:
    plan: Plan
    templates: list[str]
    out: Path
    fuzzops: Path
    pack: Path
    namespace: str
    control_namespace: str
    kind_cluster: str
    sdo_version: str
    window: str = "600s"
    kubeconfig: str = ""
    sdo_root: Path = field(default_factory=lambda: Path(__file__).resolve().parents[2])
    operator_binary: str = ""
    controller_image: str = ""
    extra: list[str] = field(default_factory=list)
    unhealthy_retries: int = 2
    evidence_dir: str = ""
    #: Shell command that brings a broken app back to health between rounds (``$NAMESPACE``, ``$KUBE_CONTEXT`` set),
    #: run when the app is still unhealthy before an inject: SDO missed the previous fault or never repaired it.
    heal_cmd: str = ""
    reflection_wait: float = REFLECTION_WAIT_S

    def __post_init__(self) -> None:
        for t in self.templates:
            self.plan.item(t)
        if self.namespace == self.control_namespace:
            raise ValueError("namespace and control namespace must differ")

    @property
    def context(self) -> str:
        return f"kind-{self.kind_cluster}"

    def observer(self) -> str:
        cmd = [
            "uv",
            "run",
            "-q",
            "python",
            "-m",
            "benchmarks.fuzzops.observe",
            "--control-namespace",
            self.control_namespace,
        ]
        if self.operator_binary and self.controller_image:
            cmd += ["--operator-binary", self.operator_binary, "--controller-image", self.controller_image]
        return f"cd {shlex.quote(str(self.sdo_root))} && exec " + shlex.join(cmd)

    def run_args(self, round_: str, instance: str, seed: int) -> list[str]:
        return [
            str(self.fuzzops / "bin" / "fuzzops"),
            "run",
            "--pack",
            str(self.pack),
            "--namespace",
            self.namespace,
            "--skip-reset",
            "--operator",
            f"exec:sdo-{round_}",
            "--instance-file",
            self.plan.instances_file,
            "--instance",
            instance,
            "--window",
            self.window,
            "--durability=false",
            "--seed",
            str(seed),
            *self.extra,
        ]

    def env(self, run_dir: Path) -> dict[str, str]:
        env = dict(os.environ)
        env.update(
            {
                "FUZZOPS_KIND_CLUSTER": self.kind_cluster,
                "KIND_CLUSTER": self.kind_cluster,
                "KUBE_CONTEXT": self.context,
                "FUZZOPS_EXEC_OUT": str(run_dir / "exec"),
            }
        )
        for r in ROUNDS:
            env[f"FUZZOPS_EXEC_CMD_SDO_{r.upper()}"] = self.observer()
        if self.kubeconfig:
            env["KUBECONFIG"] = self.kubeconfig
        return env


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_lane(cfg: LaneConfig, log: Callable[[str], None] = print) -> int:
    kube = Kube(cfg.context, cfg.kubeconfig)
    cfg.out.mkdir(parents=True, exist_ok=True)
    meta = {
        "app": cfg.plan.app,
        "sdo_version": cfg.sdo_version,
        "kind_cluster": cfg.kind_cluster,
        "namespace": cfg.namespace,
        "control_namespace": cfg.control_namespace,
        "window": cfg.window,
        "evidence_dir": cfg.evidence_dir,
        "templates": cfg.templates,
        "plan": json.loads(cfg.plan.dump()),
        "started": _now(),
    }
    (cfg.out / "lane.json").write_text(json.dumps(meta, indent=2) + "\n")
    runs = cfg.out / "runs"
    runs.mkdir(exist_ok=True)
    n = len(list(runs.iterdir()))
    failures = 0
    healed_rounds: list[str] = []
    for template in cfg.templates:
        for round_, instance in cfg.plan.item(template).rounds():
            done = [
                d
                for d in runs.iterdir()
                if d.name.endswith(f"-{slug(template, 40)}-{round_}")
                and (d / "record.json").is_file()
                and (d / "record.json").stat().st_size > 0
            ]
            if done:
                log(f"{template} {round_}: already done ({done[0].name})")
                continue
            rc = 1
            for attempt in range(cfg.unhealthy_retries + 1):
                try:
                    waited, soft = settle(kube, cfg.control_namespace, log, cfg.reflection_wait)
                except CampaignError as exc:
                    log(f"{template} {round_}: skipped, {exc}")
                    break
                n += 1
                run_dir = runs / f"{n:03d}-{slug(template, 40)}-{round_}"
                run_dir.mkdir()
                info = {
                    "template": template,
                    "round": round_,
                    "instance": instance,
                    "attempt": attempt,
                    "idle_wait_s": round(waited),
                    "soft_idle": soft,
                    "started": _now(),
                    "sdo_version": cfg.sdo_version,
                }
                log(f"{template} {round_} -> {instance} (run {run_dir.name})")
                with (run_dir / "record.json").open("w") as out, (run_dir / "run.log").open("w") as err:
                    rc = subprocess.run(
                        cfg.run_args(round_, instance, n),
                        stdout=out,
                        stderr=err,
                        env=cfg.env(run_dir),
                        cwd=cfg.fuzzops,
                        check=False,
                    ).returncode
                info.update({"exit": rc, "ended": _now()})
                (run_dir / "run.json").write_text(json.dumps(info, indent=2) + "\n")
                rec = _read_json(run_dir / "record.json")
                unhealthy = rec is not None and "no healthy baseline" in str(rec.get("error", ""))
                if not unhealthy:
                    break
                log(f"{template} {round_}: app not healthy before inject, retrying after SDO is idle")
                run_dir.rename(run_dir.with_name(run_dir.name + "-unhealthy"))
                if cfg.heal_cmd and attempt == 0:
                    healed = heal(cfg)
                    log(f"{template} {round_}: healed the app with the lane's heal command (exit {healed})")
                    healed_rounds.append(f"{template}/{round_}")
            if rc != 0:
                failures += 1
            try:
                settle(kube, cfg.control_namespace, log, cfg.reflection_wait)
                kube.copy_memory(cfg.control_namespace, cfg.out / "memory")
            except CampaignError as exc:
                log(f"{template} {round_}: {exc}")
                failures += 1
    meta["healed_before"] = healed_rounds
    meta["ended"] = _now()
    (cfg.out / "lane.json").write_text(json.dumps(meta, indent=2) + "\n")
    return 1 if failures else 0


def heal(cfg: LaneConfig) -> int:
    env = {**os.environ, "NAMESPACE": cfg.namespace, "KUBE_CONTEXT": cfg.context}
    if cfg.kubeconfig:
        env["KUBECONFIG"] = cfg.kubeconfig
    return subprocess.run(["sh", "-c", cfg.heal_cmd], env=env, check=False, timeout=900).returncode


def settle(
    kube: Kube, control_namespace: str, log: Callable[[str], None], reflection_wait: float = REFLECTION_WAIT_S
) -> tuple[float, str]:
    """Wait for SDO to be idle; (seconds waited, the reflection reason when the lane stopped waiting for it)."""
    try:
        return wait_idle(lambda: kube.state(control_namespace), log=log, reflection_wait=reflection_wait), ""
    except SoftIdle as soft:
        return soft.waited, soft.why


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


# ---- CLI -----------------------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("plan", help="pick first/repeat/variant rounds from a fuzzops instances file")
    pl.add_argument("--app", required=True)
    pl.add_argument("--instances-file", type=Path, required=True)
    pl.add_argument("--templates", default="", help="comma-separated templates in run order (default: all)")
    pl.add_argument(
        "--visible", type=Path, help="a fuzzops runs.jsonl whose screening runs say which instances users see"
    )
    pl.add_argument("--out", type=Path, required=True)
    ln = sub.add_parser("lane", help="run the plan's rounds on one installed SDO lane")
    ln.add_argument("--plan", type=Path, required=True)
    ln.add_argument("--templates", default="", help="this lane's templates (default: the whole plan)")
    ln.add_argument("--out", type=Path, required=True, help="lane results directory (in sdo-exp)")
    ln.add_argument("--fuzzops", type=Path, required=True, help="fuzzops checkout with bin/fuzzops built")
    ln.add_argument("--pack", type=Path, required=True, help="the attached pack install wrote (<work>/pack)")
    ln.add_argument("--namespace", required=True)
    ln.add_argument("--control-namespace", required=True)
    ln.add_argument("--kind-cluster", required=True)
    ln.add_argument("--sdo-version", required=True, help="the SDO commit the lane's images were built from")
    ln.add_argument("--window", default="600s")
    ln.add_argument("--kubeconfig", default="")
    ln.add_argument("--operator-binary", default="", help="seal an evidence snapshot at each closure")
    ln.add_argument("--controller-image", default="")
    ln.add_argument(
        "--evidence-dir", default="", help="the host directory of this lane's evidence sink run (<sink>/<run-id>)"
    )
    ln.add_argument("--heal-cmd", default="", help="shell command restoring a broken app before a retried round")
    ln.add_argument(
        "--reflection-wait",
        type=float,
        default=REFLECTION_WAIT_S,
        help="seconds a round waits for a queued reflection before it runs on the active memory",
    )
    a = p.parse_args(argv)
    if a.cmd == "plan":
        pairs = instance_ids(json.loads(a.instances_file.read_text()))
        visible = screened_visible(a.visible) if a.visible else None
        templates = [t for t in a.templates.split(",") if t] or None
        plan = Plan(a.app, str(a.instances_file.resolve()), tuple(choose(pairs, templates, visible)))
        a.out.write_text(plan.dump())
        print(f"{len(plan.items)} templates, {sum(len(i.rounds()) for i in plan.items)} rounds -> {a.out}")
        return 0
    plan = Plan.load(a.plan)
    cfg = LaneConfig(
        plan=plan,
        templates=[t for t in a.templates.split(",") if t] or [i.template for i in plan.items],
        out=a.out,
        fuzzops=a.fuzzops.resolve(),
        pack=a.pack.resolve(),
        namespace=a.namespace,
        control_namespace=a.control_namespace,
        kind_cluster=a.kind_cluster,
        sdo_version=a.sdo_version,
        window=a.window,
        kubeconfig=a.kubeconfig,
        operator_binary=a.operator_binary,
        controller_image=a.controller_image,
        evidence_dir=a.evidence_dir,
        heal_cmd=a.heal_cmd,
        reflection_wait=a.reflection_wait,
    )
    return run_lane(cfg, log=lambda m: print(f"{_now()} {m}", flush=True))


def screened_visible(runs_jsonl: Path) -> set[str]:
    """Instance IDs a fuzzops suite's screening runs found visible to users (not masked)."""
    visible: set[str] = set()
    for line in runs_jsonl.read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec.get("kind") == "screen" and rec.get("valid"):
            bad = (rec.get("window") or {}).get("BadSeconds", 0) + (rec.get("bad_seconds_added") or 0)
            lost = ((rec.get("verify") or {}).get("missing") or 0) + ((rec.get("verify") or {}).get("wrong") or 0)
            if bad > 0 or lost > 0:
                visible.add(rec.get("screened_as") or rec["fault"])
    return visible


if __name__ == "__main__":
    sys.exit(main())
