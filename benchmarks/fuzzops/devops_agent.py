"""Lanes subject for AWS DevOps Agent: it investigates and plans, a fixed applier runs the plan's commands.

The service only recommends changes to a Kubernetes cluster, so a row is three separate facts: its diagnosis (the
FuzzOps report), its plan, and what the plan did when its commands ran unchanged. The lane side (page text and time,
namespace ServiceAccount, sandbox, onset wait, golden copy) is the plain Claude subject's (:mod:`vanilla`).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.fuzzops import eks, vanilla
from benchmarks.fuzzops.cellevidence import atomic_write_json
from benchmarks.fuzzops.hooks import HookError
from benchmarks.fuzzops.repage import first_bad_start
from benchmarks.fuzzops.view import redact, redact_json_line

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

ARM = "devops-agent"
MODULE = "benchmarks.fuzzops.devops_agent"
POLL_SECONDS = 5.0
COMMAND_SECONDS = 300.0
USD_PER_AGENT_SECOND = 0.0083
TERMINAL = frozenset({"COMPLETED", "FAILED", "TIMED_OUT", "CANCELED", "LINKED", "SKIPPED"})
#: Triage folded the page into another investigation or dropped it: the page was not investigated, so no row.
NOT_INVESTIGATED = frozenset({"LINKED", "SKIPPED"})
RUN_STEPS = ("prepare", "apply")
EVIDENCE_FILES = (
    "start.json",
    "onset.json",
    vanilla.ONSET_EVIDENCE,
    "page.json",
    "paging.json",
    "task.json",
    "executions.json",
    "journal.jsonl",
    "diagnosis.json",
    "plan.json",
    "apply.json",
    "usage.json",
    "space.json",
    "done.json",
)
_ACCOUNT = re.compile(r"(?<!\d)\d{12}(?!\d)")
#: The plan's own kubeconfig step has nothing to do here; every other AWS call has no credentials to use.
AWS_STUB = '#!/bin/sh\n[ "$1 $2" = "eks update-kubeconfig" ] && exit 0\necho "aws: not available" >&2\nexit 1\n'


class Service(Protocol):
    """The AWS DevOps Agent operations a row needs."""

    def create(self, title: str, description: str, token: str) -> dict[str, Any]: ...

    def task(self, task_id: str) -> dict[str, Any]: ...

    def tasks(self, created_after: str) -> list[dict[str, Any]]: ...

    def cancel(self, task_id: str) -> None: ...

    def executions(self, task_id: str) -> list[dict[str, Any]]: ...

    def journal(self, execution_id: str) -> list[dict[str, Any]]: ...

    def assets(self) -> list[dict[str, Any]]: ...

    def usage(self) -> dict[str, Any]: ...

    def space(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class Lane:
    agent_space_id: str
    cluster: str

    def __post_init__(self) -> None:
        for name in ("agent_space_id", "cluster"):
            value = getattr(self, name)
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
            if not value:
                raise ValueError(f"{name} must not be empty")


@dataclass(frozen=True)
class Paging:
    """Who pages the service: the harness at onset (default), or the account's alarm with the harness as fallback."""

    mode: str = "harness"
    fallback_seconds: float = 120.0
    event_channel_association_id: str = ""
    relay_ledger: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in ("harness", "alarm"):
            raise ValueError("paging.mode must be 'harness' or 'alarm'")
        if self.mode == "harness":
            return
        seconds = self.fallback_seconds
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError("paging.fallback_seconds must be a number")
        if not seconds > 0:
            raise ValueError("paging.fallback_seconds must be positive")
        if not isinstance(self.event_channel_association_id, str):
            raise TypeError("paging.event_channel_association_id must be a string")
        if not self.event_channel_association_id:
            raise ValueError("paging.event_channel_association_id must not be empty")
        if self.relay_ledger is not None:
            if not isinstance(self.relay_ledger, str):
                raise TypeError("paging.relay_ledger must be a string")
            if not os.path.isabs(self.relay_ledger):
                raise ValueError("paging.relay_ledger must be an absolute path")


@dataclass(frozen=True)
class Settings:
    region: str
    lanes: dict[str, Lane]
    paging: Paging = field(default_factory=Paging)

    def __post_init__(self) -> None:
        if not isinstance(self.paging, Paging):
            raise TypeError("paging must be a Paging")
        if not isinstance(self.region, str):
            raise TypeError("region must be a string")
        if not self.region or not self.lanes:
            raise ValueError("devops_agent needs a region and at least one lane")
        for name in ("agent_space_id", "cluster"):
            values = [getattr(lane, name) for lane in self.lanes.values()]
            if len(set(values)) != len(values):
                # An agent that reaches two lanes reads the other lane's fault.
                raise ValueError(f"each lane needs its own {name}")


def settings(env: Mapping[str, str]) -> Settings:
    block = vanilla.operator_settings(env).get("devops_agent")
    try:
        if not isinstance(block, dict):
            raise TypeError("missing block")
        lanes = {name: Lane(**lane) for name, lane in block["lanes"].items()}
        paging = block.get("paging")
        if paging is not None and not isinstance(paging, dict):
            raise TypeError("paging must be an object")
        return Settings(block["region"], lanes, Paging(**paging) if paging else Paging())
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        raise HookError(f"invalid settings.devops_agent: {exc}") from exc


def lane(env: Mapping[str, str]) -> Lane:
    found = settings(env).lanes.get(env["BENCH_LANE"])
    if found is None:
        raise HookError(f"settings.devops_agent.lanes has no entry for lane {env['BENCH_LANE']}")
    return found


@dataclass(frozen=True)
class AwsCli:
    env: Mapping[str, str] = field(repr=False)
    region: str
    agent_space_id: str

    def call(self, operation: str, *args: str, space: bool = True) -> dict[str, Any]:
        argv = [vanilla.binary(self.env, "aws"), "devops-agent", operation, "--region", self.region]
        argv += ["--output", "json", "--no-cli-pager"]
        if space:
            argv += ["--agent-space-id", self.agent_space_id]
        try:
            proc = subprocess.run([*argv, *args], env=self.env, capture_output=True, text=True, timeout=60, check=False)
        except subprocess.TimeoutExpired as exc:
            raise HookError(f"aws devops-agent {operation} timed out") from exc
        if proc.returncode:
            why = redact(_ACCOUNT.sub("ACCOUNT", proc.stderr.strip()))[-300:]
            raise HookError(f"aws devops-agent {operation} failed (exit {proc.returncode}): {why}")
        try:
            return json.loads(proc.stdout) if proc.stdout.strip() else {}
        except ValueError as exc:
            raise HookError(f"aws devops-agent {operation} returned invalid JSON") from exc

    def create(self, title: str, description: str, token: str) -> dict[str, Any]:
        args = ["--task-type", "INVESTIGATION", "--priority", "HIGH", "--title", title]
        return self.call("create-backlog-task", *args, "--description", description, "--client-token", token)["task"]

    def task(self, task_id: str) -> dict[str, Any]:
        return self.call("get-backlog-task", "--task-id", task_id)["task"]

    def tasks(self, created_after: str) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        token = ""
        while True:
            args = ["--filter", json.dumps({"createdAfter": created_after})]
            if token:
                args += ["--next-token", token]
            page = self.call("list-backlog-tasks", *args)
            found += page.get("tasks") or []
            token = page.get("nextToken") or ""
            if not token:
                return found

    def cancel(self, task_id: str) -> None:
        self.call("update-backlog-task", "--task-id", task_id, "--task-status", "CANCELED")

    # The CLI paginates these three itself.
    def executions(self, task_id: str) -> list[dict[str, Any]]:
        return self.call("list-executions", "--task-id", task_id)["executions"]

    def journal(self, execution_id: str) -> list[dict[str, Any]]:
        return self.call("list-journal-records", "--execution-id", execution_id, "--order", "ASC")["records"]

    def assets(self) -> list[dict[str, Any]]:
        return self.call("list-assets")["items"]

    def usage(self) -> dict[str, Any]:
        return self.call("get-account-usage", space=False)

    def space(self) -> dict[str, Any]:
        return self.call("get-agent-space")["agentSpace"]


def connect(env: Mapping[str, str]) -> Service:
    return AwsCli(env, settings(env).region, lane(env).agent_space_id)


@dataclass(frozen=True)
class Instruction:
    step: str
    kind: str
    content: str


def content(record: Mapping[str, Any]) -> dict[str, Any]:
    """A journal record's content: a JSON document, or that document as a string."""
    value = record.get("content")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def last(records: Sequence[Mapping[str, Any]], kind: str) -> dict[str, Any]:
    found = [content(r) for r in records if r.get("recordType") == kind]
    return found[-1] if found else {}


def instructions(plan: Mapping[str, Any]) -> list[Instruction]:
    found = []
    for step in plan.get("execution_plan") or []:
        for item in step.get("instructions") or []:
            body = item.get("instruction") or {}
            found.append(Instruction(str(step.get("step")), str(body.get("type")), str(body.get("content") or "")))
    return found


def diagnosis_text(summary: Mapping[str, Any]) -> str:
    findings = [f for f in summary.get("findings") or [] if isinstance(f, dict)]
    causes = [f for f in findings if f.get("type") == "root_cause"] or findings
    return "\n".join(f"{f.get('title', '')}: {f.get('description', '')}" for f in causes)


def agent_seconds(executions: Sequence[Mapping[str, Any]]) -> float | None:
    """The service bills active agent time; its account counters lag, so a row takes the executions' own spans."""
    try:
        spans = [datetime.fromisoformat(e["updatedAt"]) - datetime.fromisoformat(e["createdAt"]) for e in executions]
    except (KeyError, TypeError, ValueError):
        return None
    return round(sum(s.total_seconds() for s in spans), 3) if spans else None


def clean(value: Any, secrets: Sequence[str] = ()) -> Any:
    line = redact_json_line(json.dumps(value, default=str), secrets)
    return json.loads(_redact_accounts(line))


def _redact_accounts(line: str) -> str:
    def walk(value: Any) -> Any:
        if isinstance(value, str):
            return _ACCOUNT.sub("ACCOUNT", value)
        if isinstance(value, list):
            return [walk(v) for v in value]
        if isinstance(value, dict):
            return {k: walk(v) for k, v in value.items()}
        return value

    return json.dumps(walk(json.loads(line)))


def kube_token(env: Mapping[str, str]) -> str:
    return json.loads((vanilla.state(env) / "kubeconfig").read_text())["users"][0]["user"]["token"]


def run_command(env: Mapping[str, str], box: Path, command: str, limit: float) -> tuple[int | None, str]:
    """Run one plan command in the plain Claude agent's sandbox with its kubeconfig; (None, ...) ran out of time."""
    stub = box / "aws"
    stub.write_text(AWS_STUB)
    stub.chmod(0o755)
    mounts = (
        (vanilla.binary(env, "kubectl"), "/tools/kubectl"),
        (str(stub), "/tools/aws"),
        (str(vanilla.state(env) / "kubeconfig"), "/kubeconfig"),
    )
    variables = {"HOME": "/home", "KUBECONFIG": "/kubeconfig", "PATH": "/tools:/usr/local/bin:/usr/bin:/bin"}
    argv = [*vanilla.isolated(env, box, mounts, variables), "/bin/bash", "-c", command]
    with (box / "output").open("w+b") as output:
        try:
            code = subprocess.run(
                argv,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=limit,
                check=False,
            ).returncode
        except subprocess.TimeoutExpired:
            code = None
        output.seek(0)
        return code, output.read().decode(errors="replace")[-20_000:]


def apply(env: Mapping[str, str], target: Path, plan: Mapping[str, Any], deadline: float) -> dict[str, Any]:
    """Run the plan's prepare and apply commands as written, in order; the first failed apply command ends it."""
    todo = instructions(plan)
    vanilla.refresh_token(env)  # a fresh scoped token for every apply, whatever the run's age
    secrets = [kube_token(env)]
    commands: list[dict[str, Any]] = []
    result: dict[str, Any] = {
        "commands": commands,
        "manual": sum(i.kind != "command" for i in todo if i.step in RUN_STEPS),
        "status": "no_apply_command",
    }
    path = target / "apply.json"
    atomic_write_json(path, result)
    if not any(i.step == "apply" and i.kind == "command" for i in todo):
        return result
    result["status"] = "applied"
    with tempfile.TemporaryDirectory(prefix="devops-agent-apply-") as temp:
        for name in RUN_STEPS:
            for item in (i for i in todo if i.step == name and i.kind == "command"):
                started = time.time()
                if started >= deadline:
                    result["status"] = "capped"
                    break
                code, output = run_command(env, Path(temp), item.content, min(COMMAND_SECONDS, deadline - started))
                commands.append(
                    clean(
                        {
                            "step": name,
                            "command": item.content,
                            "started_at": vanilla.timestamp(started),
                            "ended_at": vanilla.timestamp(time.time()),
                            "exit": code,
                            "output": output,
                        },
                        secrets,
                    )
                )
                # Written as it goes: a killed supervisor leaves what it changed on record.
                atomic_write_json(path, result)
                if name == "apply" and code != 0:
                    result["status"] = "capped" if time.time() >= deadline else "failed"
                    break
            if result["status"] != "applied":
                break
    atomic_write_json(path, result)
    return result


def gather(service: Service, target: Path, task: Mapping[str, Any], secrets: Sequence[str]) -> dict[str, Any]:
    """Save everything the service kept about the task; returns the plan (empty: none)."""
    atomic_write_json(target / "task.json", clean(task, secrets))
    executions = service.executions(task["taskId"])
    atomic_write_json(target / "executions.json", clean(executions, secrets))
    records = [r for e in executions for r in service.journal(e["executionId"])]
    with (target / "journal.jsonl").open("w") as out:
        for record in records:
            out.write(json.dumps(clean(record, secrets)) + "\n")
    summary, plan = last(records, "investigation_summary"), last(records, "mitigation_summary")
    atomic_write_json(target / "diagnosis.json", clean(summary, secrets))
    atomic_write_json(target / "plan.json", clean(plan, secrets))
    seconds = agent_seconds(executions)
    cost = round(seconds * USD_PER_AGENT_SECOND, 4) if seconds is not None else None
    atomic_write_json(
        target / "usage.json",
        {
            "agent_seconds": seconds,
            "usd_per_agent_second": USD_PER_AGENT_SECOND,
            "total_cost_usd": cost,
            "source": "execution spans at list price",
        },
    )
    return plan


def optional(read: Any) -> Any:
    """Side evidence: what it cannot say never costs the row."""
    try:
        return read()
    except HookError as exc:
        return {"error": str(exc)}


def space_state(service: Service) -> dict[str, Any]:
    """What the Agent Space knows (rows are independent only while it stays the same) and the billing counters."""
    return clean({"assets": optional(service.assets), "account_usage": optional(service.usage)})


def parse_time(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def first_bad_time(target: Path, default: float) -> float:
    """The first harmed user second, from the onset sample vanilla wrote (FuzzOps' first bad bucket, ``Start``); the
    supervisor's start when it has none."""
    try:
        found = first_bad_start(json.loads((target / "onset.json").read_text()))
    except (OSError, ValueError, AttributeError):
        found = None
    return default if found is None else found.timestamp()


def alarm_tasks(service: Service, paging: Paging, injected: float) -> list[dict[str, Any]]:
    """Tasks the account's own alarm raised since the inject time, earliest first."""
    since = vanilla.timestamp(injected)
    found = []
    for task in service.tasks(since):
        reference = task.get("reference") or {}
        created = parse_time(task.get("createdAt"))
        if reference.get("associationId") != paging.event_channel_association_id:
            continue
        if created is None or created < injected:
            continue
        found.append((created, task))
    return [task for _, task in sorted(found, key=lambda pair: pair[0])]


def ledger_entry(paging: Paging, reference_id: Any) -> dict[str, Any]:
    """The relay's record of the alarm that became this task (empty: no ledger or no match)."""
    if not paging.relay_ledger or reference_id is None:
        return {}
    try:
        lines = Path(paging.relay_ledger).read_text().splitlines()
    except OSError:
        return {}
    for line in lines:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if (
            isinstance(entry, dict)
            and entry.get("incident_id") == reference_id
            and entry.get("action", "created") == "created"
        ):
            return entry
    return {}


def linked_primary(service: Service, task: Mapping[str, Any], injected: float) -> dict[str, Any] | None:
    """The primary task a LINKED alarm task points to, when it was created at or after the inject."""
    primary_id = task.get("primaryTaskId")
    if not primary_id or primary_id == task.get("taskId"):
        return None
    try:
        primary = service.task(primary_id)
    except HookError:
        return None
    created = parse_time(primary.get("createdAt"))
    if created is None or created < injected:
        return None
    return primary


def page_by_alarm(service: Service, paging: Paging, injected: float, due: float, poll: float) -> dict[str, Any] | None:
    """Wait for the alarm's task until the fallback is due; None means the harness must page."""
    while True:
        try:
            found = alarm_tasks(service, paging, injected)
        except HookError:
            found = []
        if found:
            return found[0]
        remaining = due - time.time()
        if remaining <= 0:
            return None
        time.sleep(min(poll, remaining))


def respond(env: Mapping[str, str], service: Service, injected: float, poll: float = POLL_SECONDS) -> None:
    target, ns = vanilla.run_dir(env), env["FUZZOPS_NAMESPACE"]
    deadline = injected + vanilla.AGENT_SECONDS
    vanilla.refresh_token(env)  # the prepare-time token lives 2 h; every run starts with a fresh one
    secrets = [kube_token(env)]
    paging = settings(env).paging
    started = time.time()
    first_bad = first_bad_time(target, started)
    due = first_bad + paging.fallback_seconds
    alarm = page_by_alarm(service, paging, injected, due, poll) if paging.mode == "alarm" else None
    if alarm is not None:
        task = alarm
    else:
        page = vanilla.page_text(ns)
        # Only where to look is added: the service is not handed a kubeconfig as the other agents are.
        description = f"{page}\n\nAmazon EKS cluster: {lane(env).cluster} ({settings(env).region})."
        title = page.split(";")[0]
        token = hashlib.sha256(f"{env['BENCH_RUN']}|{injected}".encode()).hexdigest()
        atomic_write_json(
            target / "page.json", {"at": vanilla.timestamp(time.time()), "title": title, "description": description}
        )
        task = service.create(title, description, token)
    task_id = task["taskId"]
    paged_by = "harness" if paging.mode == "harness" else "alarm" if alarm is not None else "fallback"
    reference_id = (task.get("reference") or {}).get("referenceId")
    entry = ledger_entry(paging, reference_id) if alarm is not None else {}
    paging_evidence: dict[str, Any] = {"mode": paging.mode, "paged_by": paged_by}
    paging_evidence["page_at"] = task.get("createdAt") or vanilla.timestamp(time.time())
    paging_evidence["task_id"] = task_id
    if paging.mode == "alarm":
        paging_evidence.update(
            {
                "first_bad": vanilla.timestamp(first_bad),
                "fallback_due": vanilla.timestamp(due),
                "reference_id": reference_id,
                "alarm_name": entry.get("alarm_name"),
                "alarm_transition": entry.get("transition"),
                "late_alarm_tasks": [],
            }
        )
    atomic_write_json(target / "paging.json", clean(paging_evidence, secrets))
    atomic_write_json(target / "task.json", clean(task, secrets))
    capped, poll_errors = False, 0
    while True:
        if task["status"] == "LINKED":
            # Triage folded this page into the investigation of an earlier one of the same incident: follow that
            # primary (the work is done there). A primary from before the inject is another episode: not followed.
            primary = linked_primary(service, task, injected)
            if primary is not None:
                paging_evidence["linked_from_task_id"] = task_id
                task, task_id = primary, primary["taskId"]
                paging_evidence["task_id"] = task_id
                atomic_write_json(target / "paging.json", clean(paging_evidence, secrets))
                atomic_write_json(target / "task.json", clean(task, secrets))
                continue
        if task["status"] in TERMINAL:
            break
        if time.time() >= deadline:
            capped = True
            break
        time.sleep(min(poll, max(0.0, deadline - time.time())))
        try:
            task = service.task(task_id)
        except HookError:
            poll_errors += 1
    investigated = time.time()
    if capped:
        optional(lambda: service.cancel(task_id))
    if paging.mode == "alarm":
        record_late_alarms(service, paging, injected, task_id, target, paging_evidence, secrets)
    plan = gather(service, target, task, secrets)
    summary = diagnosis_text(json.loads((target / "diagnosis.json").read_text()))
    if summary and env.get("FUZZOPS_REPORT"):
        atomic_write_json(Path(env["FUZZOPS_REPORT"]), {"summary": summary[:600]})
    applied = {"status": "not_run"}
    if task["status"] == "COMPLETED":
        applied = apply(env, target, plan, deadline)
    ended = time.time()
    atomic_write_json(target / "space.json", space_state(service))
    atomic_write_json(
        target / "done.json",
        {
            "started_at": vanilla.timestamp(started),
            "investigated_at": vanilla.timestamp(investigated),
            "ended_at": vanilla.timestamp(ended),
            "task_status": task["status"],
            "capped": capped or applied["status"] == "capped",
            "poll_errors": poll_errors,
            "apply_status": applied["status"],
            "paged_by": paged_by,
            "model": "AWS DevOps Agent",
            "result_event": True,
        },
    )


def record_late_alarms(
    service: Service,
    paging: Paging,
    injected: float,
    followed: str,
    target: Path,
    evidence: dict[str, Any],
    secrets: Sequence[str],
) -> None:
    """Alarm tasks that arrived after the followed task was chosen are recorded, never followed."""
    found = optional(lambda: alarm_tasks(service, paging, injected))
    if not isinstance(found, list):
        return
    evidence["late_alarm_tasks"] = [
        {
            "task_id": t.get("taskId"),
            "created_at": t.get("createdAt"),
            "reference_id": (t.get("reference") or {}).get("referenceId"),
        }
        for t in found
        if t.get("taskId") != followed
    ]
    atomic_write_json(target / "paging.json", clean(evidence, secrets))


def supervise(env: Mapping[str, str], injected: float, lock_fd: int) -> None:
    os.set_inheritable(lock_fd, False)
    respond(env, connect(env), injected)


def problems_of(dest: Path, done: Mapping[str, Any]) -> list[str]:
    if done.get("error"):
        return [f"DevOps Agent run failed ({done['error']})"]
    if not done.get("result_event"):
        return ["DevOps Agent run has no end record"]
    status = done.get("task_status")
    if status in NOT_INVESTIGATED:
        return [f"the page was not investigated (task {status})"]
    problems = [] if status == "COMPLETED" or done.get("capped") else [f"investigation ended {status}"]
    journal = dest / "journal.jsonl"
    if not journal.is_file() or not journal.read_text().strip():
        problems.append("investigation journal missing or empty")
    return problems


def finish_abandoned(env: Mapping[str, str]) -> None:
    """A supervisor that died left a task the service still works on and bills: cancel it and keep its journal."""
    target = vanilla.run_dir(env)
    done = json.loads((target / "done.json").read_text()) if (target / "done.json").is_file() else {}
    if not (target / "task.json").is_file() or done.get("result_event"):
        return
    service, task = connect(env), json.loads((target / "task.json").read_text())
    task = service.task(task["taskId"])
    if task["status"] not in TERMINAL:
        service.cancel(task["taskId"])
    gather(service, target, task, [kube_token(env)])


def collect(env: Mapping[str, str]) -> int:
    with vanilla.lane_lock(env):
        optional(lambda: finish_abandoned(env))
        return vanilla.publish(env, ARM, EVIDENCE_FILES, problems_of)


def prepare(env: Mapping[str, str]) -> None:
    if env.get("BENCH_LANE_VIRTUAL") == "1":
        raise HookError(
            "AWS DevOps Agent reads EKS clusters only: a vcluster lane is invisible to it (use a full lane)"
        )
    space = connect(env).space()
    if (space.get("preferences") or {}).get("elevatedActionsEnabled"):
        raise HookError("the Agent Space allows directed actions; this arm is recommend-only")
    vanilla.prepare(env, "AWS DevOps Agent, plan applied mechanically")


def preflight(env: Mapping[str, str]) -> None:
    with tempfile.TemporaryDirectory(prefix="devops-agent-preflight-") as temp:
        argv = vanilla.isolated(env, Path(temp), (), {"PATH": "/usr/bin:/bin"})
        ok = subprocess.run([*argv, "/bin/bash", "-c", "true"], capture_output=True, timeout=20, check=False).returncode
    detail = "the plan sandbox starts" if ok == 0 else "the plan sandbox does not start"
    print(json.dumps({"checks": [{"check": "devops-agent-sandbox", "ok": ok == 0, "detail": detail}]}))


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    environ = dict(os.environ if env is None else env)
    eks.export_entry_host(environ)
    verb = args[0] if args else ""
    try:
        if verb == "describe":
            settings(environ)
            print(json.dumps({"max_startup": "0s", "needs_credentials": True}))
        elif verb == "check":
            settings(environ)
            for tool in ("bwrap", "kubectl", "aws"):
                vanilla.binary(environ, tool)
        elif verb == "preflight":
            preflight(environ)
        elif verb == "watch":
            return vanilla.watch(environ, MODULE, "AWS DevOps Agent")
        elif verb == "supervise":
            supervise(environ, float(args[1]), int(args[2]))
        elif verb == "collect":
            return collect(environ)
        elif verb == "transcripts":
            print("row transcripts are verified by collect")
        elif verb in vanilla.LANE_VERBS:
            vanilla.lifecycle(environ, verb, prepare)
        else:
            return 64
    except (HookError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        why = str(exc) if isinstance(exc, HookError) else f"{verb} failed ({type(exc).__name__})"
        if verb == "supervise":
            atomic_write_json(vanilla.run_dir(environ) / "done.json", {"error": why})
        if verb == "preflight":
            print(json.dumps({"checks": [{"check": "devops-agent-sandbox", "ok": False, "detail": why}]}))
            return 0
        print(why, file=sys.stderr if verb in ("watch", "supervise") else sys.stdout)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
