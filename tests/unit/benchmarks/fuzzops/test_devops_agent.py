"""Exercise the DevOps Agent subject at the hook boundary with a fake service and a fake sandbox."""

import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

from benchmarks.fuzzops import devops_agent, vanilla
from benchmarks.fuzzops.hooks import HookError
from benchmarks.fuzzops.row_metrics import measure

BWRAP = """#!/usr/bin/python3
# Substitute just the namespace boundary.
import os, sys
args, env, bind = iter(sys.argv[1:]), {}, {}
for flag in args:
    if flag in ('--bind', '--ro-bind'):
        src, dst = next(args), next(args)
        bind[dst] = src
    elif flag == '--setenv':
        key = next(args)
        env[key] = next(args)
    elif flag == '--chdir':
        cwd = bind[next(args)]
    elif flag in ('--proc', '--dev', '--tmpfs'):
        next(args)
    elif flag == '--':
        break
env['KUBECONFIG'] = bind['/kubeconfig']
env['PATH'] = os.path.dirname(bind['/tools/aws']) + ':' + os.path.dirname(bind['/tools/kubectl']) + ':/usr/bin:/bin'
cmd = list(args)
os.chdir(cwd)
os.execve(cmd[0], cmd, env)
"""
KUBECTL = """#!/bin/sh
echo "$@" >> LOG
case "$*" in *broken*) echo no such object >&2; exit 1;; esac
echo "deployment.apps/web patched for account 123456789012 with synthetic-kube-token"
"""


def step(name, *commands, kind="command"):
    return {"step": name, "instructions": [{"instruction": {"type": kind, "content": c}} for c in commands]}


class FakeService:
    def __init__(self, plan=None, statuses=("IN_PROGRESS", "COMPLETED")):
        self.plan, self.statuses = plan, list(statuses)
        self.created, self.cancelled, self.queries = [], [], []
        self.listed = []
        self.by_id: dict = {}

    def create(self, title, description, token):
        self.created.append((title, description, token))
        return {"taskId": "t1", "status": "PENDING_START"}

    def task(self, task_id):
        if task_id in self.by_id:
            return dict(self.by_id[task_id])
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if status == "ERROR":
            raise HookError("throttled")
        return {"taskId": task_id, "status": status}

    def tasks(self, created_after):
        self.queries.append(created_after)
        visible = [t for t in self.listed if t.get("after_create") is None or self.created]
        return [{k: v for k, v in t.items() if k != "after_create"} for t in visible]

    def cancel(self, task_id):
        self.cancelled.append(task_id)

    def executions(self, task_id):
        return [
            {
                "executionId": "e1",
                "createdAt": "2026-01-01T00:00:00+00:00",
                "updatedAt": "2026-01-01T00:03:20+00:00",
            }
        ]

    def journal(self, execution_id):
        summary = {
            "findings": [
                {"type": "cause", "title": "pods fail", "description": "d"},
                {"type": "root_cause", "title": "selector", "description": "web selects no pod"},
            ]
        }
        records = [
            {"recordType": "message", "content": json.dumps({"text": "arn:aws:eks:r:123456789012:cluster/c"})},
            {"recordType": "investigation_summary", "content": json.dumps(summary)},
        ]
        if self.plan is not None:
            plan = {"type": "mitigation_summary", "execution_plan": self.plan}
            records.append({"recordType": "mitigation_summary", "content": json.dumps(plan)})
        return records

    def assets(self):
        return [{"assetId": "a1", "updatedAt": "2026-01-01T00:00:00+00:00"}]

    def usage(self):
        raise HookError("denied")

    def space(self):
        return {"preferences": {"elevatedActionsEnabled": False}}


@pytest.fixture(autouse=True)
def scoped_credentials(monkeypatch):
    """The scoped token is minted through the cluster's kubectl; these tests have none (see the refresh tests)."""
    monkeypatch.setattr(vanilla, "refresh_token", lambda env: None)
    monkeypatch.setattr(vanilla, "credentials_work", lambda env: True)


@pytest.fixture
def env(tmp_path):
    tools = tmp_path / "bin"
    tools.mkdir()
    for name, text in (("bwrap", BWRAP), ("kubectl", KUBECTL.replace("LOG", str(tmp_path / "kubectl.log")))):
        (tools / name).write_text(text)
        (tools / name).chmod(0o755)
    lane = tmp_path / "lane" / "vanilla"
    (lane / "proof").mkdir(parents=True)
    (lane / "kubeconfig").write_text('{"users":[{"user":{"token":"synthetic-kube-token"}}]}')
    operator = tmp_path / "operator.json"
    lanes = {"g1": {"agent_space_id": "space-1", "cluster": "web-1"}}
    operator.write_text(
        json.dumps({"settings": {"window_seconds": 600, "devops_agent": {"region": "us-west-2", "lanes": lanes}}})
    )
    return {
        **os.environ,
        "PATH": f"{tools}:{os.environ['PATH']}",
        "BENCH_OPERATOR": str(operator),
        "BENCH_OUT": str(tmp_path / "run" / "operator"),
        "BENCH_LANE": "g1",
        "BENCH_LANE_DIR": str(tmp_path / "lane"),
        "BENCH_RUN": "proof",
        "FUZZOPS_NAMESPACE": "fg1",
        "FUZZOPS_REPORT": str(tmp_path / "report.json"),
    }


def respond(env, service, injected=None):
    devops_agent.respond(env, service, injected or time.time(), poll=0.0)
    return json.loads((vanilla.run_dir(env) / "done.json").read_text())


def calls(tmp_path):
    log = tmp_path / "kubectl.log"
    return log.read_text().splitlines() if log.exists() else []


def test_page_plan_and_evidence(env, tmp_path):
    plan = [
        step(
            "prepare",
            "aws eks update-kubeconfig --name web-1 && kubectl get deploy web -o yaml > web.yaml",
            "kubectl get broken",
        ),
        step("pre_validate", "kubectl get pods"),
        step("apply", "test -s web.yaml && kubectl patch deployment/web -n fg1 -p x", "kubectl scale broken"),
        step("apply", "kubectl delete pod never-reached"),
        step("post_validate", "kubectl get svc"),
        step("rollback", "kubectl rollout undo deployment/web"),
        step("apply", "ask the owner", kind="text"),
    ]
    service = FakeService(plan, ("IN_PROGRESS", "ERROR", "COMPLETED"))
    done = respond(env, service)
    title, description, token = service.created[0]
    assert title == "Users are reporting problems with the application in namespace fg1"
    assert description.splitlines()[0] == vanilla.page_text("fg1")
    assert "web-1" in description
    assert "proof" not in title + description
    assert len(token) == 64
    assert calls(tmp_path) == [
        "get deploy web -o yaml",
        "get broken",
        "patch deployment/web -n fg1 -p x",
        "scale broken",
    ]
    assert (done["task_status"], done["apply_status"], done["poll_errors"]) == ("COMPLETED", "failed", 1)
    assert devops_agent.collect(env) == 0
    out = tmp_path / "run" / "operator"
    assert json.loads((out / "evidence-manifest.json").read_text())["arm"] == "devops-agent"
    assert json.loads((out / "apply.json").read_text())["manual"] == 1
    usage = json.loads((out / "usage.json").read_text())
    assert (usage["agent_seconds"], usage["total_cost_usd"]) == (200.0, 1.66)
    assert json.loads((out / "space.json").read_text())["account_usage"] == {"error": "denied"}
    assert json.loads((tmp_path / "report.json").read_text())["summary"] == "selector: web selects no pod"
    text = "".join(p.read_text() for p in out.iterdir())
    assert "123456789012" not in text
    assert "synthetic-kube-token" not in text
    row = measure(tmp_path / "run")
    assert (row.arm, row.responder_cost_usd, row.responder_input_tokens) == ("devops-agent", 1.66, None)
    assert row.last_write_source == "applied_plan_tool_start"
    assert devops_agent.collect(env) == 0  # a second collect keeps the verdict


def test_plan_without_apply_command_changes_nothing(env, tmp_path):
    for plan in (None, [step("prepare", "kubectl get a"), step("apply", "do it by hand", kind="text")]):
        assert respond(env, FakeService(plan))["apply_status"] == "no_apply_command"
    assert calls(tmp_path) == []
    assert devops_agent.collect(env) == 0  # a graded row: the agent gave nothing to run


def test_cap_cancels_and_applies_nothing(env, tmp_path):
    service = FakeService([step("apply", "kubectl patch x")], ("IN_PROGRESS",))
    done = respond(env, service, time.time() - vanilla.AGENT_SECONDS)
    assert (done["capped"], done["apply_status"], service.cancelled) == (True, "not_run", ["t1"])
    assert calls(tmp_path) == []
    assert devops_agent.collect(env) == 0


def test_a_hanging_command_ends_at_the_deadline(env, tmp_path):
    deadline = time.time() + 0.5
    plan = {"execution_plan": [step("apply", "sleep 30", "kubectl patch x")]}
    result = devops_agent.apply(env, vanilla.run_dir(env), plan, deadline)
    assert (result["status"], [c["exit"] for c in result["commands"]]) == ("capped", [None])
    assert calls(tmp_path) == []


@pytest.mark.parametrize("status", ["LINKED", "SKIPPED", "FAILED"])
def test_uninvestigated_page_is_no_row(env, tmp_path, status):
    respond(env, FakeService([step("apply", "kubectl patch x")], (status,)))
    assert calls(tmp_path) == []
    assert devops_agent.collect(env) == 1


def test_collect_cancels_a_task_whose_supervisor_died(env, monkeypatch):
    service = FakeService(statuses=("IN_PROGRESS",))
    monkeypatch.setattr(devops_agent, "connect", lambda env: service)
    (vanilla.run_dir(env) / "task.json").write_text('{"taskId": "t1", "status": "IN_PROGRESS"}')
    assert devops_agent.collect(env) == 1
    assert service.cancelled == ["t1"]


def test_prepare_refuses_lanes_it_cannot_measure(env, monkeypatch, capsys):
    assert devops_agent.main(["prepare"], {**env, "BENCH_LANE_VIRTUAL": "1"}) == 1
    assert "vcluster" in capsys.readouterr().out
    service = FakeService()
    monkeypatch.setattr(service, "space", lambda: {"preferences": {"elevatedActionsEnabled": True}})
    monkeypatch.setattr(devops_agent, "connect", lambda env: service)
    assert devops_agent.main(["prepare"], env) == 1
    assert "directed actions" in capsys.readouterr().out


def test_settings_keep_lanes_apart():
    one = devops_agent.Lane("space-1", "web-1")
    with pytest.raises(ValueError, match="own agent_space_id"):
        devops_agent.Settings("us-west-2", {"a": one, "b": devops_agent.Lane("space-1", "web-2")})
    with pytest.raises(ValueError, match="own cluster"):
        devops_agent.Settings("us-west-2", {"a": one, "b": devops_agent.Lane("space-2", "web-1")})


def test_watch_launches_this_subject_on_harm_only(env, tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    traffic = tmp_path / "traffic.jsonl"
    sample = {"inject_at": now.isoformat(), "observed_at": now.isoformat(), "requests": 6, "first_bad": {"Failed": 1}}
    traffic.write_text(json.dumps(sample) + "\n")
    launched = []

    class Child:
        def __init__(self, argv, **kwargs):
            launched.append(argv)

        def wait(self):
            return 0

    monkeypatch.setattr(vanilla.subprocess, "Popen", Child)
    run = {
        **env,
        "BENCH_RUN": "fresh",
        "FUZZOPS_TRAFFIC": str(traffic),
        "FUZZOPS_DEADLINE": (now + timedelta(seconds=60)).isoformat(),
    }
    assert devops_agent.main(["watch"], run) == 0
    assert launched[0][1:4] == ["-m", devops_agent.MODULE, "supervise"]
    assert devops_agent.main(["watch"], {**run, "BENCH_RUN": "healthy", "BENCH_FAULT": "none"}) == 0
    assert len(launched) == 1


def test_cli_client_speaks_the_documented_operations(env, tmp_path):
    aws = tmp_path / "bin" / "aws"
    aws.write_text(
        f"""#!/usr/bin/python3
import json, sys
open({str(tmp_path / "aws.log")!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[2] == 'get-backlog-task':
    sys.exit('AccessDenied for 123456789012')
print(json.dumps({{'task': {{'taskId': 't1', 'status': 'PENDING_START'}}, 'records': [1]}}))
"""
    )
    aws.chmod(0o755)
    client = devops_agent.connect(env)
    assert client.create("T", "D", "tok")["taskId"] == "t1"
    assert client.journal("e1") == [1]
    client.cancel("t1")
    with pytest.raises(HookError, match="AccessDenied for ACCOUNT"):
        client.task("t1")
    argv = [json.loads(line) for line in (tmp_path / "aws.log").read_text().splitlines()]
    assert all(a[0] == "devops-agent" and a[a.index("--agent-space-id") + 1] == "space-1" for a in argv)
    assert argv[0][argv[0].index("--task-type") + 1] == "INVESTIGATION"
    assert argv[2][argv[2].index("--task-status") + 1] == "CANCELED"


ASSOCIATION = "assoc-1"


def alarm_task(task_id="alarm-t", created=None, association=ASSOCIATION, reference_id="inc-1", after_create=None):
    created = created or datetime.now(timezone.utc) + timedelta(seconds=1)
    return {
        "taskId": task_id,
        "status": "COMPLETED",
        "createdAt": created.isoformat(),
        "reference": {"associationId": association, "referenceId": reference_id},
        "after_create": after_create,
    }


@pytest.fixture
def alarm_env(env, tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        "\n".join(
            json.dumps(row)
            for row in (
                {"incident_id": "other", "alarm_name": "wrong", "transition": "OK"},
                {"incident_id": "inc-1", "alarm_name": "web-5xx", "transition": "ALARM", "extra": 1},
            )
        )
    )
    return alarm_settings(env, tmp_path, {"mode": "alarm", "fallback_seconds": 0.3, **paging(ledger)})


def paging(ledger):
    return {"event_channel_association_id": ASSOCIATION, "relay_ledger": str(ledger)}


def alarm_settings(env, tmp_path, block):
    operator = json.loads((tmp_path / "operator.json").read_text())
    operator["settings"]["devops_agent"]["paging"] = block
    path = tmp_path / "operator-alarm.json"
    path.write_text(json.dumps(operator))
    first_bad = datetime.now(timezone.utc)
    (vanilla.run_dir({**env, "BENCH_OPERATOR": str(path)})).mkdir(parents=True, exist_ok=True)
    onset = vanilla.run_dir(env) / "onset.json"
    # FuzzOps' onset sample: the first bad one-second bucket.
    bucket = {"Start": first_bad.strftime("%Y-%m-%dT%H:%M:%SZ"), "OK": 5, "Failed": 1, "Wrong": 0, "Slow": 0}
    onset.write_text(json.dumps({"first_bad": bucket}))
    return {**env, "BENCH_OPERATOR": str(path)}


def evidence(env, name):
    return json.loads((vanilla.run_dir(env) / name).read_text())


def test_alarm_task_is_followed_without_a_harness_page(alarm_env):
    service = FakeService([step("apply", "kubectl patch x")])
    service.listed = [alarm_task(created=datetime.now(timezone.utc) + timedelta(seconds=1))]
    done = respond(alarm_env, service, time.time() - 1)
    assert service.created == []
    assert (done["paged_by"], done["task_status"]) == ("alarm", "COMPLETED")
    paged = evidence(alarm_env, "paging.json")
    assert (paged["mode"], paged["paged_by"], paged["task_id"], paged["reference_id"]) == (
        "alarm",
        "alarm",
        "alarm-t",
        "inc-1",
    )
    assert (paged["alarm_name"], paged["alarm_transition"]) == ("web-5xx", "ALARM")
    assert paged["late_alarm_tasks"] == []
    assert not (vanilla.run_dir(alarm_env) / "page.json").exists()


def test_a_linked_alarm_task_follows_its_primary_when_the_primary_is_from_this_episode(alarm_env):
    service = FakeService([step("apply", "kubectl patch x")])
    now = datetime.now(timezone.utc)
    service.listed = [
        {**alarm_task(created=now + timedelta(seconds=1)), "status": "LINKED", "primaryTaskId": "primary-t"}
    ]
    service.by_id = {
        "primary-t": {
            "taskId": "primary-t",
            "status": "COMPLETED",
            "createdAt": (now + timedelta(seconds=0.5)).isoformat(),
        },
    }
    done = respond(alarm_env, service, time.time() - 1)
    assert done["task_status"] == "COMPLETED"
    paged = evidence(alarm_env, "paging.json")
    assert (paged["task_id"], paged["linked_from_task_id"]) == ("primary-t", "alarm-t")


def test_a_linked_alarm_task_with_a_primary_from_before_the_inject_is_not_followed(alarm_env):
    service = FakeService()
    now = datetime.now(timezone.utc)
    service.listed = [{**alarm_task(created=now + timedelta(seconds=1)), "status": "LINKED", "primaryTaskId": "old-t"}]
    service.by_id = {
        "old-t": {"taskId": "old-t", "status": "COMPLETED", "createdAt": (now - timedelta(hours=1)).isoformat()},
    }
    done = respond(alarm_env, service, time.time() - 1)
    assert done["task_status"] == "LINKED"
    assert "linked_from_task_id" not in evidence(alarm_env, "paging.json")


def test_the_ledger_entry_of_an_alarm_task_is_its_created_record_not_the_resolution(alarm_env, tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        ledger.read_text()
        + "\n"
        + json.dumps({"incident_id": "inc-1", "alarm_name": "web-5xx", "transition": "OK", "action": "resolved"})
    )
    service = FakeService()
    service.listed = [alarm_task()]
    respond(alarm_env, service, time.time() - 1)
    assert evidence(alarm_env, "paging.json")["alarm_transition"] == "ALARM"


def test_alarm_task_without_ledger_match_leaves_alarm_fields_null(alarm_env, tmp_path):
    (tmp_path / "ledger.jsonl").unlink()
    service = FakeService()
    service.listed = [alarm_task()]
    respond(alarm_env, service, time.time() - 1)
    paged = evidence(alarm_env, "paging.json")
    assert (paged["paged_by"], paged["alarm_name"], paged["alarm_transition"]) == ("alarm", None, None)


def test_no_alarm_task_pages_as_the_fallback(alarm_env):
    service = FakeService()
    started = time.time()
    done = respond(alarm_env, service, started - 1)
    assert len(service.created) == 1
    assert done["paged_by"] == "fallback"
    paged = evidence(alarm_env, "paging.json")
    assert time.time() >= datetime.fromisoformat(paged["fallback_due"]).timestamp()
    assert (paged["paged_by"], paged["task_id"], paged["alarm_name"]) == ("fallback", "t1", None)
    assert evidence(alarm_env, "page.json")["title"].startswith("Users are reporting")
    first_bad = datetime.fromisoformat(paged["first_bad"])
    assert datetime.fromisoformat(paged["fallback_due"]) == first_bad + timedelta(seconds=0.3)


def test_foreign_and_early_alarm_tasks_are_ignored(alarm_env):
    injected = time.time()
    service = FakeService()
    before = datetime.fromtimestamp(injected - 60, timezone.utc)
    service.listed = [alarm_task("foreign", association="other"), alarm_task("early", created=before)]
    done = respond(alarm_env, service, injected)
    assert (done["paged_by"], len(service.created)) == ("fallback", 1)
    assert evidence(alarm_env, "paging.json")["late_alarm_tasks"] == []


def test_alarm_task_after_the_fallback_is_recorded_not_followed(alarm_env):
    service = FakeService()
    service.listed = [alarm_task("late", reference_id="inc-late", after_create=True)]
    done = respond(alarm_env, service, time.time() - 1)
    assert (done["paged_by"], len(service.created)) == ("fallback", 1)
    paged = evidence(alarm_env, "paging.json")
    assert paged["task_id"] == "t1"
    assert [(t["task_id"], t["reference_id"]) for t in paged["late_alarm_tasks"]] == [("late", "inc-late")]


def test_harness_mode_records_its_page(env):
    service = FakeService()
    done = respond(env, service)
    assert done["paged_by"] == "harness"
    assert len(service.created) == 1
    assert service.queries == []
    paged = evidence(env, "paging.json")
    assert (paged["mode"], paged["paged_by"], paged["task_id"]) == ("harness", "harness", "t1")
    assert "page_at" in paged
    assert (vanilla.run_dir(env) / "page.json").exists()


def test_paging_validation():
    assert devops_agent.Paging().mode == "harness"
    ok = {"mode": "alarm", "fallback_seconds": 120, "event_channel_association_id": "a"}
    assert devops_agent.Paging(**ok, relay_ledger="/tmp/x.jsonl").fallback_seconds == 120
    for bad, error in (
        ({"mode": "page"}, ValueError),
        ({**ok, "fallback_seconds": 0}, ValueError),
        ({**ok, "fallback_seconds": -1}, ValueError),
        ({**ok, "fallback_seconds": "120"}, TypeError),
        ({**ok, "event_channel_association_id": ""}, ValueError),
        ({**ok, "event_channel_association_id": 3}, TypeError),
        ({**ok, "relay_ledger": "ledger.jsonl"}, ValueError),
        ({**ok, "relay_ledger": 4}, TypeError),
    ):
        with pytest.raises(error):
            devops_agent.Paging(**bad)


def test_settings_reject_invalid_paging_block(env, tmp_path):
    bad = alarm_settings(env, tmp_path, {"mode": "alarm", "fallback_seconds": 0, "event_channel_association_id": "a"})
    with pytest.raises(HookError, match="paging"):
        devops_agent.settings(bad)


def test_cli_lists_alarm_tasks_across_pages(env, tmp_path):
    aws = tmp_path / "bin" / "aws"
    aws.write_text(
        f"""#!/usr/bin/python3
import json, sys
open({str(tmp_path / "aws.log")!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')
token = sys.argv[sys.argv.index('--next-token') + 1] if '--next-token' in sys.argv else None
print(json.dumps({{'tasks': [{{'taskId': 'b'}}]}} if token else {{'tasks': [{{'taskId': 'a'}}], 'nextToken': 'n1'}}))
"""
    )
    aws.chmod(0o755)
    found = devops_agent.connect(env).tasks("2026-01-01T00:00:00+00:00")
    assert [t["taskId"] for t in found] == ["a", "b"]
    first = json.loads((tmp_path / "aws.log").read_text().splitlines()[0])
    assert first[0:2] == ["devops-agent", "list-backlog-tasks"]
    assert json.loads(first[first.index("--filter") + 1]) == {"createdAfter": "2026-01-01T00:00:00+00:00"}


def test_the_scoped_token_is_refreshed_before_every_apply(env, monkeypatch):
    refreshed = []
    monkeypatch.setattr(vanilla, "refresh_token", lambda e: refreshed.append(True))
    target = vanilla.run_dir(env)
    target.mkdir(parents=True, exist_ok=True)
    devops_agent.apply(env, target, {"steps": []}, time.time() + 30)
    assert refreshed == [True]


def test_refresh_token_replaces_the_token_and_proves_it(env, monkeypatch):
    calls = []

    def kube(e, config, *args, stdin=None):
        calls.append((config, args))
        if args[:2] == ("create", "token"):
            return "fresh-token"
        return "yes"

    monkeypatch.setattr(vanilla, "kubectl", kube)
    monkeypatch.undo()  # keep the module's own functions, only kubectl is replaced below
    monkeypatch.setattr(vanilla, "kubectl", kube)
    vanilla.refresh_token(env)
    assert json.loads((vanilla.state(env) / "kubeconfig").read_text())["users"][0]["user"]["token"] == "fresh-token"
    assert calls[-1][1][:2] == ("auth", "can-i")
