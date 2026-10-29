"""Account routing uses reference-only configuration and real CLI frames."""

from __future__ import annotations

import base64
import json
import os
import sys
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from libs.agent_cli.claude_accounts import AccountRouter, prepare_accounts, sync_cswap
from libs.agent_cli.errors import AccountExhausted
from libs.agent_cli.provider_capacity import ProviderCapacity
from libs.agent_cli.structured import StructuredTurnError, run_structured_turn
from tests.structured_turns import ScriptedAgent, failure, reply

AUTH = (
    'API Error: 401 {"error":{"type":"authentication_error","message":"OAuth token has expired. '
    'Please obtain a new token or refresh your existing token."}}'
)
LIMIT = "You've hit your weekly limit · resets 8pm (UTC)"
SCHEMA = {"type": "object", "properties": {"answer": {"type": "string"}}}


def bundle(root: Path, count: int = 2) -> dict[str, str]:
    secret = root / "secret"
    secret.mkdir()
    accounts = []
    for slot in range(count):
        alias = f"a{slot + 1:02d}"
        (secret / f"{alias}.token").write_text(uuid.uuid4().hex)
        accounts.append({"alias": alias, "generation": uuid.uuid4().hex, "token_file": f"{alias}.token"})
    (secret / "accounts.json").write_text(json.dumps({"accounts": accounts}))
    prepare_accounts(secret, root / "pool", [], capacity=1)
    return {
        "SDO_CLAUDE_ACCOUNTS_DIR": str(secret),
        "SDO_CLAUDE_ACCOUNT_PRIMARY": "a01",
        "SDO_OPUS_POOL_DIR": str(root / "pool"),
        "SDO_OPUS_POOL_MODE": "per_account",
        "SDO_CLAUDE_ACCOUNT_MAX_SESSIONS": "1",
        "SDO_PROVIDER_WAIT_LOG": str(root / "provider-waits.jsonl"),
    }


@pytest.mark.parametrize(
    "diagnostic",
    [
        AUTH,
        "Invalid API key · Please run /login",
        LIMIT,
        'API Error: 529 {"error":{"type":"overloaded_error","message":"Overloaded"}}',
    ],
)
def test_failover_finishes_and_keeps_session_and_config_home(tmp_path: Path, diagnostic: str) -> None:
    env = bundle(tmp_path)
    ticks = [0.0]
    manager = ProviderCapacity(
        environment=env, monotonic=lambda: ticks[0], sleep=lambda n: ticks.__setitem__(0, ticks[0] + n)
    )
    seen: list[str] = []

    def respond(request):
        token = request.env.get("CLAUDE_CODE_OAUTH_TOKEN")
        alias = next(a for a in ("a01", "a02") if token == (tmp_path / "secret" / f"{a}.token").read_text())
        seen.append(alias)
        if alias == "a01":
            return failure(
                stdout=json.dumps({"type": "system", "subtype": "init", "session_id": "session-1"}), stderr=diagnostic
            )
        return reply("claude", {"answer": "ok"}, session_id="session-1")

    agent = ScriptedAgent(respond)
    work = tmp_path / "work"
    work.mkdir()
    turn = run_structured_turn(
        "claude",
        "go",
        output_schema=SCHEMA,
        cwd=work,
        access="read-only",
        model="claude-opus-5-5",
        executor=agent.executor,
        environment=env,
        provider_capacity=manager,
    )
    assert seen[0] == "a01"
    assert seen[-1] == "a02"
    assert turn.session_id == "session-1"
    assert "--resume" in agent.argvs[-1]
    assert len({r.env.get("CLAUDE_CONFIG_DIR") for r in agent.requests}) == 1
    records = (tmp_path / "provider-waits.jsonl").read_text()
    assert '"account_alias": "a01"' in records
    assert '"account_alias": "a02"' in records
    for path in (tmp_path / "secret").glob("*.token"):
        assert path.read_text() not in records


def test_auth_disable_is_shared_until_registration_changes(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    router = AccountRouter.from_environment(env)
    first = router.acquire()
    assert first.alias == "a01"
    first.close()
    router.fail(first, "auth_failed")
    other = AccountRouter.from_environment(env)
    selected = other.acquire()
    assert selected.alias == "a02"
    selected.close()
    data = json.loads((tmp_path / "secret/accounts.json").read_text())
    data["accounts"][0]["generation"] = uuid.uuid4().hex
    (tmp_path / "secret/accounts.json").write_text(json.dumps(data))
    refreshed = other.acquire()
    assert refreshed.alias == "a01"
    refreshed.close()


def test_staggered_secret_versions_cannot_reenable_a_disabled_registration(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    router = AccountRouter.from_environment(env)
    original = (tmp_path / "secret/accounts.json").read_text()
    old = router.acquire()
    old.close()
    router.fail(old, "auth_failed")
    new = json.loads(original)
    new["accounts"][0]["generation"] = uuid.uuid4().hex
    (tmp_path / "secret/accounts.json").write_text(json.dumps(new))
    current = router.acquire()
    assert current.alias == "a01"
    current.close()
    (tmp_path / "secret/accounts.json").write_text(original)  # Lagging projected Secret.
    lagging = router.acquire()
    assert lagging.alias == "a02"
    lagging.close()


@pytest.mark.parametrize("late_failure", ["rate_limited", "credential_unavailable"])
def test_late_error_cannot_clear_another_turns_auth_disable(tmp_path: Path, late_failure: str) -> None:
    env = bundle(tmp_path)
    clock = [0.0]
    router = AccountRouter.from_environment(env, clock=lambda: clock[0])
    selected = router.acquire()
    selected.close()
    router.fail(selected, "auth_failed")
    router.fail(selected, late_failure)
    clock[0] = 301
    peer = router.acquire()
    assert peer.alias == "a02"
    peer.close()


def test_all_accounts_disabled_is_typed_and_never_uses_default_login(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    agent = ScriptedAgent(lambda _: failure(stderr=AUTH))
    work = tmp_path / "work"
    work.mkdir()
    with pytest.raises(AccountExhausted, match="account_exhausted"):
        run_structured_turn(
            "claude", "go", output_schema=SCHEMA, cwd=work, access="read-only", executor=agent.executor, environment=env
        )
    assert len(agent.requests) == 2


def test_missing_primary_credential_uses_healthy_peer(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    (tmp_path / "secret/a01.token").unlink()
    agent = ScriptedAgent(lambda _: reply("claude", {"answer": "ok"}, session_id="s"))
    work = tmp_path / "work"
    work.mkdir()
    turn = run_structured_turn(
        "claude", "go", output_schema=SCHEMA, cwd=work, access="read-only", executor=agent.executor, environment=env
    )
    assert turn.session_id == "s"
    assert len(agent.requests) == 1


def test_torn_health_write_preserves_disabled_registration(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    router = AccountRouter.from_environment(env)
    first = router.acquire()
    first.close()
    router.fail(first, "auth_failed")
    with (tmp_path / "pool/a01/health.lock").open("a") as stream:
        stream.write('{"torn":')
    peer = router.acquire()
    assert peer.alias == "a02"
    peer.close()


def test_crash_between_state_post_and_health_commit_does_not_duplicate_post(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    router = AccountRouter.from_environment(env)
    first = router.acquire()
    first.close()
    health = tmp_path / "pool/a01/health.lock"
    before = health.read_text()
    router.fail(first, "auth_failed")
    health.write_text(before)  # Crash lost health append after the durable post.
    router.fail(first, "auth_failed")
    posts = [json.loads(line) for line in (tmp_path / "pool/account-state.jsonl").read_text().splitlines()]
    disabled = [post for post in posts if post["account_alias"] == "a01" and post["state"] == "disabled"]
    assert len(disabled) == 1


@settings(max_examples=12, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=32, max_size=60))
def test_credentials_cannot_enter_errors_or_capacity_telemetry(tmp_path: Path, canary: str) -> None:
    root = tmp_path / uuid.uuid4().hex
    root.mkdir()
    env = bundle(root)
    for path in (root / "secret").glob("*.token"):
        path.write_text(canary)
    agent = ScriptedAgent(lambda _: failure(stderr="unexpected CLI diagnostic " + canary))
    work = root / "work"
    work.mkdir()
    with pytest.raises(StructuredTurnError) as caught:
        run_structured_turn(
            "claude", "go", output_schema=SCHEMA, cwd=work, access="read-only", executor=agent.executor, environment=env
        )
    assert canary not in "".join(traceback.format_exception(caught.value))
    assert canary not in (root / "provider-waits.jsonl").read_text()
    assert canary not in (root / "pool/account-state.jsonl").read_text()


@pytest.mark.parametrize("diagnostic", [AUTH, LIMIT, 'API Error: 403 {"error":{"type":"permission_error"}}'])
def test_real_fake_cli_fails_over_then_reflection_resumes_on_another_account(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, diagnostic: str
) -> None:
    env = bundle(tmp_path, count=3)
    script = tmp_path / "claude"
    audit = tmp_path / "calls.jsonl"
    script.write_text(f"""#!{sys.executable}
import json, os, pathlib, sys
if '--help' in sys.argv:
    raise SystemExit(0)
sys.stdin.read()
root = pathlib.Path({str(tmp_path / "secret")!r})
alias = next(p.stem for p in root.glob('*.token') if p.read_text() == os.environ.get('CLAUDE_CODE_OAUTH_TOKEN'))
sid = sys.argv[sys.argv.index('--resume') + 1] if '--resume' in sys.argv else 'session-lane'
with pathlib.Path({str(audit)!r}).open('a') as out:
    row = {{'alias':alias, 'session_id':sid, 'resumed':'--resume' in sys.argv,
           'home':os.environ.get('CLAUDE_CONFIG_DIR')}}
    out.write(json.dumps(row) + '\\n')
print(json.dumps({{'type':'system','subtype':'init','session_id':sid}}))
if alias == 'a01':
    print({diagnostic!r}, file=sys.stderr)
    raise SystemExit(1)
print(json.dumps({{'type':'result','subtype':'success','is_error':False,'session_id':sid,'result':'ok','structured_output':{{'answer':'ok'}},'num_turns':1}}))
""")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    work = tmp_path / "work"
    work.mkdir()
    env["CLAUDE_CONFIG_DIR"] = str(tmp_path / "claude-home")
    clock = [0.0]
    capacity = ProviderCapacity(
        environment=env, monotonic=lambda: clock[0], sleep=lambda n: clock.__setitem__(0, clock[0] + n)
    )
    responder = run_structured_turn(
        "claude",
        "respond",
        output_schema=SCHEMA,
        cwd=work,
        access="read-only",
        model="opus",
        environment=env,
        provider_capacity=capacity,
        session_ownership_directory=tmp_path / "sessions",
    )
    (tmp_path / "secret/a02.token").unlink()  # Reflection must switch again without losing history.
    reflection = run_structured_turn(
        "claude",
        "reflect",
        output_schema=SCHEMA,
        cwd=work,
        access="read-only",
        model="opus",
        environment=env,
        resume_session_id=responder.session_id,
        session_ownership_directory=tmp_path / "sessions",
        session_ownership_role="reflection",
    )
    calls = [json.loads(line) for line in audit.read_text().splitlines()]
    assert reflection.session_id == responder.session_id == "session-lane"
    assert calls[-1]["alias"] == "a03"
    assert calls[-1]["resumed"]
    assert len({call["home"] for call in calls}) == 1


def test_account_environment_removes_proxy_headers_and_other_auth_routes(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    router = AccountRouter.from_environment(env)
    selected = router.acquire()
    excluded = (
        "ANTHROPIC_BASE_URL",
        "ANTHROPIC_CUSTOM_HEADERS",
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY",
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
    )
    canary = uuid.uuid4().hex
    effective = router.environment(selected, {**env, **dict.fromkeys(excluded, canary)})
    selected.close()
    assert effective["ANTHROPIC_BASE_URL"] == "https://api.anthropic.com"
    assert all(not effective.get(name) for name in excluded if name != "ANTHROPIC_BASE_URL")


def test_account_concurrency_and_aliases_share_real_inodes(tmp_path: Path) -> None:
    env = bundle(tmp_path)
    alias = tmp_path / "mount/pool"
    prepare_accounts(Path(env["SDO_CLAUDE_ACCOUNTS_DIR"]), tmp_path / "pool", [alias], capacity=1)
    first = AccountRouter.from_environment(env).acquire()
    second = AccountRouter.from_environment({**env, "SDO_OPUS_POOL_DIR": str(alias)}).acquire()
    assert first.alias != second.alias
    assert AccountRouter.from_environment(env).try_acquire() is None
    first.close()
    second.close()


def test_cswap_usage_excludes_weekly_limit_but_not_setup_token_403(tmp_path: Path) -> None:
    store, secret = tmp_path / "cswap", tmp_path / "secret"
    (store / "credentials").mkdir(parents=True)
    accounts = {}
    listing = []
    reset = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    for slot in range(1, 4):
        email = f"synthetic-{slot}@example.invalid"
        accounts[str(slot)] = {"email": email, "added": f"registration-{slot}"}
        payload = json.dumps({"claudeAiOauth": {"accessToken": uuid.uuid4().hex}}).encode()
        (store / "credentials" / f".creds-{slot}-{email}.enc").write_bytes(base64.b64encode(payload))
        listing.append(
            {
                "number": str(slot),
                "usage": {"sevenDay": {"pct": 100 if slot == 1 else 0, "resetsAt": reset}} if slot != 3 else None,
                "usageError": "http-403" if slot == 3 else "",
            }
        )
    (store / "sequence.json").write_text(json.dumps({"accounts": accounts}))
    inventory = sync_cswap(store, secret, {"accounts": listing}, lanes=["50", "53", "54"], capacity=2)
    assert inventory["lanes"] == {"50": "a02", "53": "a03", "54": "a02"}
    env = {
        "SDO_CLAUDE_ACCOUNTS_DIR": str(secret),
        "SDO_OPUS_POOL_DIR": str(tmp_path / "pool"),
        "SDO_CLAUDE_ACCOUNT_MAX_SESSIONS": "2",
        "SDO_CLAUDE_ACCOUNT_PRIMARY": "a01",
    }
    prepare_accounts(secret, tmp_path / "pool", [], capacity=2)
    account = AccountRouter.from_environment(env).acquire()
    assert account.alias == "a02"
    account.close()
    for path in secret.glob("*.token"):
        assert path.read_text() not in json.dumps(inventory)
    original = json.loads((secret / "accounts.json").read_text())
    sync_cswap(store, secret, {"accounts": listing}, lanes=[], capacity=2)
    assert json.loads((secret / "accounts.json").read_text()) == original
    source = next((store / "credentials").glob(".creds-2-*.enc"))
    stamp = source.stat()
    os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns + 1000000))
    sync_cswap(store, secret, {"accounts": listing}, lanes=[], capacity=2)
    assert json.loads((secret / "accounts.json").read_text()) == original  # Ordinary refresh is not re-registration.
    accounts["2"]["added"] = "human-re-registration"
    (store / "sequence.json").write_text(json.dumps({"accounts": accounts}))
    sync_cswap(store, secret, {"accounts": listing}, lanes=[], capacity=2)
    renewed = json.loads((secret / "accounts.json").read_text())
    assert renewed["accounts"][1]["generation"] != original["accounts"][1]["generation"]
    source.unlink()
    inventory = sync_cswap(store, secret, {"accounts": listing}, lanes=["50"], capacity=2)
    assert inventory["lanes"] == {"50": "a03"}
