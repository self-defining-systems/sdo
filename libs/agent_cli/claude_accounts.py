"""Claude accounts by mounted reference, with host-wide health and admission.

Credentials live only in the private bundle and the CLI child's environment.
Shared state and telemetry contain aliases, opaque registration IDs and reasons.
The CLI config home is deliberately independent of the selected account.
"""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import math
import os
import re
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Literal, cast
from uuid import uuid4

from libs.agent_cli.errors import AccountCredentialUnavailable, AccountExhausted
from libs.agent_cli.provider_capacity import OpusPool, prepare_host_pool

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

ACCOUNT_ENVIRONMENT = (
    "SDO_CLAUDE_ACCOUNTS_DIR",
    "SDO_CLAUDE_ACCOUNTS_SECRET",
    "SDO_CLAUDE_ACCOUNT_PRIMARY",
    "SDO_CLAUDE_ACCOUNT_MAX_SESSIONS",
    "SDO_CLAUDE_ACCOUNT_COOLDOWN_SECONDS",
    "SDO_CLAUDE_ACCOUNT_BACKOFF_SECONDS",
)
_ALIAS = re.compile(r"a[0-9]{2,6}\Z")
_AUTH = re.compile(
    r"authentication_error|API Error:\s*(?:401|403)\b|OAuth token (?:has )?expired|"
    r"invalid (?:API key|OAuth token|access token)|token (?:has been )?revoked|please run /login|not logged in",
    re.IGNORECASE,
)


def is_claude_auth_failure(text: str) -> bool:
    """Ignore tool payloads; only CLI lines and provider result/error frames count."""
    for line in text.splitlines():
        try:
            frame = json.loads(line)
        except ValueError:
            if _AUTH.search(line):
                return True
            continue
        if not isinstance(frame, dict):
            continue
        frame = cast("dict[str, Any]", frame)
        if (
            (frame.get("type") == "result" and frame.get("is_error") is True) or frame.get("type") == "error"
        ) and _AUTH.search(str(frame.get("result", "")) + str(frame.get("errors", "")) + str(frame.get("error", ""))):
            return True
    return False


@dataclass(frozen=True)
class Account:
    alias: str
    generation: str
    token_file: str
    unavailable_until: float = 0
    disabled: bool = False

    def __post_init__(self) -> None:
        if not _ALIAS.fullmatch(self.alias):
            raise ValueError("invalid Claude account alias")
        if not re.fullmatch(r"[0-9a-f]{32}", self.generation):
            raise ValueError("invalid Claude account registration ID")
        if self.token_file != f"{self.alias}.token":
            raise ValueError("Claude credential reference must be an alias token filename")
        if not math.isfinite(self.unavailable_until) or self.unavailable_until < 0:
            raise ValueError("invalid Claude account quota reset")


def load_accounts(directory: Path) -> list[Account]:
    try:
        raw = json.loads((directory / "accounts.json").read_text())
        accounts = [Account(**entry) for entry in raw["accounts"]]
        if len({entry.alias for entry in accounts}) != len(accounts):
            raise ValueError
        return accounts
    except (OSError, ValueError, TypeError, KeyError):
        raise AccountExhausted("account inventory unavailable or invalid", retryable=False) from None


@dataclass
class AccountHealth:
    status: Literal["healthy", "cooldown", "disabled"] = "healthy"
    until: float = 0
    reason: str = "registered"
    transition: int = 0

    def __post_init__(self) -> None:
        if self.status not in ("healthy", "cooldown", "disabled") or self.reason not in (
            "registered",
            "cooldown_elapsed",
            "registration_disabled",
            "quota_exhausted",
            "auth_failed",
            "rate_limited",
            "credential_unavailable",
        ):
            raise ValueError("invalid account health state")
        if not math.isfinite(self.until) or self.until < 0 or type(self.transition) is not int or self.transition < 0:
            raise ValueError("invalid account health clock or transition")


@dataclass
class AccountLease:
    account: Account
    handle: IO[str] = field(repr=False)

    @property
    def alias(self) -> str:
        return self.account.alias

    def close(self) -> None:
        self.handle.close()


@dataclass
class AccountRouter:
    directory: Path
    pool_directory: Path
    primary: str = ""
    capacity: int = 2
    cooldown_seconds: float = 300
    clock: Callable[[], float] = field(default=lambda: datetime.now(timezone.utc).timestamp(), repr=False)
    record: Callable[..., None] = field(default=lambda *a, **kw: None, repr=False)

    def __post_init__(self) -> None:
        if self.primary and not _ALIAS.fullmatch(self.primary):
            raise ValueError("invalid primary Claude account alias")
        if type(self.capacity) is not int or self.capacity <= 0:
            raise ValueError("per-account concurrency must be a positive integer")
        if not math.isfinite(self.cooldown_seconds) or self.cooldown_seconds <= 0:
            raise ValueError("account cooldown must be positive and finite")

    @classmethod
    def from_environment(cls, environment: Mapping[str, str], **kwargs: Any) -> AccountRouter:
        if not environment.get("SDO_CLAUDE_ACCOUNTS_DIR") or not environment.get("SDO_OPUS_POOL_DIR"):
            raise AccountExhausted("account references and host pool are required", retryable=False)
        return cls(
            Path(environment["SDO_CLAUDE_ACCOUNTS_DIR"]).resolve(),
            Path(environment["SDO_OPUS_POOL_DIR"]).resolve(),
            environment.get("SDO_CLAUDE_ACCOUNT_PRIMARY", ""),
            int(environment.get("SDO_CLAUDE_ACCOUNT_MAX_SESSIONS", "2")),
            float(environment.get("SDO_CLAUDE_ACCOUNT_COOLDOWN_SECONDS", "300")),
            **kwargs,
        )

    def _state(self, account: Account, *, failure: str = "") -> bool:
        # Persist in the locked inode. Atomic replacement would split hard-link
        # aliases used by app-specific kind extraMounts and multiply capacity.
        try:
            handle = (self.pool_directory / account.alias / "health.lock").open("r+")
        except OSError:
            raise AccountExhausted("account pool not provisioned", retryable=False) from None
        with handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            raw = handle.read()
            complete = [line for line in raw.splitlines(keepends=True) if line.endswith("\n")]
            try:
                if not complete:
                    raise ValueError
                generations: dict[str, dict[str, Any]] = json.loads(complete[-1])
                state = AccountHealth(**generations.get(account.generation, {}))
            except (ValueError, TypeError, AttributeError):
                raise AccountExhausted("account health state unreadable", retryable=False) from None
            old_status = state.status if account.generation in generations else None
            now = self.clock()
            if state.status == "cooldown" and state.until <= now:
                state.status, state.reason, state.until = "healthy", "cooldown_elapsed", 0
            if account.disabled and state.reason != "auth_failed":
                state.status, state.reason, state.until = "disabled", "registration_disabled", 0
            elif not account.disabled and state.status == "disabled" and state.reason == "registration_disabled":
                state.status, state.reason, state.until = "healthy", "registered", 0
            if account.unavailable_until > now and state.status != "disabled":
                state.status, state.reason = "cooldown", "quota_exhausted"
                state.until = max(account.unavailable_until, state.until)
            if failure and not (state.status == "disabled" and failure != "auth_failed"):
                state.status = "disabled" if failure == "auth_failed" else "cooldown"
                state.reason = failure
                state.until = 0 if failure == "auth_failed" else max(state.until, now + self.cooldown_seconds)
            if old_status != state.status:
                state.transition += 1
                identity = f"{account.alias}:{account.generation}:{state.transition}:{state.status}"
                transition_id = hashlib.sha256(identity.encode()).hexdigest()
                post = {
                    "schema": "sdo.account-state/v1",
                    "transition_id": transition_id,
                    "account_alias": account.alias,
                    "state": state.status,
                    "reason": state.reason,
                    "recorded_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                }
                # Append before state, with a stable ID checked under the shared
                # outbox lock. A crash between the two cannot duplicate a post.
                with (self.pool_directory / "account-state.jsonl").open("a+") as output:
                    fcntl.flock(output, fcntl.LOCK_EX)
                    output.seek(0)
                    complete_posts = [line for line in output if line.endswith("\n")]
                    if not any(
                        json.loads(line).get("transition_id") == transition_id
                        for line in complete_posts
                        if line.strip()
                    ):
                        output.seek(sum(map(len, complete_posts)))
                        output.truncate()
                        output.write(json.dumps(post, sort_keys=True) + "\n")
                        output.flush()
                        os.fsync(output.fileno())
            if generations.get(account.generation) != asdict(state):
                generations[account.generation] = asdict(state)
                # Remove only a torn trailing append. Prior committed records
                # remain intact even if this write is interrupted.
                handle.seek(len("".join(complete)))
                handle.truncate()
                handle.write(json.dumps(generations, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            if state.status != "healthy" and not failure:
                self.record("account", "failover", account_alias=account.alias, reason=state.reason)
            return state.status == "healthy"

    def try_acquire(self) -> AccountLease | None:
        accounts = load_accounts(self.directory)  # Reopen projected Secret on EVERY attempt.
        if self.primary:
            index = next((i for i, entry in enumerate(accounts) if entry.alias == self.primary), None)
            if index is None:
                raise AccountExhausted("primary account is absent from inventory", retryable=False)
            accounts = accounts[index:] + accounts[:index]
        healthy = False
        for account in accounts:
            try:
                if not self._state(account):
                    continue
            except AccountExhausted:
                # One unavailable/corrupt account must not hide healthy peers.
                continue
            healthy = True
            try:
                handle = OpusPool(self.pool_directory / account.alias, self.capacity, create=False).try_acquire()
            except (OSError, ValueError):
                raise AccountExhausted("account concurrency pool unavailable", retryable=False) from None
            if handle is not None:
                try:
                    if self._state(account):
                        return AccountLease(account, handle)
                except AccountExhausted:
                    pass
                handle.close()
        if not healthy:
            raise AccountExhausted("no healthy registered account remains", retryable=False)
        return None

    def acquire(self) -> AccountLease:
        selected = self.try_acquire()
        if selected is None:
            raise AccountExhausted("healthy accounts are busy", retryable=False)
        return selected

    def environment(self, lease: AccountLease, base: Mapping[str, str]) -> dict[str, str]:
        try:
            token = (self.directory / lease.account.token_file).read_text().strip()
            if not token or any(char.isspace() for char in token):
                raise ValueError
        except (OSError, ValueError):
            raise AccountCredentialUnavailable("account credential reference unreadable") from None
        # Explicitly blank competing authentication. Claude must use this lease.
        return {
            **base,
            "CLAUDE_CODE_OAUTH_TOKEN": token,
            "ANTHROPIC_API_KEY": "",
            "ANTHROPIC_AUTH_TOKEN": "",
            "CLAUDE_CODE_API_KEY": "",
            "ANTHROPIC_BASE_URL": "https://api.anthropic.com",
            "ANTHROPIC_CUSTOM_HEADERS": "",
            "CLAUDE_CODE_USE_BEDROCK": "",
            "CLAUDE_CODE_USE_VERTEX": "",
            "CLAUDE_CODE_USE_FOUNDRY": "",
        }

    def fail(self, lease: AccountLease, reason: str) -> None:
        if reason not in ("auth_failed", "rate_limited", "credential_unavailable"):
            raise ValueError("invalid account failure reason")
        current = next((entry for entry in load_accounts(self.directory) if entry.alias == lease.alias), None)
        if current is None or current.generation != lease.account.generation:
            return  # A stopped old turn cannot disable a newly registered credential.
        self._state(lease.account, failure=reason)
        self.record("account", "failover", account_alias=lease.alias, reason=reason)


def prepare_accounts(directory: Path, pool: Path, aliases: list[Path], *, capacity: int = 2) -> None:
    """Provision the same account lock/state inodes through all existing mounts."""
    pool.mkdir(parents=True, exist_ok=True)
    posts = pool / "account-state.jsonl"
    posts.touch(exist_ok=True)
    for alias in aliases:
        alias.mkdir(parents=True, exist_ok=True)
        if not (alias / posts.name).exists():
            os.link(posts, alias / posts.name)
        elif not posts.samefile(alias / posts.name):
            raise ValueError("account posts alias uses a different inode")
    for account in load_accounts(directory):
        prepare_host_pool(pool / account.alias, [alias / account.alias for alias in aliases], capacity=capacity)
        state = pool / account.alias / "health.lock"
        try:
            with state.open("x") as stream:
                stream.write("{}\n")
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            pass
        for alias in aliases:
            target = alias / account.alias / state.name
            if not target.exists():
                os.link(state, target)
            elif not state.samefile(target):
                raise ValueError("account health alias uses a different inode")


def _private_write(path: Path, content: str) -> None:
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=".account-")
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _cswap_token(source: Path) -> str | None:
    try:
        credentials = json.loads(base64.b64decode(source.read_bytes(), validate=True))
        token: object = credentials["claudeAiOauth"]["accessToken"]
        if isinstance(token, str) and token and not any(char.isspace() for char in token):
            return token
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def sync_cswap(
    store: Path, directory: Path, listing: Mapping[str, Any], *, lanes: list[str], capacity: int = 2
) -> dict[str, Any]:
    """Linux cswap 0.28 file backend; never consult the unmanaged current login.

    `listing` comes from captured `cswap list --json`; usage API 403 is unknown,
    not an auth failure. Only measured exhausted 5h/7d windows exclude accounts.
    """
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    previous = (
        {entry.alias: entry for entry in load_accounts(directory)} if (directory / "accounts.json").exists() else {}
    )
    try:
        registry: dict[str, Any] = (
            json.loads((store / "sequence.json").read_text())
            if (store / "sequence.json").exists()
            else {"accounts": {}}
        )
        usage = {str(entry["number"]): entry for entry in listing["accounts"]}
        accounts: list[Account] = []
        now = datetime.now(timezone.utc).timestamp()
        registrations_path = directory / "registrations.json"
        old_registrations: dict[str, str] = (
            json.loads(registrations_path.read_text()) if registrations_path.exists() else {}
        )
        registrations: dict[str, str] = {}
        registered = cast("dict[str, dict[str, Any]]", registry["accounts"])
        for slot, entry in sorted(registered.items(), key=lambda pair: int(pair[0])):
            alias = f"a{int(slot):02d}"
            email = entry["email"]
            if Path(email).name != email or not _ALIAS.fullmatch(alias):
                raise ValueError
            source = store / "credentials" / f".creds-{slot}-{email}.enc"
            token = _cswap_token(source)
            target = directory / f"{alias}.token"
            same = (target.exists() and target.read_text() == token) if token is not None else not target.exists()
            try:
                stamp = source.stat()
                identity = f"{stamp.st_dev}:{stamp.st_ino}:{stamp.st_mtime_ns}:{stamp.st_ctime_ns}"
            except OSError:
                identity = "unavailable"
            registrations[alias] = identity
            # cswap rewrites `added` only for explicit add/add-token. Routine
            # OAuth refresh must not clear a human-required auth quarantine.
            if entry.get("added"):
                identity = hashlib.sha256(
                    json.dumps([entry["added"], entry.get("uuid"), email, entry.get("organizationUuid")]).encode()
                ).hexdigest()
                registrations[alias] = identity
                unchanged = old_registrations.get(alias) == identity
            else:
                unchanged = same and old_registrations.get(alias) == identity
            generation = previous[alias].generation if unchanged and alias in previous else uuid4().hex
            row = usage.get(str(slot), {})
            until = 0.0
            for name in ("fiveHour", "sevenDay"):
                window = cast("dict[str, Any]", row.get("usage") or {}).get(name)
                if not isinstance(window, dict):
                    continue
                window = cast("dict[str, Any]", window)
                if window.get("pct", 0) >= 100:
                    reset = window.get("resetsAt")
                    until = max(until, datetime.fromisoformat(reset).timestamp() if reset else now + 300)
            accounts.append(Account(alias, generation, target.name, until, token is None or bool(row.get("disabled"))))
            if token is not None:
                _private_write(target, token)
            else:
                target.unlink(missing_ok=True)
        _private_write(registrations_path, json.dumps(registrations, sort_keys=True))
        _private_write(
            directory / "accounts.json",
            json.dumps({"accounts": [vars(account) for account in accounts]}, sort_keys=True),
        )
    except (OSError, ValueError, TypeError, KeyError):
        raise ValueError("cswap inventory or stored credential is unreadable; no credential data emitted") from None
    eligible = [account.alias for account in accounts if not account.disabled and account.unavailable_until <= now]
    return {
        "accounts": [
            {
                "alias": account.alias,
                "state": "healthy" if account.alias in eligible else "unhealthy",
                "unavailable_until": account.unavailable_until,
            }
            for account in accounts
        ],
        "lanes": {lane: eligible[index % len(eligible)] for index, lane in enumerate(lanes)} if eligible else {},
        "capacity_per_account": capacity,
    }
