"""Failures owned by the structured coding-agent boundary."""


class StructuredTurnError(RuntimeError):
    """A structured turn failed or returned something unusable."""


class ProviderRateLimited(StructuredTurnError):
    """Provider capacity prevented execution; never an agent-quality failure.

    The diagnostic is deliberately values-free: CLI streams can contain tools,
    prompts and credentials and must not enter capacity telemetry.
    """

    def __init__(
        self,
        reason: str = "provider retry budget exhausted",
        *,
        session_id: str = "",
        retryable: bool = True,
        shell_commands: tuple[str, ...] = (),
        tool_calls: int = 0,
        usage: dict[str, int | float] | None = None,
    ) -> None:
        self.shell_commands = shell_commands
        self.tool_calls = tool_calls
        self.usage = usage or {}
        self.retryable = retryable
        self.session_id = session_id
        super().__init__(f"{self.failure_reason}: {reason}")

    failure_reason = "rate_limited"


class AccountExhausted(ProviderRateLimited):
    """No registered healthy Claude account remains; infrastructure, not agent quality."""

    failure_reason = "account_exhausted"


class AccountAuthenticationFailed(ProviderRateLimited):
    """A CLI authentication failure, carrying no provider output or credential."""

    failure_reason = "auth_failed"
    account_failure_reason = "auth_failed"


class AccountCredentialUnavailable(AccountAuthenticationFailed):
    """One account's mounted credential cannot be read; continue with peers."""

    account_failure_reason = "credential_unavailable"


def rate_limit_failure(exc: BaseException) -> ProviderRateLimited | None:
    """Preserve typed provider failure through role-specific exception wrappers."""
    seen: set[int] = set()
    cause: BaseException | None = exc
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        if isinstance(cause, ProviderRateLimited):
            return cause
        cause = cause.__cause__ or cause.__context__
    return None
