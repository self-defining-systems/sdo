"""Failures owned by the structured coding-agent boundary."""


class StructuredTurnError(RuntimeError):
    """A structured turn failed or returned something unusable."""


class ProviderRateLimited(StructuredTurnError):
    """Provider capacity prevented execution; never an agent-quality failure.

    The diagnostic is deliberately values-free: CLI streams can contain tools,
    prompts and credentials and must not enter capacity telemetry.
    """

    def __init__(
        self, reason: str = "provider retry budget exhausted", *, session_id: str = "", retryable: bool = True
    ) -> None:
        self.retryable = retryable
        self.session_id = session_id
        super().__init__(f"rate_limited: {reason}")


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
