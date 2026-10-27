"""Secrets that a benchmark fault's own injector writes and reads back (benchmark-only metadata).

The lane guard fences this harness's reads of Secret data. A fault that rotates a Secret has its
SREGym injector read the Secret back inside the SREGym worker. Each such fault class is declared
here with the Secrets it owns in the application namespace; the lane guard lets the worker's
inject, recover and refresh calls read only those. Harness and SDO code never read the data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

INJECTOR_SECRETS: dict[str, frozenset[str]] = {
    "SecretRotationStaleEnvCredentialsAstronomyShop": frozenset({"product-catalog-db-conn"}),
}


def declared_secrets(problems: Iterable[Any]) -> frozenset[str]:
    """The Secret names declared by these SREGym problem objects (a composite passes its components too)."""
    return frozenset().union(*(INJECTOR_SECRETS.get(type(problem).__name__, frozenset()) for problem in problems))
