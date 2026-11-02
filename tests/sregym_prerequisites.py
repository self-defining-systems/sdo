"""One collection gate for tests that consume the external pinned input tree."""

from __future__ import annotations

import pytest

from scripts.init_test_prereqs import PrerequisiteError, check_prerequisites


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "requires_sregym: consumes the committed SREGym test prerequisites")


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not any(
        item.get_closest_marker("requires_sregym") is not None
        or "committed_sregym_registry" in getattr(item, "fixturenames", ())
        for item in items
    ):
        return
    try:
        check_prerequisites(config.rootpath)
    except PrerequisiteError as error:
        raise pytest.UsageError(str(error)) from error
