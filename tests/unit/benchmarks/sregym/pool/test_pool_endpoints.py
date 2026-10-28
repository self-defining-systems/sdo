"""Memory endpoints: every stream app needs an explicit repository and run branch."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.pool.endpoints import MemoryEndpoint, load_memory_endpoints

if TYPE_CHECKING:
    from pathlib import Path


def write(path: Path, value: object) -> Path:
    path.write_text(json.dumps(value))
    return path


def test_stream_apps_map_to_hyphenated_endpoint_keys(tmp_path: Path) -> None:
    path = write(
        tmp_path / "memory-endpoints.json",
        {
            "hotel-reservation": {"repo": "vibesys-playground/hotel-reservation-sdo", "branch": "live/20261028-x"},
            "social-network": {"repo": "vibesys-playground/social-network-sdo", "branch": "live/20261028-x"},
        },
    )
    assert load_memory_endpoints(path, {"hotel_reservation"}) == {
        "hotel_reservation": MemoryEndpoint(repo="vibesys-playground/hotel-reservation-sdo", branch="live/20261028-x")
    }


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        (None, "no memory endpoint"),
        ({"repo": "", "branch": "live/x"}, "repo"),
        ({"repo": "vibesys-playground/toy-sdo"}, "branch"),
        ({"repo": "vibesys-playground/toy-sdo", "branch": ""}, "branch"),
        ({"repo": "vibesys-playground/toy-sdo", "branch": "main"}, "main"),
        ({"repo": "not a repo", "branch": "live/x"}, "repo"),
        ({"repo": "vibesys-playground/toy-sdo", "branch": "live/x", "broker_url": "http://x"}, "broker_url"),
    ],
)
def test_refuses_missing_or_unsafe_endpoint(tmp_path: Path, entry: object, message: str) -> None:
    path = write(tmp_path / "memory-endpoints.json", {} if entry is None else {"toy": entry})
    with pytest.raises(ValueError, match=message):
        load_memory_endpoints(path, {"toy"})


def test_refuses_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="memory endpoints"):
        load_memory_endpoints(tmp_path / "absent.json", {"toy"})
