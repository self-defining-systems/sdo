"""Social Network user ids must survive the nginx gateway's double parse.

The Lua handlers read ``user_id`` from the login token with ``tonumber``, an IEEE
double. The user service mints ids as machine|millisecond|per-millisecond counter,
so ids minted in one millisecond must stay distinct and exact as doubles.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HANDLER = ROOT / "apps/social_network/source-overlays/src/UserService/UserHandler.h"
COUNTER_BITS = 12
TIMESTAMP_BITS = 40


def _mask() -> int:
    match = re.search(r"int64_t user_id = stoul\(user_id_str, nullptr, 16\) & (0x[0-9A-Fa-f]+);", HANDLER.read_text())
    assert match is not None, "the overlay must keep the user_id composition line"
    return int(match[1], 16)


def _mint(machine: int, timestamp: int, counter: int) -> int:
    value = (machine << (TIMESTAMP_BITS + COUNTER_BITS)) | (timestamp << COUNTER_BITS) | counter
    return value & _mask()


def test_ids_minted_in_one_millisecond_are_exact_and_distinct_after_the_gateway_parse() -> None:
    for machine in (0x000, 0x001, 0x7FF, 0xFFF):
        ids = [_mint(machine, (1 << TIMESTAMP_BITS) - 1, counter) for counter in range(1 << COUNTER_BITS)]
        assert len(set(ids)) == len(ids)
        assert all(int(float(value)) == value for value in ids)
        assert len({float(value) for value in ids}) == len(ids)


def test_mask_keeps_the_timestamp_and_counter_bits() -> None:
    assert _mask() & ((1 << (TIMESTAMP_BITS + COUNTER_BITS)) - 1) == (1 << (TIMESTAMP_BITS + COUNTER_BITS)) - 1
    assert _mask() < 1 << 53
