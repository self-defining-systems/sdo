"""The derived usage models keep the cache split and the request count the price and the cache rate need."""

from __future__ import annotations

import dataclasses

from benchmarks.sregym.analysis.cells import Usage
from benchmarks.sregym.fastloop.records import TokenCounts

OUTCOME_USAGE = {
    "input_tokens": 1700,
    "uncached_input_tokens": 1000,
    "cache_read_input_tokens": 500,
    "cache_write_input_tokens": 200,
    "output_tokens": 100,
    "model_requests": 4,
    "total_cost_usd": 0.5,
}


def test_cells_usage_keeps_the_split_requests_and_cost() -> None:
    usage = Usage.from_mapping(OUTCOME_USAGE)
    assert (usage.input_tokens, usage.output_tokens, usage.cost_usd) == (1700, 100, 0.5)
    assert (usage.cache_read, usage.cache_write, usage.requests) == (500, 200, 4)
    assert usage.uncached_input == 1000
    assert usage.cache_hit_rate == 500 / 1700


def test_cells_usage_adds_every_field_and_a_cost_replacement_keeps_the_split() -> None:
    total = Usage.from_mapping(OUTCOME_USAGE) + Usage.from_mapping(OUTCOME_USAGE)
    assert (total.input_tokens, total.cache_read, total.cache_write, total.output_tokens, total.requests) == (
        3400,
        1000,
        400,
        200,
        8,
    )
    assert total.cost_usd == 1.0
    repriced = dataclasses.replace(total, cost_usd=0.25)
    assert (repriced.cache_read, repriced.requests, repriced.cost_usd) == (1000, 8, 0.25)


def test_cells_usage_without_the_new_fields_reads_as_before() -> None:
    usage = Usage.from_mapping({"input_tokens": 10, "output_tokens": 2})
    assert (usage.cache_read, usage.cache_write, usage.requests) == (0, 0, 0)
    assert Usage().cache_hit_rate is None
    assert Usage(10, 2, 0.1).tokens == 12
    assert Usage.from_mapping(None) == Usage()


def test_token_counts_keep_the_request_count() -> None:
    counts = TokenCounts.from_usage(OUTCOME_USAGE)
    assert counts.model_requests == 4
    assert counts.cached_input_tokens == 500
    assert counts.cache_write_input_tokens == 200
    total = counts + TokenCounts.from_usage({**OUTCOME_USAGE, "model_requests": 0, "llm_calls": 3})
    assert total.model_requests == 7, "llm_calls is the request count when model_requests is absent"
    assert total.uncached_input_tokens == 2 * 1000


def test_token_counts_of_older_rows_have_no_requests() -> None:
    old = TokenCounts.model_validate({"input_tokens": 5, "cached_input_tokens": 2, "output_tokens": 1})
    assert old.model_requests == 0
