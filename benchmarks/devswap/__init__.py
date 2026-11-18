"""Benchmark-neutral compiled swap: run a revision's Go operator on a prepared, dev-mounted lane without an image build.

Owned by neither ``benchmarks.fuzzops`` nor ``benchmarks.sregym``; both call it. Entry points: ``swap.swap_compiled``
(function; callbacks for a benchmark's own steps) and ``python -m benchmarks.devswap`` (``cli``). The fuzzops hooks'
``swap`` verb is one caller.
"""
