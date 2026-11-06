"""Hypothesis's one-time scan of repository modules happens at collection, not inside a test.

The slow-test budget charges a test's own time. Without the warm-up in tests/conftest.py the first property test of
a process parsed every imported repository module (about a second locally, ten on a cold runner). This pins the
observable effect; it reads Hypothesis internals because nothing public reports the scan.
"""

from __future__ import annotations

import sys

from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.internal.conjecture import providers


@settings(max_examples=1, database=None, deadline=None)
@given(st.just(None))
def test_repository_modules_were_scanned_before_the_first_property_test(value: None) -> None:
    assert value is None
    assert sys.modules[__name__] in providers._seen_modules
