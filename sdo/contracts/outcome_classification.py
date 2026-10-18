"""Typed interpretation of the outcome contract's string classification field.

The protobuf field remains a string to preserve stored outcome documents. Its
permitted values are checked against this enum by contract conformance tests.
"""

from enum import StrEnum


class OutcomeClassification(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    EXTERNAL_RECOVERY = "external_recovery"
    CLEARED_WITHOUT_ACTION = "cleared_without_action"
    FALSE_POSITIVE = "false_positive"
    FALSE_NEGATIVE = "false_negative"
    FAILED = "failed"
    CANCELLED = "cancelled"
