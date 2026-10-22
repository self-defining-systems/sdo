"""The adapter treats Go acceptance/rejection as generated result data."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from benchmarks.sregym.protocol import _receipt_checker as checker
from benchmarks.sregym.protocol.native_receipt import NativeReceiptValidationError, validate_native_production_receipt
from sdo.contracts import ReceiptFailure


def test_checker_transports_bytes_and_decodes_typed_incident_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(checker, "_checker_binary", lambda: "/checker")

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(
            returncode=0,
            stdout='{"failure":{"incident_id":"one","code":"CODE_REJECTED_EVIDENCE","message":"health firing"}}',
        )

    monkeypatch.setattr(checker, "run", run)
    result = checker.evaluate_receipt('{"incident_id":"one"}', build=False)
    assert result.failure.code == ReceiptFailure.CODE_REJECTED_EVIDENCE
    assert result.failure.incident_id == "one"
    assert calls[0][0] == ["/checker", "validate"]
    assert calls[0][1]["input"] == '{"incident_id":"one"}'


@pytest.mark.parametrize("payload", ["not json", "{}", '{"failure":{"message":"untyped"}}'])
def test_checker_rejects_invalid_result_contract(payload: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(checker, "_checker_binary", lambda: "/checker")
    monkeypatch.setattr(checker, "run", lambda *_, **__: SimpleNamespace(returncode=0, stdout=payload))
    with pytest.raises(checker.ReceiptCheckerError):
        checker.evaluate_receipt("{}", build=False)


def test_checker_transport_failure_is_typed_and_keeps_raw_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(checker, "_checker_binary", lambda: "/checker")

    def unavailable(*_, **__):
        raise OSError("checker unavailable")

    monkeypatch.setattr(checker, "run", unavailable)
    raw = {"incident_id": "one", "native_ledger": {"malformed": [42]}}
    with pytest.raises(NativeReceiptValidationError) as raised:
        validate_native_production_receipt(raw)
    assert raised.value.failure.incident_id == "one"
    assert raised.value.failure.code == ReceiptFailure.CODE_CHECKER_UNAVAILABLE
    assert raised.value.receipt is raw
