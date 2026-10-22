"""The benchmark transports state bytes; Go supplies lifecycle interpretation."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import native_operator
from sdo.contracts import ControllerObservationFailure

if TYPE_CHECKING:
    from pathlib import Path


def test_observation_transports_raw_state_and_decodes_typed_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = []

    def execute(command, args, *, timeout, input_text):
        seen.append((command, args, timeout, json.loads(input_text)))
        return '{"failure":{"code":"CODE_CLOSURE_FAILED","incident_id":"two","message":"closure failed"}}'

    monkeypatch.setattr(native_operator, "execute_operator", execute)
    state = {"closure_state": "CLOSURE_STATE_FAILED"}
    observed = native_operator.observe_controller_state(state, incident_id="two", timeout=2.5)
    assert observed.failure.code == ControllerObservationFailure.CODE_CLOSURE_FAILED
    assert seen == [("observe-controller", [], 2.5, {"incident_id": "two", "state": state})]


def test_source_operator_builds_once_per_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    builds = []

    def run(command, **_kwargs):
        builds.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(native_operator.subprocess, "run", run)
    first = native_operator._source_operator(tmp_path)
    assert native_operator._source_operator(tmp_path) == first
    assert len(builds) == 1
    assert builds[0][0:2] == ["go", "build"]


@pytest.mark.parametrize("payload", ['{"failure":{"message":"untyped"}}', '{"unknown":true}'])
def test_observation_rejects_invalid_result_contract(payload: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(native_operator, "execute_operator", lambda *_, **__: payload)
    with pytest.raises(native_operator.NativeOperatorError, match="invalid ProtoJSON"):
        native_operator.observe_controller_state({})
