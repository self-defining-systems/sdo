"""Byte transport to the benchmark-only Go receipt checker."""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess
import tempfile
from functools import lru_cache
from pathlib import Path
from subprocess import run

from google.protobuf import json_format

from sdo.contracts import NativeReceiptResult, validate


class ReceiptCheckerError(RuntimeError):
    """The receipt checker transport could not complete."""


@lru_cache(maxsize=1)
def _checker_binary() -> str:
    specified = os.environ.get("SDO_NATIVE_RECEIPT_BIN", "").strip()
    if specified:
        if not Path(specified).is_absolute():
            raise ReceiptCheckerError("SDO_NATIVE_RECEIPT_BIN must be an absolute path")
        return specified
    installed = shutil.which("sdo-native-receipt")
    if installed:
        return installed
    module = Path(__file__).with_name("native_receipt_go")
    if not (module / "go.mod").is_file():
        raise ReceiptCheckerError("Go receipt checker source is unavailable; set SDO_NATIVE_RECEIPT_BIN")
    directory = Path(tempfile.mkdtemp(prefix="sdo-native-receipt-"))
    atexit.register(shutil.rmtree, directory, ignore_errors=True)
    binary = directory / "sdo-native-receipt"
    try:
        built = run(
            ["go", "build", "-o", str(binary), "./cmd/sdo-native-receipt"],
            cwd=module,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReceiptCheckerError(f"Go receipt checker could not build: {exc}") from exc
    if built.returncode:
        raise ReceiptCheckerError(f"Go receipt checker build failed: {built.stderr[-4096:]}")
    return str(binary)


def evaluate_receipt(payload: str, *, build: bool) -> NativeReceiptResult:
    try:
        completed = run(
            [_checker_binary(), "build" if build else "validate"],
            input=payload,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReceiptCheckerError(f"Go receipt checker could not run: {exc}") from exc
    if completed.returncode:
        raise ReceiptCheckerError(f"Go receipt checker failed: {completed.stderr[-4096:]}")
    try:
        result = json_format.Parse(completed.stdout, NativeReceiptResult())
    except json_format.ParseError as exc:
        raise ReceiptCheckerError(f"Go receipt checker returned invalid ProtoJSON: {exc}") from exc
    if not result.HasField("receipt") and not result.HasField("failure"):
        raise ReceiptCheckerError("Go receipt checker returned no receipt or typed failure")
    if result.HasField("failure"):
        try:
            validate(result.failure)
        except ValueError as exc:
            raise ReceiptCheckerError(f"Go receipt checker returned an invalid typed failure: {exc}") from exc
    return result
