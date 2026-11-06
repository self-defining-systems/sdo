import pytest

from sdo.agents.context_output import save_context


@pytest.mark.parametrize("text", ["a\rb", "\r", "\r\n", "x\r\ny\rz\n", "tab\tnul\x00end"])
def test_saved_context_is_byte_exact_and_resaving_is_idempotent(tmp_path, monkeypatch, text):
    monkeypatch.setenv("SDO_SESSION_SCRATCH", str(tmp_path))
    path, _ = save_context(text)
    assert path.read_bytes() == text.encode()
    assert save_context(text)[0] == path
