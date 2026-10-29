"""The inventory must use Go's selected and generated tests, not source heuristics."""

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.go_integration
def test_export_inventory_matches_real_race_test_listing(tmp_path: Path) -> None:
    (tmp_path / "go.mod").write_text("module fixture.test/inventory\n\ngo 1.26.0\n")
    (tmp_path / "internal_test.go").write_text("""package inventory
import ("testing"; "sync")
func CopyLock() { var left sync.Mutex; right := left; _ = right }
func TestInternal(t *testing.T) {}
func Test(t *testing.T) {}
func HelperNotATest(t *testing.T) {}
func FuzzInput(f *testing.F) {}
""")
    (tmp_path / "external_test.go").write_text("""package inventory_test
import ("fmt"; "testing")
func TestExternal(t *testing.T) {}
func Example() { fmt.Println("hello")
// Output: hello
}
func Example_withoutOutput() {}
""")
    (tmp_path / "race_test.go").write_text(
        '//go:build race\n\npackage inventory\nimport "testing"\nfunc TestRaceOnly(t *testing.T) {}\n'
    )
    (tmp_path / "plain_test.go").write_text(
        '//go:build !race\n\npackage inventory\nimport "testing"\nfunc TestPlainOnly(t *testing.T) {}\n'
    )
    env = {**os.environ, "GOFLAGS": "-p=2 -trimpath"}
    actual = subprocess.check_output(
        ["go", "run", str(ROOT / "scripts/go_test_inventory.go"), str(tmp_path)], env=env, text=True
    )
    result = subprocess.run(
        ["go", "test", "-race", "-list", ".", "-json", "./..."], cwd=tmp_path, env=env, text=True, capture_output=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
    expected = result.stdout

    def names(output: str) -> list[tuple[str, str]]:
        events = [json.loads(line) for line in output.splitlines()]
        return sorted(
            (event["Package"], event["Output"].strip())
            for event in events
            if event.get("Action") == "output"
            and event.get("Output", "").strip().startswith(("Test", "Fuzz", "Example"))
        )

    assert names(actual) == names(expected)
    assert ("fixture.test/inventory", "TestRaceOnly") in names(actual)
    assert ("fixture.test/inventory", "TestPlainOnly") not in names(actual)

    # The previous go test listing also ran vet; retain its rejection boundary.
    (tmp_path / "invalid_test.go").write_text(
        'package inventory\nimport "testing"\nfunc Testlowercase(t *testing.T) {}\n'
    )
    invalid = subprocess.run(
        ["go", "run", str(ROOT / "scripts/go_test_inventory.go"), str(tmp_path)],
        env=env,
        text=True,
        capture_output=True,
    )
    assert invalid.returncode != 0
