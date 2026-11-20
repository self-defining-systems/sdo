//go:build integration

package bench

import (
	"os"
	"strings"
	"testing"
)

// LB19: a memory with learned Go code prints validator progress (log INFO lines) before its diagnostics; progress is
// never a diagnostic. Fixture: real `sdo-operator memory migrate` output of a learned bundle (telemetry unbound only).
func TestJudgeMemoryIgnoresProgressLines(t *testing.T) {
	raw, err := os.ReadFile("testdata/memory-migrate-learned.out")
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	commit := "39a17a7595c1eeddf580b3ed89cdd6682ce01bb1"
	if ok, d := judgeMemory(out, nil, commit, []string{"telemetry.unbound"}); !ok {
		t.Fatalf("learned memory with only allowed diagnostics must pass: %s", d)
	}
	if ok, _ := judgeMemory(out, nil, commit, nil); ok {
		t.Fatal("unallowed diagnostics must still fail")
	}
	bad := strings.Replace(out, "EXIT 1", "CHECK 2026/11/20 17:35:01 ERROR vet failed\nEXIT 1", 1)
	if ok, _ := judgeMemory(bad, nil, commit, []string{"telemetry.unbound"}); ok {
		t.Fatal("an ERROR log line is a diagnostic")
	}
	progressOnly := "HEAD abc123456789\nCHECK 2026/11/20 17:34:51 INFO memory_check_stage_started stage=\"mod tidy\"\nEXIT 0\n"
	if ok, d := judgeMemory(progressOnly, nil, "abc123456789", nil); !ok {
		t.Fatalf("progress only: %s", d)
	}
	if ok, _ := judgeMemory(strings.Replace(progressOnly, "EXIT 0", "EXIT 1", 1), nil, "abc123456789", nil); ok {
		t.Fatal("nonzero exit with no diagnostic must fail")
	}
}
