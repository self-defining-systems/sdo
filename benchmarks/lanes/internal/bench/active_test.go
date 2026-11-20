//go:build integration

package bench

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// docs/contract.md: `run active` and `run undo` answer {"active": bool} on their last stdout line. A workload whose
// fault is gone after the run leaves the lane up; one whose fault stays active takes it down, naming the fault.
func TestSafetyReadsActiveFromTheResultLine(t *testing.T) {
	for _, tc := range []struct {
		name, script string
		down         bool
	}{
		{"inactive", `case "$WORKLOAD_ACTION" in active|undo) echo '{"active": false}';; esac`, false},
		{"active", `case "$WORKLOAD_ACTION" in active|undo) echo '{"active": true}';; esac`, true},
		{"no answer", `case "$WORKLOAD_ACTION" in active|undo) echo '{}';; esac`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRunner(t, presentCluster)
			r.wl = adapter(t, tc.script)
			p := &Prepared{Namespace: "ns", AppPack: "pack"}
			r.safety(context.Background(), p, &RunSpec{ID: "r1", Fault: "f@x", Instances: "i.json"})
			if got := r.res.LaneDown != ""; got != tc.down {
				t.Fatalf("lane down %v (%q), want %v", got, r.res.LaneDown, tc.down)
			}
		})
	}
}

// The built-in fuzzops workload keeps printing `active=<bool>`; its call turns that into the structured answer.
func TestFuzzopsActiveTextBecomesTheStructuredAnswer(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\necho 'instance f@x active=true (checked)'\n"
	if err := os.WriteFile(filepath.Join(dir, "bin", "fuzzops"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	code, _, err := NewWorkload("", dir).Call(context.Background(), WorkloadCall{Verb: VerbRun, Action: "active", Stdout: &out, Log: &bytes.Buffer{}})
	if err != nil || code != 0 {
		t.Fatal(code, err)
	}
	if got := strings.TrimSpace(out.String()); got != `{"active":true}` {
		t.Fatalf("got %q", got)
	}
}
