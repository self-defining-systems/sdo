//go:build integration

package bench

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunPhaseReadsFuzzopsRunOutput(t *testing.T) {
	for line, want := range map[string]string{
		"19:54:17 [fg1] ready gate: journeys clean for 2m0s (at most 20m0s)": PhaseGate,
		"19:55:37 [fg1] ready gate: journeys: 1 bad seconds":                 PhaseGate,
		"20:00:43 [fg1] inject gitaly-repository-lost@dependency:sts/x:8075": PhaseWindow,
		"20:06:16 [fg1] undo gitaly-repository-lost@dependency:sts/x:8075":   PhaseUndo,
		"20:00:44 [fg1] operator noop until 20:06:13 at the latest":          "",
		"20:11:06 [fg1] done: window 330s bad=110s(+0)":                      "",
		"04:11:09 fuzzops recover --namespace fg1":                           "",
	} {
		if got := runPhase(line); got != want {
			t.Errorf("%q: %q, want %q", line, got, want)
		}
	}
}

func TestPhaseWriterRecordsEachNewPhaseOnce(t *testing.T) {
	r := &stepRunner{dir: t.TempDir()}
	w := &phaseWriter{r: r}
	read := func() Phase {
		var p Phase
		_ = readJSON(filepath.Join(r.dir, phaseFile), &p)
		return p
	}
	_, _ = w.Write([]byte("10:00:00 [ns] ready gate: journeys clean\n10:00:05 [ns] inj"))
	if p := read(); p.Name != PhaseGate {
		t.Fatalf("phase %+v, want gate", p)
	}
	_, _ = w.Write([]byte("ect f@x\n"))
	first := read()
	if first.Name != PhaseWindow {
		t.Fatalf("phase %+v, want window", first)
	}
	time.Sleep(5 * time.Millisecond)
	_, _ = w.Write([]byte("10:00:06 [ns] operator noop until 10:05:00\n10:00:07 [ns] inject again\n"))
	if p := read(); !p.Since.Equal(first.Since) {
		t.Fatalf("a repeated phase moved its start: %+v then %+v", first, p)
	}
}

// The probe reports the phase of a running step; a finished step has none.
func TestProbeReportsTheRunningStepsPhase(t *testing.T) {
	staging := t.TempDir()
	dir := filepath.Join(staging, "c1", "s1")
	must(t, os.MkdirAll(dir, 0o755))
	must(t, writeJSON(filepath.Join(dir, "step.json"), Step{ID: "s1"}))
	r := &stepRunner{dir: dir}
	r.phase(PhaseWindow, "")
	p := ProbeStaging(staging, map[string]string{"c1": "s1"}, nil)
	if p.Steps["c1"] != StepRunning || p.Phases["c1"] == nil || p.Phases["c1"].Name != PhaseWindow {
		t.Fatalf("probe %+v phases %+v", p.Steps, p.Phases)
	}
	must(t, writeJSON(filepath.Join(dir, "manifest.json"), []ManifestEntry{}))
	if p := ProbeStaging(staging, map[string]string{"c1": "s1"}, nil); p.Phases["c1"] != nil {
		t.Fatalf("a finished step has a phase: %+v", p.Phases["c1"])
	}
}

// The settle waits for the cell's own incident (the one detected at BENCH_OWN_DETECTED_AT) and only records another.
func TestSettleIgnoresAnotherIncidentAndRecordsIt(t *testing.T) {
	dir := t.TempDir()
	hook := filepath.Join(dir, "hook.sh")
	script := `#!/bin/sh
[ "$1" = idle ] || exit 0
if [ -n "$BENCH_OWN_DETECTED_AT" ]; then echo "ignored: incident open other-1"; exit 0; fi
echo "alarm: incident open other-1"; exit 1
`
	if err := os.WriteFile(hook, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	p := &Prepared{Hooks: "t", HooksPath: dir, Command: hook}
	// The cap is measured from injectAt, so a test says "the cap has passed" by
	// putting injectAt before it and "it has not" with a cap no loaded host
	// reaches; no case depends on how long a hook process takes to start.
	run := func(own time.Time, capped bool) *stepRunner {
		r := &stepRunner{dir: t.TempDir(), poll: time.Millisecond, hookLog: io.Discard}
		r.s.Cap = Seconds(time.Hour.Seconds())
		injectAt := time.Now()
		if capped {
			injectAt = injectAt.Add(-2 * time.Hour)
		}
		r.settle(context.Background(), p, injectAt, own)
		return r
	}
	if r := run(time.Now(), false); r.res.Capped || len(r.res.OtherIncidents) != 1 || r.res.OtherIncidents[0] != "incident open other-1" {
		t.Errorf("own known: capped=%v others=%v", r.res.Capped, r.res.OtherIncidents)
	}
	if r := run(time.Time{}, true); !r.res.Capped {
		t.Error("own unknown: any open incident must still hold the settle")
	}
}
