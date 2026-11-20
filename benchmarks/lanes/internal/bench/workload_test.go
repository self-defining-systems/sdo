//go:build integration

package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The built-in fuzzops workload runs the command lines the bench has always run: every call site's old bin/fuzzops
// argv is what the verb, action and options map to.
func TestFuzzopsWorkloadKeepsEveryCommandLine(t *testing.T) {
	for _, tc := range []struct {
		call WorkloadCall
		want []string
	}{
		{WorkloadCall{Verb: VerbIdentity}, []string{"version"}},
		{WorkloadCall{Verb: VerbAppUp, Action: "check-pack", Args: []string{"--pack", "p"}}, []string{"pack", "load", "--pack", "p"}},
		{WorkloadCall{Verb: VerbAppUp, Action: "down", Args: []string{"--pack", "p", "--namespace", "n"}}, []string{"app", "down", "--pack", "p", "--namespace", "n"}},
		{WorkloadCall{Verb: VerbAppUp, Action: "up", Args: []string{"--pack", "p", "--namespace", "n"}}, []string{"app", "up", "--pack", "p", "--namespace", "n"}},
		{WorkloadCall{Verb: VerbAppUp, Action: "images", Args: []string{"--pack", "p", "--cluster", "c"}}, []string{"app", "images", "--pack", "p", "--cluster", "c"}},
		{WorkloadCall{Verb: VerbAppSnapshot, Action: "write", Args: []string{"--out", "f"}}, []string{"app", "fingerprint", "--out", "f"}},
		{WorkloadCall{Verb: VerbAppSnapshot, Action: "verify", Args: []string{"--against", "f"}}, []string{"app", "fingerprint", "--against", "f"}},
		{WorkloadCall{Verb: VerbAppReset, Action: "recover", Args: []string{"--namespace", "n"}}, []string{"recover", "--namespace", "n"}},
		{WorkloadCall{Verb: VerbGate, Action: "check", Args: []string{"--clean"}}, []string{"check", "--clean"}},
		{WorkloadCall{Verb: VerbRun, Action: "run", Args: []string{"--skip-reset"}}, []string{"run", "--skip-reset"}},
		{WorkloadCall{Verb: VerbRun, Action: "active", Args: []string{"--id", "x"}}, []string{"active-instance", "--id", "x"}},
		{WorkloadCall{Verb: VerbRun, Action: "undo", Args: []string{"--id", "x"}}, []string{"undo-instance", "--id", "x"}},
		{WorkloadCall{Verb: VerbGrade, Action: "report", Args: []string{"d"}}, []string{"report", "d"}},
	} {
		got, err := fuzzopsArgv(tc.call)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s/%s: %v %v, want %v", tc.call.Verb, tc.call.Action, got, err, tc.want)
		}
	}
	for _, c := range []WorkloadCall{{Verb: VerbFootprint}, {Verb: "no-such-verb"}} {
		if _, err := fuzzopsArgv(c); !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s: %v, want unsupported", c.Verb, err)
		}
	}
}

func adapter(t *testing.T, script string) Workload {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "wl.sh"), []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return NewWorkload("./wl.sh", dir)
}

// An adapter executable gets the call in its environment, and its result is the last stdout line.
func TestExecWorkloadEnvInLastLineOut(t *testing.T) {
	w := adapter(t, `echo progress; echo "{\"verb\":\"$WORKLOAD_VERB\",\"action\":\"$WORKLOAD_ACTION\",\"args\":$WORKLOAD_ARGS,\"x\":\"$X\"}"`)
	var out bytes.Buffer
	code, _, err := w.Call(context.Background(), WorkloadCall{Verb: VerbGate, Action: "check", Args: []string{"--a", "b c"}, Env: []string{"X=1"}, Stdout: &out, Log: &bytes.Buffer{}})
	if err != nil || code != 0 {
		t.Fatal(code, err)
	}
	if got, want := strings.TrimSpace(out.String()), `{"verb":"gate","action":"check","args":["--a","b c"],"x":"1"}`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// An adapter that wraps the built-in workload is told the command line the built-in one would run, for the calls it
// maps; a call it does not map carries no such variable.
func TestExecWorkloadIsToldTheBuiltinCommandLine(t *testing.T) {
	w := adapter(t, `echo "{\"builtin\":${WORKLOAD_BUILTIN:-null}}"`)
	var out bytes.Buffer
	if _, _, err := w.Call(context.Background(), WorkloadCall{Verb: VerbRun, Action: "run", Args: []string{"--seed", "3"}, Stdout: &out, Log: &bytes.Buffer{}}); err != nil {
		t.Fatal(err)
	}
	var got struct{ Builtin []string }
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || len(got.Builtin) != 4 || !strings.HasSuffix(got.Builtin[0], "/bin/fuzzops") || strings.Join(got.Builtin[1:], " ") != "run --seed 3" {
		t.Fatalf("builtin %v (%v)", got.Builtin, err)
	}
	out.Reset()
	if _, _, err := w.Call(context.Background(), WorkloadCall{Verb: VerbFootprint, Stdout: &out, Log: &bytes.Buffer{}}); err != nil || strings.TrimSpace(out.String()) != `{"builtin":null}` {
		t.Fatalf("unmapped call: %q %v", out.String(), err)
	}
}

// A verb the adapter does not implement is "unsupported" by exit 64 or by an {"unsupported":true} line, not a failure.
func TestExecWorkloadUnsupportedVerbs(t *testing.T) {
	for name, script := range map[string]string{"exit": `exit 64`, "line": `echo '{"unsupported":true}'`} {
		w := adapter(t, script)
		_, _, err := w.Call(context.Background(), WorkloadCall{Verb: VerbFootprint, Log: &bytes.Buffer{}})
		if !errors.Is(err, ErrUnsupported) {
			t.Errorf("%s: %v", name, err)
		}
	}
	w := adapter(t, `echo oops >&2; exit 3`)
	code, tail, err := w.Call(context.Background(), WorkloadCall{Verb: VerbGate, Action: "check", Log: &bytes.Buffer{}})
	if err != nil || code != 3 || tail != "oops" {
		t.Fatalf("a failing verb is a result, not unsupported: %d %q %v", code, tail, err)
	}
}

func TestSubjectVerbsAreBoundedAndUnsupportedIsRecognized(t *testing.T) {
	for _, v := range SubjectVerbs {
		if v != "watch" && hookTimeouts[v] == 0 {
			t.Errorf("subject verb %s has no time limit", v)
		}
	}
	for name, tc := range map[string]struct {
		r    HookResult
		want bool
	}{
		"exit 64":       {HookResult{Exit: 64}, true},
		"json":          {HookResult{Last: `{"unsupported": true}`}, true},
		"usage error":   {HookResult{Exit: 2}, true},
		"failed verb":   {HookResult{Exit: 1, First: "boom", Last: "boom"}, false},
		"exit 2 output": {HookResult{Exit: 2, First: "bad"}, false},
		"fine":          {HookResult{Last: `{"pack":"p"}`}, false},
	} {
		if got := tc.r.Unsupported(); got != tc.want {
			t.Errorf("%s: %v", name, got)
		}
	}
}

// A hooks checkout that implements `preflight` answers for itself; one that does not falls back to the SDO checks.
func TestParseHookPreflight(t *testing.T) {
	checks, ok, err := parseHookPreflight("RC 0\nFIRST x\nLAST {\"checks\":[{\"check\":\"cred\",\"ok\":true,\"detail\":\"fine\"},{\"check\":\"net\",\"ok\":false,\"detail\":\"down\"}]}\n", nil)
	if err != nil || !ok || len(checks) != 2 || checks[1].OK || checks[0].Check != "cred" {
		t.Fatalf("%+v %v %v", checks, ok, err)
	}
	for name, out := range map[string]string{"usage": "RC 2\nFIRST \nLAST \n", "64": "RC 64\nFIRST \nLAST \n", "line": "RC 0\nFIRST x\nLAST {\"unsupported\":true}\n"} {
		if _, ok, _ := parseHookPreflight(out, nil); ok {
			t.Errorf("%s: a hook with no preflight verb must be unsupported", name)
		}
	}
	if _, ok, err := parseHookPreflight("RC 0\nFIRST x\nLAST not json\n", nil); !ok || err == nil {
		t.Errorf("a verb that answers with no JSON is an error: %v %v", ok, err)
	}
}

func subjectLab() (Inventory, []specFile) {
	inv := Inventory{HookCommand: map[string]string{"s": "./hook.sh"}}
	inv.Checkouts.Hooks = map[string]map[string]map[string]string{"s": {"abc1234": {"h": "/hooks"}}}
	spec := Spec{Operator: Operator{Hooks: "s", Revision: "abc1234"}}
	return inv, []specFile{{Spec: spec}}
}

// A subject whose hook answers `preflight` supplies the checks; the SDO operator's are not run.
func TestHookSubjectUsesThePreflightVerb(t *testing.T) {
	inv, specs := subjectLab()
	var scripts []string
	run := func(_ context.Context, _, script string) (string, error) {
		scripts = append(scripts, script)
		return "RC 0\nFIRST x\nLAST {\"checks\":[{\"check\":\"mine\",\"ok\":true,\"detail\":\"fine\"}]}\n", nil
	}
	var got []string
	hookSubject{}.Host(context.Background(), inv, "h", specs, "", run, func(_ int, check, _ string, ok bool, f string, a ...any) {
		got = append(got, check)
	})
	if len(scripts) != 1 || !strings.Contains(scripts[0], "preflight") || !reflect.DeepEqual(got, []string{"mine"}) {
		t.Fatalf("scripts %q checks %v", scripts, got)
	}
}

// A hook with no `preflight` verb gets the SDO checks: credentials, sink and proto, as before.
func TestHookSubjectFallsBackToTheSDOChecks(t *testing.T) {
	inv, specs := subjectLab()
	run := func(_ context.Context, _, script string) (string, error) {
		if strings.Contains(script, "./hook.sh preflight") {
			return "RC 2\nFIRST \nLAST \n", nil
		}
		return "ok", nil
	}
	var got []string
	hookSubject{}.Host(context.Background(), inv, "h", specs, "", run, func(_ int, check, _ string, ok bool, f string, a ...any) {
		got = append(got, check)
	})
	joined := strings.Join(got, ",")
	if !strings.Contains(joined, "sink-claude-transcripts") || !strings.Contains(joined, "proto-binding-source") {
		t.Fatalf("checks %v", got)
	}
}
