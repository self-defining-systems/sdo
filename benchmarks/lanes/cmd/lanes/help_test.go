package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

// allVerbs is every verb run dispatches. Add a verb here when main.go gains one.
var allVerbs = []string{
	"serve", "add", "remove", "pause", "resume", "status", "results", "lane", "gc", "preflight",
	"step", "ledger", "host", "plan", "probe", "gc-staging",
}

// captureOut runs f and returns its result and what it wrote to stdout and stderr.
func captureOut(t *testing.T, f func() int) (code int, out string) {
	t.Helper()
	oo, oe := os.Stdout, os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout, os.Stderr = w, w
	done := make(chan string)
	go func() { b, _ := io.ReadAll(r); done <- string(b) }()
	code = f()
	os.Stdout, os.Stderr = oo, oe
	w.Close()
	return code, <-done
}

// Every subcommand answers --help and -h with its own usage, from an empty directory with no hosts.yaml, no root and
// no environment: help needs nothing the verb itself needs.
func TestEveryVerbPrintsItsOwnHelp(t *testing.T) {
	t.Setenv("FUZZOPS_BENCH_ROOT", "")
	t.Chdir(t.TempDir())
	for _, v := range allVerbs {
		for _, flag := range []string{"--help", "-h"} {
			code, out := captureOut(t, func() int { return run(v, []string{flag}) })
			if code != 0 {
				t.Errorf("lanes %s %s: exit %d, output %q", v, flag, code, out)
			}
			if !strings.Contains(out, "lanes "+v) {
				t.Errorf("lanes %s %s: output lacks its own name: %q", v, flag, out)
			}
			if strings.Contains(out, "usage: lanes VERB") {
				t.Errorf("lanes %s %s prints the shared verb list: %q", v, flag, out)
			}
		}
	}
}

// Every verb has a help entry, so a verb cannot be added to allVerbs (and so to run) without one.
func TestHelpTableMatchesVerbs(t *testing.T) {
	if len(helpTable) != len(allVerbs) {
		t.Errorf("helpTable has %d entries, allVerbs %d", len(helpTable), len(allVerbs))
	}
	for _, v := range allVerbs {
		if _, ok := helpTable[v]; !ok {
			t.Errorf("no help for %q", v)
		}
	}
}
