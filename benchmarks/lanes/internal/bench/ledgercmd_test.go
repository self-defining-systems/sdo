//go:build integration

package bench

import (
	"strings"
	"testing"
)

func TestLedgerCommandShowAdoptPrune(t *testing.T) {
	dir := t.TempDir()
	var out, errw strings.Builder
	run := func(args ...string) int {
		out.Reset()
		errw.Reset()
		return LedgerCommand(append([]string{"--state", dir}, args...), &out, &errw)
	}
	if run("show") != 0 || !strings.Contains(out.String(), "no lane holds cores") {
		t.Fatalf("%q %q", out.String(), errw.String())
	}
	if run("adopt", "c2p1", "4-11", "27G") != 0 {
		t.Fatal(errw.String())
	}
	if run("show") != 0 || !strings.Contains(out.String(), "c2p1") || !strings.Contains(out.String(), "4-11") || !strings.Contains(out.String(), "27.0GiB") {
		t.Fatalf("%q", out.String())
	}
	if run("adopt", "c2p2", "x", "27G") == 0 || run("adopt", "c2p2") == 0 || run("frobnicate") == 0 {
		t.Fatal("nonsense accepted")
	}
}
