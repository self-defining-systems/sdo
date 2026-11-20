//go:build integration

package bench

import (
	"os"
	"strings"
	"testing"
)

// A spec whose lane asks for more cores than any of its hosts' lane range holds is refused when it is added, naming
// the host and the range, instead of building lanes that wait for cores that do not exist.
func TestAddRefusesALaneLargerThanEveryHostsRange(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.extra["h1"] = `, lanes: {cpus: "0-3"}`
	l.writeInventory()
	err := l.addErr("e1", []string{"h1"}, "a@x", 0)
	if err == nil || !strings.Contains(err.Error(), "h1") || !strings.Contains(err.Error(), "0-3") {
		t.Fatalf("add error %v, want a capacity refusal naming h1 and 0-3", err)
	}
	if _, serr := os.Stat(l.root.SpecPath("e1")); serr == nil {
		t.Fatal("the refused spec was added")
	}
}
