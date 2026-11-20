//go:build integration

package bench

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A ledger entry is held only while its owner exists: a lane that left hosts.yaml, or whose cluster was deleted,
// gives its cores back on the manager's tick, after a short grace so a cluster still being created is not reaped.

type ledgerClock struct{ now time.Time }

func (c *ledgerClock) Now() time.Time { return c.now }

func ledgerAlloc(t *testing.T, clk *ledgerClock, live map[string]bool) CPUAllocator {
	t.Helper()
	a := allocFor(t, topo(8, 1), "0-7")
	a.Now = clk.Now
	a.Live = func() (map[string]bool, error) { return live, nil }
	return a
}

func TestReconcileReleasesALaneThatLeftTheInventory(t *testing.T) {
	clk := &ledgerClock{t0}
	live := map[string]bool{"gone": true, "kept": true}
	a := ledgerAlloc(t, clk, live)
	for _, l := range []string{"gone", "kept"} {
		if _, err := a.Take(l, 4, 0); err != nil {
			t.Fatal(err)
		}
		must(t, a.MarkCreated(l))
	}
	clk.now = clk.now.Add(ledgerGrace + time.Second)
	released, err := a.ReconcileLanes(map[string]bool{"kept": true}, ledgerGrace)
	must(t, err)
	if !reflect.DeepEqual(released, []string{"gone"}) {
		t.Fatalf("released %v, want [gone]", released)
	}
	// The freed cores serve a new lane at once, with no `ledger prune`.
	if _, err := a.Take("new", 4, 0); err != nil {
		t.Fatalf("a new lane waits although the removed lane's cores were released: %v", err)
	}
}

func TestReconcileReleasesADeletedClusterAfterTheGrace(t *testing.T) {
	clk := &ledgerClock{t0}
	live := map[string]bool{"c1": true}
	a := ledgerAlloc(t, clk, live)
	if _, err := a.Take("c1", 8, 0); err != nil {
		t.Fatal(err)
	}
	must(t, a.MarkCreated("c1"))
	known := map[string]bool{"c1": true}
	delete(live, "c1") // the cluster is deleted
	if got, _ := a.ReconcileLanes(known, ledgerGrace); len(got) != 0 {
		t.Fatalf("released %v inside the grace period", got)
	}
	clk.now = clk.now.Add(ledgerGrace + time.Second)
	got, err := a.ReconcileLanes(known, ledgerGrace)
	must(t, err)
	if !reflect.DeepEqual(got, []string{"c1"}) {
		t.Fatalf("released %v after the grace, want [c1]", got)
	}
	if _, err := a.Take("c2", 8, 0); err != nil && errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("the deleted cluster's 8 cores are still held: %v", err)
	}
}

func TestReconcileKeepsAnEntryUnderConstruction(t *testing.T) {
	clk := &ledgerClock{t0}
	a := ledgerAlloc(t, clk, map[string]bool{}) // no cluster yet
	if _, err := a.Take("c1", 4, 0); err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{"c1": true}
	clk.now = clk.now.Add(10 * time.Minute) // past the grace, still being built (never marked created)
	got, err := a.ReconcileLanes(known, ledgerGrace)
	must(t, err)
	if len(got) != 0 {
		t.Fatalf("released %v: a build in progress lost its cores", got)
	}
	held, _ := a.Held()
	if _, ok := held["c1"]; !ok {
		t.Fatal("entry gone")
	}
}

func TestReconcileWithUnknownClustersStillReleasesRemovedLanes(t *testing.T) {
	clk := &ledgerClock{t0}
	a := ledgerAlloc(t, clk, nil)
	a.Live = func() (map[string]bool, error) { return nil, errors.New("kind: not available") }
	if _, err := a.Take("gone", 2, 0); err != nil {
		t.Fatal(err)
	}
	clk.now = clk.now.Add(ledgerGrace + time.Second)
	got, _ := a.ReconcileLanes(map[string]bool{}, ledgerGrace)
	if !reflect.DeepEqual(got, []string{"gone"}) {
		t.Fatalf("released %v, want [gone]", got)
	}
}

func TestLedgerReconcileVerbPrintsTheReleasedLanes(t *testing.T) {
	var out, errw bytes.Buffer
	dir := t.TempDir()
	a := CPUAllocator{Dir: dir, Allowed: []int{0, 1}, Topo: topo(2, 1), Now: func() time.Time { return t0 }}
	if _, err := a.Take("old", 1, 0); err != nil {
		t.Fatal(err)
	}
	// The entry is young: the verb with --grace 0 releases it, the lanes named after `reconcile` are kept.
	if code := LedgerCommand([]string{"--state", dir, "--grace", "0s", "reconcile", "other"}, &out, &errw); code != 0 {
		t.Fatalf("exit %d: %s", code, errw.String())
	}
	if strings.TrimSpace(out.String()) != `["old"]` {
		t.Fatalf("output %q, want [\"old\"]", out.String())
	}
}

// The manager's tick asks every host with lane limits to reconcile its ledger against the inventory, and `status`
// counts what was released.
func TestManagerReconcilesTheLedgerEachTickAndCountsReleases(t *testing.T) {
	l := newFlab(t, "h1=c1,c2")
	l.extra["h1"] = `, lanes: {cpus: "0-7"}`
	l.writeInventory()
	l.w.ledgerReleased = map[string][]string{"h1": {"c9"}}
	l.pass()
	if len(l.w.ledgerReqs) != 1 {
		t.Fatalf("%d reconciliations after one pass, want 1", len(l.w.ledgerReqs))
	}
	if got := l.w.ledgerReqs[0].Lanes; !reflect.DeepEqual(got, []string{"c1", "c2"}) {
		t.Fatalf("the host was told the inventory is %v, want [c1 c2]", got)
	}
	l.advance(time.Second) // inside the interval: not asked again
	if len(l.w.ledgerReqs) != 1 {
		t.Fatalf("reconciled again within %v", ledgerEvery)
	}
	l.advance(2 * ledgerEvery)
	if len(l.w.ledgerReqs) != 2 {
		t.Fatalf("%d reconciliations, want 2", len(l.w.ledgerReqs))
	}
	var out bytes.Buffer
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "2 stale entries released") {
		t.Fatalf("status does not count the releases:\n%s", out.String())
	}
	if !strings.Contains(readString(l.root.path("bench.log")), "lane ledger released 1 entr") {
		t.Fatalf("release not logged:\n%s", readString(l.root.path("bench.log")))
	}
}
