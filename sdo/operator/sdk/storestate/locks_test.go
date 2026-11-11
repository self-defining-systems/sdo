package storestate_test

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/storestate"
)

// withLockWaits adds a postgres lock-waits section to the healthy read.
func withLockWaits(lines ...string) string {
	return strings.Replace(healthyPostgres, "== catalog exampledb\n", "== lockwaits\n"+strings.Join(lines, "")+"== catalog exampledb\n", 1)
}

func TestPostgresAndMySQLReadLockWaits(t *testing.T) {
	f := parsePostgres(t, withLockWaits("lockwait\t4242\t10.0.0.7\tidle in transaction\tAccessExclusiveLock\tpublic.items\t3\t95\t240\n"))
	want := storestate.LockWait{Holder: "4242", Peer: "10.0.0.7", State: "idle in transaction", Mode: "AccessExclusiveLock", Object: "public.items", Waiters: 3, Waited: 95 * time.Second, Held: 240 * time.Second}
	if len(f.LockWaits) != 1 || f.LockWaits[0] != want {
		t.Fatalf("lock waits = %+v", f.LockWaits)
	}
	if f := parsePostgres(t, withLockWaits()); f.LockWaits == nil || len(f.LockWaits) != 0 {
		t.Fatalf("an answered empty section is no lock waits, not unknown: %#v", f.LockWaits)
	}
	if f := parsePostgres(t, healthyPostgres); f.LockWaits != nil {
		t.Fatalf("a read without the section claims lock waits: %+v", f.LockWaits)
	}
	// A server that cannot answer the auxiliary read leaves the waits
	// unknown and refuses nothing, so the store still gets a baseline.
	old := parsePostgres(t, withLockWaits("ERROR:  function pg_blocking_pids(integer) does not exist\n"))
	if old.LockWaits != nil || len(old.Unread) != 0 {
		t.Fatalf("an unanswered lock-waits read = %+v, unread %v", old.LockWaits, old.Unread)
	}
	for _, it := range old.Items {
		if it.Kind == storestate.KindRefusal {
			t.Fatalf("an unanswered lock-waits read is a refusal: %+v", it)
		}
	}

	my := parseAs(t, storestate.EngineMySQL, strings.Replace(healthyMySQL, "== locks\n", "== lockwaits\nlockwait\t17\tlocal\tSleep\tshared_no_read_write\tshop.items\t2\t41\t-1\n== locks\n", 1))
	wantMy := storestate.LockWait{Holder: "17", Peer: storestate.PeerLocal, State: "Sleep", Mode: "shared_no_read_write", Object: "shop.items", Waiters: 2, Waited: 41 * time.Second, Held: -1}
	if len(my.LockWaits) != 1 || my.LockWaits[0] != wantMy {
		t.Fatalf("mysql lock waits = %+v", my.LockWaits)
	}
	noSys := parseAs(t, storestate.EngineMySQL, strings.Replace(healthyMySQL, "== locks\n", "== lockwaits\nERROR 1146 (42S02) at line 1: Table 'sys.schema_table_lock_waits' doesn't exist\n== locks\n", 1))
	if noSys.LockWaits != nil || len(refusalItems(noSys)) != 0 {
		t.Fatalf("a server without the sys schema = %+v, %+v", noSys.LockWaits, noSys.Items)
	}
}

func refusalItems(f storestate.Facts) []storestate.Item {
	var out []storestate.Item
	for _, it := range f.Items {
		if it.Kind == storestate.KindRefusal {
			out = append(out, it)
		}
	}
	return out
}

// A session holding a lock other sessions have waited on past
// LockWaitThreshold is a finding that names the holder, its mode, the
// object, how long it has held and how many wait. Short waits are not
// judged, nor are waits on an object the healthy application itself kept
// sessions waiting on that long.
func TestLockHeldWithWaitersIsAFinding(t *testing.T) {
	routine := "lockwait\t77\t10.0.0.9\tactive\tExclusiveLock\tadvisory\t2\t600\t900\n"
	healthy := []string{
		withLockWaits(),
		withLockWaits("lockwait\t51\t10.0.0.7\tactive\tRowExclusiveLock\tpublic.items\t1\t1\t2\n"),
		withLockWaits(routine),
	}
	held := "lockwait\t4242\t10.0.0.7\tidle in transaction\tAccessExclusiveLock\tpublic.items\t3\t95\t240\n"
	for name, test := range map[string]struct {
		current string
		want    int
	}{
		"healthy":                    {withLockWaits(), 0},
		"short wait":                 {withLockWaits("lockwait\t4242\t10.0.0.7\tactive\tRowExclusiveLock\tpublic.items\t4\t5\t6\n"), 0},
		"long wait":                  {withLockWaits(held), 1},
		"routine wait in history":    {withLockWaits("lockwait\t78\t10.0.0.9\tactive\tExclusiveLock\tadvisory\t2\t1200\t1500\n"), 0},
		"long wait beside a routine": {withLockWaits(held, routine), 1},
		"lock waits unknown now":     {healthyPostgres, 0},
	} {
		t.Run(name, func(t *testing.T) {
			findings := promotedFindings(t, storestate.EnginePostgres, test.current, healthy...)
			if got := ruleSet(findings)[storestate.RuleLockWaiters]; got != test.want {
				t.Fatalf("lock-waiters findings = %d, want %d: %+v", got, test.want, findings)
			}
			if test.want == 0 {
				return
			}
			f := findings[0]
			for _, want := range []string{"4242", "AccessExclusiveLock", "public.items", "3 session(s)", "1m35s", "4m0s", "idle in transaction", "10.0.0.7"} {
				if !strings.Contains(f.Summary+" "+f.Evidence, want) {
					t.Errorf("finding lacks %q: %s | %s", want, f.Summary, f.Evidence)
				}
			}
			if w, ok := f.Metadata["lock_wait"].(storestate.LockWait); !ok || w.Holder != "4242" {
				t.Errorf("metadata = %+v", f.Metadata)
			}
		})
	}
}

// The finding's identity is the locked object, not the holding session, so
// it persists across reads (the detector's firing policy) while sessions keep
// waiting on the object.
func TestLockWaitFindingPersistsAcrossHolders(t *testing.T) {
	a := promotedFindings(t, storestate.EnginePostgres, withLockWaits("lockwait\t1\tlocal\tactive\tAccessExclusiveLock\tpublic.items\t2\t40\t50\n"), withLockWaits())
	b := promotedFindings(t, storestate.EnginePostgres, withLockWaits("lockwait\t2\tlocal\tactive\tAccessExclusiveLock\tpublic.items\t5\t70\t80\n"), withLockWaits())
	if len(a) != 1 || len(b) != 1 || a[0].Fingerprint != b[0].Fingerprint {
		t.Fatalf("fingerprints differ: %+v / %+v", a, b)
	}
}

// Without a healthy reference there is nothing to tell an application's own
// long waits from a fault, so no lock-waiters finding is made.
func TestLockWaitsAreNotJudgedWithoutABaseline(t *testing.T) {
	c := config(t)
	c.Component, c.Engine = "orders-db", storestate.EnginePostgres
	d := detector(t, c)
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(withLockWaits("lockwait\t1\tlocal\tactive\tAccessExclusiveLock\tpublic.items\t2\t400\t500\n"))))
	if err != nil {
		t.Fatal(err)
	}
	if n := ruleSet(findings)[storestate.RuleLockWaiters]; n != 0 {
		t.Fatalf("findings = %+v", findings)
	}
}

// The read never queues behind a lock another session holds: every postgres
// session sets a lock_timeout and the catalog takes definition digests from
// catalog rows (pg_get_*def lock the table to print expressions); every mysql
// session caps its metadata-lock wait. A statement that would wait is a
// "locked" refusal of its section, not a read stalled past its time budget.
func TestReadNeverWaitsOnAHeldLock(t *testing.T) {
	pg := readCommandIsBounded(t, storestate.EnginePostgres, 5432)
	if !strings.Contains(pg, "SET lock_timeout=") || !strings.Contains(pg, "pg_blocking_pids") {
		t.Errorf("postgres read does not bound lock waits or list lock waits")
	}
	if got := regexp.MustCompile(`pg_get_\w+def\(`).FindAllString(pg, -1); len(got) > 0 {
		t.Errorf("postgres read calls lock-taking %v", got)
	}
	my := readCommandIsBounded(t, storestate.EngineMySQL, 3306)
	if !strings.Contains(my, "lock_wait_timeout=1") || !strings.Contains(my, "sys.schema_table_lock_waits") {
		t.Errorf("mysql read does not bound metadata-lock waits or list lock waits")
	}
	locked := parsePostgres(t, strings.Replace(healthyPostgres, "== catalog postgres\ngrant", "== catalog postgres\nERROR:  canceling statement due to lock timeout\ngrant", 1))
	if got, ok := item(locked, storestate.KindRefusal, "postgres", "catalog"); !ok || got.Value != "locked" {
		t.Errorf("postgres lock timeout = %+v %v", got, ok)
	}
	mylocked := parseAs(t, storestate.EngineMySQL, strings.Replace(healthyMySQL, "== tables\n", "== tables\nERROR 1205 (HY000) at line 1: Lock wait timeout exceeded; try restarting transaction\n", 1))
	if got, ok := item(mylocked, storestate.KindRefusal, "", "tables"); !ok || got.Value != "locked" {
		t.Errorf("mysql lock timeout = %+v %v", got, ok)
	}
}
