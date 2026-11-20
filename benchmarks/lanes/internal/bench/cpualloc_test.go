//go:build integration

package bench

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// topo builds n cores of w hardware threads each, the way Linux numbers them with SMT on: thread t of core c is cpu c + t*n.
func topo(n, w int) Topology {
	var cores [][]int
	for c := 0; c < n; c++ {
		var th []int
		for t := 0; t < w; t++ {
			th = append(th, c+t*n)
		}
		cores = append(cores, th)
	}
	return Topology{Cores: cores}
}

func allocFor(t *testing.T, tp Topology, allowed string) CPUAllocator {
	t.Helper()
	a, err := ParseCPUList(allowed)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil { // whatever the umask made of it
		t.Fatal(err)
	}
	return CPUAllocator{Dir: dir, Allowed: a, Topo: tp}
}

func TestAllocatorHandsOutDisjointSets(t *testing.T) {
	a := allocFor(t, topo(16, 1), "4-15")
	seen := map[int]string{}
	for _, lane := range []string{"a", "b", "c"} {
		got, err := a.Take(lane, 4, 0)
		if err != nil || len(got) != 4 {
			t.Fatalf("%s: %v %v", lane, got, err)
		}
		for _, c := range got {
			if c < 4 {
				t.Errorf("%s got reserved cpu %d", lane, c)
			}
			if o, dup := seen[c]; dup {
				t.Errorf("cpu %d given to %s and %s", c, o, lane)
			}
			seen[c] = lane
		}
	}
}

func TestAllocatorKeepsSMTSiblingsTogether(t *testing.T) {
	tp := topo(8, 2) // cores {0,8} {1,9} ... {7,15}
	a := allocFor(t, tp, "2-7,10-15")
	got, err := a.Take("a", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{2, 3, 10, 11}; !reflect.DeepEqual(got, want) {
		t.Fatalf("two cores = %v, want %v", got, want)
	}
	// a core with only one thread inside the range is not whole: cpu 0 is reserved, so core {0,8} is never used
	b := allocFor(t, tp, "1-15")
	all, err := b.Take("b", 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range all {
		if c == 0 || c == 8 {
			t.Fatalf("half-reserved core handed out: %v", all)
		}
	}
	if _, err := b.Take("c", 1, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("7 whole cores in range, all taken: %v", err)
	}
}

func TestAllocatorWaitsInsteadOfOvercommitting(t *testing.T) {
	a := allocFor(t, topo(8, 1), "0-7")
	if _, err := a.Take("a", 6, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("b", 4, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("want ErrNoFreeCores, got %v", err)
	}
	// a request larger than the whole range can never be met: that is a configuration error, not a wait
	if _, err := a.Take("b", 9, 0); !errors.Is(err, ErrNeverFits) {
		t.Fatalf("want ErrNeverFits, got %v", err)
	}
	held, _ := a.Held()
	if len(held) != 1 {
		t.Fatalf("a failed take must hold nothing: %v", held)
	}
}

func TestAllocatorReleaseAndIdempotence(t *testing.T) {
	a := allocFor(t, topo(8, 1), "0-7")
	first, _ := a.Take("a", 4, 0)
	again, err := a.Take("a", 4, 0)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("a lane asking again keeps its cores: %v %v %v", first, again, err)
	}
	if _, err := a.Take("b", 4, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("c", 1, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	}
	if err := a.Release("a"); err != nil {
		t.Fatal(err)
	}
	if err := a.Release("a"); err != nil { // releasing nothing is fine
		t.Fatal(err)
	}
	got, err := a.Take("c", 3, 0)
	if err != nil || len(got) != 3 {
		t.Fatalf("freed cores are reusable: %v %v", got, err)
	}
}

// What a lane holds survives the manager and the step process: the state is a file shared by every caller on the host.
func TestAllocatorStateIsSharedAcrossCallers(t *testing.T) {
	a := allocFor(t, topo(8, 1), "0-7")
	if _, err := a.Take("a", 5, 0); err != nil {
		t.Fatal(err)
	}
	b := CPUAllocator{Dir: a.Dir, Allowed: a.Allowed, Topo: a.Topo} // another process, same directory
	if _, err := b.Take("x", 5, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("second caller must see the first's cores: %v", err)
	}
	if _, err := os.Stat(filepath.Join(a.Dir, "cpus.json")); err != nil {
		t.Fatal(err)
	}
}

func TestResizeShrinksAndGrows(t *testing.T) {
	a := allocFor(t, topo(8, 1), "0-7")
	if _, err := a.Take("a", 2, 0); err != nil {
		t.Fatal(err)
	}
	got, err := a.Take("a", 4, 0)
	if err != nil || len(got) != 4 {
		t.Fatalf("grow: %v %v", got, err)
	}
	got, err = a.Take("a", 1, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("shrink: %v %v", got, err)
	}
}

func TestReadTopologyFromSysfs(t *testing.T) {
	root := t.TempDir()
	for cpu, sib := range map[string]string{"0": "0,4", "1": "1,5", "4": "0,4", "5": "1,5", "2": "2-3", "3": "2-3"} {
		d := filepath.Join(root, "cpu"+cpu, "topology")
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "thread_siblings_list"), []byte(sib+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := ReadTopology(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := [][]int{{0, 4}, {1, 5}, {2, 3}}; !reflect.DeepEqual(got.Cores, want) {
		t.Fatalf("%v", got.Cores)
	}
}

// Many callers at once (the steps of several lanes, of several benches): exactly as many win as there are cores, disjoint.
func TestAllocatorConcurrentTakesAreDisjoint(t *testing.T) {
	a := allocFor(t, topo(16, 1), "0-7")
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := map[string][]int{}
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lane := string(rune('a' + i))
			// a caller of its own, as a process would be
			cp := CPUAllocator{Dir: a.Dir, Allowed: a.Allowed, Topo: a.Topo}
			for try := 0; try < 200; try++ {
				got, err := cp.Take(lane, 1, 0)
				if err == nil {
					mu.Lock()
					wins[lane] = got
					mu.Unlock()
					return
				}
				if !errors.Is(err, ErrNoFreeCores) {
					t.Errorf("%s: %v", lane, err)
					return
				}
				time.Sleep(time.Millisecond)
			}
			_ = cp.Dequeue(lane)
		}()
	}
	wg.Wait()
	seen := map[int]string{}
	for lane, cpus := range wins {
		for _, c := range cpus {
			if o, dup := seen[c]; dup {
				t.Errorf("cpu %d held by %s and %s", c, o, lane)
			}
			seen[c] = lane
		}
	}
	if len(wins) != 8 {
		t.Errorf("%d lanes got a core, want exactly 8: %v", len(wins), wins)
	}
}

func liveSet(lanes ...string) func() (map[string]bool, error) {
	return func() (map[string]bool, error) {
		m := map[string]bool{}
		for _, l := range lanes {
			m[l] = true
		}
		return m, nil
	}
}

// What a dead lane held is not held for ever: a lane whose cluster is gone (cluster-up failed, kind delete by hand, the lane
// left hosts.yaml) is dropped the next time anyone asks for cores.
func TestAllocatorDropsLanesWhoseClusterIsGone(t *testing.T) {
	a := allocFor(t, topo(8, 1), "0-7")
	for _, l := range []string{"dead", "alive"} {
		if _, err := a.Take(l, 4, 0); err != nil {
			t.Fatal(err)
		}
		if err := a.MarkCreated(l); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Take("new", 2, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal("setup: the range is full", err)
	}
	a.Live = liveSet("alive")
	got, err := a.Take("new", 4, 0)
	if err != nil || len(got) != 4 {
		t.Fatalf("the dead lane's cores were not reclaimed: %v %v", got, err)
	}
	if held, _ := a.Held(); len(held) != 2 || held["dead"] != nil {
		t.Fatalf("held %v", held)
	}
}

// A build in progress has no cluster yet: it is believed until its grace passes.
func TestAllocatorBelievesABuildInProgressForAWhile(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	a := allocFor(t, topo(8, 1), "0-7")
	a.Now = func() time.Time { return now }
	if _, err := a.Take("building", 8, 0); err != nil { // not MarkCreated: its cluster is being made
		t.Fatal(err)
	}
	a.Live = liveSet()
	if _, err := a.Take("other", 1, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("a lane that is being built lost its cores: %v", err)
	}
	now = now.Add(pendingGrace + time.Minute)
	if _, err := a.Take("other", 1, 0); err != nil {
		t.Fatalf("a build that never produced a cluster still holds cores: %v", err)
	}
}

// Not knowing which clusters exist drops nothing.
func TestAllocatorDropsNothingWhenLiveIsUnknown(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	if _, err := a.Take("a", 4, 0); err != nil {
		t.Fatal(err)
	}
	_ = a.MarkCreated("a")
	a.Live = func() (map[string]bool, error) { return nil, errors.New("kind is not answering") }
	if _, err := a.Take("b", 1, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	}
	if dropped, err := a.Prune(); err != nil || len(dropped) != 0 {
		t.Fatalf("pruned %v %v", dropped, err)
	}
}

func TestPruneAndAdopt(t *testing.T) {
	a := allocFor(t, topo(8, 1), "0-7")
	if err := a.Adopt("old", []int{0, 1, 2}, 10<<30); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("new", 6, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("an adopted lane's cores are taken: %v", err)
	}
	a.Live = liveSet()
	if dropped, err := a.Prune(); err != nil || !reflect.DeepEqual(dropped, []string{"old"}) {
		t.Fatalf("pruned %v %v", dropped, err)
	}
}

// First come, first served: a lane that would fit waits behind a bigger one that came first; a waiter that stops asking
// loses its place; Dequeue gives it up.
func TestAllocatorQueueIsFirstComeFirstServed(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	a := allocFor(t, topo(8, 1), "0-7")
	a.Now = func() time.Time { return now }
	if _, err := a.Take("holder", 4, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("big", 6, 0); !errors.Is(err, ErrNoFreeCores) { // first in line
		t.Fatal(err)
	}
	_, err := a.Take("small", 2, 0) // would fit now, but is behind big
	if !errors.Is(err, ErrNoFreeCores) || !strings.Contains(err.Error(), "behind big") {
		t.Fatalf("small jumped the queue: %v", err)
	}
	if err := a.Release("holder"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("small", 2, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("small was served before big: %v", err)
	}
	if got, err := a.Take("big", 6, 0); err != nil || len(got) != 6 {
		t.Fatalf("big: %v %v", got, err)
	}
	if got, err := a.Take("small", 2, 0); err != nil || len(got) != 2 {
		t.Fatalf("small after big: %v %v", got, err)
	}
	// a head that stopped asking does not block the others
	if _, err := a.Take("x", 8, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	}
	if _, err := a.Take("y", 0+1, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	}
	now = now.Add(queueStale + time.Second)
	_ = a.Release("big")
	if got, err := a.Take("y", 1, 0); err != nil || len(got) != 1 {
		t.Fatalf("a dead head blocks the queue: %v %v", got, err)
	}
	// Dequeue
	if _, err := a.Take("z", 8, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	}
	_ = a.Dequeue("z")
	if got, err := a.Take("w", 1, 0); err != nil || len(got) != 1 {
		t.Fatalf("a dequeued lane still blocks: %v %v", got, err)
	}
}

// Two lanes growing on a nearly full range do not wait for each other for ever: a lane that asks for a different size
// gives its old share up (its cluster is being made anew).
func TestAllocatorGrowingLanesDoNotDeadlock(t *testing.T) {
	a := allocFor(t, topo(10, 1), "0-9")
	if _, err := a.Take("a", 5, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("b", 5, 0); err != nil {
		t.Fatal(err)
	}
	if got, err := a.Take("a", 8, 0); err != nil && !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	} else if err == nil {
		t.Fatalf("a was given 8 cores while b holds 5 of 10: %v", got)
	}
	// b asks for 8 as well: a is first in line, and the cores a gave up are free, so a is served after b gave up its own
	if _, err := a.Take("b", 8, 0); !errors.Is(err, ErrNoFreeCores) {
		t.Fatal(err)
	}
	if got, err := a.Take("a", 8, 0); err != nil || len(got) != 8 {
		t.Fatalf("a never gets its 8 cores: %v %v", got, err)
	}
}

func TestAllocatorMemoryBudget(t *testing.T) {
	a := allocFor(t, topo(16, 1), "0-15")
	a.MemBudget = 40 << 30
	if _, err := a.Take("a", 2, 25<<30); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("b", 2, 20<<30); !errors.Is(err, ErrNoFreeMemory) || !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("b must wait for memory: %v", err)
	}
	if _, err := a.Take("c", 2, 41<<30); !errors.Is(err, ErrNeverFits) {
		t.Fatalf("more than the whole budget can never fit: %v", err)
	}
	_ = a.Release("a")
	if _, err := a.Take("b", 2, 20<<30); err != nil {
		t.Fatalf("after a's memory was released: %v", err)
	}
	b := allocFor(t, topo(16, 1), "0-15") // no budget: memory is not checked
	if _, err := b.Take("a", 2, 1<<50); err != nil {
		t.Fatal(err)
	}
}

func TestAllocatorSaysWhatToDoWithACorruptLedger(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	if err := os.MkdirAll(a.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(a.Dir, "cpus.json")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := a.Take("a", 1, 0)
	if err == nil || !strings.Contains(err.Error(), p) || !strings.Contains(err.Error(), "ledger adopt") {
		t.Fatalf("the error must name the file and the remedy: %v", err)
	}
}

// A ledger every account of the host can write is one any of them can tamper with: lanes waiting forever, cores
// handed out twice. The default is per user, private to that user.
func TestDefaultLedgerIsPerUserAndPrivate(t *testing.T) {
	varTmp := withVarTmp(t)
	want := filepath.Join(varTmp, fmt.Sprintf("fuzzops-lanes-%d", os.Getuid()))
	if got := DefaultLedgerDir(); got != want {
		t.Fatalf("default ledger dir %q, want %q", got, want)
	}
	a := CPUAllocator{Allowed: []int{0, 1, 2, 3}, Topo: topo(4, 1)} // Dir "": the default
	if _, err := a.Take("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	for name, mode := range map[string]os.FileMode{".": 0o700, "lock": 0o600, "cpus.json": 0o600} {
		st, err := os.Stat(filepath.Join(want, name))
		if err != nil || st.Mode().Perm() != mode {
			t.Errorf("%s: %v %v, want %v", name, st, err, mode)
		}
	}
	if _, err := os.Stat(filepath.Join(varTmp, "fuzzops-lanes")); err == nil {
		t.Error("the old shared directory must not be created")
	}
}

// A configured path is made private too, and every later use checks it.
func TestConfiguredLedgerDirIsCreatedPrivate(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	a.Dir = filepath.Join(a.Dir, "nested", "ledger")
	if _, err := a.Take("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(a.Dir)
	if err != nil || st.Mode().Perm() != 0o700 {
		t.Fatalf("%v %v", st, err)
	}
}

func TestLedgerDirOthersCanWriteIsRefused(t *testing.T) {
	for _, mode := range []os.FileMode{0o777, 0o770, 0o757, 0o722} {
		a := allocFor(t, topo(4, 1), "0-3")
		if err := os.Chmod(a.Dir, mode); err != nil {
			t.Fatal(err)
		}
		_, err := a.Take("a", 1, 0)
		if err == nil || !strings.Contains(err.Error(), a.Dir) || !strings.Contains(err.Error(), "chmod") || !strings.Contains(err.Error(), "state_shared") {
			t.Fatalf("mode %v: the error must name the path and the fix: %v", mode, err)
		}
		if _, serr := os.Stat(filepath.Join(a.Dir, "cpus.json")); serr == nil {
			t.Errorf("mode %v: nothing may be written into a directory others can write", mode)
		}
	}
	a := allocFor(t, topo(4, 1), "0-3")
	if err := os.Chmod(a.Dir, 0o755); err != nil { // readable by others is fine: they cannot change it
		t.Fatal(err)
	}
	if _, err := a.Take("a", 1, 0); err != nil {
		t.Fatalf("0755: %v", err)
	}
}

func TestLedgerDirOfAnotherUserIsRefused(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	withOwner(t, func(os.FileInfo) int { return os.Getuid() + 1 })
	_, err := a.Take("a", 1, 0)
	if err == nil || !strings.Contains(err.Error(), a.Dir) || !strings.Contains(err.Error(), "owned by") || !strings.Contains(err.Error(), "lanes.state") {
		t.Fatalf("the error must name the path, the owner and the fix: %v", err)
	}
}

func TestLedgerDirThatIsASymlinkIsRefused(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	real := a.Dir
	a.Dir = filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, a.Dir); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("a", 1, 0); err == nil || !strings.Contains(err.Error(), a.Dir) || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("%v", err)
	}
}

// Files in the directory are checked as well: one another account made, or left writable, is not trusted.
func TestLedgerFilesAreOwnedAndPrivate(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	if _, err := a.Take("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(a.Dir, "cpus.json")
	if err := os.Chmod(p, 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Take("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(p); st.Mode().Perm() != 0o600 {
		t.Errorf("a writable ledger file of ours is made private again: %v", st.Mode())
	}
	withOwner(t, func(fi os.FileInfo) int {
		if fi.Name() == "cpus.json" {
			return os.Getuid() + 1
		}
		return os.Getuid()
	})
	if _, err := a.Take("a", 1, 0); err == nil || !strings.Contains(err.Error(), p) || !strings.Contains(err.Error(), "owned by") {
		t.Fatalf("a ledger file of another user: %v", err)
	}
}

// lanes.state_shared: the old behaviour, open to every account, for a directory the operator chose to trust.
func TestSharedLedgerIsOpenToEveryAccount(t *testing.T) {
	a := allocFor(t, topo(4, 1), "0-3")
	a.Dir, a.Shared = filepath.Join(a.Dir, "fuzzops-lanes"), true
	if _, err := a.Take("a", 1, 0); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]os.FileMode{".": 0o777, "lock": 0o666, "cpus.json": 0o666} {
		st, err := os.Stat(filepath.Join(a.Dir, name))
		if err != nil || st.Mode().Perm() != want {
			t.Errorf("%s: %v %v, want %v", name, st, err, want)
		}
	}
	withOwner(t, func(os.FileInfo) int { return os.Getuid() + 1 })
	if _, err := a.Take("b", 1, 0); err != nil {
		t.Fatalf("a shared ledger trusts whoever owns it: %v", err)
	}
}

// writeLegacy makes the old world-writable ledger holding lane l on cpu 0.
func writeLegacy(t *testing.T, varTmp string) string {
	t.Helper()
	dir := filepath.Join(varTmp, "fuzzops-lanes")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "cpus.json")
	doc := `{"lanes":{"old":{"cpus":[0],"memory":1024,"since":"2026-11-18T00:00:00Z","created":true}}}`
	if err := os.WriteFile(p, []byte(doc), 0o666); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLegacyLedgerOfTheUserIsAdoptedOnce(t *testing.T) {
	varTmp := withVarTmp(t)
	legacy := writeLegacy(t, varTmp)
	var log []string
	a := CPUAllocator{Allowed: []int{0, 1, 2, 3}, Topo: topo(4, 1), Log: func(f string, args ...any) { log = append(log, fmt.Sprintf(f, args...)) }}
	got, err := a.Take("new", 1, 0)
	if err != nil || reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("the adopted lane holds cpu 0, so the new one must not get it: %v %v", got, err)
	}
	es, err := a.Entries()
	if err != nil || len(es) != 2 || !reflect.DeepEqual(es["old"].CPUs, []int{0}) || es["old"].Memory != 1024 {
		t.Fatalf("entries %+v %v", es, err)
	}
	if !strings.Contains(strings.Join(log, "\n"), legacy) {
		t.Fatalf("a note in the log must say what was adopted from where: %q", log)
	}
	if _, err := os.Stat(legacy); err == nil {
		t.Error("the old file stays where it is: it must be moved aside so that it is adopted once")
	}
	// Once: a lane released from the new ledger is not brought back by the next call.
	if err := a.Release("old"); err != nil {
		t.Fatal(err)
	}
	if es, _ := a.Entries(); len(es) != 1 {
		t.Fatalf("adopted again: %+v", es)
	}
}

func TestLegacyLedgerOfAnotherUserIsNotAdopted(t *testing.T) {
	varTmp := withVarTmp(t)
	legacy := writeLegacy(t, varTmp)
	withOwner(t, func(fi os.FileInfo) int {
		if fi.Name() == "fuzzops-lanes" { // the old directory belongs to someone else
			return os.Getuid() + 1
		}
		return os.Getuid()
	})
	var log []string
	a := CPUAllocator{Allowed: []int{0, 1, 2, 3}, Topo: topo(4, 1), Log: func(f string, args ...any) { log = append(log, fmt.Sprintf(f, args...)) }}
	got, err := a.Take("new", 1, 0)
	if err != nil || !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("nothing was adopted, the new lane gets cpu 0: %v %v", got, err)
	}
	if _, err := os.Stat(legacy); err != nil {
		t.Errorf("another user's file is left alone: %v", err)
	}
	if !strings.Contains(strings.Join(log, "\n"), legacy) {
		t.Errorf("the log says why the old ledger was ignored: %q", log)
	}
}

// withVarTmp points the default ledger and the old shared one at a temporary directory.
func withVarTmp(t *testing.T) string {
	t.Helper()
	old := varTmp
	varTmp = t.TempDir()
	t.Cleanup(func() { varTmp = old })
	return varTmp
}

// withOwner replaces what the allocator believes owns a file.
func withOwner(t *testing.T, f func(os.FileInfo) int) {
	t.Helper()
	old := fileOwner
	fileOwner = f
	t.Cleanup(func() { fileOwner = old })
}

// Containers other runners pinned inside the block (their kind nodes) are not ours to share cores with: a lane is
// given the cores the ledger and those pins leave, a core whole, and waits, naming them, when too few remain.
func TestAllocatorAvoidsForeignPins(t *testing.T) {
	a := allocFor(t, topo(8, 2), "0-7,8-15") // 8 cores, cpu c and c+8 are siblings
	// docker ps/inspect output as containerProbe prints it: NAME CGROUPPARENT CPUSET.
	probe := "sdm-worker docker 2,3\novh4-node system.slice 12\nlane1-node lanes.slice 0-1\nunpinned docker \n"
	a.Foreign = func() ([]ContainerPin, error) { return ForeignPins(probe), nil }
	got, err := a.Take("a", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Cores 2 and 3 (sdm-worker) and core 4 (cpu 12, a sibling of 4) are out; the lane's own node is not foreign.
	for _, c := range got {
		if c%8 == 2 || c%8 == 3 || c%8 == 4 {
			t.Errorf("lane got cpu %d, pinned by a foreign container: %v", c, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("two cores of two threads expected, got %v", got)
	}
	// Five cores remain minus the two just taken: three. Four do not fit and the reason names both containers.
	_, err = a.Take("b", 4, 0)
	if !errors.Is(err, ErrNoFreeCores) {
		t.Fatalf("want a wait, got %v", err)
	}
	for _, name := range []string{"sdm-worker", "ovh4-node"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("idle reason does not name %s: %v", name, err)
		}
	}
	if strings.Contains(err.Error(), "lane1-node") {
		t.Errorf("our own container is named as foreign: %v", err)
	}
	if _, err := a.Take("b", 3, 0); err != nil {
		t.Errorf("the three cores left should fit: %v", err)
	}
}
