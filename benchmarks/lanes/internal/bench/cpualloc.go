package bench

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Lane CPUs and memory. A host that lists `lanes.cpus` hands out its cores to lanes, whole and disjoint: every
// hardware thread of a core goes to the same lane, so a lane never shares a core's execution resources with another,
// and the sum of the lanes' memory stays within the host's budget. What each lane holds is one JSON ledger on the host
// (a fixed directory per user, DefaultLedgerDir, whatever the bench's staging), under a file lock, so every process
// that builds or deletes a lane there (the steps of one bench, or of several, of the account) sees the same picture.
// The directory and its files belong to that user alone (mode 0700 and 0600), and a directory or file that is owned by
// someone else, or that others can write, is refused, because a ledger another account can edit lets it make lanes
// wait forever or be handed cores twice. A deliberately shared ledger (several accounts, hosts.yaml
// `lanes.state_shared`) is the one exception and trusts everyone who can write it. Lanes that
// cannot be served wait in a first-come queue; entries of lanes whose cluster no longer exists are dropped.

// varTmp is the host's persistent temporary directory (a variable for tests).
var varTmp = "/var/tmp"

// DefaultLedgerDir is where the ledger of the current user lives unless hosts.yaml `lanes.state` says otherwise:
// the same for every bench of the user on the machine, and private to the user.
func DefaultLedgerDir() string {
	return filepath.Join(varTmp, fmt.Sprintf("fuzzops-lanes-%d", os.Getuid()))
}

// LegacyLedgerDir is where the ledger lived before it was per user: world-writable. A user's own entries in it are
// adopted into DefaultLedgerDir once.
func LegacyLedgerDir() string { return filepath.Join(varTmp, "fuzzops-lanes") }

// fileOwner is the uid that owns fi (a variable for tests).
var fileOwner = func(fi os.FileInfo) int {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int(st.Uid)
	}
	return -1
}

// Errors of Take: ErrNoFreeCores and ErrNoFreeMemory are waits (other lanes hold the cores or the memory budget),
// ErrNeverFits a mistake (the request is larger than the lanes' range or the whole budget).
var (
	ErrNoFreeCores  = errors.New("no free cores")
	ErrNoFreeMemory = fmt.Errorf("no free memory: %w", ErrNoFreeCores)
	ErrNeverFits    = errors.New("more than the host's lanes can ever hold")
)

// queueStale is how long a waiting lane that stops asking keeps its place in the queue.
const queueStale = 2 * time.Minute

// pendingGrace is how long an allocation whose cluster has not been created yet (a build in progress) is believed
// when no such cluster exists.
const pendingGrace = time.Hour

// Topology is a host's physical cores, each as its hardware threads (CPU numbers), sorted.
type Topology struct {
	Cores [][]int
}

// ReadTopology reads <sysfs>/cpu*/topology/thread_siblings_list (sysfs is /sys/devices/system/cpu).
func ReadTopology(sysfs string) (Topology, error) {
	files, _ := filepath.Glob(filepath.Join(sysfs, "cpu[0-9]*", "topology", "thread_siblings_list"))
	if len(files) == 0 {
		return Topology{}, fmt.Errorf("no cpu topology under %s", sysfs)
	}
	seen := map[string]bool{}
	var t Topology
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return Topology{}, err
		}
		th, err := ParseCPUList(string(trimSpace(b)))
		if err != nil {
			return Topology{}, fmt.Errorf("%s: %w", f, err)
		}
		if k := FormatCPUList(th); !seen[k] {
			seen[k] = true
			t.Cores = append(t.Cores, th)
		}
	}
	sort.Slice(t.Cores, func(i, j int) bool { return t.Cores[i][0] < t.Cores[j][0] })
	return t, nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}

// CPUAllocator hands out whole cores of Allowed and memory out of MemBudget to named lanes; Dir holds the ledger.
type CPUAllocator struct {
	// Dir is the ledger directory ("": the current user's DefaultLedgerDir, which also adopts the user's entries of
	// the old shared ledger once). It must be owned by the current user and not writable by group or others.
	Dir string
	// Shared trusts Dir although others may write it (and makes it and its files open to everyone): the old
	// behaviour, for a ledger several accounts are meant to share.
	Shared bool
	// Log receives notes about the ledger (nil: none).
	Log     func(format string, args ...any)
	Allowed []int
	Topo    Topology
	// MemBudget is the most memory the lanes' totals may add up to (0: not checked).
	MemBudget int64
	// Live lists the lanes whose cluster exists (nil: the ledger is not reconciled). An error means it is not known,
	// and nothing is dropped.
	Live func() (map[string]bool, error)
	// Foreign lists the containers on the host that are not ours and are pinned to cpus (nil: none known). A core
	// any of them is pinned to is not given out. An error means it is not known, and nothing is excluded.
	Foreign func() ([]ContainerPin, error)
	// Now is the clock (nil: time.Now).
	Now func() time.Time
}

type ledgerEntry struct {
	CPUs   []int     `json:"cpus"`
	Memory int64     `json:"memory,omitempty"`
	Since  time.Time `json:"since"`
	// Created: the lane's cluster has been made. Until then the entry is a build in progress.
	Created bool `json:"created,omitempty"`
}

type queueEntry struct {
	Lane string    `json:"lane"`
	Seen time.Time `json:"seen"`
}

type ledger struct {
	Lanes map[string]ledgerEntry `json:"lanes"`
	Queue []queueEntry           `json:"queue,omitempty"`
}

func (a CPUAllocator) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// usable is the cores whose every thread is in Allowed (a core half inside the range is never given out).
func (a CPUAllocator) usable() [][]int {
	in := map[int]bool{}
	for _, c := range a.Allowed {
		in[c] = true
	}
	var out [][]int
	for _, core := range a.Topo.Cores {
		whole := true
		for _, t := range core {
			whole = whole && in[t]
		}
		if whole {
			out = append(out, core)
		}
	}
	return out
}

func (a CPUAllocator) logf(format string, args ...any) {
	if a.Log != nil {
		a.Log(format, args...)
	}
}

func (a CPUAllocator) dir() string {
	if a.Dir != "" {
		return a.Dir
	}
	return DefaultLedgerDir()
}

// ledgerRefusal is the error for a ledger directory or file that cannot be trusted, with the way out.
func ledgerRefusal(path, what string) error {
	return fmt.Errorf("lane ledger %s: %s; the ledger decides which cores every lane gets, so anyone who can write it can make lanes wait forever or hand out overlapping cores. "+
		"Fix it (`chmod go-w %s`, or remove it and let the bench recreate it, or set hosts.yaml lanes.state to a directory you own), "+
		"or, if every account that can write it is trusted, set lanes.state_shared: true", path, what, path)
}

// vet checks one filesystem object of a private ledger: not a symbolic link, owned by the current user, not
// writable by group or others.
func vet(path string, fi os.FileInfo, wantDir bool) error {
	if fi.Mode()&os.ModeSymlink != 0 {
		return ledgerRefusal(path, "is a symbolic link")
	}
	if wantDir != fi.IsDir() {
		return ledgerRefusal(path, "is not "+map[bool]string{true: "a directory", false: "a regular file"}[wantDir])
	}
	if uid := fileOwner(fi); uid != os.Getuid() {
		return ledgerRefusal(path, fmt.Sprintf("is owned by uid %d, not by you (uid %d)", uid, os.Getuid()))
	}
	if fi.Mode().Perm()&0o022 != 0 {
		return ledgerRefusal(path, fmt.Sprintf("is writable by group or others (mode %04o)", fi.Mode().Perm()))
	}
	return nil
}

// prepareDir makes the ledger directory (private to the user when it is made here) and refuses one that is not
// the user's own or that others can write. A shared ledger's directory is opened to every account.
func (a CPUAllocator) prepareDir(dir string) error {
	if a.Shared {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			return err
		}
		_ = os.Chmod(dir, 0o777) // another account's bench must be able to open it (the umask took bits off)
		return nil
	}
	fi, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
			return err
		}
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		fi, err = os.Lstat(dir)
	}
	if err != nil {
		return err
	}
	return vet(dir, fi, true)
}

// openFile opens one file of the ledger: private (0600, the user's own) or, for a shared ledger, open to everyone.
// A private file of the user's that others can write is made private again.
func (a CPUAllocator) openFile(path string, flag int) (*os.File, error) {
	if a.Shared {
		f, err := os.OpenFile(path, flag, 0o666)
		if err == nil {
			_ = f.Chmod(0o666)
		}
		return f, err
	}
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode().Perm()&0o022 != 0 && fi.Mode().IsRegular() && fileOwner(fi) == os.Getuid() {
			_ = os.Chmod(path, 0o600)
			fi, err = os.Lstat(path)
			if err != nil {
				return nil, err
			}
		}
		if err := vet(path, fi, false); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, flag|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if fi, err := f.Stat(); err != nil || fileOwner(fi) != os.Getuid() {
		f.Close()
		return nil, ledgerRefusal(path, "is not owned by you")
	}
	return f, nil
}

// adoptLegacy brings the current user's entries of the old shared ledger into st (a new, empty private ledger) and
// returns the old file to move aside once st is saved. Another account's ledger, or one that cannot be read, is left
// alone and a note says so.
func (a CPUAllocator) adoptLegacy(st *ledger) (moved string) {
	dir := LegacyLedgerDir()
	p := filepath.Join(dir, "cpus.json")
	fi, err := os.Lstat(p)
	if err != nil {
		return ""
	}
	di, derr := os.Lstat(dir)
	if derr != nil || !di.IsDir() || !fi.Mode().IsRegular() || fileOwner(di) != os.Getuid() || fileOwner(fi) != os.Getuid() {
		a.logf("lane ledger: not adopting the old shared ledger %s: it is not a plain file in a directory owned by you (uid %d), so another account could have written it", p, os.Getuid())
		return ""
	}
	b, err := os.ReadFile(p)
	if err != nil {
		a.logf("lane ledger: not adopting the old shared ledger %s: %v", p, err)
		return ""
	}
	var old ledger
	if err := json.Unmarshal(b, &old); err != nil {
		a.logf("lane ledger: not adopting the old shared ledger %s: not valid JSON (%v)", p, err)
		return ""
	}
	for lane, e := range old.Lanes {
		st.Lanes[lane] = e
	}
	a.logf("lane ledger: adopted %d lane(s) from the old shared ledger %s into %s (the old file is renamed to cpus.json.adopted; the old directory is no longer used and can be removed)", len(old.Lanes), p, a.dir())
	return p
}

// locked runs f on the ledger under the directory's lock and saves what f changed.
func (a CPUAllocator) locked(f func(*ledger) error) error {
	dir := a.dir()
	if err := a.prepareDir(dir); err != nil {
		return err
	}
	lk, err := a.openFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR)
	if err != nil {
		return fmt.Errorf("lane ledger lock %s: %w", filepath.Join(dir, "lock"), err)
	}
	defer lk.Close()
	if err := syscall.Flock(int(lk.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lk.Fd()), syscall.LOCK_UN)
	st := ledger{Lanes: map[string]ledgerEntry{}}
	p := filepath.Join(dir, "cpus.json")
	var adopted string
	if rf, err := a.openFile(p, os.O_RDONLY); err == nil {
		b, rerr := io.ReadAll(rf)
		rf.Close()
		if rerr != nil {
			return rerr
		}
		if err := json.Unmarshal(b, &st); err != nil {
			return fmt.Errorf("lane ledger %s is not valid JSON (%v): while no lane is being built, remove the file and re-register the lanes that exist with `fuzzops-bench ledger adopt`", p, err)
		}
	} else if errors.Is(err, fs.ErrNotExist) {
		if a.Dir == "" && !a.Shared {
			adopted = a.adoptLegacy(&st)
		}
	} else {
		return err
	}
	if st.Lanes == nil {
		st.Lanes = map[string]ledgerEntry{}
	}
	if err := f(&st); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(st, "", " ")
	tmp := p + ".tmp"
	tf, err := a.openFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC)
	if err != nil {
		return err
	}
	_, werr := tf.Write(b)
	if serr := tf.Sync(); werr == nil {
		werr = serr
	}
	if cerr := tf.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return werr
	}
	if err := os.Rename(tmp, p); err != nil {
		return err
	}
	if adopted != "" {
		if err := os.Rename(adopted, adopted+".adopted"); err != nil {
			a.logf("lane ledger: could not move the old shared ledger %s aside (%v); it is adopted only while %s does not exist", adopted, err, p)
		}
	}
	return nil
}

// reconcile drops the entries of lanes (other than keep) whose cluster is gone: a finished one at once, one still
// being built once its grace has passed.
func (a CPUAllocator) reconcile(st *ledger, keep string) {
	if a.Live == nil {
		return
	}
	live, err := a.Live()
	if err != nil {
		return
	}
	for lane, e := range st.Lanes {
		if lane != keep && !live[lane] && (e.Created || a.now().Sub(e.Since) > pendingGrace) {
			delete(st.Lanes, lane)
		}
	}
}

// ledgerGrace is how long an entry is believed after it was made when nothing shows its lane exists: a cluster is
// being created, or a lane was just added to hosts.yaml (the manager's tick reconciles with it).
const ledgerGrace = 2 * time.Minute

// ReconcileLanes releases the entries whose owner is gone and returns the lanes released. An entry is held only
// while its lane is in known (the inventory) and, once its cluster has been made, while the cluster exists (live).
// Entries younger than grace are kept, so a cluster that is still being created is not reaped; an entry whose
// cluster is not yet made (a build in progress) is kept for pendingGrace. When the clusters cannot be listed, only
// the lanes missing from known are released.
func (a CPUAllocator) ReconcileLanes(known map[string]bool, grace time.Duration) ([]string, error) {
	var released []string
	err := a.locked(func(st *ledger) error {
		var live map[string]bool
		if a.Live != nil {
			l, err := a.Live()
			if err != nil {
				a.logf("lane ledger: clusters not known, only lanes outside the inventory are released: %v", err)
			} else {
				live = l
			}
		}
		now := a.now()
		for lane, e := range st.Lanes {
			age := now.Sub(e.Since)
			switch {
			case !known[lane]:
				if age <= grace {
					continue
				}
			case live != nil && !live[lane]:
				if age <= grace || (!e.Created && age <= pendingGrace) {
					continue
				}
			default:
				continue
			}
			delete(st.Lanes, lane)
			st.dequeue(lane)
			released = append(released, lane)
			a.logf("lane ledger: released %s (cpus %s, held since %s): its lane or cluster no longer exists", lane, FormatCPUList(e.CPUs), e.Since.Format(time.RFC3339))
		}
		sort.Strings(released)
		return nil
	})
	return released, err
}

func (st *ledger) dequeue(lane string) {
	var q []queueEntry
	for _, e := range st.Queue {
		if e.Lane != lane {
			q = append(q, e)
		}
	}
	st.Queue = q
}

// Take gives lane cores whole cores (the CPU numbers of all their threads, sorted) and mem bytes of the memory
// budget: what it already holds if that is exactly this request, else a fresh share (its old one is dropped: the
// cluster is being made anew), the lowest numbered free cores first. A lane that cannot be served is queued, first
// come first served, and gets ErrNoFreeCores or ErrNoFreeMemory until it is first and the room is there; the
// caller asks again, which keeps its place, and gives it up with Dequeue. ErrNeverFits: the request is larger than
// the range or the budget could ever hold.
func (a CPUAllocator) Take(lane string, cores int, mem int64) ([]int, error) {
	var got []int
	var wait error // a wait is not a failure of the ledger: the queue and what was dropped are kept
	err := a.locked(func(st *ledger) error {
		now := a.now()
		a.reconcile(st, lane)
		usable := a.usable()
		if cores > len(usable) {
			return fmt.Errorf("%d cores asked, the range holds %d: %w", cores, len(usable), ErrNeverFits)
		}
		if a.MemBudget > 0 && mem > a.MemBudget {
			return fmt.Errorf("%d bytes of memory asked, the lanes' budget is %d: %w", mem, a.MemBudget, ErrNeverFits)
		}
		var live []queueEntry
		for _, q := range st.Queue {
			if q.Lane == lane || now.Sub(q.Seen) < queueStale {
				live = append(live, q)
			}
		}
		st.Queue = live
		if e, ok := st.Lanes[lane]; ok {
			held := 0
			for _, c := range usable {
				if slices.Contains(e.CPUs, c[0]) {
					held++
				}
			}
			if held == cores && e.Memory == mem {
				st.dequeue(lane)
				got = append([]int(nil), e.CPUs...)
				return nil
			}
			delete(st.Lanes, lane) // a different size: it is made anew
		}
		pinned := map[int][]string{} // cpu -> foreign containers pinned to it
		if a.Foreign != nil {
			if fs, err := a.Foreign(); err != nil {
				a.logf("foreign pins not known, none excluded: %v", err)
			} else {
				for _, f := range fs {
					for _, c := range f.CPUs {
						pinned[c] = append(pinned[c], f.Name)
					}
				}
			}
		}
		// What the block can ever place: its cores minus those a foreign container pins (a core is whole or not given
		// out). A request above that is no wait for our own lanes to finish, so it fails now rather than queueing.
		placeable, foreign := 0, map[string]bool{}
		for _, core := range usable {
			clash := false
			for _, t := range core {
				for _, n := range pinned[t] {
					clash, foreign[n] = true, true
				}
			}
			if !clash {
				placeable++
			}
		}
		if cores > placeable {
			names := make([]string, 0, len(foreign))
			for n := range foreign {
				names = append(names, n)
			}
			sort.Strings(names)
			return fmt.Errorf("%d cores asked, the block holds %d and foreign containers pin all but %d of them (%s): %w",
				cores, len(usable), placeable, strings.Join(names, ", "), ErrNeverFits)
		}
		seen := false
		for i := range st.Queue {
			if st.Queue[i].Lane == lane {
				st.Queue[i].Seen, seen = now, true
			}
		}
		if !seen {
			st.Queue = append(st.Queue, queueEntry{lane, now})
		}
		if st.Queue[0].Lane != lane {
			wait = fmt.Errorf("%d cores asked, queued behind %s: %w", cores, st.Queue[0].Lane, ErrNoFreeCores)
			return nil
		}
		taken := map[int]bool{}
		var used int64
		for _, e := range st.Lanes {
			used += e.Memory
			for _, c := range e.CPUs {
				taken[c] = true
			}
		}
		var free [][]int
		blockedBy := map[string]bool{}
		for _, core := range usable {
			if taken[core[0]] {
				continue
			}
			clash := false // a core is whole or not given out: one pinned thread takes its siblings too
			for _, t := range core {
				for _, n := range pinned[t] {
					clash, blockedBy[n] = true, true
				}
			}
			if !clash {
				free = append(free, core)
			}
		}
		if len(free) < cores {
			if len(blockedBy) > 0 {
				names := make([]string, 0, len(blockedBy))
				for n := range blockedBy {
					names = append(names, n)
				}
				sort.Strings(names)
				wait = fmt.Errorf("%d cores asked, %d free; foreign containers pinned inside the block hold the rest: %s: %w", cores, len(free), strings.Join(names, ", "), ErrNoFreeCores)
				return nil
			}
			wait = fmt.Errorf("%d cores asked, %d free: %w", cores, len(free), ErrNoFreeCores)
			return nil
		}
		if a.MemBudget > 0 && used+mem > a.MemBudget {
			wait = fmt.Errorf("%d bytes of memory asked, %d of the budget %d is held by other lanes: %w", mem, used, a.MemBudget, ErrNoFreeMemory)
			return nil
		}
		for _, core := range free[:cores] {
			got = append(got, core...)
		}
		sort.Ints(got)
		st.Lanes[lane] = ledgerEntry{CPUs: got, Memory: mem, Since: now}
		st.dequeue(lane)
		return nil
	})
	if err == nil && wait != nil {
		return nil, wait
	}
	return got, err
}

// Dequeue gives up a place in the queue (the lane's build stopped waiting).
func (a CPUAllocator) Dequeue(lane string) error {
	return a.locked(func(st *ledger) error { st.dequeue(lane); return nil })
}

// MarkCreated records that the lane's cluster now exists: from here an entry whose cluster is gone is dropped.
func (a CPUAllocator) MarkCreated(lane string) error {
	return a.locked(func(st *ledger) error {
		if e, ok := st.Lanes[lane]; ok {
			e.Created = true
			st.Lanes[lane] = e
		}
		return nil
	})
}

// Release gives the lane's cores and memory back (nothing held is fine).
func (a CPUAllocator) Release(lane string) error {
	return a.locked(func(st *ledger) error { delete(st.Lanes, lane); st.dequeue(lane); return nil })
}

// Adopt records a lane that already exists (made before lane limits, or by hand) with the cores and memory it uses.
func (a CPUAllocator) Adopt(lane string, cpus []int, mem int64) error {
	return a.locked(func(st *ledger) error {
		st.Lanes[lane] = ledgerEntry{CPUs: append([]int(nil), cpus...), Memory: mem, Since: a.now(), Created: true}
		return nil
	})
}

// Prune drops the entries whose cluster is gone (live says which exist) and returns the lanes dropped.
func (a CPUAllocator) Prune() ([]string, error) {
	var dropped []string
	err := a.locked(func(st *ledger) error {
		before := map[string]bool{}
		for l := range st.Lanes {
			before[l] = true
		}
		a.reconcile(st, "")
		for l := range before {
			if _, ok := st.Lanes[l]; !ok {
				dropped = append(dropped, l)
			}
		}
		sort.Strings(dropped)
		return nil
	})
	return dropped, err
}

// Held is the CPUs each lane holds now.
func (a CPUAllocator) Held() (map[string][]int, error) {
	out := map[string][]int{}
	es, err := a.Entries()
	for l, e := range es {
		out[l] = e.CPUs
	}
	return out, err
}

// Entries is the whole ledger: what each lane holds.
func (a CPUAllocator) Entries() (map[string]ledgerEntry, error) {
	out := map[string]ledgerEntry{}
	err := a.locked(func(st *ledger) error {
		for l, e := range st.Lanes {
			e.CPUs = append([]int(nil), e.CPUs...)
			out[l] = e
		}
		return nil
	})
	return out, err
}
