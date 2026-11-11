package storestate

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// MaxLockWaits bounds the lock waits of one read; the read lists the holders
// with the most waiters first.
const MaxLockWaits = 8

// LockWaitThreshold is how long sessions must have waited on one holder's
// lock before the lock-waiters rule judges it. Ordinary contention clears in
// milliseconds; a lock still blocking others after this long is held by a
// session that is not finishing.
const LockWaitThreshold = 30 * time.Second

// LockWait is one session holding a lock other sessions wait for. It is
// value-free: a session id, a peer address, lock mode and object names and
// durations, never a query or a record.
type LockWait struct {
	// Holder is the store's id of the holding session (a postgres backend
	// pid, a mysql processlist id): what the store's own terminate command
	// takes.
	Holder string `json:"holder"`
	// Peer is the holder's client address, PeerLocal for a session inside the
	// store's Pod.
	Peer string `json:"peer"`
	// State is the holder session's state (for example idle in transaction,
	// active, Sleep), empty when the store does not show it.
	State string `json:"state,omitempty"`
	// Mode is the lock mode the holder has on Object, empty when unknown.
	Mode string `json:"mode,omitempty"`
	// Object is the locked relation or table, or the lock type when the lock
	// is on no relation (a transaction id, an advisory lock).
	Object string `json:"object"`
	// Waiters counts the sessions waiting on the holder for Object.
	Waiters int `json:"waiters"`
	// Waited is the longest current wait among them.
	Waited time.Duration `json:"waited"`
	// Held is the age of the holder's transaction or session, the longest the
	// lock can have been held; negative when the store does not show it.
	Held time.Duration `json:"held"`
}

// lockWaitLine parses one lock-waits line:
// "lockwait\t<holder>\t<peer>\t<state>\t<mode>\t<object>\t<waiters>\t<waited s>\t<held s>".
func lockWaitLine(line string) (LockWait, error) {
	fields := strings.Split(line, "\t")
	if len(fields) != 9 || fields[0] != "lockwait" || fields[1] == "" || fields[5] == "" {
		return LockWait{}, fmt.Errorf("lock-waits line is malformed: %q", line)
	}
	waiters, err := strconv.Atoi(fields[6])
	if err != nil || waiters < 1 {
		return LockWait{}, fmt.Errorf("lock-waits waiter count %q", fields[6])
	}
	seconds := func(s string) (time.Duration, error) {
		if s == "" {
			return -1, nil
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("lock-waits duration %q", s)
		}
		if n < 0 {
			return -1, nil
		}
		return time.Duration(n * float64(time.Second)).Round(time.Second), nil
	}
	waited, err := seconds(fields[7])
	if err != nil {
		return LockWait{}, err
	}
	held, err := seconds(fields[8])
	if err != nil {
		return LockWait{}, err
	}
	return LockWait{Holder: fields[1], Peer: peer(fields[2]), State: fields[3], Mode: fields[4], Object: fields[5], Waiters: waiters, Waited: waited, Held: held}, nil
}

// parseLockWaits parses a lock-waits section. A missing section leaves the
// lock waits unknown (nil); an answered empty one is none.
func parseLockWaits(lines []string, present bool) ([]LockWait, error) {
	if !present {
		return nil, nil
	}
	out := []LockWait{}
	for _, line := range lines {
		w, err := lockWaitLine(line)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Waiters != out[j].Waiters {
			return out[i].Waiters > out[j].Waiters
		}
		return out[i].Waited > out[j].Waited
	})
	if len(out) > MaxLockWaits {
		out = out[:MaxLockWaits]
	}
	return out, nil
}

// routineLockWait reports whether the healthy application itself kept
// sessions waiting on object for LockWaitThreshold or longer (a leader lock
// its standbys wait on, a long maintenance holder): such waits are its own
// behavior and are not judged.
func routineLockWait(healthy []Facts, object string) bool {
	for _, f := range healthy {
		for _, w := range f.LockWaits {
			if w.Object == object && w.Waited >= LockWaitThreshold {
				return true
			}
		}
	}
	return false
}

// String is a short value-free account of the lock wait.
func (w LockWait) String() string {
	mode := w.Mode
	if mode == "" {
		mode = "a lock"
	}
	held := "unknown"
	if w.Held >= 0 {
		held = w.Held.String()
	}
	state := ""
	if w.State != "" {
		state = ", " + w.State
	}
	return fmt.Sprintf("session %s (from %s%s) holds %s on %s; %d session(s) waiting, longest %s; holder's transaction age %s",
		w.Holder, w.Peer, state, mode, w.Object, w.Waiters, w.Waited, held)
}
