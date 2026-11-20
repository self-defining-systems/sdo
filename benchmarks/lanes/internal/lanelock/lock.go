// Package lanelock is the one-owner rule of a lane: every fuzzops command that
// changes what runs in a namespace (run, recover, app up and down) holds an
// exclusive lock on <StateDir>/locks/<context>_<namespace>.lock while it works.
// A second command on the same lane fails at once instead of racing the
// first; the lock is a flock, so it is released when its holder exits, killed
// or not.
package lanelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/env"
)

// Exit codes of the commands that take a lane and that a step of the bench waits on or gives up on.
const (
	// ExitBusy: another process holds the lane. Transient: it is released when its holder ends.
	ExitBusy = 2
	// ExitInvalid: the command's input cannot work however often it is tried (a pack that is missing or does not
	// parse, an unknown field, a flag error). Permanent: a caller must fail with the reason, not retry.
	ExitInvalid = 3
)

// ErrBusy means another process holds the lane.
var ErrBusy = errors.New("lane busy")

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// Path is the lock file of e's lane.
func Path(e env.Env) string {
	e = e.WithDefaults()
	return filepath.Join(e.StateDir, "locks", unsafeChars.ReplaceAllString(e.KubeContext+"_"+e.Namespace, "_")+".lock")
}

// Take locks e's lane for this process, or fails with ErrBusy (naming the
// holder's pid and command) when another process holds it. Release unlocks.
func Take(e env.Env, what string) (release func(), err error) {
	p := Path(e)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		holder, _ := os.ReadFile(p)
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s (held by %s)", ErrBusy, e.WithDefaults().KubeContext+"/"+e.WithDefaults().Namespace, strings.TrimSpace(string(holder)))
		}
		return nil, err
	}
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("pid %d: %s\n", os.Getpid(), what)), 0)
	return func() {
		_ = f.Truncate(0)
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// Held reports whether some process holds e's lane right now.
func Held(e env.Env) bool {
	f, err := os.OpenFile(Path(e), os.O_RDWR, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}
