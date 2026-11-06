package sandboxio_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"syscall"
	"testing"
	"time"

	"sdo.dev/operator/internal/sandboxio"
)

// Handlers run in a child of this test binary: Register serves them from init
// when the binary is started as their child.
func init() {
	sandboxio.Register("test-echo", func(in []byte) []byte { return append([]byte("echo:"), in...) })
	sandboxio.Register("test-hog", func([]byte) []byte {
		var held [][]byte
		for {
			held = append(held, bytes.Repeat([]byte("x"), 64<<20))
		}
	})
	// Fills the data limit with touched heap (below the soft heap limit, so the
	// GC never runs), then asks the runtime for threads whose stacks cannot fit.
	sandboxio.Register("test-threads", func([]byte) []byte {
		held := make([][]byte, 0, 64)
		for i := 0; i < 12; i++ { // 12 x 16 MiB of the 256 MiB data limit, then stacks
			held = append(held, bytes.Repeat([]byte("x"), 16<<20))
		}
		_ = held
		block := make(chan struct{})
		for {
			go func() { runtime.LockOSThread(); <-block }()
			runtime.Gosched()
		}
	})
	sandboxio.Register("test-spin", func([]byte) []byte {
		for {
		}
	})
	sandboxio.Register("test-flood", func([]byte) []byte { return bytes.Repeat([]byte("y"), 2<<20) })
	// Killed by something other than the parent, as the OOM killer does.
	sandboxio.Register("test-selfkill", func([]byte) []byte {
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
		select {}
	})
	sandboxio.Register("test-panic", func([]byte) []byte { panic("handler bug") })
}

var small = sandboxio.Limits{Memory: 128 << 20, Output: 1 << 20, Procs: 1}

func TestChildAnswersTheRequest(t *testing.T) {
	out, err := sandboxio.Run(t.Context(), "test-echo", []byte("ping"), small)
	if err != nil || string(out) != "echo:ping" {
		t.Fatalf("got %q, %v", out, err)
	}
}

func TestChildFailuresAreTyped(t *testing.T) {
	for name, kind := range map[string]sandboxio.Kind{
		"test-hog":   sandboxio.KindMemory,
		"test-flood": sandboxio.KindOutput,
		"test-panic": sandboxio.KindCrash,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sandboxio.Run(t.Context(), name, nil, small)
			var serr *sandboxio.Error
			if !errors.As(err, &serr) || serr.Kind != kind || !errors.Is(err, sandboxio.ErrChild) {
				t.Fatalf("want a %s child error, got %v", kind, err)
			}
		})
	}
}

func TestContextEndKillsTheChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := sandboxio.Run(ctx, "test-spin", nil, small)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sandboxio.ErrChild) {
		t.Fatalf("want the context's error, got %v", err)
	}
}

func TestUnregisteredHandlerIsRefused(t *testing.T) {
	if _, err := sandboxio.Run(t.Context(), "nope", nil, small); err == nil {
		t.Fatal("ran an unregistered handler")
	}
}

// A child that cannot be started (fork/exec failure, EAGAIN, ENOMEM at spawn)
// says nothing about what the handler would have done: it is ErrSpawn, never
// an *Error (ErrChild), so callers can tell a host resource blip from a
// verdict on the request.
func TestSpawnFailureIsNotAChildFailure(t *testing.T) {
	restore := sandboxio.SetExecutable(func() (string, error) { return "/nonexistent/sdo-child", nil })
	defer restore()
	_, err := sandboxio.Run(t.Context(), "test-echo", []byte("x"), sandboxio.Limits{})
	if !errors.Is(err, sandboxio.ErrSpawn) {
		t.Fatalf("spawn failure = %v, want ErrSpawn", err)
	}
	if errors.Is(err, sandboxio.ErrChild) {
		t.Fatalf("spawn failure matches ErrChild: %v", err)
	}
}

// Failures after the child runs stay child failures, never spawn failures.
func TestRunningChildFailureIsNotASpawnFailure(t *testing.T) {
	_, err := sandboxio.Run(t.Context(), "test-panic", nil, sandboxio.Limits{})
	if !errors.Is(err, sandboxio.ErrChild) || errors.Is(err, sandboxio.ErrSpawn) {
		t.Fatalf("crash = %v, want ErrChild and not ErrSpawn", err)
	}
}

// RLIMIT_DATA counts thread stacks, so a child near its limit can fail to
// create a thread before the heap itself is refused. That is still the memory
// limit being hit (a verdict on the request), not a generic crash.
func TestThreadStarvationUnderTheLimitIsMemory(t *testing.T) {
	_, err := sandboxio.Run(t.Context(), "test-threads", nil, small)
	var serr *sandboxio.Error
	if !errors.As(err, &serr) || serr.Kind != sandboxio.KindMemory {
		t.Fatalf("want a memory child error, got %v", err)
	}
}

// The runtime reports a refused thread stack as a pthread_create failure. At a
// large peak that is the memory limit; at a small one it is the host (pid or
// process limits), which must not be blamed on the request.
func TestThreadCreationFailureIsMemoryOnlyNearTheLimit(t *testing.T) {
	const msg = "runtime/cgo: pthread_create failed: Resource temporarily unavailable\nSIGABRT"
	limits := sandboxio.Limits{Memory: 512 << 20}
	for name, tc := range map[string]struct {
		peak int64
		want sandboxio.Kind
	}{"near the limit": {400 << 20, sandboxio.KindMemory}, "small peak": {20 << 20, sandboxio.KindCrash}} {
		var serr *sandboxio.Error
		if err := sandboxio.Classify(msg, tc.peak, limits); !errors.As(err, &serr) || serr.Kind != tc.want {
			t.Errorf("%s: got %v, want %s", name, err, tc.want)
		}
	}
}

// Only a failure the request caused is a verdict on it. A SIGKILL we did not
// send (the cgroup or host OOM killer: the child is its first choice) at a
// small peak says nothing about the request.
func TestKillFromOutsideIsNotAChildFailure(t *testing.T) {
	_, err := sandboxio.Run(t.Context(), "test-selfkill", nil, small)
	if !errors.Is(err, sandboxio.ErrInterrupted) || errors.Is(err, sandboxio.ErrChild) {
		t.Fatalf("external SIGKILL at a small peak = %v, want ErrInterrupted and not ErrChild", err)
	}
}

// A SIGKILL we did not send, after the child grew past half its limit, is the
// memory limit (the kernel's OOM killer got there first); below it, the host.
func TestKillFromOutsideIsMemoryOnlyNearTheLimit(t *testing.T) {
	limits := sandboxio.Limits{Memory: 128 << 20}
	var serr *sandboxio.Error
	if err := sandboxio.ClassifyKill(80<<20, limits); !errors.As(err, &serr) || serr.Kind != sandboxio.KindMemory {
		t.Errorf("kill near the limit = %v, want a memory child error", err)
	}
	if err := sandboxio.ClassifyKill(20<<20, limits); !errors.Is(err, sandboxio.ErrInterrupted) || errors.Is(err, sandboxio.ErrChild) {
		t.Errorf("kill at a small peak = %v, want ErrInterrupted", err)
	}
}

// Our own deadline kill is the request's verdict, and it is told apart from
// the caller's context.
func TestWallClockLimitIsOurDeadline(t *testing.T) {
	limits := small
	limits.Wall = 200 * time.Millisecond
	_, err := sandboxio.Run(t.Context(), "test-spin", nil, limits)
	if !errors.Is(err, sandboxio.ErrWallClock) || errors.Is(err, sandboxio.ErrChild) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrWallClock only, got %v", err)
	}
}

// Waiting for a child slot is not the request's time: the wall clock starts
// once the slot is held, and the wait is bounded by the caller's context.
func TestWallClockStartsAfterTheSlot(t *testing.T) {
	limits := small
	limits.Wall = 600 * time.Millisecond
	release := sandboxio.HoldSlots(sandboxio.MaxChildren)
	clock, stop := context.WithTimeout(t.Context(), 800*time.Millisecond) // longer than Wall
	defer stop()
	go func() { <-clock.Done(); release() }()
	out, err := sandboxio.Run(t.Context(), "test-echo", []byte("late"), limits)
	if err != nil || string(out) != "echo:late" {
		t.Fatalf("a render queued past Wall then ran: got %q, %v", out, err)
	}
}

func TestQueueWaitEndsWithTheCallersContext(t *testing.T) {
	defer sandboxio.HoldSlots(sandboxio.MaxChildren)()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	limits := small
	limits.Wall = 10 * time.Second
	_, err := sandboxio.Run(ctx, "test-echo", nil, limits)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sandboxio.ErrWallClock) || errors.Is(err, sandboxio.ErrChild) {
		t.Fatalf("queue expiry = %v, want the caller's deadline", err)
	}
}

// A child's rusage peak starts at its parent's own high-water mark, so a big
// parent (the controller) must not make an external kill look like memory use.
func TestKillFromOutsideIsJudgedByTheChildsOwnPeak(t *testing.T) {
	parent := bytes.Repeat([]byte("x"), 160<<20) // over half of small.Memory
	_, err := sandboxio.Run(t.Context(), "test-selfkill", nil, small)
	runtime.KeepAlive(parent)
	if !errors.Is(err, sandboxio.ErrInterrupted) {
		t.Fatalf("external SIGKILL under a large parent = %v, want ErrInterrupted", err)
	}
}
