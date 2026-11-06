package sandboxio_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
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
