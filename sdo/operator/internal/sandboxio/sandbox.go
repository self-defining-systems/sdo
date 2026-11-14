// Package sandboxio runs a registered byte-in, byte-out function in a child
// process of the running binary, so that whatever the function does with its
// input (exhaust memory, which is a Go fatal no recover() catches, or loop
// without end) kills or stalls the child, never the caller.
//
// The child is the same executable started with a hidden argument. Any binary
// that links the package registering a handler (with Register, from an init
// function) therefore also serves it: the controller, the operator CLI and test
// binaries alike. The parent bounds the child's memory (RLIMIT_DATA plus a soft
// Go heap limit), its wall-clock time (the caller's context; the child is
// killed by its own PID), its stdout and its environment (empty but for the
// limits). The child cannot outlive the parent on Linux (parent-death signal)
// and is the first choice of the kernel's OOM killer.
package sandboxio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// childArg is the hidden first argument that turns a process into a child.
const childArg = "__sdo-sandbox-child"

// envMemory carries Limits.Memory to the child.
const envMemory = "SDO_SANDBOX_MEMORY"

// Handler computes a response from a request. It runs in the child process.
type Handler func(request []byte) []byte

// Limits bound one child. Zero fields take the defaults.
type Limits struct {
	// Memory is the child's Go heap limit in bytes; RLIMIT_DATA is twice it,
	// because the data limit also counts heap the runtime has not yet returned.
	Memory int64
	// Output caps the response (the child's stdout) in bytes.
	Output int64
	// Procs is the child's GOMAXPROCS.
	Procs int
	// Wall bounds the child's wall-clock time, counted from when it holds a
	// slot (time queued behind other children is not the request's). Zero
	// means only the caller's context bounds it. Expiry is ErrWallClock.
	Wall time.Duration
}

// DefaultLimits suit one Helm chart render: the largest real charts render in
// well under 200 MiB of heap.
var DefaultLimits = Limits{Memory: 512 << 20, Output: 64 << 20, Procs: 2}

func (l Limits) withDefaults() Limits {
	if l.Memory <= 0 {
		l.Memory = DefaultLimits.Memory
	}
	if l.Output <= 0 {
		l.Output = DefaultLimits.Output
	}
	if l.Procs <= 0 {
		l.Procs = DefaultLimits.Procs
	}
	return l
}

// maxChildren bounds concurrent children per process, so a burst of renders
// cannot multiply the memory limit beyond the controller Pod's.
const maxChildren = 2

var (
	slots    = make(chan struct{}, maxChildren)
	mu       sync.Mutex
	handlers = map[string]Handler{}
)

// Kind classifies a failed child.
type Kind string

const (
	// KindMemory: the child ran out of its memory limit.
	KindMemory Kind = "memory"
	// KindOutput: the response passed Limits.Output.
	KindOutput Kind = "output"
	// KindCrash: the child exited abnormally for another reason.
	KindCrash Kind = "crash"
)

// ErrChild matches every *Error: the child failed because of what the handler
// did with its request. A child that could not be started, or one stopped by
// the caller's context, is not an *Error.
var ErrChild = errors.New("sandboxed child failed")

// Error reports a child that exceeded a limit or crashed.
type Error struct {
	Name   string
	Kind   Kind
	Detail string
}

func (e *Error) Error() string {
	return fmt.Sprintf("sandboxed %s: %s: %s", e.Name, e.Kind, e.Detail)
}

// Is makes the error match ErrChild.
func (e *Error) Is(target error) bool { return target == ErrChild }

// ErrSpawn matches a failure to start the child at all: no executable, a pipe
// that could not be made, or fork/exec refused (EAGAIN, ENOMEM). It says
// nothing about the request, so it is never an *Error and never ErrChild; a
// caller treats it as a transient host condition.
var ErrSpawn = errors.New("sandboxio: child could not be started")

// ErrWallClock matches a child stopped because it passed Limits.Wall: our own
// deadline kill, a verdict on the request.
var ErrWallClock = errors.New("sandboxio: child passed its wall-clock limit")

// ErrInterrupted matches a child killed by SIGKILL that neither we sent (our
// deadline, output cap and caller cancellation are accounted for) nor its
// memory use explains: the host's OOM killer or an operator. It says nothing
// about the request, so it is never an *Error and never ErrChild; a caller
// treats it as a transient host condition.
var ErrInterrupted = errors.New("sandboxio: child was killed from outside")

// executableFn finds the child's executable; tests replace it.
var executableFn = executable

// Register names h for Run. When this process was started as the child for
// name, Register serves one request from stdin and exits; call it from an init
// function so the child never runs anything else of the binary.
func Register(name string, h Handler) {
	mu.Lock()
	if _, dup := handlers[name]; dup {
		mu.Unlock()
		panic("sandboxio: handler " + name + " registered twice")
	}
	handlers[name] = h
	mu.Unlock()
	if len(os.Args) == 3 && os.Args[1] == childArg && os.Args[2] == name {
		os.Exit(serve(h))
	}
}

func serve(h Handler) int {
	memory, err := strconv.ParseInt(os.Getenv(envMemory), 10, 64)
	if err != nil || memory <= 0 {
		fmt.Fprintf(os.Stderr, "sandbox child: invalid %s\n", envMemory)
		return 3
	}
	if err := limitSelf(memory); err != nil {
		fmt.Fprintf(os.Stderr, "sandbox child: %v\n", err)
		return 3
	}
	debug.SetMemoryLimit(memory)
	request, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sandbox child: read request: %v\n", err)
		return 3
	}
	if _, err := os.Stdout.Write(h(request)); err != nil {
		return 3
	}
	return 0
}

// Run sends request to the child serving name and returns its response. A
// child that exceeds a limit or crashes is an *Error, one that cannot be
// started wraps ErrSpawn; when ctx ends first the
// child is killed and the error wraps context.Cause(ctx). Waiting for a slot
// is bounded by ctx alone; Limits.Wall starts after the slot is held.
func Run(ctx context.Context, name string, request []byte, limits Limits) ([]byte, error) {
	mu.Lock()
	_, ok := handlers[name]
	mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("sandboxio: no handler registered as %q", name)
	}
	limits = limits.withDefaults()
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	case <-ctx.Done():
		return nil, fmt.Errorf("sandboxed %s not started: %w", name, context.Cause(ctx))
	}
	parent := ctx
	if limits.Wall > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeoutCause(ctx, limits.Wall, ErrWallClock)
		defer stop()
	}
	exe, err := executableFn()
	if err != nil {
		return nil, fmt.Errorf("sandboxed %s: find own executable: %w: %w", name, ErrSpawn, err)
	}
	cmd := exec.CommandContext(ctx, exe, childArg, name)
	cmd.Env = []string{
		envMemory + "=" + strconv.FormatInt(limits.Memory, 10),
		"GOMAXPROCS=" + strconv.Itoa(limits.Procs),
		"GOTRACEBACK=single",
	}
	cmd.Stdin = bytes.NewReader(request)
	stderr := &headBuffer{max: 4 << 10}
	cmd.Stderr = stderr
	cmd.WaitDelay = time.Second
	cmd.SysProcAttr = childAttributes()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("sandboxed %s: %w: %w", name, ErrSpawn, err)
	}
	if err := start(cmd); err != nil {
		return nil, fmt.Errorf("sandboxed %s: start child: %w: %w", name, ErrSpawn, err)
	}
	watch := watchPeak(cmd.Process.Pid)
	response, readErr := io.ReadAll(io.LimitReader(stdout, limits.Output+1))
	overflow := int64(len(response)) > limits.Output
	if overflow {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	peak := watch.finish(cmd.ProcessState)
	switch {
	case parent.Err() != nil:
		return nil, fmt.Errorf("sandboxed %s stopped: %w", name, context.Cause(parent))
	case ctx.Err() != nil:
		return nil, fmt.Errorf("sandboxed %s stopped after %s: %w", name, limits.Wall, context.Cause(ctx))
	case overflow:
		return nil, &Error{Name: name, Kind: KindOutput, Detail: fmt.Sprintf("response passed %d bytes", limits.Output)}
	case waitErr != nil:
		return nil, classify(name, waitErr, stderr.String(), peak, killedBySIGKILL(cmd.ProcessState), limits)
	case readErr != nil:
		return nil, fmt.Errorf("sandboxed %s: read response: %w", name, readErr)
	}
	return response, nil
}

// threadFailures are the runtime's fatal messages when it cannot get memory
// for a new OS thread (its stack counts against RLIMIT_DATA, like the heap).
var threadFailures = []string{
	"pthread_create failed",              // cgo runtime
	"failed to create new OS thread",     // pure-Go runtime
	"cannot allocate memory",             // mmap/ENOMEM fatals
	"ThreadSanitizer failed to allocate", // race-enabled runtime, errno 12
}

// classify names why a started child failed. Running into RLIMIT_DATA is
// reported by the Go runtime in one of several ways depending on what asks for
// memory first: the heap (out of memory) or a new thread whose stack no longer
// fits (pthread_create/clone failed). Both are the memory limit when the child
// had grown to a large share of it; the same thread failure at a small peak is
// a host condition (process or pid limits), so it stays a crash.
//
// A SIGKILL reaching here is not ours (Run accounts for its own kills before
// classifying). At a peak near the limit it is the memory limit (the kernel's
// OOM killer reaching the child first); otherwise it is the host (the child is
// the OOM killer's first choice, oom_score_adj 1000, whatever the chart does)
// and is ErrInterrupted, never a verdict on the request.
func classify(name string, waitErr error, stderr string, peak int64, sigkill bool, limits Limits) error {
	detail := strings.TrimSpace(stderr)
	if i := strings.IndexByte(detail, '\n'); i >= 0 {
		// The first line of a Go fatal names it; the rest is a stack trace.
		detail = detail[:i]
	}
	memoryExit := strings.Contains(stderr, "out of memory")
	if !memoryExit && peak >= limits.Memory/2 {
		for _, m := range threadFailures {
			if strings.Contains(stderr, m) {
				memoryExit = true
				break
			}
		}
	}
	if sigkill && !memoryExit {
		if peak < limits.Memory/2 {
			return fmt.Errorf("sandboxed %s: %w: %w (peak %d MiB of a %d MiB limit)", name, ErrInterrupted, waitErr, peak>>20, limits.Memory>>20)
		}
		memoryExit = true
	}
	if memoryExit {
		return &Error{Name: name, Kind: KindMemory, Detail: fmt.Sprintf("exceeded the %d MiB memory limit (%s)", limits.Memory>>20, detail)}
	}
	if detail == "" {
		detail = waitErr.Error()
	} else {
		detail = waitErr.Error() + ": " + detail
	}
	return &Error{Name: name, Kind: KindCrash, Detail: detail}
}

// killedBySIGKILL reports whether the child died of SIGKILL.
func killedBySIGKILL(ps *os.ProcessState) bool {
	ws, ok := ps.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}

// headBuffer keeps the first max bytes written and discards the rest, so a
// child cannot grow the parent through stderr.
type headBuffer struct {
	bytes.Buffer
	max int
}

func (b *headBuffer) Write(p []byte) (int, error) {
	if room := b.max - b.Len(); room > 0 {
		if len(p) > room {
			b.Buffer.Write(p[:room])
		} else {
			b.Buffer.Write(p)
		}
	}
	return len(p), nil
}
