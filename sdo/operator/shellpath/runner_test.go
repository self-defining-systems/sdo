package shellpath

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResultsPersistenceAndDetectorErrors(t *testing.T) {
	at := time.Unix(100, 0)
	for _, tc := range []struct {
		name  string
		codes []int
		errs  []bool
		fired []bool
	}{
		{"clear", []int{0, 0}, nil, []bool{false, false}},
		{"twice", []int{1, 1}, nil, []bool{false, true}},
		{"blip", []int{1, 0, 1}, nil, []bool{false, false, false}},
		{"error resets", []int{1, -1, 1, 1}, []bool{false, true, false, false}, []bool{false, false, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewState([]Script{{Path: "health/check.sh", Class: "health"}})
			for i, code := range tc.codes {
				r := Result{Script: "health/check.sh", ExitCode: code, At: at.Add(time.Duration(i) * time.Second)}
				if tc.errs != nil && tc.errs[i] {
					r.Error = "timeout"
				}
				state.Record(r)
				if state.Results()[0].Fired != tc.fired[i] {
					t.Fatalf("run %d: %#v", i, state.Results())
				}
			}
		})
	}
}

func TestDebouncedBurstAndPeriodicSweep(t *testing.T) {
	now := time.Unix(100, 0)
	clock := NewSchedule(now)
	if !clock.Due(now) {
		t.Fatal("initial sweep missing")
	}
	for _, offset := range []time.Duration{time.Millisecond, 500 * time.Millisecond, time.Second} {
		clock.Event(now.Add(offset))
	}
	if clock.Due(now.Add(2 * time.Second)) {
		t.Fatal("burst was not debounced")
	}
	if !clock.Due(now.Add(3 * time.Second)) {
		t.Fatal("debounced event did not run")
	}
	if clock.Due(now.Add(29 * time.Second)) {
		t.Fatal("early sweep")
	}
	if !clock.Due(now.Add(30 * time.Second)) {
		t.Fatal("periodic sweep missing")
	}
}

func TestClosureStabilityRequiresEveryScriptClear(t *testing.T) {
	at := time.Unix(100, 0)
	state := NewState([]Script{{Path: "health/a.sh"}, {Path: "incidents/b.sh"}})
	state.Record(Result{Script: "health/a.sh", At: at, ExitCode: 0})
	if state.Healthy(at.Add(time.Minute)) {
		t.Fatal("unevaluated script cleared")
	}
	state.Record(Result{Script: "incidents/b.sh", At: at, ExitCode: 0})
	if state.Healthy(at.Add(14*time.Second)) || !state.Healthy(at.Add(15*time.Second)) {
		t.Fatal("stability window changed")
	}
	state.Record(Result{Script: "incidents/b.sh", At: at.Add(16 * time.Second), Error: "crash", ExitCode: -1})
	if state.Healthy(at.Add(time.Minute)) {
		t.Fatal("detector error established health")
	}
}

func TestBashExitTimeoutAndBoundedTail(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		name, body string
		exit       int
		timeout    bool
	}{
		{"clear", "exit 0", 0, false}, {"fault", "echo fault >&2; exit 1", 1, false},
		{"timeout", "sleep 30", -1, true}, {"cap", "printf '%020000dEND' 1; printf 'err' >&2", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := tc.name + ".sh"
			if err := os.WriteFile(filepath.Join(root, name), []byte(tc.body), 0700); err != nil {
				t.Fatal(err)
			}
			timeout := time.Second
			if tc.timeout {
				timeout = 20 * time.Millisecond
			}
			r := (BashExecutor{Timeout: timeout}).Execute(context.Background(), root, Script{Path: name})
			if r.ExitCode != tc.exit || r.TimedOut != tc.timeout {
				t.Fatalf("%#v", r)
			}
			if len(r.Stdout)+len(r.Stderr) > MaxOutputBytes {
				t.Fatal("unbounded output")
			}
			if tc.name == "cap" && (!r.Truncated || !strings.HasSuffix(r.Stdout, "END")) {
				t.Fatalf("tail: %#v", r)
			}
		})
	}
}

type concurrencyExecutor struct {
	mu          sync.Mutex
	active, max int
	release     <-chan struct{}
	started     chan<- struct{}
}

func (e *concurrencyExecutor) Execute(ctx context.Context, _ string, s Script) Result {
	e.mu.Lock()
	e.active++
	if e.active > e.max {
		e.max = e.active
	}
	e.mu.Unlock()
	select {
	case e.started <- struct{}{}:
	case <-ctx.Done():
	}
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	e.mu.Lock()
	e.active--
	e.mu.Unlock()
	return Result{Script: s.Path, At: time.Unix(100, 0)}
}
func TestRunnerBoundsParallelism(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	executor := &concurrencyExecutor{release: release, started: started}
	scripts := make([]Script, 12)
	for i := range scripts {
		scripts[i] = Script{Path: strings.Repeat("a", i+1) + ".sh"}
	}
	done := make(chan []Result, 1)
	go func() { done <- RunBatch(context.Background(), "root", scripts, executor) }()
	for range 4 {
		<-started
	}
	close(release)
	results := <-done
	if executor.max > 4 || len(results) != len(scripts) {
		t.Fatalf("max=%d results=%d", executor.max, len(results))
	}
}

func TestDiscoveryRejectsEscapingPaths(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".sdo/diagnostics/detectors/health/ready")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "check.sh"), []byte("exit 0"), 0700); err != nil {
		t.Fatal(err)
	}
	scripts, err := Discover(root)
	if err != nil || len(scripts) != 1 {
		t.Fatalf("%v %v", scripts, err)
	}
	if err := os.Symlink("/tmp", filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := Discover(root); err == nil {
		t.Fatal("symlink accepted")
	}
}
