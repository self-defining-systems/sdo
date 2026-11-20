package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// Hook verbs and their time limits (docs/bench.md, "Hook contract").
var hookTimeouts = map[string]time.Duration{
	"check":           time.Minute,
	"prepare":         30 * time.Minute,
	"idle":            time.Minute,
	"collect":         10 * time.Minute,
	"reset-incidents": 5 * time.Minute,
	// reset: the hooks' own bound is a controller resumed with up to 15 minutes to start, a second pass and the
	// app restore (about 25 minutes at worst); the limit is above it so the manager's kill never lands between the
	// hook stopping the controller and its restart in a finally.
	"reset": 35 * time.Minute,
	// swap: a claim that restores the golden copy (at most two full restores, each bounded as reset is, after the
	// reset a moved lane gets first: three resets) is about 45 minutes at worst; a kill earlier than the hooks' own
	// bounds would land between the controller stop and its restart.
	"swap": 45 * time.Minute,
	// snapshot: the copy, and after a late incident a lane reset: copy, the reset's 5 minutes, the put-back
	// (up to 35 minutes) and 5 more minutes; stated, not the default hour.
	"snapshot":    90 * time.Minute,
	"teardown":    15 * time.Minute,
	"transcripts": 2 * time.Minute,
	// The verbs the contract adds (docs/contract.md); no step calls them yet except preflight.
	"preflight":       10 * time.Minute,
	"export-artifact": 10 * time.Minute,
	"verify-artifact": 5 * time.Minute,
	"reseed":          30 * time.Minute,
	"verify-start":    5 * time.Minute,
}

// SubjectVerbs are the verbs of the subject contract (docs/contract.md): the operator under test.
var SubjectVerbs = []string{"check", "prepare", "swap", "idle", "watch", "collect", "snapshot", "reset", "teardown", "transcripts",
	"preflight", "export-artifact", "verify-artifact", "reseed", "verify-start"}

// Unsupported reports that the hook does not implement the verb: exit 64, a last stdout line {"unsupported":true},
// or (hooks that predate the contract's newer verbs) a usage error, exit 2 with nothing on stdout.
func (r HookResult) Unsupported() bool {
	return isUnsupported(r.Exit, r.Last) || (r.Exit == 2 && r.First == "")
}

// HookOperator is the BENCH_OPERATOR file: the spec's operator block with the
// baseline resolved to its copy on the host.
type HookOperator struct {
	Hooks    string         `json:"hooks"`
	Revision string         `json:"revision"`
	Baseline string         `json:"baseline,omitempty"`
	Seed     string         `json:"seed,omitempty"`
	Settings map[string]any `json:"settings,omitempty"`
}

// Hook is one operator's hook executable on one host for one lane.
type Hook struct {
	Dir     string   // the pinned hooks checkout (working directory)
	Command string   // run as `sh -c "<Command> <verb>"`
	Env     []string // BENCH_* and FUZZOPS_* for the lane
}

// HookResult is how a hook call ended.
type HookResult struct {
	Exit   int    // -1: it could not run or timed out
	First  string // its first stdout line
	Last   string // its last stdout line
	Err    string
	Output string // stdout and stderr, kept for the log
}

// OK reports exit 0.
func (r HookResult) OK() bool { return r.Exit == 0 }

// Why is a one-line reason for a failed call: the last line it wrote (stdout or stderr), where a failing
// command says what went wrong.
func (r HookResult) Why(verb string) string {
	last := ""
	for _, l := range strings.Split(strings.TrimSpace(r.Output), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			last = l
		}
	}
	switch {
	case r.Err != "":
		return fmt.Sprintf("%s: %s", verb, r.Err)
	case r.Exit != 0 && last != "":
		return fmt.Sprintf("%s exit %d: %s", verb, r.Exit, last)
	case r.First != "":
		return fmt.Sprintf("%s exit %d: %s", verb, r.Exit, r.First)
	default:
		return fmt.Sprintf("%s exit %d", verb, r.Exit)
	}
}

// Run calls verb with extra environment, bounded by the verb's time limit; the
// whole output goes to log too (nil: none).
func (h Hook) Run(ctx context.Context, verb string, extra []string, log io.Writer) HookResult {
	d := hookTimeouts[verb]
	if d == 0 {
		d = time.Hour
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", h.Command+" "+verb)
	cmd.Dir = h.Dir
	cmd.Env = append(append(os.Environ(), h.Env...), extra...)
	var stdout, all bytes.Buffer
	w := io.Writer(&all)
	if log != nil {
		w = io.MultiWriter(&all, log)
	}
	cmd.Stdout = io.MultiWriter(&stdout, w)
	cmd.Stderr = w
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 10 * time.Second
	err := cmd.Run()
	res := HookResult{Output: all.String()}
	sc := bufio.NewScanner(&stdout)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if res.First == "" {
			res.First = line
		}
		res.Last = line
	}
	switch e := err.(type) {
	case nil:
	case *exec.ExitError:
		res.Exit = e.ExitCode()
		if ctx.Err() != nil {
			res.Exit, res.Err = -1, fmt.Sprintf("timed out after %s", d)
		}
	default:
		res.Exit, res.Err = -1, err.Error()
	}
	return res
}

// Prepared is the JSON on prepare's last stdout line.
type prepareOut struct {
	Pack            string `json:"pack"`
	OperatorVersion string `json:"operator_version"`
	Memory          string `json:"memory"`
}

func parsePrepare(last string) (prepareOut, error) {
	var p prepareOut
	if err := json.Unmarshal([]byte(last), &p); err != nil {
		return p, fmt.Errorf("prepare: last line is not JSON: %q", last)
	}
	if p.Pack == "" {
		return p, fmt.Errorf("prepare: no pack in %q", last)
	}
	return p, nil
}
