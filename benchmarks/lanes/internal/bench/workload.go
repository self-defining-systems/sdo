package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// The workload adapter role (docs/contract.md): the executable that owns the app and its grading. The platform
// calls it by verb; the built-in "fuzzops" workload maps every verb onto the pinned checkout's bin/fuzzops, so a
// spec with no workload runs exactly as it always did.
const (
	VerbIdentity    = "identity" // which workload revision answers (fuzzops: `version`)
	VerbAppUp       = "app-up"
	VerbAppSnapshot = "app-snapshot"
	VerbAppReset    = "app-reset"
	VerbGate        = "gate"
	VerbRun         = "run"
	VerbGrade       = "grade"
	VerbFootprint   = "footprint"
)

// WorkloadFuzzops is the name of the built-in workload (and the default).
const WorkloadFuzzops = "fuzzops"

// WorkloadVerbs are the verbs of the workload contract.
var WorkloadVerbs = []string{VerbIdentity, VerbAppUp, VerbAppSnapshot, VerbAppReset, VerbGate, VerbRun, VerbGrade, VerbFootprint}

// ErrUnsupported is a verb the adapter does not implement (docs/contract.md, "Unsupported verbs").
var ErrUnsupported = errors.New("verb not supported by the adapter")

// WorkloadCall is one call of a workload verb. Action says which of the verb's operations (app-up: up, down,
// images, check-pack; app-snapshot: write, verify; app-reset: recover; gate: check; run: run, active, undo;
// grade: report); Args are the operation's options, flag by flag.
type WorkloadCall struct {
	Verb, Action string
	Args         []string
	Env          []string
	Stdout       io.Writer // the verb's result stream (nil: the log)
	Log          io.Writer // the adapter's stderr (and, with no Stdout, stdout)
}

// Workload is an adapter that owns an app and its grading.
type Workload interface {
	// Call runs a verb. code is the adapter's exit status (-1: it could not run) and tail the end of what it said
	// on stderr. A verb the adapter does not implement returns ErrUnsupported.
	Call(ctx context.Context, c WorkloadCall) (code int, tail string, err error)
	// Grade turns the runs under dir into results (fuzzops: `report DIR`).
	Grade(ctx context.Context, dir string, log io.Writer) error
}

// NewWorkload is the workload a spec names: "" or "fuzzops" is the built-in one; anything else is a command run
// with `sh -c` in the pinned checkout dir.
func NewWorkload(name, dir string) Workload {
	if name == "" || name == WorkloadFuzzops {
		return fuzzopsWorkload{dir: dir}
	}
	return execWorkload{dir: dir, command: name}
}

// fuzzopsWorkload runs bin/fuzzops of the pinned checkout.
type fuzzopsWorkload struct{ dir string }

// fuzzopsArgv is the bin/fuzzops command line of a call: the verb's operation, then its options.
func fuzzopsArgv(c WorkloadCall) ([]string, error) {
	var op []string
	switch c.Verb + "/" + c.Action {
	case VerbIdentity + "/":
		op = []string{"version"}
	case VerbAppUp + "/up":
		op = []string{"app", "up"}
	case VerbAppUp + "/down":
		op = []string{"app", "down"}
	case VerbAppUp + "/images":
		op = []string{"app", "images"}
	case VerbAppUp + "/check-pack":
		op = []string{"pack", "load"}
	case VerbAppSnapshot + "/write", VerbAppSnapshot + "/verify":
		op = []string{"app", "fingerprint"}
	case VerbAppReset + "/recover":
		op = []string{"recover"}
	case VerbGate + "/check":
		op = []string{"check"}
	case VerbRun + "/run":
		op = []string{"run"}
	case VerbRun + "/active":
		op = []string{"active-instance"}
	case VerbRun + "/undo":
		op = []string{"undo-instance"}
	case VerbGrade + "/report":
		op = []string{"report"}
	default:
		return nil, fmt.Errorf("%w: %s %s", ErrUnsupported, c.Verb, c.Action)
	}
	return append(op, c.Args...), nil
}

// activeRE is how `fuzzops active-instance` and `undo-instance` say whether the fault is active.
var activeRE = regexp.MustCompile(`active=(true|false)`)

func (w fuzzopsWorkload) Call(ctx context.Context, c WorkloadCall) (int, string, error) {
	argv, err := fuzzopsArgv(c)
	if err != nil {
		return -1, "", err
	}
	if c.Verb == VerbRun && (c.Action == "active" || c.Action == "undo") && c.Stdout != nil {
		// The contract's answer is {"active": bool}; fuzzops prints active=<bool>, which goes to the log.
		var text bytes.Buffer
		final := c.Stdout
		c.Stdout = io.MultiWriter(&text, c.Log)
		code, tail, err := runAdapter(ctx, w.dir, exec.CommandContext(ctx, filepath.Join(w.dir, "bin", "fuzzops"), argv...), c, "fuzzops "+strings.Join(argv, " "))
		if m := activeRE.FindStringSubmatch(text.String()); m != nil {
			fmt.Fprintf(final, "{\"active\":%s}\n", m[1])
		}
		return code, tail, err
	}
	return runAdapter(ctx, w.dir, exec.CommandContext(ctx, filepath.Join(w.dir, "bin", "fuzzops"), argv...), c, "fuzzops "+strings.Join(argv, " "))
}

func (w fuzzopsWorkload) Grade(ctx context.Context, dir string, log io.Writer) error {
	cmd := exec.CommandContext(ctx, filepath.Join(w.dir, "bin", "fuzzops"), "report", dir)
	cmd.Dir, cmd.Stdout, cmd.Stderr = w.dir, log, log
	return cmd.Run()
}

// runAdapter runs one adapter process the way every workload call has always run: its own process group, SIGTERM
// on cancel (a cancelled run still undoes its fault, which gets up to ten minutes), stdout to c.Stdout and stderr
// to the log with its tail kept as the reason of a failure.
func runAdapter(ctx context.Context, dir string, cmd *exec.Cmd, c WorkloadCall, what string) (int, string, error) {
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), c.Env...)
	out := c.Stdout
	if out == nil {
		out = c.Log
	}
	var tail tailWriter
	cmd.Stdout, cmd.Stderr = out, io.MultiWriter(c.Log, &tail)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Minute
	fmt.Fprintf(c.Log, "%s %s\n", time.Now().UTC().Format("15:04:05"), what)
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), tail.String(), nil
	}
	if err != nil {
		return -1, tail.String(), err
	}
	return 0, tail.String(), nil
}

// execWorkload is an adapter executable (docs/contract.md): `sh -c "<command> <verb>"` in the pinned checkout,
// the call in the environment, the verb's result the last line of stdout.
type execWorkload struct{ dir, command string }

// WorkloadEnv is the environment of one call of an adapter executable.
func WorkloadEnv(c WorkloadCall) []string {
	args, _ := json.Marshal(append([]string{}, c.Args...))
	return append(append([]string{}, c.Env...), "WORKLOAD_VERB="+c.Verb, "WORKLOAD_ACTION="+c.Action, "WORKLOAD_ARGS="+string(args))
}

// lastNonEmpty is the last non-empty line of b.
func lastNonEmpty(b []byte) string {
	last := ""
	for _, l := range strings.Split(string(b), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			last = l
		}
	}
	return last
}

// isUnsupported reports the adapters' "not implemented" answer: exit 64 or a last stdout line {"unsupported":true}.
func isUnsupported(code int, last string) bool {
	if code == UnsupportedExit {
		return true
	}
	var v struct {
		Unsupported bool `json:"unsupported"`
	}
	return json.Unmarshal([]byte(last), &v) == nil && v.Unsupported
}

// UnsupportedExit is the exit status of an adapter verb it does not implement.
const UnsupportedExit = 64

func (w execWorkload) Call(ctx context.Context, c WorkloadCall) (int, string, error) {
	c.Env = WorkloadEnv(c)
	var out bytes.Buffer
	final := c.Stdout
	c.Stdout = &out
	cmd := exec.CommandContext(ctx, "sh", "-c", w.command+" "+c.Verb)
	code, tail, err := runAdapter(ctx, w.dir, cmd, c, w.command+" "+c.Verb+" "+c.Action)
	last := lastNonEmpty(out.Bytes())
	if err == nil && isUnsupported(code, last) {
		return code, tail, fmt.Errorf("%w: %s %s", ErrUnsupported, c.Verb, c.Action)
	}
	if final != nil && last != "" {
		fmt.Fprintln(final, last)
	}
	return code, tail, err
}

func (w execWorkload) Grade(ctx context.Context, dir string, log io.Writer) error {
	code, tail, err := w.Call(ctx, WorkloadCall{Verb: VerbGrade, Action: "report", Args: []string{dir}, Env: []string{"WORKLOAD_RESULTS=" + dir}, Log: log})
	if err == nil && code != 0 {
		err = fmt.Errorf("grade exit %d: %s", code, tail)
	}
	return err
}
