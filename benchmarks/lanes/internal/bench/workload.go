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

// builtinWorkload is a workload adapter that lanes carries, for specs written before the contract existed.
type builtinWorkload struct {
	New func(dir string) Workload
	// Identity is the shell command that prints the identity JSON ({"revision","modified"}) of the checkout at dir.
	Identity func(dir string) string
}

// builtinWorkloads are the built-in adapters by the workload name a spec may carry (adapter_*.go registers them).
var builtinWorkloads = map[string]builtinWorkload{}

// NewWorkload is the workload a spec names: a built-in one by its name, else a command run with `sh -c` in the
// pinned checkout dir.
func NewWorkload(name, dir string) Workload {
	if b, ok := builtinWorkloads[name]; ok {
		return b.New(dir)
	}
	return execWorkload{dir: dir, command: name}
}

// IdentityCommand is the shell command that asks the workload `name`, checked out at dir, for its identity: the
// `identity` verb of the contract (a built-in adapter may answer it its own way).
func IdentityCommand(name, dir string) string {
	if b, ok := builtinWorkloads[name]; ok {
		return b.Identity(dir)
	}
	return "cd " + shellQuote(dir) + " && WORKLOAD_VERB=" + VerbIdentity + " WORKLOAD_ACTION= WORKLOAD_ARGS='[]' sh -c " + shellQuote(name+" "+VerbIdentity)
}

// WorkloadName is the name the spec's workload goes by: the name it carries, or the default built-in adapter's.
func (s Spec) WorkloadName() string {
	if s.Workload == "" {
		return defaultWorkload
	}
	return s.Workload
}

// runAdapter runs one adapter process the way every workload call has always run: its own process group, SIGTERM
// on cancel (a cancelled run still undoes its fault, which gets up to ten minutes), stdout to c.Stdout and stderr
// to the log with its tail kept as the reason of a failure.
func runAdapter(ctx context.Context, dir string, cmd *exec.Cmd, c WorkloadCall, what string) (int, string, error) {
	// Each verb has a limit (hookTimeouts): past it the adapter's process group is killed and the reason says so.
	limit := hookTimeouts[c.Verb]
	parent := ctx
	if limit > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limit)
		defer cancel()
		cmd = exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
	}
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), c.Env...)
	out := c.Stdout
	if out == nil {
		out = c.Log
	}
	var tail tailWriter
	cmd.Stdout, cmd.Stderr = out, io.MultiWriter(c.Log, &tail)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if parent.Err() == nil { // our own limit: no undo to wait for
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = 10 * time.Minute
	fmt.Fprintf(c.Log, "%s %s\n", time.Now().UTC().Format("15:04:05"), what)
	err := cmd.Run()
	if limit > 0 && parent.Err() == nil && ctx.Err() == context.DeadlineExceeded {
		return -1, tail.String(), fmt.Errorf("workload verb %s exceeded its limit %s and was killed", c.Verb, limit)
	}
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
