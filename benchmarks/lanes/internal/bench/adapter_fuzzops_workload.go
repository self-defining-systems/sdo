package bench

// The built-in fuzzops workload: the default adapter for a spec that names no workload. It maps every verb of the
// workload contract (docs/contract.md) onto the pinned checkout's bin/fuzzops, with the command lines, environment and
// process-group handling specs written before the contract rely on. Other workloads are adapter executables
// (execWorkload) and need none of this.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// fuzzopsFootprint is the lane size of the apps that need more than the default. A GitLab lane is busy on about 8 cores
// during a rollout and holds 11-13 GiB of anonymous memory in a quiet window, but with an operator beside it and its
// build's page cache it needs more: at 20Gi (two workers of 8.5 GiB) a GitLab run with an attached operator had 6
// processes OOM-killed inside its nodes, so a GitLab lane gets 30Gi (workers of 12.75 GiB).
func fuzzopsFootprint(app string) *LaneLimit {
	if strings.Contains(strings.ToLower(filepath.Base(strings.TrimRight(app, "/"))), "gitlab") {
		return &LaneLimit{CPUs: 8, Memory: "30Gi"}
	}
	return nil
}

// WorkloadFuzzops is the name of the built-in workload (and the default).
const WorkloadFuzzops = "fuzzops"

const defaultWorkload = WorkloadFuzzops

func init() {
	reg := builtinWorkload{
		New:       func(dir string) Workload { return fuzzopsWorkload{dir: dir} },
		Identity:  func(dir string) string { return shellQuote(filepath.Join(dir, "bin", "fuzzops")) + " version" },
		Footprint: fuzzopsFootprint,
		CommandLine: func(dir string, c WorkloadCall) ([]string, error) {
			argv, err := fuzzopsArgv(c)
			if err != nil {
				return nil, err
			}
			return append([]string{filepath.Join(dir, "bin", "fuzzops")}, argv...), nil
		},
	}
	builtinWorkloads[""] = reg
	builtinWorkloads[WorkloadFuzzops] = reg
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

func (w fuzzopsWorkload) Grade(ctx context.Context, dir string, env []string, log io.Writer) error {
	cmd := exec.CommandContext(ctx, filepath.Join(w.dir, "bin", "fuzzops"), "report", dir)
	cmd.Dir, cmd.Stdout, cmd.Stderr = w.dir, log, log
	cmd.Env = append(os.Environ(), env...)
	return cmd.Run()
}
