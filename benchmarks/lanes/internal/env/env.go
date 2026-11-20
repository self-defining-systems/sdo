// Package env holds the cluster settings every helper needs and the
// functions that shell out to kubectl and docker.
package env

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Env describes the cluster and app under test.
type Env struct {
	KindCluster string // kind cluster name, default "fuzzops"
	KubeContext string // default "kind-" + KindCluster
	Namespace   string // app namespace, e.g. "shop"
	NodePort    int    // api NodePort, also mapped to localhost on the host
	RepoRoot    string
	// StateDir holds the state a fault captured before it changed something
	// (see internal/fault/state.go); it lives outside the app namespace and
	// survives a restart of the fuzzops process. Default <RepoRoot>/results/state.
	StateDir string
	// ReportPath is the file an operator writes its diagnosis report to (see
	// CONTRACT.md). The run loop sets a fresh path per run and reads it after
	// the window. Empty: $FUZZOPS_REPORT is used when set.
	ReportPath string
	// TimeScale stretches rollout and readiness waits on a slow host (the
	// run sets it from the measured slowdown; 0 or 1: none). Pack scripts
	// get it as FUZZOPS_TIME_SCALE.
	TimeScale float64
	// LaneProvider is how the lane's cluster is made: "" or "kind" (docker containers as nodes, the default) or
	// "vcluster" (a virtual cluster on a shared host cluster: no node to exec into). Default $FUZZOPS_LANE_PROVIDER.
	LaneProvider string
	// EntryHost is the address users reach the app's NodePort on. Empty: the kind control-plane container's
	// address. Default $FUZZOPS_ENTRY_HOST.
	EntryHost string

	// run executes a command; nil means use os/exec. Tests set it.
	run    Runner
	stream Streamer
}

// TimelineFile is the name of the timeline file next to the report: an
// append-only JSONL file an operator may write lifecycle events to as it goes
// (CONTRACT.md, "Operator timeline").
const TimelineFile = "timeline.jsonl"

// TimelinePath is the timeline file next to the report file report ("" when
// report is "").
func TimelinePath(report string) string {
	if report == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(report), TimelineFile)
}

// Runner runs a command and returns its combined output.
type Runner func(ctx context.Context, stdin []byte, name string, args ...string) ([]byte, error)

// Default returns an Env with the default values filled in.
func Default() Env {
	return Env{}.WithDefaults()
}

// WithDefaults fills empty fields: cluster $FUZZOPS_KIND_CLUSTER or "fuzzops", context "kind-<cluster>",
// namespace "shop", NodePort 30080, StateDir <RepoRoot>/results/state.
func (e Env) WithDefaults() Env {
	if e.KindCluster == "" {
		e.KindCluster = os.Getenv("FUZZOPS_KIND_CLUSTER")
	}
	if e.KindCluster == "" {
		e.KindCluster = "fuzzops"
	}
	if e.KubeContext == "" {
		e.KubeContext = "kind-" + e.KindCluster
	}
	if e.LaneProvider == "" {
		e.LaneProvider = os.Getenv("FUZZOPS_LANE_PROVIDER")
	}
	if e.EntryHost == "" {
		e.EntryHost = os.Getenv("FUZZOPS_ENTRY_HOST")
	}
	if e.Namespace == "" {
		e.Namespace = "shop"
	}
	if e.NodePort == 0 {
		e.NodePort = 30080
	}
	if e.StateDir == "" {
		root := e.RepoRoot
		if root == "" {
			root = "."
		}
		e.StateDir = filepath.Join(root, "results", "state")
	}
	return e
}

// Scaled stretches d by TimeScale (bounded to 1..3).
func (e Env) Scaled(d time.Duration) time.Duration {
	s := min(e.TimeScale, 3)
	if s <= 1 {
		return d
	}
	return time.Duration(float64(d) * s)
}

// RolloutTimeout is the --timeout flag for a rollout wait of d, scaled.
func (e Env) RolloutTimeout(d time.Duration) string {
	return fmt.Sprintf("--timeout=%ds", int(e.Scaled(d).Seconds()))
}

// WithRunner returns a copy that uses r to run commands (for tests).
func (e Env) WithRunner(r Runner) Env {
	e.run = r
	return e
}

// BaseURL is the api address as seen from the host.
func (e Env) BaseURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", e.NodePort)
}

func (e Env) kubectlArgs(args []string) []string {
	e = e.WithDefaults()
	out := []string{"--context", e.KubeContext, "-n", e.Namespace}
	return append(out, args...)
}

func (e Env) exec(ctx context.Context, stdin []byte, name string, args ...string) ([]byte, error) {
	r := e.run
	if r == nil {
		r = osRunner
	}
	out, err := r(ctx, stdin, name, args...)
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

// CommandTimeout bounds one command whose context has no deadline of its own,
// so a hung kubectl, docker exec or script can never stall a run forever.
var CommandTimeout = 10 * time.Minute

func osRunner(ctx context.Context, stdin []byte, name string, args ...string) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, CommandTimeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != nil {
		cmd.Stdin = strings.NewReader(string(stdin))
	}
	KillGroup(cmd)
	return cmd.CombinedOutput()
}

// KillGroup makes a cancelled command take its children with it: it runs in
// its own process group, the whole group is killed when ctx ends, and output
// pipes a stray child still holds are given up after 10 s instead of blocking
// the caller.
func KillGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 10 * time.Second
}

// Kubectl runs kubectl with --context and -n Namespace added.
func (e Env) Kubectl(ctx context.Context, args ...string) ([]byte, error) {
	return e.exec(ctx, nil, "kubectl", e.kubectlArgs(args)...)
}

// KubectlStdin is like Kubectl but feeds stdin (for `apply -f -`).
func (e Env) KubectlStdin(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	if stdin == nil {
		stdin = []byte{}
	}
	return e.exec(ctx, stdin, "kubectl", e.kubectlArgs(args)...)
}

// ProviderVcluster is the lane provider whose cluster has no nodes the bench can exec into.
const ProviderVcluster = "vcluster"

// ErrNoNodes is returned by NodeExec on a lane whose provider has no nodes to exec into.
var ErrNoNodes = errors.New("this lane's cluster has no nodes to exec into (lane provider " + ProviderVcluster + ")")

// HasNodes reports whether the lane's nodes can be exec'd into (kind: yes; vcluster: no). Node audit, node
// faults and node drain churn need it.
func (e Env) HasNodes() bool { return e.WithDefaults().LaneProvider != ProviderVcluster }

// NodeExec runs a command inside a kind node (a docker container).
func (e Env) NodeExec(ctx context.Context, node string, args ...string) ([]byte, error) {
	if !e.HasNodes() {
		return nil, ErrNoNodes
	}
	return e.exec(ctx, nil, "docker", append([]string{"exec", node}, args...)...)
}

// PodNode returns the name of the node a pod runs on.
func (e Env) PodNode(ctx context.Context, pod string) (string, error) {
	out, err := e.Kubectl(ctx, "get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
	if err != nil {
		return "", err
	}
	node := strings.TrimSpace(string(out))
	if node == "" {
		return "", fmt.Errorf("pod %s is not scheduled to a node", pod)
	}
	return node, nil
}

// PodsByLabel returns pod names matching a label selector like "app=api".
func (e Env) PodsByLabel(ctx context.Context, selector string) ([]string, error) {
	out, err := e.Kubectl(ctx, "get", "pods", "-l", selector, "-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(out)), nil
}

// Streamer runs a command with stdin and writes its stdout to out (stderr to
// errOut). Tests set it with WithStreamer.
type Streamer func(ctx context.Context, stdin io.Reader, out, errOut io.Writer, name string, args ...string) error

// WithStreamer returns a copy that uses s for Stream (for tests).
func (e Env) WithStreamer(s Streamer) Env {
	e.stream = s
	return e
}

// Stream runs a long-lived command (such as an agent in a container) and
// streams its output as it is produced. Use Env.exec-style helpers for
// everything that returns a result.
func (e Env) Stream(ctx context.Context, stdin io.Reader, out, errOut io.Writer, name string, args ...string) error {
	if e.stream != nil {
		return e.stream(ctx, stdin, out, errOut, name, args...)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, out, errOut
	KillGroup(cmd)
	return cmd.Run()
}
