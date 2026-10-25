package deployment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// HealthProbe evaluates the compiled, accepted detectors against live state.
// The default implementation builds from the accepted repository first.
type HealthProbe interface {
	Evaluate(context.Context, string, string, string) (string, error)
}

type AcceptedDetectorProbe struct {
	RequireComplete bool
	Kubeconfig      string
	MetricsURL      string
	Limit           time.Duration
	// Resolver supplies the host DNS transport preflight; nil uses the OS resolver.
	Resolver interface {
		LookupHost(context.Context, string) ([]string, error)
	}
}

func (probe AcceptedDetectorProbe) Evaluate(ctx context.Context, acceptedRoot, operatorRoot, namespace string) (string, error) {
	limit := probe.Limit
	if limit <= 0 {
		limit = 5 * time.Minute
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	projected, err := memory.AcceptedDiagnostics(bounded, acceptedRoot)
	if err != nil {
		return "", fmt.Errorf("resolve accepted diagnostics: %w", err)
	}
	workspace, err := builder.BuildWorkspace(bounded, builder.Config{ApplicationRoot: acceptedRoot, DiagnosticsRoot: projected.Directory, OperatorRoot: operatorRoot})
	if err != nil {
		return "", fmt.Errorf("stage accepted diagnostics: %w", err)
	}
	defer workspace.Close()
	if workspace.HasProber {
		if err := probe.checkTransport(bounded); err != nil {
			return "", err
		}
	}
	binary, err := workspace.ValidateAndBuild(bounded)
	if err != nil {
		return "", fmt.Errorf("compile accepted health detectors: %w", err)
	}
	args := []string{"--evaluate-once", "--namespace", namespace, "--app-root", acceptedRoot}
	if probe.RequireComplete {
		args = append(args, "--accept-seed")
	}
	if probe.MetricsURL != "" {
		args = append(args, "--metrics-url", probe.MetricsURL)
	}
	environment := os.Environ()
	if probe.Kubeconfig != "" {
		environment = append(environment, "KUBECONFIG="+probe.Kubeconfig)
	}
	if workspace.HasProber {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return "", fmt.Errorf("probe transport unavailable: reserve local prober address: %w", err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		prober := exec.CommandContext(bounded, filepath.Join(workspace.Directory, "sdo-prober"), "--namespace", namespace, "--listen", address)
		prober.Dir = workspace.Directory
		prober.Env = environment
		if err := prober.Start(); err != nil {
			return "", fmt.Errorf("probe transport unavailable: start bounded health prober: %w", err)
		}
		defer func() { _ = prober.Process.Kill(); _ = prober.Wait() }()
		args = append(args, "--prober-url", "http://"+address)
	}
	command := exec.CommandContext(bounded, binary, args...)
	command.Dir = workspace.Directory
	command.Env = environment
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	if err != nil {
		return "", fmt.Errorf("evaluate accepted health detectors: %w (stdout=%q stderr=%q)", err, boundedHealthOutput(stdout.Bytes()), boundedHealthOutput(stderr.Bytes()))
	}
	return decodeHealthVerdict(stdout.Bytes(), stderr.Bytes())
}

func decodeHealthVerdict(stdout, stderr []byte) (string, error) {
	var verdict struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(stdout, &verdict); err != nil {
		return "", fmt.Errorf("decode health detector verdict: %w (stdout=%q stderr=%q)", err, boundedHealthOutput(stdout), boundedHealthOutput(stderr))
	}
	if verdict.Status != "clear" && verdict.Status != "firing" && verdict.Status != "pending" && verdict.Status != "unavailable" {
		return "", fmt.Errorf("unknown health detector verdict %q (stdout=%q stderr=%q)", verdict.Status, boundedHealthOutput(stdout), boundedHealthOutput(stderr))
	}
	return verdict.Status, nil
}

func boundedHealthOutput(data []byte) string {
	const limit = 2048
	if len(data) > limit {
		return string(data[:limit]) + "…"
	}
	return string(data)
}

// IndependentHealthVerifier rechecks the deployed source and accepted detector
// commit before returning a bounded live verdict to the lifecycle coordinator.
type IndependentHealthVerifier struct {
	AcceptedRoot string
	OperatorRoot string
	Readiness    *Registry
	Probe        HealthProbe
}

func (v IndependentHealthVerifier) Verify(ctx context.Context, acceptedCommit, namespace, objective string, source lifecycle.DeploymentReceipt) (bool, string, error) {
	if v.AcceptedRoot == "" || v.OperatorRoot == "" || v.Readiness == nil || namespace == "" || strings.TrimSpace(objective) == "" || acceptedCommit == "" || !source.Ready {
		return false, "", errors.New("accepted repository, operator, observer, namespace, objective, commit, and source evidence are required")
	}
	git := func(args ...string) (string, error) {
		output, err := exec.CommandContext(ctx, "git", append([]string{"-C", v.AcceptedRoot}, args...)...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("accepted git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return false, "", err
	}
	if head != acceptedCommit {
		return false, "", errors.New("health judge commit is not the accepted repository HEAD")
	}
	if status, err := git("status", "--porcelain"); err != nil {
		return false, "", err
	} else if status != "" {
		return false, "", errors.New("accepted repository changed before health verification")
	}
	ancestorTrees, err := git("log", "--format=%H %T", acceptedCommit)
	if err != nil {
		return false, "", err
	}
	sourceCommit := ""
	for _, line := range strings.Split(ancestorTrees, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[1] == source.ProposalTreeDigest {
			sourceCommit = parts[0]
			break
		}
	}
	if sourceCommit == "" {
		return false, "", errors.New("deployed source tree is not an accepted ancestor of health diagnostics")
	}
	detectorTree, err := git("rev-parse", acceptedCommit+":"+layout.DiagnosticsDir)
	if err != nil {
		return false, "", fmt.Errorf("accepted health diagnostics: %w", err)
	}
	if detectorTree == "" {
		return false, "", errors.New("accepted detector tree is empty")
	}
	if _, err := v.Readiness.VerifyAccepted(ctx, sourceCommit, namespace, source); err != nil {
		return false, "", fmt.Errorf("deployed source changed before health verification: %w", err)
	}
	probe := v.Probe
	if probe == nil {
		probe = AcceptedDetectorProbe{}
	}
	status, err := probe.Evaluate(ctx, v.AcceptedRoot, v.OperatorRoot, namespace)
	if err != nil {
		return false, "", err
	}
	if status != "clear" {
		return false, "accepted health detectors report " + status, nil
	}
	if got, err := git("rev-parse", "HEAD:"+layout.DiagnosticsDir); err != nil || got != detectorTree {
		return false, "", errors.New("accepted health detectors changed during verification")
	}
	if status, err := git("status", "--porcelain"); err != nil || status != "" {
		return false, "", errors.New("accepted repository changed during health verification")
	}
	return true, "", nil
}

// Verify the configured operator root before a build can import any modules.
func (v IndependentHealthVerifier) Validate() error {
	for _, path := range []string{v.AcceptedRoot, v.OperatorRoot} {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("verification root is not a directory: %s", path)
		}
	}
	if _, err := os.Stat(filepath.Join(v.OperatorRoot, "runtime", "go.mod")); err != nil {
		return fmt.Errorf("operator runtime module: %w", err)
	}
	return nil
}

// A host without cluster Service DNS cannot interpret failed traffic probes as
// application health. Onboarding never uses this legacy host verifier.
func (probe AcceptedDetectorProbe) checkTransport(ctx context.Context) error {
	resolver := probe.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := resolver.LookupHost(ctx, "kubernetes.default.svc.cluster.local"); err != nil {
		return fmt.Errorf("probe transport unavailable: host cannot resolve cluster Service DNS: %w", err)
	}
	return nil
}
