package deployment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// HealthProbe evaluates the compiled, accepted detectors against live state.
// The default implementation builds from the accepted repository first.
type HealthProbe interface {
	Evaluate(context.Context, string, string, string) (HealthEvaluation, error)
}

// HealthEvaluation is one probe result: the aggregate status and, when the
// probe reports them, the per-detector states and findings behind it.
type HealthEvaluation struct {
	Status    string
	Detectors []lifecycle.DetectorState
}

type AcceptedDetectorProbe struct {
	Replay          string
	RequireComplete bool
	Kubeconfig      string
	MetricsURL      string
	Limit           time.Duration
}

func (probe AcceptedDetectorProbe) Evaluate(ctx context.Context, acceptedRoot, operatorRoot, namespace string) (HealthEvaluation, error) {
	limit := probe.Limit
	if limit <= 0 {
		limit = 5 * time.Minute
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	projected, err := memory.AcceptedDiagnostics(bounded, acceptedRoot)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("resolve accepted diagnostics: %w", err)
	}
	workspace, err := builder.BuildWorkspace(bounded, builder.Config{Replay: probe.Replay, ApplicationRoot: acceptedRoot, DiagnosticsRoot: projected.Directory, OperatorRoot: operatorRoot})
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("stage accepted diagnostics: %w", err)
	}
	defer workspace.Close()
	binary, err := workspace.ValidateAndBuild(bounded)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("compile accepted health detectors: %w", err)
	}
	artifact, err := workspace.BuildMemoryHost(bounded)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("compile accepted memory host: %w", err)
	}
	args := append(probe.evaluationArgs(acceptedRoot, namespace), "--memory-host-binary", artifact.Path, "--memory-host-digest", artifact.Digest, "--memory-commit", projected.AcceptedRevision)
	var environment []string
	if probe.Kubeconfig != "" {
		environment = append(environment, "KUBECONFIG="+probe.Kubeconfig)
	}
	stdout, stderr, err := deploymentio.Evaluate(bounded, workspace.Directory, binary, args, environment)

	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("evaluate accepted health detectors: %w (stdout=%q stderr=%q)", err, boundedHealthOutput(stdout), boundedHealthOutput(stderr))
	}
	return decodeHealthVerdict(stdout, stderr)
}

// decodeHealthVerdict keeps the per-detector evidence --evaluate-once reports:
// pending and unavailable detectors with their reasons, and each active
// finding with its summary, so the verdict can say which detector and why.
func decodeHealthVerdict(stdout, stderr []byte) (HealthEvaluation, error) {
	verdict, err := evaluation.Decode(stdout)
	if err != nil {
		return HealthEvaluation{}, fmt.Errorf("decode health detector verdict: %w (stdout=%q stderr=%q)", err, boundedHealthOutput(stdout), boundedHealthOutput(stderr))
	}
	result := HealthEvaluation{Status: string(verdict.Status)}
	for _, state := range verdict.DetectorStates {
		if state.Status != "pending" {
			continue
		}
		result.Detectors = append(result.Detectors, lifecycle.DetectorState{DetectorID: state.DetectorID, Status: state.Status, Reason: firstNonEmpty(state.Reason, state.Error)})
	}
	for _, item := range verdict.Unavailable {
		result.Detectors = append(result.Detectors, lifecycle.DetectorState{DetectorID: item.DetectorID, Status: "unavailable", Reason: item.Reason})
	}
	for _, finding := range verdict.Findings {
		if finding.Status == "active" {
			result.Detectors = append(result.Detectors, lifecycle.DetectorState{DetectorID: finding.DetectorID, Status: "firing", Reason: firstNonEmpty(finding.Summary, finding.RuleID) + evidenceSuffix(finding.Evidence)})
		}
	}
	return result, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func evidenceSuffix(evidence string) string {
	if evidence == "" {
		return ""
	}
	return "; evidence: " + boundedHealthOutput([]byte(evidence))
}

// verdictOf maps a probe status to the lifecycle's typed outcome. Pending and
// unavailable are waited out; everything else that is not clear is the
// deployed source's problem.
func verdictOf(evaluation HealthEvaluation) lifecycle.HealthVerdict {
	verdict := lifecycle.HealthVerdict{Summary: "accepted health detectors report " + evaluation.Status, Detectors: evaluation.Detectors}
	switch evaluation.Status {
	case "clear":
		verdict.Outcome = lifecycle.VerificationClear
	case "pending", "unavailable":
		verdict.Outcome = lifecycle.VerificationPending
	default:
		verdict.Outcome = lifecycle.VerificationSourceFault
	}
	return verdict
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

// Verify judges the accepted HEAD: VerifyCandidate at the accepted repository.
func (v IndependentHealthVerifier) Verify(ctx context.Context, acceptedCommit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	return v.VerifyCandidate(ctx, v.AcceptedRoot, acceptedCommit, namespace, objective, source)
}

// VerifyCandidate judges commit, which must be the clean HEAD of root, with
// the detectors of that commit against the live deployment source: the
// accepted repository after onboarding, or a broker proposal checkout whose
// candidate is not accepted yet (an upstream advance). The deployed tree must
// be commit's tree or an ancestor's, the live workloads must still match it
// (re-verified in root, where commit lives), and neither root nor its
// detector tree may change during the evaluation.
func (v IndependentHealthVerifier) VerifyCandidate(ctx context.Context, root, commit, namespace, objective string, source lifecycle.DeploymentReceipt) (lifecycle.HealthVerdict, error) {
	if root == "" || v.OperatorRoot == "" || v.Readiness == nil || namespace == "" || strings.TrimSpace(objective) == "" || commit == "" || !source.Ready {
		return lifecycle.HealthVerdict{}, errors.New("repository, operator, observer, namespace, objective, commit, and source evidence are required")
	}
	git := func(args ...string) (string, error) {
		output, err := deploymentio.Run(ctx, "git", append([]string{"-C", root}, args...), nil)
		if err != nil {
			return "", fmt.Errorf("verified repository git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	if head != commit {
		return lifecycle.HealthVerdict{}, fmt.Errorf("verified commit %s is not the HEAD of %s", commit, root)
	}
	if status, err := git("status", "--porcelain"); err != nil {
		return lifecycle.HealthVerdict{}, err
	} else if status != "" {
		return lifecycle.HealthVerdict{}, errors.New("verified repository changed before health verification")
	}
	ancestorTrees, err := git("log", "--format=%H %T", commit)
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	sourceCommit := ""
	for _, line := range strings.Split(ancestorTrees, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[1] == source.BaseTree {
			sourceCommit = parts[0]
			break
		}
	}
	if sourceCommit == "" {
		return lifecycle.HealthVerdict{}, errors.New("deployed source tree is not an ancestor of the verified health diagnostics")
	}
	detectorTree, err := git("rev-parse", commit+":"+layout.DiagnosticsDir)
	if err != nil {
		return lifecycle.HealthVerdict{}, fmt.Errorf("verified health diagnostics: %w", err)
	}
	if detectorTree == "" {
		return lifecycle.HealthVerdict{}, errors.New("verified detector tree is empty")
	}
	// The commit lives in root, which is not the accepted repository for a
	// candidate: re-verify the deployment against root.
	readiness := *v.Readiness
	readiness.AcceptedRoot = root
	if _, err := readiness.VerifyAccepted(ctx, sourceCommit, namespace, source); err != nil {
		return lifecycle.HealthVerdict{}, fmt.Errorf("deployed source changed before health verification: %w", err)
	}
	probe := v.Probe
	if probe == nil {
		probe = AcceptedDetectorProbe{}
	}
	evaluation, err := probe.Evaluate(ctx, root, v.OperatorRoot, namespace)
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	if verdict := verdictOf(evaluation); verdict.Outcome != lifecycle.VerificationClear {
		return verdict, nil
	}
	if got, err := git("rev-parse", "HEAD:"+layout.DiagnosticsDir); err != nil || got != detectorTree {
		return lifecycle.HealthVerdict{}, errors.New("verified health detectors changed during verification")
	}
	if status, err := git("status", "--porcelain"); err != nil || status != "" {
		return lifecycle.HealthVerdict{}, errors.New("verified repository changed during health verification")
	}
	return verdictOf(evaluation), nil
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

func (probe AcceptedDetectorProbe) evaluationArgs(acceptedRoot, namespace string) []string {
	args := []string{"--evaluate-once", "--namespace", namespace, "--app-root", acceptedRoot}
	if probe.RequireComplete {
		args = append(args, "--accept-seed")
	}
	if probe.MetricsURL != "" {
		args = append(args, "--metrics-url", probe.MetricsURL)
	}
	return args
}
