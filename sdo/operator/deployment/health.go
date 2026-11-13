package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// HealthProbe evaluates the compiled, accepted detectors against live state.
// The default implementation builds from the accepted repository first.
type HealthProbe interface {
	Evaluate(ctx context.Context, root, operatorRoot, namespace string, scope HealthScope) (HealthEvaluation, error)
}

// HealthScope says what a verification judges. Classes are the detector
// classes that gate (evaluation.GatingClasses; empty means every class). Pins
// are the images the deployment under verification applied: desired-state
// reads in the evaluation return them, so a detector comparing live workloads
// with the accepted source compares with the candidate's own deployment, not
// with the source's unpinned tags.
type HealthScope struct {
	Classes []sdk.DetectorClass
	Pins    rendered.ReceiptPins
}

// scopeOf is the scope of verifying the deployment source describes.
func scopeOf(source lifecycle.DeploymentReceipt) HealthScope {
	return HealthScope{
		Classes: evaluation.GatingClasses,
		Pins:    rendered.ReceiptPins{EffectiveTree: source.EffectiveTree, BaseTree: source.BaseTree, Images: source.ContainerImages},
	}
}

// HealthEvaluation is one probe result: the aggregate status and, when the
// probe reports them, the per-detector states and findings behind it.
type HealthEvaluation struct {
	Status    string
	Detectors []lifecycle.DetectorState
	// CoverageNotes are clear detectors that named what they could not
	// observe; they never hold the verdict.
	CoverageNotes []lifecycle.DetectorState
	// ExistingRecordsExercised counts probe samples whose marked step
	// returned a record the probe did not write.
	ExistingRecordsExercised int
}

type AcceptedDetectorProbe struct {
	// Settings is the operator configuration file the probe and its --evaluate-once run read (the sdo-config mount
	// or the controller's record of it); empty evaluates under settings.Default().
	Settings        string
	RequireComplete bool
	Kubeconfig      string
	// TelemetryEnvironment is the telemetry environment document the probe's
	// evaluation reads; the accepted tree's own bindings come from the memory
	// host built from it. Empty provides no operator source.
	TelemetryEnvironment string
	Limit                time.Duration
}

func (probe AcceptedDetectorProbe) Evaluate(ctx context.Context, acceptedRoot, operatorRoot, namespace string, scope HealthScope) (HealthEvaluation, error) {
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
	policy := settings.Default()
	if probe.Settings != "" {
		if policy, err = settings.Read(probe.Settings); err != nil {
			return HealthEvaluation{}, err
		}
	}
	workspace, err := builder.BuildWorkspace(bounded, builder.Config{Replay: policy.Learning.Replay, ApplicationRoot: acceptedRoot, DiagnosticsRoot: projected.Directory, OperatorRoot: operatorRoot})
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
	pinsFile := ""
	if len(scope.Pins.Images) > 0 {
		pinsFile = filepath.Join(workspace.Directory, "image-pins.json")
		encoded, err := json.Marshal(scope.Pins)
		if err == nil {
			err = os.WriteFile(pinsFile, encoded, 0o600)
		}
		if err != nil {
			return HealthEvaluation{}, fmt.Errorf("write image pins: %w", err)
		}
	}
	args := append(probe.evaluationArgs(acceptedRoot, namespace, scope, pinsFile), "--memory-host-binary", artifact.Path, "--memory-host-digest", artifact.Digest, "--memory-commit", projected.AcceptedRevision)
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
	result := HealthEvaluation{Status: string(verdict.Status), ExistingRecordsExercised: verdict.ExistingRecordsExercised}
	for _, state := range verdict.DetectorStates {
		if state.Status == "clear" && state.Reason != "" {
			result.CoverageNotes = append(result.CoverageNotes, lifecycle.DetectorState{DetectorID: state.DetectorID, Status: state.Status, Reason: state.Reason})
			continue
		}
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
	verdict := lifecycle.HealthVerdict{Summary: "accepted health detectors report " + evaluation.Status, Detectors: evaluation.Detectors, CoverageNotes: evaluation.CoverageNotes}
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
	Readiness    *Deployer
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
	evaluation, err := probe.Evaluate(ctx, root, v.OperatorRoot, namespace, scopeOf(source))
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	verdict := verdictOf(evaluation)
	verdict.ExistingRecords = lifecycle.ExistingRecordsEvidence{Required: recordsRequired(root), Exercised: evaluation.ExistingRecordsExercised}
	if verdict.Outcome != lifecycle.VerificationClear {
		return verdict, nil
	}
	if got, err := git("rev-parse", "HEAD:"+layout.DiagnosticsDir); err != nil || got != detectorTree {
		return lifecycle.HealthVerdict{}, errors.New("verified health detectors changed during verification")
	}
	if status, err := git("status", "--porcelain"); err != nil || status != "" {
		return lifecycle.HealthVerdict{}, errors.New("verified repository changed during health verification")
	}
	return verdict, nil
}

// recordsRequired reports whether the candidate's application map needs a
// marked step to read records the probe did not write. An unreadable or
// unparsable map requires nothing here: the shared validator owns that finding.
func recordsRequired(root string) bool {
	data, err := os.ReadFile(filepath.Join(root, layout.ArchitectureFile))
	if err != nil {
		return false
	}
	region, found, err := appmap.Find(string(data))
	if err != nil || !found {
		return false
	}
	parsed, err := appmap.Parse(region.YAML)
	return err == nil && parsed.NeedsExistingRecords()
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

func (probe AcceptedDetectorProbe) evaluationArgs(acceptedRoot, namespace string, scope HealthScope, pinsFile string) []string {
	args := []string{"--evaluate-once", "--namespace", namespace, "--app-root", acceptedRoot}
	if len(scope.Classes) > 0 {
		args = append(args, "--classes", evaluation.FormatClasses(scope.Classes))
	}
	if pinsFile != "" {
		args = append(args, "--image-pins", pinsFile)
	}
	if probe.RequireComplete {
		args = append(args, "--accept-seed")
	}
	if probe.TelemetryEnvironment != "" {
		args = append(args, "--telemetry-environment", probe.TelemetryEnvironment)
	}
	if probe.Settings != "" {
		args = append(args, "--settings", probe.Settings)
	}
	return args
}
